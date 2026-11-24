"""Idempotent entry points. The caller owns the tenant transaction."""
import hashlib
import json


def request_key(nonce, payload):
    return hashlib.sha256(json.dumps([nonce, payload], ensure_ascii=False,
                                     default=str, sort_keys=True).encode()).hexdigest()


def insert_shifts(cur, records):
    """One shift per registered source/day, including old/imported records.

    Lock the tenant import lane before checking: concurrent imports and manual
    weekly schedules cannot both pass the existence check. Never rewrite a paid
    shift when a file with different values is uploaded.
    """
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':shift-import'))")
    inserted = 0
    for row in records:
        cur.execute("""SELECT 1 FROM lancamentos WHERE tipo='Entrada'
            AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(%s))
            AND data_competencia=%s AND descricao LIKE 'Plantão %%' LIMIT 1""",
            (row[2], row[13]))
        if cur.fetchone():
            continue
        cur.execute("""INSERT INTO lancamentos
            (tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,
             total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", row)
        inserted += 1
    return inserted


def reorganize_coverage(cur, year, month, undo=False):
    """Persist coverage choices atomically without mutating financial records."""
    import datetime as dt
    from psycopg2.extras import Json
    start = dt.date(year, month, 1)
    end = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    key = f"cobertura_reorganizacao:{year:04d}-{month:02d}"
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':coverage'))")
    cur.execute("SELECT valor FROM preferencias_app WHERE chave=%s", (key,))
    row=cur.fetchone()
    old=json.loads(row[0]) if row else {'ids':[]}
    if undo:
        if 'anterior' not in old: return
        new=old['anterior']
    else:
        cur.execute("""SELECT id FROM lancamentos WHERE tipo='Entrada' AND pago=1
            AND data_vencimento >= %s AND data_vencimento < %s ORDER BY id FOR UPDATE""", (start,end))
        selected=[r[0] for r in cur.fetchall()]
        if old.get('ativo') and selected==old.get('ids',[]): return
        new={'ativo':True,'ids':selected,'anterior':{k:v for k,v in old.items() if k!='anterior'}}
    cur.execute("""INSERT INTO preferencias_app(chave,valor,atualizado_em)
        VALUES (%s,%s,NOW()) ON CONFLICT(chave)
        DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()""",(key,json.dumps(new)))
    cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
        VALUES ('cobertura',%s,%s,%s,current_setting('app.actor',true))""",
        ('DESFAZER_REORGANIZACAO' if undo else 'REORGANIZAR',Json(dict(old,periodo=start.isoformat())),Json(dict(new,periodo=start.isoformat()))))


def income_due_date(competence, delay, day):
    import datetime as dt
    import calendar
    offset=competence.year*12+competence.month-1+int(delay)
    year,month=divmod(offset,12);month+=1
    return dt.date(year,month,min(int(day),calendar.monthrange(year,month)[1]))


def edit_income_source(cur, source_id, value, day, delay, recurring, shifts, modality, today):
    """Update the definition and pending current/future dates in one transaction."""
    from psycopg2.extras import Json
    cur.execute("SELECT row_to_json(c) FROM categorias_personalizadas c WHERE id=%s AND tipo='Entrada' FOR UPDATE",(source_id,))
    result=cur.fetchone()
    if not result: raise ValueError('Esta fonte foi excluída. Atualize a tela.')
    old=result[0]
    if shifts:
        value=old.get('valor_padrao')
        recurring=0
    cur.execute("""UPDATE categorias_personalizadas SET valor_padrao=%s,dia_pagamento=%s,
        atraso_meses=%s,is_recorrente=%s,is_producao_variavel=%s,modalidade_renda=%s WHERE id=%s""",
        (value,day,delay,recurring,shifts,modality,source_id))
    count=0
    # Reconcile even when the saved settings already match: old shifts may be stale.
    cur.execute("""SELECT count(*) FROM categorias_personalizadas WHERE tipo='Entrada'
        AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(COALESCE(%s,'')))""",(old['subgrupo'],))
    unique_location=bool(old['subgrupo']) and cur.fetchone()[0]==1
    cur.execute("""SELECT id,data_competencia,data_vencimento FROM lancamentos
        WHERE tipo='Entrada' AND pago=0 AND data_vencimento >= %s
        AND ((categoria=%s AND COALESCE(subgrupo,'')=COALESCE(%s,''))
            OR (%s AND descricao LIKE 'Plantão %%'
                AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(COALESCE(%s,''))))) FOR UPDATE""",
        (today.replace(day=1),old['categoria'],old['subgrupo'],unique_location,old['subgrupo']))
    for item,competence,due in cur.fetchall():
        if competence is None:
            competence=income_due_date(due,-int(old.get('atraso_meses') or 0),1)
        new_due=income_due_date(competence,delay,day)
        if new_due==due: continue
        cur.execute('UPDATE lancamentos SET data_vencimento=%s WHERE id=%s',(new_due,item))
        cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
            VALUES ('lancamento','REAGENDAR_RENDA',%s,%s,current_setting('app.actor',true))""",
            (Json({'id':item,'data_vencimento':due.isoformat()}),Json({'id':item,'data_vencimento':new_due.isoformat()})))
        count+=1
    cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
        VALUES ('fonte_renda','EDITAR',%s,%s,current_setting('app.actor',true))""",
        (Json(old),Json({'id':source_id,'dia_pagamento':day,'atraso_meses':delay,'reagendados':count})))
    return count


def delete_income_source(cur, category, subgroup, cutoff):
    """Delete the source and its shifts from cutoff, retaining earlier shifts."""
    from psycopg2.extras import Json
    cur.execute("""SELECT row_to_json(c) FROM categorias_personalizadas c
        WHERE tipo='Entrada' AND categoria=%s AND COALESCE(subgrupo,'')=COALESCE(%s,'') FOR UPDATE""",(category,subgroup))
    old=[r[0] for r in cur.fetchall()]
    cur.execute("""SELECT count(*) FROM categorias_personalizadas WHERE tipo='Entrada'
        AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(COALESCE(%s,'')))""",(subgroup,))
    unique_location=bool(subgroup) and cur.fetchone()[0]<=1
    cur.execute("""SELECT row_to_json(l) FROM lancamentos l WHERE tipo='Entrada'
        AND descricao LIKE 'Plantão %%' AND COALESCE(data_competencia,data_vencimento)>=%s
        AND ((categoria=%s AND COALESCE(subgrupo,'')=COALESCE(%s,''))
            OR (%s AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(COALESCE(%s,''))))) FOR UPDATE""",
        (cutoff,category,subgroup,unique_location,subgroup))
    shifts=[r[0] for r in cur.fetchall()]
    for shift in shifts:
        cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
            VALUES ('lancamento','EXCLUIR_PLANTAO_FONTE',%s,%s,current_setting('app.actor',true))""",
            (Json(shift),Json({'excluido':True,'data_corte':cutoff.isoformat()})))
    if shifts:
        cur.execute('DELETE FROM lancamentos WHERE id=ANY(%s)',([r['id'] for r in shifts],))
    key='fonte_excluida:'+request_key('source',[category,subgroup or ''])
    cur.execute("""INSERT INTO preferencias_app(chave,valor) VALUES (%s,%s)
        ON CONFLICT(chave) DO UPDATE SET valor=EXCLUDED.valor""",
        (key,json.dumps({'categoria':category,'subgrupo':subgroup or ''})))
    cur.execute("""DELETE FROM categorias_personalizadas WHERE tipo='Entrada'
        AND categoria=%s AND COALESCE(subgrupo,'')=COALESCE(%s,'')""",(category,subgroup))
    cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
        VALUES ('fonte_renda','EXCLUIR',%s,%s,current_setting('app.actor',true))""",
        (Json(old),Json({'categoria':category,'subgrupo':subgroup,'data_corte':cutoff.isoformat(),'plantoes_excluidos':len(shifts),'anteriores_preservados':True})))
    return len(shifts)


def update_shift_values(cur, changes):
    """Save schedule amounts; confirmed receipts and their history stay intact."""
    from finance import money
    from psycopg2.extras import Json
    count=0
    for item,value in changes:
        value=money(value)
        if value<0: raise ValueError('O valor do plantão não pode ser negativo.')
        cur.execute("SELECT valor,pago FROM lancamentos WHERE id=%s AND tipo='Entrada' AND descricao LIKE 'Plantão %%' FOR UPDATE",(item,))
        row=cur.fetchone()
        if not row or row[1] or money(row[0])==value: continue
        cur.execute('UPDATE lancamentos SET valor=%s WHERE id=%s',(value,item))
        cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
            VALUES ('lancamento','EDITAR_PLANTAO',%s,%s,current_setting('app.actor',true))""",
            (Json({'id':item,'valor':str(row[0])}),Json({'id':item,'valor':str(value)})))
        count+=1
    return count
