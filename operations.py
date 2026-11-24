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


def validate_source_rule(value, day, delay):
    from finance import money
    if not 1<=int(day)<=31 or not 0<=int(delay)<=6:
        raise ValueError('Informe dia entre 1 e 31 e prazo de recebimento entre 0 e 6 meses.')
    if value is not None and money(value)<0:
        raise ValueError('O valor padrão não pode ser negativo.')


def edit_income_source(cur, source_id, value, day, delay, recurring, shifts, modality, today):
    """Update the definition and pending current/future dates in one transaction."""
    from psycopg2.extras import Json
    validate_source_rule(value,day,delay)
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
    cur.execute("""SELECT id,data_competencia,data_vencimento,competencia_inferida,valor,excecao_recorrencia FROM lancamentos
        WHERE tipo='Entrada' AND pago=0 AND data_vencimento >= %s
        AND (fonte_id=%s OR (fonte_id IS NULL AND categoria=%s AND COALESCE(subgrupo,'')=COALESCE(%s,''))
            OR (%s AND descricao LIKE 'Plantão %%'
                AND lower(trim(COALESCE(subgrupo,'')))=lower(trim(COALESCE(%s,''))))) FOR UPDATE""",
        (today.replace(day=1),source_id,old['categoria'],old['subgrupo'],unique_location,old['subgrupo']))
    from finance import money
    for item,competence,due,inferred,planned,exception in cur.fetchall():
        if exception: continue
        if competence is None or inferred:
            competence=income_due_date(due,-int(old.get('atraso_meses') or 0),1)
        new_due=income_due_date(competence,delay,day)
        new_value = planned
        # Individual/manual values remain exceptions; only the old standard follows the new one.
        if not shifts and value is not None and old.get('valor_padrao') is not None and money(planned)==money(old['valor_padrao']):
            new_value=money(value)
        if new_due==due and money(new_value)==money(planned) and not inferred: continue
        cur.execute('''UPDATE lancamentos SET data_vencimento=%s,valor=%s,data_competencia=%s,
                       competencia_inferida=FALSE,fonte_id=%s WHERE id=%s''',
                    (new_due,new_value,competence,source_id,item))
        cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
            VALUES ('lancamento','REAGENDAR_RENDA',%s,%s,current_setting('app.actor',true))""",
            (Json({'id':item,'data_vencimento':due.isoformat()}),Json({'id':item,'data_vencimento':new_due.isoformat()})))
        count+=1
    cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
        VALUES ('fonte_renda','EDITAR',%s,%s,current_setting('app.actor',true))""",
        (Json(old),Json({'id':source_id,'dia_pagamento':day,'atraso_meses':delay,'reagendados':count})))
    return count


def lock_entry(cur, item, expected_version):
    """Optimistic version check under lock, shared by every administrative edit."""
    cur.execute('SELECT fonte_id,compra_id FROM lancamentos WHERE id=%s',(int(item),))
    identity=cur.fetchone()
    if identity:
        source,group=identity
        if source:cur.execute('SELECT id FROM categorias_personalizadas WHERE id=%s FOR UPDATE',(source,))
        elif group:cur.execute('SELECT compra_id FROM series_manuais WHERE compra_id=%s FOR UPDATE',(group,))
    cur.execute('SELECT row_to_json(l) FROM lancamentos l WHERE id=%s FOR UPDATE',(int(item),))
    row=cur.fetchone()
    if not row or row[0]['versao'] != int(expected_version):
        raise ValueError('Este lançamento mudou em outra tela. Atualize antes de salvar; nenhuma alteração foi aplicada.')
    return row[0]


def guard_financial_edit(cur, entry, fields):
    if entry.get('ajuste_pagamento_ids'):
        raise ValueError('Ajustes pertencem à baixa da fatura. Estorne a fatura completa.')
    cur.execute('SELECT 1 FROM lancamentos WHERE ajuste_pagamento_ids @> ARRAY[%s]::integer[] LIMIT 1',(entry['id'],))
    if cur.fetchone():
        raise ValueError('Este lançamento pertence a uma fatura com ajuste. Estorne a fatura antes de editá-lo.')
    if entry['pago'] and fields:
        raise ValueError('Desfaça a baixa antes de alterar os dados financeiros de um lançamento concluído.')
    if str(entry.get('descricao') or '').startswith(('Plantão ', 'Plantao ')) and fields:
        raise ValueError('Altere os valores dos plantões pela agenda; a renda acompanha automaticamente.')


def delete_entries(cur, ids):
    ids=sorted(set(int(i) for i in ids))
    if not ids:return
    cur.execute('SELECT row_to_json(l) FROM lancamentos l WHERE id=ANY(%s) ORDER BY id FOR UPDATE',(ids,))
    entries=[r[0] for r in cur.fetchall()]
    if len(entries)!=len(ids):raise ValueError('Um lançamento foi removido. Atualize a tela.')
    for entry in entries:
        if entry['pago']:raise ValueError('Desfaça a baixa antes de excluir um lançamento concluído.')
        guard_financial_edit(cur,entry,False)
    cur.execute('DELETE FROM lancamentos WHERE id=ANY(%s)',(ids,))


def validate_entry_changes(updates):
    import datetime as dt
    from finance import money
    if 'data_vencimento' in updates and not isinstance(updates['data_vencimento'],dt.date):
        raise ValueError('Informe uma data de vencimento válida.')
    if 'descricao' in updates and not str(updates['descricao'] or '').strip():
        raise ValueError('Informe a descrição do lançamento.')
    if 'valor' in updates:
        try:
            if money(updates['valor'])<0:raise ValueError('O valor planejado não pode ser negativo.')
        except (ArithmeticError,TypeError):
            raise ValueError('Informe um valor planejado válido.') from None


def change_series(cur, entry, updates=None, delete=False, future=False):
    """Update the rule and materialized pending entries; keep monthly exceptions."""
    import datetime as dt
    from psycopg2 import sql
    from finance import money
    updates=updates or {}
    if not delete:validate_entry_changes(updates)
    old_due=dt.date.fromisoformat(entry['data_vencimento'])
    target=updates.get('data_vencimento',old_due)
    month_delta=(target.year-old_due.year)*12+target.month-old_due.month
    group=entry.get('compra_id')
    contract_id=int(group[4:]) if group and group.startswith('rec_') and group[4:].isdigit() else None
    # Serialize with lazy generation before locking any series rows.
    if contract_id:
        cur.execute('SELECT atraso_meses FROM categorias_personalizadas WHERE id=%s FOR UPDATE',(contract_id,))
        rule=cur.fetchone()
        if future and not delete and rule:
            validate_source_rule(updates.get('valor'),target.day,int(rule[0] or 0)+month_delta)
    elif group:
        cur.execute('SELECT compra_id FROM series_manuais WHERE compra_id=%s FOR UPDATE',(group,))
    if future and group:
        cur.execute('''SELECT row_to_json(l) FROM lancamentos l WHERE compra_id=%s
            AND data_vencimento>=%s AND pago=0 ORDER BY id FOR UPDATE''',(group,old_due))
        rows=[r[0] for r in cur.fetchall()]
    else:rows=[entry]
    for r in rows:
        guard_financial_edit(cur,r,True)
    if future and contract_id:
        if delete:
            # End by competence, so delayed receipts after the cutoff are not regenerated.
            competence=dt.date.fromisoformat(entry.get('data_competencia') or entry['data_vencimento'])
            cur.execute('UPDATE categorias_personalizadas SET data_fim=%s WHERE id=%s',(competence.replace(day=1)-dt.timedelta(days=1),contract_id))
        else:
            for key,column in [('valor','valor_padrao'),('tipo','tipo'),('categoria','categoria'),('subgrupo','subgrupo'),('descricao','descricao_padrao'),('forma_pagamento','forma_padrao'),('prioridade','prioridade_padrao')]:
                if key in updates:
                    cur.execute(sql.SQL('UPDATE categorias_personalizadas SET {}=%s WHERE id=%s').format(sql.Identifier(column)),(updates[key],contract_id))
            if 'data_vencimento' in updates:
                cur.execute('UPDATE categorias_personalizadas SET dia_pagamento=%s,atraso_meses=COALESCE(atraso_meses,0)+%s WHERE id=%s',(target.day,month_delta,contract_id))
    if future and group and not contract_id:
        if delete:
            competence=dt.date.fromisoformat(entry.get('data_competencia') or entry['data_vencimento'])
            cur.execute('UPDATE series_manuais SET fim=%s WHERE compra_id=%s',(competence.replace(day=1)-dt.timedelta(days=1),group))
        else:
            for key in ('tipo','categoria','subgrupo','descricao','valor','forma_pagamento','prioridade'):
                if key in updates:
                    cur.execute(sql.SQL('UPDATE series_manuais SET {}=%s WHERE compra_id=%s').format(sql.Identifier(key)),(updates[key],group))
            if 'data_vencimento' in updates:
                cur.execute('UPDATE series_manuais SET dia=%s,atraso_meses=atraso_meses+%s WHERE compra_id=%s',(target.day,month_delta,group))
    if delete:
        # Generation markers remain as tombstones for individual deleted occurrences.
        delete_entries(cur,[r['id'] for r in rows])
        return
    for r in rows:
        if future and r.get('excecao_recorrencia') and r['id']!=entry['id']:continue
        changes=dict(updates)
        if 'valor' in changes:changes['valor']=money(changes['valor'])
        if 'data_vencimento' in changes:
            due=dt.date.fromisoformat(r['data_vencimento'])
            changes['data_vencimento']=target if r['id']==entry['id'] else income_due_date(due,month_delta,target.day)
        changes['excecao_recorrencia']=not future
        clause=sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in changes)
        cur.execute(sql.SQL('UPDATE lancamentos SET {} WHERE id=%s').format(clause),tuple(changes.values())+(r['id'],))


def quick_edit(cur, original, updated, delete=False, future=False):
    """Single-entry editor, including settlement, with no stale snapshot writes."""
    import datetime as dt
    from finance import money
    from payments import settle, reverse, validate_payment_date
    if not delete:validate_entry_changes(updated)
    current=lock_entry(cur,original['id'],original['versao'])
    structural={k:v for k,v in updated.items() if k in ('valor','data_vencimento','descricao','prioridade')
                and str(v)!=str(current.get(k))}
    if 'valor' in structural and money(structural['valor'])==money(current['valor']):structural.pop('valor')
    if delete:
        change_series(cur,current,delete=True,future=future)
        return
    paid=bool(updated.get('pago',current['pago']))
    real=updated.get('valor_pago',current['valor_pago'])
    date=updated.get('data_pagamento',current.get('data_pagamento'))
    if paid and isinstance(date,str):date=dt.date.fromisoformat(date)
    if paid:validate_payment_date(date)
    if current.get('forma_pagamento')=='Crédito' and (paid!=bool(current['pago']) or money(real)!=money(current['valor_pago'])):
        raise ValueError('Pague ou estorne compras no crédito pela linha do tempo da fatura.')
    if bool(current['pago'])==paid and paid:
        if money(real)!=money(current['valor_pago']) or str(date)!=str(current['data_pagamento']):
            raise ValueError('Para corrigir uma baixa, desfaça e registre novamente o valor e a data reais.')
    if structural:
        guard_financial_edit(cur,current,set(structural)-{'prioridade'})
        from psycopg2 import sql
        clause=sql.SQL(',').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in structural)
        cur.execute(sql.SQL('UPDATE lancamentos SET {},excecao_recorrencia=TRUE WHERE id=%s').format(clause),tuple(structural.values())+(current['id'],))
    if not current['pago'] and paid:settle(cur,[current['id']],real,date)
    elif current['pago'] and not paid:reverse(cur,[current['id']])


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
        AND pago=0 AND descricao LIKE 'Plantão %%' AND COALESCE(data_competencia,data_vencimento)>=%s
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


def edit_category(cur, source_id, category, subgroup, value, delay, day, recurring, today):
    """Both category and income screens use the same reconciliation rules."""
    from finance import money
    validate_source_rule(value,day,delay)
    cur.execute('SELECT row_to_json(c) FROM categorias_personalizadas c WHERE id=%s FOR UPDATE',(source_id,))
    result=cur.fetchone()
    if not result:raise ValueError('Categoria removida. Atualize a tela.')
    old=result[0]
    if not category:raise ValueError('Informe a categoria.')
    if old['tipo']=='Entrada':
        edit_income_source(cur,source_id,value,day,delay,recurring,old.get('is_producao_variavel') or 0,
                           old.get('modalidade_renda') or 'Variável',today)
    else:
        cur.execute('''UPDATE categorias_personalizadas SET valor_padrao=%s,dia_pagamento=%s,
            atraso_meses=%s,is_recorrente=%s WHERE id=%s''',(value,day,delay,recurring,source_id))
        cur.execute('''SELECT id,data_vencimento,valor FROM lancamentos WHERE fonte_id=%s
            AND pago=0 AND NOT excecao_recorrencia AND data_vencimento>=%s ORDER BY id FOR UPDATE''',
            (source_id,today.replace(day=1)))
        for item,due,planned in cur.fetchall():
            v=value if value is not None and old.get('valor_padrao') is not None and money(planned)==money(old['valor_padrao']) else planned
            cur.execute('UPDATE lancamentos SET data_vencimento=%s,valor=%s WHERE id=%s',(income_due_date(due,0,day),v,item))
    cur.execute('UPDATE categorias_personalizadas SET categoria=%s,subgrupo=%s WHERE id=%s',(category,subgroup,source_id))
    cur.execute('''UPDATE lancamentos SET categoria=%s,subgrupo=%s WHERE fonte_id=%s
        AND pago=0 AND NOT excecao_recorrencia AND data_vencimento>=%s''',(category,subgroup,source_id,today.replace(day=1)))
