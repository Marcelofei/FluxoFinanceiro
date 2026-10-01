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
