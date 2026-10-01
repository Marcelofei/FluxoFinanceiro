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


def reorganize_coverage(cur, ids=(), undo=False):
    """Persist coverage choices atomically without mutating financial records."""
    from psycopg2.extras import Json
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':coverage'))")
    cur.execute("SELECT valor FROM preferencias_app WHERE chave='cobertura_reorganizacao'")
    row=cur.fetchone()
    old=json.loads(row[0]) if row else {'ids':[]}
    if undo:
        if 'anterior' not in old: return
        new={'ids':old['anterior']}
    else:
        selected=sorted(set(int(i) for i in ids))
        if not selected: raise ValueError('Selecione ao menos um recebimento.')
        cur.execute("SELECT id FROM lancamentos WHERE id=ANY(%s) AND tipo='Entrada' AND pago=1 FOR UPDATE",(selected,))
        if len(cur.fetchall())!=len(selected): raise ValueError('Um recebimento mudou. Atualize a tela.')
        merged=sorted(set(old.get('ids',[]))|set(selected))
        if merged==old.get('ids',[]): return
        new={'ids':merged,'anterior':old.get('ids',[])}
    cur.execute("""INSERT INTO preferencias_app(chave,valor,atualizado_em)
        VALUES ('cobertura_reorganizacao',%s,NOW()) ON CONFLICT(chave)
        DO UPDATE SET valor=EXCLUDED.valor,atualizado_em=NOW()""",(json.dumps(new),))
    cur.execute("""INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
        VALUES ('cobertura',%s,%s,%s,current_setting('app.actor',true))""",
        ('DESFAZER_REORGANIZACAO' if undo else 'REORGANIZAR',Json(old),Json(new)))
