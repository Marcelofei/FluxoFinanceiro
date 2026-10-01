from tests.test_database import db
from operations import request_key


def test_retries_share_identity_but_new_draft_does_not():
    payload=['Despesa','Mercado',100,'2026-09-30']
    assert request_key('draft',payload)==request_key('draft',payload.copy())
    assert request_key('draft',payload)!=request_key('new-draft',payload)
    assert request_key('draft',payload)!=request_key('draft',payload+[2])


def test_reorganization_month_isolation_and_repeat(db):
    import json
    from operations import reorganize_coverage
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO lancamentos(tipo,descricao,valor,pago,valor_pago,data_vencimento,data_pagamento)
          VALUES ('Entrada','setembro',100,1,100,'2026-09-30','2026-10-01'),
                 ('Entrada','outubro',200,1,200,'2026-10-01','2026-10-01') RETURNING id""")
        september,october=[r[0] for r in cur.fetchall()]
        reorganize_coverage(cur,2026,9)
        reorganize_coverage(cur,2026,9)
        reorganize_coverage(cur,2026,10)
        reorganize_coverage(cur,2026,9,undo=True)
        cur.execute("SELECT valor FROM preferencias_app WHERE chave='cobertura_reorganizacao:2026-10'")
        assert json.loads(cur.fetchone()[0])['ids']==[october]
        cur.execute("SELECT valor FROM preferencias_app WHERE chave='cobertura_reorganizacao:2026-09'")
        assert json.loads(cur.fetchone()[0])=={'ids':[]}
        cur.execute("SELECT count(*) FROM auditoria WHERE entidade='cobertura'")
        assert cur.fetchone()[0]==3
