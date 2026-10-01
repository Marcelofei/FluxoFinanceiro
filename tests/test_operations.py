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


def test_income_dates_and_delete_preserve_history(db):
    import datetime as dt
    from operations import edit_income_source,delete_income_source,income_due_date
    assert income_due_date(dt.date(2026,12,1),2,31)==dt.date(2027,2,28)
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo,dia_pagamento,atraso_meses,is_recorrente)
            VALUES ('Entrada','Rendas','Hospital datas',10,1,1) RETURNING id""")
        source=cur.fetchone()[0]
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_competencia,data_vencimento,pago,valor_pago,data_pagamento)
            VALUES ('Entrada','Rendas','Hospital datas','atual',100,'2026-09-01','2026-10-10',0,0,NULL),
                   ('Entrada','Rendas','Hospital datas','futuro',200,'2026-10-01','2026-11-10',0,0,NULL),
                   ('Entrada','Rendas','Hospital datas','recebido',300,'2026-09-01','2026-10-10',1,300,'2026-10-01'),
                   ('Entrada','Rendas','Hospital datas','antigo',400,'2026-08-01','2026-09-10',0,0,NULL),
                   ('Entrada','Rendas','Outro hospital','outro',500,'2026-09-01','2026-10-10',0,0,NULL)""")
        assert edit_income_source(cur,source,100,31,2,1,0,'Mensal',dt.date(2026,10,1))==2
        cur.execute('SELECT descricao,data_vencimento FROM lancamentos')
        dates=dict(cur.fetchall())
        assert dates=={'atual':dt.date(2026,11,30),'futuro':dt.date(2026,12,31),'recebido':dt.date(2026,10,10),'antigo':dt.date(2026,9,10),'outro':dt.date(2026,10,10)}
        cur.execute('SELECT row_to_json(l) FROM lancamentos l ORDER BY id');before=cur.fetchall()
        delete_income_source(cur,'Rendas','Hospital datas')
        cur.execute('SELECT row_to_json(l) FROM lancamentos l ORDER BY id');assert cur.fetchall()==before
        cur.execute('SELECT count(*) FROM categorias_personalizadas WHERE id=%s',(source,));assert cur.fetchone()[0]==0
