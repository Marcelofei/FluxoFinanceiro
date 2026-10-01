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
        delete_income_source(cur,'Rendas','Hospital datas',dt.date(2026,10,1))
        cur.execute('SELECT row_to_json(l) FROM lancamentos l ORDER BY id');assert cur.fetchall()==before
        cur.execute('SELECT count(*) FROM categorias_personalizadas WHERE id=%s',(source,));assert cur.fetchone()[0]==0


def test_shift_value_edit_preserves_received_amounts(db):
    from operations import update_shift_values
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,pago,valor_pago,data_vencimento,data_pagamento)
          VALUES ('Entrada','Rendas','Hospital A','Plantão pendente',100,0,0,'2026-10-10',NULL),
                 ('Entrada','Rendas','Hospital A','Plantão recebido',200,1,200,'2026-10-10','2026-10-10') RETURNING id""")
        a,b=[r[0] for r in cur.fetchall()]
        assert update_shift_values(cur,[(a,150),(b,999)])==1
        cur.execute('SELECT valor,valor_pago FROM lancamentos WHERE id=%s',(b,));assert cur.fetchone()==(200,200)
        cur.execute('SELECT valor FROM lancamentos WHERE id=%s',(a,));assert cur.fetchone()[0]==150


def test_delete_source_cascades_by_shift_day_not_receipt_day(db):
    import datetime as dt
    from operations import delete_income_source
    with db,db.cursor() as cur:
        cur.execute("INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo) VALUES ('Entrada','Rendas','Hospital corte')")
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,pago,data_competencia,data_vencimento)
          VALUES ('Entrada','Plantões','Hospital corte','Plantão anterior',100,0,'2026-09-30','2026-11-10'),
                 ('Entrada','Rendas','Hospital corte','Plantão hoje',200,0,'2026-10-01','2026-11-10'),
                 ('Entrada','Plantões','Hospital corte','Plantão futuro',300,0,'2026-11-01','2026-12-10'),
                 ('Entrada','Rendas','Outro','Plantão outro',400,0,'2026-10-01','2026-11-10'),
                 ('Entrada','Rendas','Hospital corte','Renda avulsa',500,0,'2026-10-01','2026-11-10')""")
        assert delete_income_source(cur,'Rendas','Hospital corte',dt.date(2026,10,1))==2
        cur.execute('SELECT descricao FROM lancamentos')
        assert {r[0] for r in cur.fetchall()}=={'Plantão anterior','Plantão outro','Renda avulsa'}
        cur.execute("SELECT count(*) FROM auditoria WHERE operacao='EXCLUIR_PLANTAO_FONTE'")
        assert cur.fetchone()[0]==2
