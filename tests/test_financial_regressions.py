import datetime as dt
from decimal import Decimal
from contextlib import contextmanager
import io
import pandas as pd
import pytest
import finance
from finance import parse_currency
from operations import edit_income_source, quick_edit, lock_entry, change_series, delete_entries, edit_category
from payments import settle, reverse
from recurrences import generate_month
from tests.test_database import db
from tests.helpers import app_functions

TODAY=dt.date(2026,11,23)


@pytest.mark.parametrize('text,expected',[('1.234,56','1234.56'),('1234,56','1234.56'),
    ('100.50','100.50'),('0','0'),('R$ 1.000,00','1000'),(100.5,'100.50'),('0,01','0.01')])
def test_currency_accepts_valid_inputs(text,expected):
    assert parse_currency(text)==Decimal(expected)


@pytest.mark.parametrize('text',['abc','','1,2,3','NaN','Infinity','10.5555','--1','1.000,001'])
def test_currency_rejects_invalid_without_zero_fallback(text):
    with pytest.raises((ValueError,ArithmeticError)):parse_currency(text)


def test_future_payment_rejected_before_any_query(monkeypatch):
    monkeypatch.setattr(finance,'today_local',lambda:TODAY)
    with pytest.raises(ValueError,match='futura'):
        settle(None,[1],100,TODAY+dt.timedelta(days=1))


def test_random_cent_allocations_conserve_money_under_stress():
    import random
    rng=random.Random(46017)
    for _ in range(1500):
        total=Decimal(rng.randrange(0,10000000))/100
        weights=[Decimal(rng.randrange(0,100000))/100 for _ in range(rng.randrange(1,80))]
        result=finance.allocate_amount(total,weights)
        assert sum(result)==total and all(v>=0 for v in result)
        count=rng.randrange(1,241)
        assert sum(finance.split_total(total,count))==total


def source(cur,delay=0,day=10,start='2026-09-01',kind='Entrada'):
    cur.execute('''INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo,valor_padrao,
        atraso_meses,dia_pagamento,is_recorrente,data_inicio)
        VALUES (%s,'Rendas','Fonte teste',100,%s,%s,1,%s) RETURNING id''',(kind,delay,day,start))
    return cur.fetchone()[0]


def entry(cur,source_id=None,group=None,due='2026-11-28',competence='2026-09-01',
          inferred=False,exception=False,value=100,paid=False,real=0,credit=False,total=1):
    cur.execute('''INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,
        data_competencia,competencia_inferida,excecao_recorrencia,fonte_id,compra_id,
        pago,valor_pago,data_pagamento,forma_pagamento,total_parcelas,parcela_atual)
        VALUES ('Entrada','Rendas','Fonte teste','Teste',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1) RETURNING id''',
        (value,due,competence,inferred,exception,source_id,group,int(paid),real,TODAY if paid else None,'Crédito' if credit else 'Outros',total))
    return cur.fetchone()[0]


def read(cur,item):
    cur.execute('SELECT row_to_json(l) FROM lancamentos l WHERE id=%s',(item,))
    return cur.fetchone()[0]


@pytest.mark.parametrize('delay,day,expected',[(2,1,'2026-11-01'),(3,1,'2026-12-01'),(2,31,'2026-11-30')])
def test_income_legacy_competence_never_double_applies_delay(db,delay,day,expected):
    with db,db.cursor() as c:
        sid=source(c,delay=2)
        item=entry(c,sid,f'rec_{sid}',competence='2026-11-28',inferred=True)
        for _ in range(2):edit_income_source(c,sid,100,day,delay,1,0,'Mensal',TODAY)
        result=read(c,item)
        assert result['data_vencimento']==expected
        assert result['data_competencia']=='2026-09-01'


def test_source_value_sync_preserves_paid_and_individual_exceptions(db):
    with db,db.cursor() as c:
        sid=source(c)
        normal=entry(c,sid);custom=entry(c,sid,value=150)
        ex=entry(c,sid,exception=True);paid=entry(c,sid,paid=True,real=90)
        edit_income_source(c,sid,200,20,0,1,0,'Mensal',TODAY)
        assert read(c,normal)['valor']==200
        assert read(c,custom)['valor']==150
        assert read(c,ex)['data_vencimento']=='2026-11-28'
        assert read(c,paid)['valor_pago']==90


@pytest.mark.parametrize('month,year,delay,day,expected,competence',[
    (11,2026,2,1,'2026-11-01','2026-09-01'),
    (1,2027,3,31,'2027-01-31','2026-10-01'),
    (2,2027,0,31,'2027-02-28','2027-02-01')])
def test_generate_receipt_month_without_opening_earlier_months(db,month,year,delay,day,expected,competence):
    with db,db.cursor() as c:
        sid=source(c,delay,day)
        for _ in range(3):generate_month(c,month,year)
        c.execute('SELECT data_vencimento,data_competencia FROM lancamentos WHERE fonte_id=%s',(sid,))
        assert c.fetchall()==[(dt.date.fromisoformat(expected),dt.date.fromisoformat(competence))]


def test_start_date_and_rule_changes_do_not_create_prestart_income(db):
    with db,db.cursor() as c:
        sid=source(c,3,10,start='2026-11-01')
        for m in (8,9,10,11,12):generate_month(c,m,2026)
        generate_month(c,1,2027)
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==0
        generate_month(c,2,2027)
        c.execute('SELECT data_competencia FROM lancamentos');assert c.fetchone()[0]==dt.date(2026,11,1)


def test_quick_editor_blank_uses_plan_explicit_zero_remains_zero(db):
    ns=app_functions(['resolver_valor_real'],dict(pd=pd,float_seguro=finance.float_seguro))
    with db,db.cursor() as c:
        for supplied,expected in [(float('nan'),100),(0,0),(90,90)]:
            item=entry(c)
            original=read(c,item)
            value=ns['resolver_valor_real'](True,100,supplied)
            quick_edit(c,original,dict(pago=1,valor_pago=value,data_pagamento=TODAY))
            assert read(c,item)['valor_pago']==expected


def test_stale_editor_rejects_overwrite_of_payment(db):
    with db,db.cursor() as c:
        item=entry(c);old=read(c,item)
        settle(c,[item],90,TODAY)
        with pytest.raises(ValueError,match='outra tela'):
            quick_edit(c,old,dict(pago=0,valor_pago=0,prioridade='Alta 🔴'))
        assert read(c,item)['valor_pago']==90


@pytest.mark.parametrize('difference',[10,-10])
def test_cannot_reverse_or_delete_fee_or_discount_directly(db,difference):
    with db,db.cursor() as c:
        item=entry(c,credit=True)
        c.execute("UPDATE lancamentos SET tipo='Despesa' WHERE id=%s",(item,))
        settle(c,[item],100+difference,TODAY)
        c.execute('SELECT id FROM lancamentos WHERE ajuste_pagamento_ids IS NOT NULL')
        adjustment=c.fetchone()[0]
        with pytest.raises(ValueError):reverse(c,[adjustment])
        with pytest.raises(ValueError):delete_entries(c,[adjustment])
        reverse(c,[item])
        assert not read(c,item)['pago']
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==1


def test_future_series_edits_change_rule_dates_and_new_generation(db):
    with db,db.cursor() as c:
        sid=source(c,0,10)
        for m in (11,12):generate_month(c,m,2026)
        c.execute('SELECT id FROM lancamentos ORDER BY data_vencimento LIMIT 1');item=c.fetchone()[0]
        change_series(c,read(c,item),{'valor':120,'data_vencimento':dt.date(2026,11,20)},future=True)
        generate_month(c,1,2027)
        c.execute('SELECT valor,data_vencimento FROM lancamentos ORDER BY data_vencimento')
        assert c.fetchall()==[(120,dt.date(2026,11,20)),(120,dt.date(2026,12,20)),(120,dt.date(2027,1,20))]


def test_series_single_exception_and_deletion_do_not_regenerate(db):
    with db,db.cursor() as c:
        sid=source(c)
        generate_month(c,11,2026);generate_month(c,12,2026)
        c.execute('SELECT id FROM lancamentos ORDER BY data_vencimento');first,second=[r[0] for r in c.fetchall()]
        change_series(c,read(c,first),{'valor':99},future=False)
        edit_income_source(c,sid,200,20,0,1,0,'Mensal',TODAY)
        assert read(c,first)['valor']==99
        change_series(c,read(c,second),delete=True)
        generate_month(c,12,2026)
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==1


def test_delete_future_ends_contract_without_deleting_paid_history(db):
    with db,db.cursor() as c:
        sid=source(c)
        for m in (11,12):generate_month(c,m,2026)
        c.execute('SELECT id FROM lancamentos ORDER BY data_vencimento');first,second=[r[0] for r in c.fetchall()]
        settle(c,[first],90,TODAY)
        change_series(c,read(c,second),delete=True,future=True)
        generate_month(c,12,2026);generate_month(c,1,2027)
        c.execute('SELECT id,valor_pago FROM lancamentos');assert c.fetchall()==[(first,90)]


def test_manual_recurring_rule_extends_beyond_sixty_months(db):
    with db,db.cursor() as c:
        c.execute("""INSERT INTO series_manuais VALUES ('repeat','Despesa','Casa',NULL,'Mensalidade',100,'2026-01-31',NULL,31,'Outros','Média 🟡',0)""")
        for month,year in [(2,2026),(1,2032),(2,2026)]:generate_month(c,month,year)
        c.execute("SELECT data_vencimento FROM lancamentos ORDER BY data_vencimento")
        assert c.fetchall()==[(dt.date(2026,2,28),),(dt.date(2032,1,31),)]


def test_debt_discount_reconciles_open_obligations(db):
    with db,db.cursor() as c:
        a=entry(c,group='debt',total=2);b=entry(c,group='debt',total=2)
        c.execute("UPDATE lancamentos SET tipo='Despesa' WHERE id=ANY(%s)",([a,b],))
        settle(c,[a,b],180,TODAY)
    def fetch(q,*args):
        with db,db.cursor() as c:
            c.execute(q,args or None)
            return pd.DataFrame(c.fetchall(),columns=[x[0] for x in c.description])
    ns=app_functions(['_planejamento_dividas'],dict(fetch_dataframe=fetch,pd=pd))
    result=ns['_planejamento_dividas']().iloc[0]
    assert result['saldo']==0 and result['parcelas_restantes']==0 and result['progresso']==1


def test_same_category_edit_path_reconciles_income(db):
    with db,db.cursor() as c:
        sid=source(c,2,28);item=entry(c,sid,competence='2026-11-28',inferred=True)
        edit_category(c,sid,'Rendas','Fonte renomeada',200,2,1,1,TODAY)
        r=read(c,item)
        assert (r['data_vencimento'],r['valor'],r['subgrupo'])==('2026-11-01',200,'Fonte renomeada')


def test_backup_preserves_rule_tombstones_versions_and_source_links(db):
    from backup import export_snapshot,read_snapshot
    with db,db.cursor() as c:
        sid=source(c);generate_month(c,11,2026)
        c.execute('SELECT id FROM lancamentos');item=c.fetchone()[0]
        change_series(c,read(c,item),{'valor':75},future=False)
        c.execute("INSERT INTO series_manuais VALUES ('repeat','Despesa','Casa',NULL,'Mensalidade',100,'2026-01-31',NULL,31,'Outros','Média 🟡',0)")
        generate_month(c,12,2026)
    snapshot=export_snapshot(db);db.rollback()
    dfs=read_snapshot(io.BytesIO(snapshot))
    assert len(dfs['series_manuais.csv'])==1
    assert dfs['lancamentos.csv']['excecao_recorrencia'].any()
    assert 'versao' in dfs['lancamentos.csv']


def test_concurrent_double_settlement_has_one_winner(db):
    import os
    import psycopg2
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    with db,db.cursor() as c:item=entry(c)
    barrier=Barrier(2)
    def worker():
        conn=psycopg2.connect(os.environ['TEST_DATABASE_URL'])
        try:
            barrier.wait(timeout=10)
            with conn,conn.cursor() as c:
                c.execute('SET LOCAL search_path TO tenant_test')
                settle(c,[item],100,TODAY)
            return 'ok'
        except ValueError:return 'rejected'
        finally:conn.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(lambda _:worker(),range(2)))==['ok','rejected']
    with db,db.cursor() as c:
        c.execute('SELECT count(*) FROM pagamentos WHERE lancamento_id=%s',(item,))
        assert c.fetchone()[0]==1
