import datetime as dt
import io
import os
from contextlib import contextmanager
from decimal import Decimal
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pytest
from migrate import migrate
from payments import settle, reverse
from backup import export_snapshot,read_snapshot
from finance import int_seguro
from tests.helpers import app_functions


@pytest.fixture
def db():
    url=os.environ.get('TEST_DATABASE_URL')
    if not url: pytest.skip('Requires disposable PostgreSQL TEST_DATABASE_URL')
    conn=psycopg2.connect(url)
    with conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS tenant_test CASCADE; CREATE SCHEMA tenant_test; SET search_path TO tenant_test')
    migrate(conn,'tenant_test')
    with conn:
        with conn.cursor() as cur: cur.execute('SET search_path TO tenant_test')
    yield conn
    conn.close()


def add(cur,value=100,paid=0,real=0,credit=True):
    cur.execute("""INSERT INTO lancamentos(tipo,categoria,descricao,valor,data_vencimento,data_competencia,pago,valor_pago,forma_pagamento)
        VALUES ('Despesa','Mercado','Teste',%s,'2026-10-05','2026-09-30',%s,%s,%s) RETURNING id""",(value,paid,real,'Crédito' if credit else 'À vista'))
    return cur.fetchone()[0]


def test_migration_idempotent_and_payment_zero(db):
    migrate(db,'tenant_test')
    with db,db.cursor() as cur:
        i=add(cur)
        settle(cur,[i],0,dt.date(2026,9,30))
        cur.execute('SELECT valor,valor_pago,pago FROM lancamentos WHERE id=%s',(i,))
        assert cur.fetchone()==(100,100,1) # discount is separate, not attributed to groceries
        cur.execute("SELECT valor FROM lancamentos WHERE tipo='Entrada'")
        assert cur.fetchone()[0]==100
        reverse(cur,[i])
        cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==1


def test_non_credit_zero_and_previous_payment_protection(db):
    with db,db.cursor() as cur:
        i=add(cur,credit=False);settle(cur,[i],0,dt.date(2026,9,30))
        cur.execute('SELECT valor,valor_pago,pago FROM lancamentos WHERE id=%s',(i,));assert cur.fetchone()==(100,0,1)
        with pytest.raises(ValueError): settle(cur,[i],50,dt.date(2026,10,1))


def test_credit_fee_keeps_categories(db):
    with db,db.cursor() as cur:
        i=add(cur,100);j=add(cur,200)
        settle(cur,[i,j],315,dt.date(2026,10,5))
        cur.execute('SELECT valor_pago FROM lancamentos WHERE id=ANY(%s) ORDER BY id',([i,j],))
        assert cur.fetchall()==[(100,),(200,)]
        cur.execute("SELECT valor FROM lancamentos WHERE categoria='Ajustes de fatura'");assert cur.fetchone()[0]==15
        with pytest.raises(ValueError): reverse(cur,[i])
        reverse(cur,[i,j])
        cur.execute('SELECT count(*) FROM lancamentos WHERE pago=1');assert cur.fetchone()[0]==0


def test_backup_roundtrip_and_checksum(db):
    with db,db.cursor() as cur: add(cur)
    data=export_snapshot(db);db.rollback()
    frames=read_snapshot(io.BytesIO(data));assert len(frames['lancamentos.csv'])==1
    @contextmanager
    def transaction():
        with db,db.cursor() as cur: yield cur
    ns=dict(pd=pd,sql=sql,execute_values=execute_values,int_seguro=int_seguro,transaction=transaction)
    names=['validar_csv_lancamentos','_limpar_df_para_banco','_insert_dataframe','importar_backup']
    app_functions(names,ns)
    file=io.BytesIO(data);file.name='snapshot.zip'
    ok,msg=ns['importar_backup'](file);assert ok,msg
    with db,db.cursor() as cur:
        cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==1


def test_tenant_isolation_and_constraints(db):
    migrate(db,'tenant_other')
    with db,db.cursor() as cur:
        cur.execute('SET search_path TO tenant_test');add(cur)
        cur.execute('SET search_path TO tenant_other');cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==0
        cur.execute("SELECT count(*) FROM pg_constraint WHERE conrelid='lancamentos'::regclass AND conname='ck_lanc_valor'");assert cur.fetchone()[0]==1


def test_transaction_rolls_back_entire_batch(db):
    with db,db.cursor() as cur: i=add(cur);j=add(cur,paid=1,real=100)
    with pytest.raises(ValueError),db,db.cursor() as cur:
        settle(cur,[i],None,dt.date(2026,9,30));settle(cur,[j],None,dt.date(2026,9,30))
    with db,db.cursor() as cur:
        cur.execute('SELECT pago FROM lancamentos WHERE id=%s',(i,));assert cur.fetchone()[0]==0


def test_reverse_preserves_audit_for_payment_and_receipt(db):
    with db, db.cursor() as cur:
        for kind in ('Despesa', 'Entrada'):
            i=add(cur, credit=False)
            cur.execute('UPDATE lancamentos SET tipo=%s WHERE id=%s',(kind,i))
            settle(cur,[i],90,dt.date(2026,9,30))
            reverse(cur,[i])
            cur.execute('SELECT valor,pago,valor_pago,data_pagamento FROM lancamentos WHERE id=%s',(i,))
            assert cur.fetchone()==(100,0,0,None)
            cur.execute("""SELECT anterior->>'valor_pago',posterior->>'pago' FROM auditoria
                WHERE anterior->>'id'=%s AND anterior->>'pago'='1'
                AND posterior->>'pago'='0'""",(str(i),))
            assert cur.fetchone()==('90.00','0')
            cur.execute('SELECT count(*) FROM auditoria')
            count=cur.fetchone()[0]
            reverse(cur,[i])
            cur.execute('SELECT count(*) FROM auditoria')
            assert cur.fetchone()[0]==count


def test_draft_duplicate_and_intentional_repeat(db):
    with db,db.cursor() as cur:
        query="""INSERT INTO lancamentos(tipo,descricao,valor,parcela_atual,requisicao_id)
            VALUES ('Despesa','Compra',10,%s,%s) ON CONFLICT DO NOTHING"""
        for _ in range(2):
            for installment in (1,2): cur.execute(query,(installment,'draft-a'))
        cur.execute(query,(1,'draft-b'))
        cur.execute('SELECT count(*) FROM lancamentos'); assert cur.fetchone()[0]==3


def test_import_replay_preserves_original(db):
    from operations import insert_shifts
    row=('Entrada','Rendas','Hospital','Plantão Hospital (30/09/2026)',100,
         dt.date(2026,10,10),1,1,0,'first','Outros','Baixa 🟢',0,dt.date(2026,9,30))
    with db,db.cursor() as cur:
        assert insert_shifts(cur,[row,row])==1
        changed=list(row);changed[4]=200;changed[9]='other'
        assert insert_shifts(cur,[changed])==0
        cur.execute('SELECT valor FROM lancamentos');assert cur.fetchone()[0]==100


def test_recurrences_generated_once(db):
    class UI:
        session_state={}
        @staticmethod
        def error(message): raise AssertionError(message)
    @contextmanager
    def transaction():
        with db,db.cursor() as cur: yield cur
    def fetch(query):
        with db,db.cursor() as cur:
            cur.execute(query)
            return pd.DataFrame(cur.fetchall(),columns=[c[0] for c in cur.description])
    import calendar
    ns=app_functions(['processar_recorrencias_lazy'],dict(st=UI,pd=pd,datetime=dt,calendar=calendar,
                                                        transaction=transaction,fetch_dataframe=fetch))
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO categorias_personalizadas(tipo,categoria,valor_padrao,
            is_recorrente,dia_pagamento,data_inicio) VALUES ('Despesa','Escola',100,1,5,'2026-09-01')""")
    for _ in range(2):
        UI.session_state.clear()
        ns['processar_recorrencias_lazy'](10,2026)
    with db,db.cursor() as cur:
        cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==1
