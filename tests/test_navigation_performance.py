"""Bound database round trips while preserving fresh, concurrent financial data."""
import datetime as dt
import os
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pytest
from recurrences import generate_months
from operations import edit_income_source
from tests.test_database import db
from tests.helpers import app_functions

PERIODS=[(11,2026),(12,2026),(1,2027)]


class CountingCursor:
    def __init__(self, cursor):
        self.cursor=cursor
        self.queries=0
    def execute(self,*args,**kwargs):
        self.queries+=1
        return self.cursor.execute(*args,**kwargs)


def seed(cur, sources, manual):
    if sources:
        execute_values(cur,'''INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo,
            valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio)
            VALUES %s''',[('Entrada','Rendas',f'Fonte {i}',100,0,31,1,dt.date(2026,1,1)) for i in range(sources)])
    if manual:
        execute_values(cur,'''INSERT INTO series_manuais(compra_id,tipo,categoria,descricao,
            valor,inicio,dia,forma_pagamento,prioridade) VALUES %s''',
            [(f'manual-{i}','Despesa','Casa',f'Conta {i}',50,dt.date(2026,1,1),31,'Outros','Média 🟡') for i in range(manual)])


@pytest.mark.parametrize('sources,manual',[(0,0),(1,1),(40,10),(150,50)])
def test_generation_query_budget_is_constant_for_all_rule_counts(db,sources,manual):
    with db,db.cursor() as cur:
        seed(cur,sources,manual)
        counted=CountingCursor(cur)
        start=perf_counter()
        generate_months(counted,PERIODS)
        elapsed=perf_counter()-start
        assert counted.queries==2
        cur.execute('SELECT count(*),sum(valor) FROM lancamentos')
        count,total=cur.fetchone()
        assert count==3*(sources+manual)
        assert (total or 0)==3*(sources*100+manual*50)
        cur.execute('SELECT count(*) FROM auditoria');before=cur.fetchone()[0]
        for _ in range(5):
            repeated=CountingCursor(cur)
            generate_months(repeated,PERIODS)
            assert repeated.queries==2
        cur.execute('SELECT count(*) FROM auditoria');assert cur.fetchone()[0]==before
        print(f'Generation benchmark: sources={sources} manual={manual} months=3 queries={counted.queries} seconds={elapsed:.4f}')


def test_warm_generation_does_not_lock_already_generated_sources(db):
    with db,db.cursor() as cur:
        seed(cur,1,1);generate_months(cur,PERIODS)
    with db,db.cursor() as cur:
        generate_months(cur,PERIODS)
        other=psycopg2.connect(os.environ['TEST_DATABASE_URL'])
        try:
            with other,other.cursor() as oc:
                oc.execute('SET LOCAL search_path TO tenant_test')
                oc.execute('SELECT id FROM categorias_personalizadas FOR UPDATE NOWAIT')
                assert len(oc.fetchall())==1
                oc.execute('SELECT compra_id FROM series_manuais FOR UPDATE NOWAIT')
                assert len(oc.fetchall())==1
        finally:other.close()


def test_four_concurrent_generators_do_not_duplicate_reservations(db):
    with db,db.cursor() as cur:seed(cur,10,5)
    barrier=Barrier(4)
    def worker(_):
        other=psycopg2.connect(os.environ['TEST_DATABASE_URL'])
        try:
            with other,other.cursor() as oc:
                oc.execute("SET LOCAL search_path TO tenant_test; SET LOCAL lock_timeout='10s'")
                barrier.wait(timeout=10)
                generate_months(oc,PERIODS)
        finally:other.close()
    with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(worker,range(4)))
    with db,db.cursor() as cur:
        cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==45
        cur.execute('SELECT count(*) FROM recorrencias_geradas');assert cur.fetchone()[0]==30
        cur.execute('SELECT count(*) FROM series_manuais_geradas');assert cur.fetchone()[0]==15


def test_concurrent_source_edit_and_batch_generation_use_current_rule(db):
    with db,db.cursor() as cur:
        seed(cur,1,0)
        cur.execute('SELECT id FROM categorias_personalizadas');source=cur.fetchone()[0]
    barrier=Barrier(2)
    def worker(edit):
        other=psycopg2.connect(os.environ['TEST_DATABASE_URL'])
        try:
            with other,other.cursor() as oc:
                oc.execute("SET LOCAL search_path TO tenant_test; SET LOCAL lock_timeout='10s'")
                barrier.wait(timeout=10)
                if edit:edit_income_source(oc,source,200,20,0,1,0,'Mensal',dt.date(2026,11,1))
                else:generate_months(oc,PERIODS)
        finally:other.close()
    with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(worker,[True,False]))
    with db,db.cursor() as cur:
        generate_months(cur,PERIODS)
        cur.execute('SELECT valor,EXTRACT(DAY FROM data_vencimento)::int FROM lancamentos')
        assert cur.fetchall()==[(200,20)]*3


def test_repeated_month_arguments_are_deduplicated(db):
    with db,db.cursor() as cur:
        seed(cur,1,1)
        generate_months(cur,PERIODS+PERIODS)
        cur.execute('SELECT count(*) FROM lancamentos');assert cur.fetchone()[0]==6


def test_planning_reuses_precomputed_units_without_new_queries():
    def must_not_query(*args,**kwargs):raise AssertionError('Unnecessary planning query')
    ns=app_functions(['_planejamento_orcamentos'],dict(pd=pd,_planejamento_unidades=must_not_query))
    units=pd.DataFrame([dict(categoria='Casa',subgrupo='Luz',nome='Luz',planejado=200,
        realizado=50,comprometido=100,percentual=25,tem_orcamento=True)])
    result=ns['_planejamento_orcamentos'](None,2026,11,unidades=units)
    assert result.iloc[0]['disponivel']==50


def test_monthly_operational_view_reuses_input_and_returns_independent_copy():
    def must_not_query(*args,**kwargs):raise AssertionError('Duplicate monthly query')
    ns=app_functions(['_dados_operacionais'],dict(fetch_dataframe=must_not_query))
    for data in [pd.DataFrame(),pd.DataFrame([dict(id=1,valor=100)])]:
        result=ns['_dados_operacionais'](data)
        assert result.equals(data) and result is not data
