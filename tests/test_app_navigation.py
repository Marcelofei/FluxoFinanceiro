"""Real Streamlit interactions against an isolated PostgreSQL test schema."""
from decimal import Decimal
import datetime as dt
import json
import os
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest
import psycopg2.pool
from security import new_password_hash
from tests.test_database import db

TODAY = dt.date(2026,9,30)
PASSWORD = 'test-only-navigation-password'


def healthy(app):
    assert not app.exception, [e.message for e in app.exception]
    assert not app.error, [e.value for e in app.error]


def widget(app, kind, label):
    matches=[w for w in getattr(app,kind) if w.label==label]
    assert len(matches)==1, (kind,label,[w.label for w in getattr(app,kind)])
    return matches[0]


@pytest.fixture
def ui(db, monkeypatch):
    import finance
    monkeypatch.setattr(finance,'today_local',lambda: TODAY)
    url=os.environ['TEST_DATABASE_URL']
    monkeypatch.setenv('DATABASE_URL',url)
    monkeypatch.setenv('APP_USERS_JSON',json.dumps({'test':{'schema':'tenant_test','password_hash':new_password_hash(PASSWORD)}}))
    original=psycopg2.pool.ThreadedConnectionPool
    pools=[]
    def test_pool(*args,**kwargs):
        # The disposable CI database has no TLS; production code still requires TLS.
        kwargs['dsn']=url
        pool=original(*args,**kwargs);pools.append(pool);return pool
    monkeypatch.setattr(psycopg2.pool,'ThreadedConnectionPool',test_pool)
    st.cache_resource.clear();st.cache_data.clear()
    with db,db.cursor() as cur:
        cur.execute("INSERT INTO preferencias_app(chave,valor) VALUES ('onboarding_concluido','true') ON CONFLICT DO NOTHING")
        cur.execute("INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo,is_producao_variavel) VALUES ('Entrada','Rendas','Hospital Teste',1),('Despesa','Casa','Escola',0)")
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,descricao,valor,data_vencimento,data_competencia,pago,valor_pago,data_pagamento)
            VALUES ('Entrada','Rendas','Renda confirmada',3000,'2026-09-25','2026-09-25',1,3000,'2026-09-25'),
                   ('Entrada','Rendas','Renda futura',2000,'2026-10-05','2026-10-05',0,0,NULL),
                   ('Despesa','Casa','Conta teste',100,'2026-09-30','2026-09-30',0,0,NULL),
                   ('Despesa','Casa','Conta antiga',50,'2026-08-31','2026-08-31',0,0,NULL)""")
    app=AppTest.from_file('app.py',default_timeout=30).run()
    widget(app,'text_input','Usuário').set_value('test')
    widget(app,'text_input','Senha').set_value(PASSWORD)
    widget(app,'button','Entrar').click().run()
    healthy(app)
    yield app,db
    for pool in pools: pool.closeall()
    st.cache_resource.clear();st.cache_data.clear()


@pytest.mark.parametrize('key',['nav_inicio','nav_fluxo','nav_planejamento','nav_rendas','nav_mais'])
def test_main_navigation(ui,key):
    app,_=ui
    app.button(key=key).click().run()
    healthy(app)
    assert len(app.button)>0


def test_create_retry_pay_reverse_and_history(ui):
    app,db=ui
    app.session_state['menu_atual']='📝 Lançamentos';app.run();healthy(app)
    widget(app,'text_input','Descrição').set_value('Conta navegação')
    widget(app,'text_input','Valor (R$)').set_value('123,45')
    widget(app,'button','Agendar conta').click().run();healthy(app)
    widget(app,'button','Agendar conta').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute("SELECT id FROM lancamentos WHERE descricao='Conta navegação'");rows=cur.fetchall()
        assert len(rows)==1
        record_id=rows[0][0]
    app.button(key='nav_fluxo').click().run();healthy(app)
    actions=[b for b in app.button if b.label=='Pagar' and str(record_id) in str(b.key).split('_')[-1:]]
    assert len(actions)==1, [(b.key,b.label) for b in app.button]
    actions[0].click().run();healthy(app)
    widget(app,'button','Confirmar pagamento').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute('SELECT pago,valor_pago FROM lancamentos WHERE id=%s',(record_id,))
        assert cur.fetchone()==(1,Decimal('123.45'))
    # AppTest 1.45 retains stale form nodes after st.rerun. A fresh login also
    # verifies that undo works with persisted data in a subsequent session.
    app=AppTest.from_file('app.py',default_timeout=30).run()
    widget(app,'text_input','Usuário').set_value('test')
    widget(app,'text_input','Senha').set_value(PASSWORD)
    widget(app,'button','Entrar').click().run();healthy(app)
    app.button(key='nav_fluxo').click().run();healthy(app)
    undo=[b for b in app.button if '_undo_' in str(b.key) and str(b.key).endswith('_'+str(record_id))]
    assert len(undo)==1,[(b.key,b.label) for b in app.button]
    undo[0].click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute('SELECT valor,pago,valor_pago FROM lancamentos WHERE id=%s',(record_id,))
        assert cur.fetchone()==(Decimal('123.45'),0,0)
    assert any('Histórico' in exp.label for exp in app.expander)


def test_credit_purchase_is_pending_and_split_exactly(ui):
    app,db=ui
    app.session_state['menu_atual']='📝 Lançamentos';app.run();healthy(app)
    widget(app,'text_input','Descrição').set_value('Compra cartão teste')
    widget(app,'text_input','Valor (R$)').set_value('100,00')
    widget(app,'radio','Como foi a compra?').set_value('Crédito').run();healthy(app)
    widget(app,'text_input','Nome do cartão').set_value('Cartão teste')
    widget(app,'radio','Repetição').set_value('Parcelada').run()
    widget(app,'number_input','Número de parcelas').set_value(3)
    widget(app,'radio','O valor informado é').set_value('Total da compra')
    widget(app,'button','Registrar compra no cartão').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute("SELECT count(*),sum(valor),sum(pago),count(DISTINCT fatura_id) FROM lancamentos WHERE descricao='Compra cartão teste'")
        assert cur.fetchone()==(3,100,0,3)


def test_period_navigation_keeps_actual_reference(ui):
    app,_=ui
    app.button(key='nav_fluxo').click().run();healthy(app)
    app.button(key='sb_next').click().run();healthy(app)
    assert any('Situação em 30/09/2026' in c.value for c in app.caption)
    assert any('não representa seu saldo bancário' in c.value for c in app.caption)


def test_reorganize_pending_preserves_records_and_can_undo(ui):
    app,db=ui
    app.button(key='nav_fluxo').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute('SELECT id,pago,valor,valor_pago,data_pagamento FROM lancamentos ORDER BY id');before=cur.fetchall()
        cur.execute("SELECT id FROM lancamentos WHERE descricao='Renda confirmada'");income=cur.fetchone()[0]
    app.button(key='reorganizar_contas').click().run();healthy(app)
    assert not any(w.key=='reorganizar_ids' for w in app.multiselect)
    with db,db.cursor() as cur:
        cur.execute("SELECT valor FROM preferencias_app WHERE chave='cobertura_reorganizacao:2026-09'")
        assert json.loads(cur.fetchone()[0])['ids']==[income]
        cur.execute('SELECT id,pago,valor,valor_pago,data_pagamento FROM lancamentos ORDER BY id');assert cur.fetchall()==before
    # A fresh session confirms persistence and avoids AppTest's stale conditional form nodes.
    app=AppTest.from_file('app.py',default_timeout=30).run()
    widget(app,'text_input','Usuário').set_value('test');widget(app,'text_input','Senha').set_value(PASSWORD)
    widget(app,'button','Entrar').click().run();healthy(app)
    app.button(key='nav_fluxo').click().run();healthy(app)
    app.button(key='desfazer_reorganizacao').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute("SELECT valor FROM preferencias_app WHERE chave='cobertura_reorganizacao:2026-09'")
        assert json.loads(cur.fetchone()[0])['ids']==[]
        cur.execute("SELECT count(*) FROM auditoria WHERE entidade='cobertura'");assert cur.fetchone()[0]==2


def test_invoice_composition_shows_exact_members_without_mutation(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,descricao,valor,forma_pagamento,
          data_vencimento,data_competencia,parcela_atual,total_parcelas,pago)
          VALUES ('Despesa','Casa','Notebook detalhado',3000,'Crédito','2026-09-30','2026-08-10',2,3,0),
                 ('Despesa','Casa','Compra detalhada',500,'Crédito','2026-09-30','2026-09-15',1,1,0),
                 ('Despesa','Casa','Outra fatura',900,'Crédito','2026-10-10','2026-09-15',1,1,0)""")
    app.button(key='nav_fluxo').click().run();healthy(app)
    toggle=next(w for w in app.toggle if w.label=='Ver composição da fatura · 2 lançamento(s)')
    toggle.set_value(True).run();healthy(app)
    tables=[w.value for w in app.dataframe if 'Valor nesta fatura' in w.value.columns]
    assert len(tables)==1
    assert tables[0]['Descrição'].tolist()==['Notebook detalhado','Compra detalhada']
    assert tables[0]['Parcela'].tolist()==['2/3','À vista']
    assert any('Total dos lançamentos: R$ 3.500,00' in w.value for w in app.markdown)
    with db,db.cursor() as cur:
        cur.execute("SELECT sum(valor),sum(pago) FROM lancamentos WHERE forma_pagamento='Crédito'")
        assert cur.fetchone()==(4400,0)


def test_delete_income_source_does_not_reappear_from_history(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,pago) VALUES ('Entrada','Rendas','Hospital Teste','Histórico preservado',123,'2026-09-30',0)")
    app.button(key='nav_rendas').click().run();healthy(app)
    buttons=[b for b in app.button if b.label=='Excluir fonte' and 'hospital teste' in str(b.key)]
    assert len(buttons)==1
    buttons[0].click().run();healthy(app)
    widget(app,'button','Confirmar exclusão da fonte').click().run();healthy(app)
    assert not any(b.label=='Excluir fonte' and 'hospital teste' in str(b.key) for b in app.button)
    with db,db.cursor() as cur:
        cur.execute("SELECT valor FROM lancamentos WHERE descricao='Histórico preservado'");assert cur.fetchone()[0]==123


def test_edit_income_source_updates_pending_dates(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_competencia,data_vencimento,pago) VALUES ('Entrada','Rendas','Hospital Teste','Renda reagendar',123,'2026-09-01','2026-09-30',0)")
    app.button(key='nav_rendas').click().run();healthy(app)
    widget(app,'button','Editar ›').click().run();healthy(app)
    widget(app,'number_input','Dia de recebimento').set_value(20)
    widget(app,'number_input','Meses até receber').set_value(1)
    widget(app,'button','Salvar alterações').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute("SELECT data_vencimento,pago FROM lancamentos WHERE descricao='Renda reagendar'")
        assert cur.fetchone()==(dt.date(2026,10,20),0)


def test_planning_category_details_reconcile_and_isolate(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,valor_pago,pago,forma_pagamento,data_competencia,data_vencimento,data_pagamento)
            VALUES ('Despesa','Casa','Escola','Mensalidade paga',100,100,1,'Outros','2026-09-01','2026-09-10','2026-09-10'),
                   ('Despesa','Casa','Escola','Material crédito',50,0,0,'Crédito','2026-09-15','2026-10-10',NULL),
                   ('Despesa','Casa','Escola','Taxa pendente',30,0,0,'Outros','2026-09-20','2026-09-30',NULL),
                   ('Despesa','Casa','Outro','Não incluir subgrupo',900,0,0,'Outros','2026-09-01','2026-09-30',NULL),
                   ('Despesa','Casa','Escola','Não incluir mês',800,0,0,'Outros','2026-10-01','2026-10-10',NULL)""")
        cur.execute("INSERT INTO orcamentos_categorias(competencia,categoria,subgrupo,valor_planejado) VALUES ('2026-09-01','Casa','Escola',200)")
    app.button(key='nav_planejamento').click().run();healthy(app)
    widget(app,'toggle','Ver custos de Escola').set_value(True).run();healthy(app)
    tables=[w.value for w in app.dataframe if 'No realizado' in w.value.columns]
    assert len(tables)==1
    assert set(tables[0]['Descrição'])=={'Mensalidade paga','Material crédito','Taxa pendente'}
    assert any('Realizado: R$ 150,00 · Ainda previsto: R$ 30,00' in w.value for w in app.markdown)
    assert any('orçamento definido de R$ 200,00' in w.value for w in app.caption)


def test_income_source_sums_schedule_and_no_phantom_monthly_rate(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("UPDATE categorias_personalizadas SET valor_padrao=1200,dia_pagamento=10,atraso_meses=1 WHERE subgrupo='Hospital Teste'")
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,pago,data_competencia,data_vencimento)
          VALUES ('Entrada','Plantões','Hospital Teste','Plantão Hospital Teste (01/09/2026)',1200,0,'2026-09-01','2026-10-10'),
                 ('Entrada','Rendas','Hospital Teste','Plantão Hospital Teste (02/09/2026)',1500,0,'2026-09-02','2026-10-10')""")
    app.button(key='nav_rendas').click().run();healthy(app)
    # September has no shifts due: the per-shift rate must not appear as monthly income.
    source=next(m.value for m in app.markdown if "income2-source-name" in m.value and 'Hospital Teste' in m.value)
    assert 'R$ 0,00' in source
    app.button(key='sb_next').click().run();healthy(app)
    sources=[m.value for m in app.markdown if "income2-source-name" in m.value and 'Hospital Teste' in m.value]
    assert len(sources)==1 and 'R$ 2.700,00' in sources[0]
    assert any('2 plantão(ões)' in c.value for c in app.caption)
    # Removing a pending schedule item must immediately reduce the source projection.
    with db,db.cursor() as cur:
        cur.execute("DELETE FROM lancamentos WHERE descricao='Plantão Hospital Teste (02/09/2026)'")
    app.run();healthy(app)
    source=next(m.value for m in app.markdown if "income2-source-name" in m.value and 'Hospital Teste' in m.value)
    assert 'R$ 1.200,00' in source


def test_shift_source_value_is_read_only_in_income_screen(ui):
    app,db=ui
    app.button(key='nav_rendas').click().run();healthy(app)
    widget(app,'button','Editar ›').click().run();healthy(app)
    assert widget(app,'number_input','Calculado pela agenda').disabled
    assert not any(w.label=='Valor esperado' for w in app.number_input)


def test_source_delete_removes_today_and_future_shifts_only(ui):
    app,db=ui
    with db,db.cursor() as cur:
        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,pago,data_competencia,data_vencimento)
            VALUES ('Entrada','Rendas','Hospital Teste','Plantão antes corte',100,0,'2026-09-29','2026-10-10'),
                   ('Entrada','Rendas','Hospital Teste','Plantão no corte',200,0,'2026-09-30','2026-10-10'),
                   ('Entrada','Rendas','Hospital Teste','Plantão após corte',300,0,'2026-10-01','2026-11-10')""")
    app.button(key='nav_rendas').click().run();healthy(app)
    next(b for b in app.button if b.label=='Excluir fonte' and 'hospital teste' in str(b.key)).click().run();healthy(app)
    widget(app,'button','Confirmar exclusão da fonte').click().run();healthy(app)
    with db,db.cursor() as cur:
        cur.execute("SELECT descricao FROM lancamentos WHERE descricao LIKE 'Plantão %%'")
        assert cur.fetchall()==[('Plantão antes corte',)]
