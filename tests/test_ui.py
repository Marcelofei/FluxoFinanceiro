from datetime import date
import base64
import hashlib
import json
from uuid import uuid4
import streamlit as st
from streamlit.testing.v1 import AppTest
import storage


def by_label(app, kind, label):
    matches = [w for w in getattr(app,kind) if w.label == label]
    assert len(matches) == 1, (label, [w.label for w in getattr(app,kind)])
    return matches[0]


def healthy(app):
    assert not app.exception, [e.message for e in app.exception]
    assert not app.error, [e.value for e in app.error]


def test_complete_monthly_workflow(database, monkeypatch):
    salt = b'ui-test-salt'
    digest = hashlib.pbkdf2_hmac('sha256',b'local-test-only-password',salt,100000)
    encoded = 'pbkdf2_sha256$100000$'+base64.b64encode(salt).decode()+'$'+base64.b64encode(digest).decode()
    monkeypatch.setenv('APP_USERS_JSON',json.dumps({'test':{'schema':database,'password_hash':encoded}}))
    st.cache_resource.clear()
    app = AppTest.from_file('app.py',default_timeout=20).run()
    by_label(app,'text_input','Usuário').set_value('test')
    by_label(app,'text_input','Senha').set_value('local-test-only-password')
    by_label(app,'button','Entrar').click().run(); healthy(app)
    by_label(app,'number_input','Ano').set_value(2026)
    by_label(app,'selectbox','Mês').set_value(11).run(); healthy(app)
    by_label(app,'number_input','Disponível (R$)').set_value(5000)
    by_label(app,'button','Atualizar disponível').click().run(); healthy(app)
    by_label(app,'text_input','Fonte de renda').set_value('Hospital manual')
    # Both creation forms use the same human labels; distinguish by form order.
    values = [w for w in app.number_input if w.label == 'Valor previsto (R$)']
    values[0].set_value(10000)
    by_label(app,'date_input','Data prevista').set_value(date(2026,11,15))
    [w for w in app.button if w.label == 'Salvar previsão'][0].click().run(); healthy(app)
    with storage.connection(database) as conn:
        storage.create_entries(conn,str(uuid4()),'expense','Conta de novembro',700000,date(2026,11,10))
        storage.create_entries(conn,str(uuid4()),'expense','Conta de outubro',10000,date(2026,10,31))
        storage.create_entries(conn,str(uuid4()),'expense','Conta de dezembro',10000,date(2026,12,1))
    app.run(); healthy(app)
    text = ' '.join(w.value for w in app.markdown)
    assert 'Conta de novembro' in text
    assert 'Conta de outubro' not in text and 'Conta de dezembro' not in text
    assert any('Dinheiro após o vencimento' in w.value for w in app.warning)
    actuals = [w for w in app.number_input if w.label == 'Valor real (R$)']
    actuals[0].set_value(9000)
    by_label(app,'button','Confirmar recebido').click().run(); healthy(app)
    with storage.connection(database) as conn:
        rows = storage.list_entries(conn,2026,11)
        received = next(r for r in rows if r['kind']=='income')
        assert received['expected_cents']==1000000 and received['actual_cents']==900000
        assert storage.get_balance(conn)['cents']==500000
    by_label(app,'number_input','Disponível (R$)').set_value(14000)
    by_label(app,'button','Atualizar disponível').click().run(); healthy(app)
    by_label(app,'number_input','Disponível após este pagamento (R$)').set_value(7000)
    by_label(app,'button','Confirmar pago').click().run(); healthy(app)
    with storage.connection(database) as conn:
        assert storage.get_balance(conn)['cents']==700000
        assert next(r for r in storage.list_entries(conn,2026,11) if r['kind']=='expense')['settled']
    assert any('Nenhuma conta pendente neste mês' in w.value for w in app.info)
    by_label(app,'selectbox','Mês').set_value(12).run(); healthy(app)
    assert 'Conta de dezembro' in ' '.join(w.value for w in app.markdown)
    assert 'Conta de novembro' not in ' '.join(w.value for w in app.markdown)
    by_label(app,'selectbox','Mês').set_value(11).run(); healthy(app)
    by_label(app,'button','Desfazer pagamento').click().run(); healthy(app)
    with storage.connection(database) as conn:
        assert storage.get_balance(conn)['cents']==700000
        assert not next(r for r in storage.list_entries(conn,2026,11) if r['kind']=='expense')['settled']

    # Editar uma previsão altera somente a ocorrência selecionada.
    app.toggle(key='edit_toggle_'+str(next(r for r in storage_rows(database) if r['kind']=='expense')['id'])).set_value(True).run()
    healthy(app)
    [w for w in app.text_input if w.label=='Descrição'][-1].set_value('Conta revisada')
    edited_values = [w for w in app.number_input if w.label=='Valor previsto (R$)']
    edited_values[-1].set_value(6500)
    by_label(app,'button','Salvar alteração').click().run(); healthy(app)
    row = next(r for r in storage_rows(database) if r['kind']=='expense')
    assert row['description']=='Conta revisada' and row['expected_cents']==650000
    if not app.toggle(key='edit_toggle_'+str(row['id'])).value:
        app.toggle(key='edit_toggle_'+str(row['id'])).set_value(True).run()
    by_label(app,'checkbox','Confirmo excluir somente este lançamento').set_value(True).run()
    by_label(app,'button','Excluir previsão').click().run(); healthy(app)
    assert not [r for r in storage_rows(database) if r['kind']=='expense']
    by_label(app,'selectbox','Mês').set_value(1).run(); healthy(app)
    assert any('Nenhuma conta pendente neste mês' in w.value for w in app.info)


def storage_rows(schema):
    with storage.connection(schema) as conn:
        return storage.list_entries(conn,2026,11)
