"""Fluxo Essencial: uma tela mensal, sem dependência do aplicativo anterior."""
import csv
import hmac
import io
import logging
import os
import time
from datetime import datetime
from threading import Lock
from uuid import uuid4
from zoneinfo import ZoneInfo

import streamlit as st

from finance import brl, cents, schedule
from security import accounts, authenticate
import storage

st.set_page_config(page_title='Fluxo Essencial', page_icon='◷', layout='wide')
TODAY = datetime.now(ZoneInfo('America/Fortaleza')).date()
MONTHS = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
          'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']


@st.cache_resource
def login_attempts():
    return {}, Lock()


def login():
    try:
        configured = accounts()
    except (ValueError, TypeError):
        st.error('Confira a configuração de acesso no Railway.')
        st.stop()
    # Mudanças nas credenciais invalidam sessões abertas.
    fingerprint = os.getenv('APP_USERS_JSON') or os.getenv('APP_PASSWORD', '')
    now = time.time()
    auth = st.session_state.get('auth')
    if auth and now - auth['active_at'] < 1800 and now - auth['login_at'] < 43200 and hmac.compare_digest(auth['fingerprint'].encode(), fingerprint.encode()):
        auth['active_at'] = now
        return auth['schema']
    if auth:
        st.session_state.clear()
    st.title('Fluxo Essencial')
    if not configured and not os.getenv('APP_PASSWORD'):
        st.error('Configure APP_PASSWORD ou APP_USERS_JSON para iniciar.')
        st.stop()
    with st.form('login'):
        user = st.text_input('Usuário') if configured else 'owner'
        password = st.text_input('Senha', type='password')
        submitted = st.form_submit_button('Entrar', type='primary')
    if submitted:
        attempts, lock = login_attempts()
        with lock:
            recent = [t for t in attempts.get(user, []) if now - t < 300]
            blocked = len(recent) >= 5
            if not blocked:
                attempts[user] = recent + [now]
        if blocked:
            st.error('Aguarde cinco minutos antes de tentar novamente.')
        else:
            schema = authenticate(user, password)
            if schema:
                with lock:
                    attempts.pop(user, None)
                st.session_state.clear()
                st.session_state['auth'] = dict(schema=schema, active_at=now, login_at=now, fingerprint=fingerprint)
                st.rerun()
            st.error('Acesso inválido.')
    st.stop()


SCHEMA = login()


def mutate(action, message):
    try:
        with storage.connection(SCHEMA) as conn:
            action(conn)
    except ValueError as exc:
        st.error(str(exc))
        return False
    except Exception:
        logging.getLogger('fluxo').exception('Falha ao salvar')
        st.error('Não foi possível salvar. Atualize a página e tente novamente.')
        return False
    st.session_state['notice'] = message
    return True


def new_entry(kind):
    label = 'entrada' if kind == 'income' else 'conta'
    with st.expander(f'Adicionar {label}'):
        if st.session_state.get(f'created_{kind}'):
            st.success('Lançamento salvo.')
            if st.button(f'Adicionar outra {label}', key=f'another_{kind}'):
                st.session_state.pop(f'created_{kind}', None)
                st.session_state[f'request_{kind}'] = str(uuid4())
                st.rerun()
            return
        token = st.session_state.setdefault(f'request_{kind}', str(uuid4()))
        with st.form(f'new_{kind}_{token}'):
            description = st.text_input('Fonte de renda' if kind == 'income' else 'Descrição', max_chars=160)
            a, b = st.columns(2)
            value = a.number_input('Valor previsto (R$)', min_value=0.0, step=100.0, format='%.2f')
            due = b.date_input('Data prevista' if kind == 'income' else 'Vencimento', value=selected_date, format='DD/MM/YYYY')
            options = ['Uma vez', 'Repetir mensalmente'] + (['Parcelar valor total'] if kind == 'expense' else [])
            repetition = st.selectbox('Repetição', options)
            count = st.number_input('Quantidade de meses / parcelas', min_value=1, max_value=120, value=1)
            st.caption('Repetições criam previsões independentes. Você pode editar cada mês sem alterar os demais.')
            submitted = st.form_submit_button('Salvar previsão', type='primary')
        if submitted:
            if mutate(lambda conn: storage.create_entries(conn, token, kind, description, cents(value), due,
                       1 if repetition == 'Uma vez' else count, repetition == 'Parcelar valor total'), 'Previsão salva.'):
                st.session_state[f'created_{kind}'] = True
                st.rerun()


def entry_actions(entry, balance):
    label = 'recebido' if entry['kind'] == 'income' else 'pago'
    if entry['settled']:
        st.caption(f"{label.capitalize()} em {entry['settled_date']:%d/%m/%Y} · {brl(entry['actual_cents'])} · previsto {brl(entry['expected_cents'])}")
        if st.button(f'Desfazer {"recebimento" if label == "recebido" else "pagamento"}', key=f'reverse_{entry["id"]}'):
            if mutate(lambda conn: storage.reverse_entry(conn, entry), 'Confirmação desfeita. Confira e atualize o disponível, se necessário.'):
                st.rerun()
        return
    with st.form(f'settle_{entry["id"]}_{entry["version"]}'):
        a, b = st.columns(2)
        actual = a.number_input('Valor real (R$)', min_value=0.0, value=entry['expected_cents'] / 100, format='%.2f')
        paid_on = b.date_input('Data efetiva', value=TODAY, max_value=TODAY, format='DD/MM/YYYY')
        after = None
        if entry['kind'] == 'expense':
            # O usuário confirma o saldo; o app não presume sincronização bancária.
            after = st.number_input('Disponível após este pagamento (R$)', min_value=0.0,
                                    value=max(0, balance['cents'] - entry['expected_cents']) / 100, format='%.2f',
                                    help='Confira o saldo restante. O valor abaixo será usado no próximo casamento.')
        else:
            st.caption('O recebido entra no histórico. Atualize o disponível no topo com o saldo que você tem agora.')
        submitted = st.form_submit_button(f'Confirmar {label}', type='primary')
    if submitted:
        if mutate(lambda conn: storage.settle_entry(conn, entry, cents(actual), paid_on,
                   cents(after) if after is not None else None, balance['version']),
                   f'{label.capitalize()} confirmado.' + (' Atualize o disponível com seu saldo atual.' if label == 'recebido' else ' Disponível atualizado.')):
            st.rerun()
    if st.toggle('Editar ou excluir previsão', key=f'edit_toggle_{entry["id"]}'):
        with st.form(f'edit_{entry["id"]}_{entry["version"]}'):
            description = st.text_input('Descrição', value=entry['description'], max_chars=160)
            value = st.number_input('Valor previsto (R$)', min_value=0.0, value=entry['expected_cents'] / 100, format='%.2f')
            due = st.date_input('Vencimento / previsão', value=entry['due_date'], format='DD/MM/YYYY')
            submitted_edit = st.form_submit_button('Salvar alteração')
        if submitted_edit and mutate(lambda conn: storage.edit_entry(conn, entry, description, cents(value), due), 'Previsão atualizada.'):
            st.rerun()
        confirm = st.checkbox('Confirmo excluir somente este lançamento', key=f'delete_confirm_{entry["id"]}')
        if st.button('Excluir previsão', key=f'delete_{entry["id"]}', disabled=not confirm):
            if mutate(lambda conn: storage.delete_entry(conn, entry), 'Previsão excluída.'):
                st.rerun()


st.title('Fluxo Essencial')
if message := st.session_state.pop('notice', None):
    st.success(message)
a, b, c = st.columns([2, 1, 1])
month = a.selectbox('Mês', range(1, 13), index=TODAY.month - 1, format_func=lambda m: MONTHS[m-1])
year = b.number_input('Ano', min_value=2000, max_value=2100, value=TODAY.year)
if c.button('Sair'):
    st.session_state.clear()
    st.rerun()
selected_date = TODAY.replace(year=year, month=month, day=1)
st.caption('Somente vencimentos do mês selecionado, inclusive no casamento. Outros meses são planejados separadamente.')
try:
    with storage.connection(SCHEMA) as conn:
        balance = storage.get_balance(conn)
        entries = storage.list_entries(conn, year, month)
except Exception:
    st.error('Não foi possível carregar a base. Confira a conexão e execute python migrate.py antes de iniciar.')
    st.stop()

with st.container(border=True):
    st.subheader('Quanto tenho disponível para pagar contas agora')
    with st.form(f'balance_{balance["version"]}'):
        available = st.number_input('Disponível (R$)', min_value=0.0, value=balance['cents'] / 100, format='%.2f', label_visibility='collapsed')
        if st.form_submit_button('Atualizar disponível'):
            if mutate(lambda conn: storage.set_balance(conn, cents(available), balance['version']), 'Disponível atualizado. Casamento recalculado.'):
                st.rerun()
    if balance['updated_at']:
        timestamp = balance['updated_at'].astimezone(ZoneInfo('America/Fortaleza'))
        st.caption(f'Atualizado em {timestamp:%d/%m/%Y às %H:%M}. Este valor é compartilhado entre os meses; cada mês é uma simulação independente.')
    else:
        st.caption('Informe o saldo disponível para começar. Confirmar uma renda não altera este campo automaticamente.')
    st.caption('Se houver contas de outros meses que você pretende pagar, reserve esse dinheiro antes de informar o disponível deste planejamento.')

income = [e for e in entries if e['kind'] == 'income']
expenses = [e for e in entries if e['kind'] == 'expense']
a, b, c, d = st.columns(4)
a.metric('Previsto receber', brl(sum(e['expected_cents'] for e in income)))
b.metric('Recebido', brl(sum(e['actual_cents'] for e in income if e['settled'])))
c.metric('Previsto pagar', brl(sum(e['expected_cents'] for e in expenses)))
d.metric('Pago', brl(sum(e['actual_cents'] for e in expenses if e['settled'])))
st.caption('Histórico agrupado pelo vencimento previsto. O valor real é preservado junto da previsão original.')

left, right = st.columns(2)
with left:
    st.subheader('Entradas')
    new_entry('income')
    if not income:
        st.caption('Nenhuma entrada neste mês.')
    for entry in income:
        with st.expander(f"{'✓' if entry['settled'] else '○'} {entry['due_date']:%d/%m} · {entry['description']} · {brl(entry['expected_cents'])}"):
            entry_actions(entry, balance)
with right:
    st.subheader('Contas')
    new_entry('expense')
    if not expenses:
        st.caption('Nenhuma conta neste mês.')
    for entry in expenses:
        with st.expander(f"{'✓' if entry['settled'] else '○'} {entry['due_date']:%d/%m} · {entry['description']} · {brl(entry['expected_cents'])}"):
            entry_actions(entry, balance)

st.subheader('Casamento das contas pendentes')
st.caption('Recalculado ao salvar. Primeiro o disponível de hoje, depois as entradas por data. Recebimentos confirmados ficam fora. Contas anteriores ao dia de hoje já venceram.')
plan = schedule(entries, balance['cents'], TODAY)
if not plan:
    st.info('Nenhuma conta pendente neste mês.')
for item in plan:
    bill = item['entry']
    with st.container(border=True):
        st.markdown(f"**{bill['due_date']:%d/%m} · {bill['description']} · {brl(bill['expected_cents'])}**")
        if item['status'] == 'Coberta no prazo':
            st.success(item['status'])
        else:
            st.warning(item['status'])
        for allocation in item['allocations']:
            suffix = ' · renda ainda não recebida e atrasada' if allocation['overdue_income'] else ''
            st.write(f"{allocation['source']} · {allocation['date']:%d/%m/%Y} · {brl(allocation['cents'])}{suffix}")
        if item['uncovered_cents']:
            st.write(f"Faltam {brl(item['uncovered_cents'])}")

with st.expander('Baixar dados deste mês'):
    stream = io.StringIO()
    columns = ['id', 'kind', 'description', 'due_date', 'expected_cents', 'settled', 'actual_cents', 'settled_date']
    writer = csv.DictWriter(stream, fieldnames=columns, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(entries)
    st.download_button('Baixar CSV', stream.getvalue().encode('utf-8-sig'),
                       file_name=f'fluxo_{year}_{month:02d}.csv', mime='text/csv')
    st.caption('Valores monetários exportados em centavos. O CSV contém somente o mês selecionado.')
