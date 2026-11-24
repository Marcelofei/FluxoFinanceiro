from ui_styles import *
from database import _eh_erro_de_conexao, _fechar_pool_atual, db_connection, execute_query, fetch_dataframe, get_pool, init_db, transaction, ui_transaction
import streamlit as st
import finance
from operations import request_key, insert_shifts
from finance import int_seguro, float_seguro, format_brl, _descricao_exibicao, _valor_operacional, _data_operacional, _fluxo2_texto_cobertura, money, cents, split_total, allocate_amount, today_local
import logging
import time
import hmac
APP_BUILD = "fluxo-integridade-v26"
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from security import validate_schema, verify_password, load_accounts, new_password_hash
from psycopg2.pool import ThreadedConnectionPool
import plotly.express as px
import datetime
import calendar
import uuid
import os
import io
import html
import re

# UX COMPLETA SOBRE BASE FUNCIONAL PRÉ-UX — esta versão mantém as capacidades avançadas da versão pré-UX
# (edição individual e em lote, séries futuras, WhatsApp, conciliação/diagnóstico,
# manutenção histórica, CSV de plantões, plantões semanais e exclusões em lote)
# enquanto aplica a navegação e hierarquia visual revisadas.
import json
import zipfile
from contextlib import contextmanager

# =================================================================
# 1. INFRAESTRUTURA, POOL DE CONEXÕES E TRANSAÇÕES
# =================================================================


















def limites_mes(mes, ano):
    inicio = datetime.date(ano, mes, 1)
    if mes == 12:
        fim = datetime.date(ano + 1, 1, 1)
    else:
        fim = datetime.date(ano, mes + 1, 1)
    return inicio, fim


def limites_ano(ano):
    return datetime.date(ano, 1, 1), datetime.date(ano + 1, 1, 1)


def _migrar_envelopes_legados():
    from migrate import migrate_envelopes
    with transaction() as cur:
        migrate_envelopes(cur)




# =================================================================
# 2. MOTOR DE GERAÇÃO LAZY / RECORRÊNCIAS CONTRATUAIS
# =================================================================

def processar_recorrencias_lazy(mes, ano):
    from recurrences import generate_month
    # The database markers provide idempotency across sessions, not UI cache.
    with transaction() as cur:
        generate_month(cur, mes, ano)


def processar_recorrencias_periodos(periodos):
    from recurrences import generate_months
    with transaction() as cur:
        generate_months(cur, periodos)


# =================================================================
# 4. SISTEMA DE SEGURANÇA E AUXILIARES
# =================================================================

@st.cache_resource
def _login_attempts():
    from threading import Lock
    return {}, Lock()


def check_password():
    try:
        accounts = load_accounts(os.environ.get('APP_USERS_JSON'))
    except ValueError:
        st.error('Configuração de acesso inválida. Contate o administrador.')
        return False
    # Existing private deployments continue to work; named accounts use isolated schemas.
    now = time.time()
    if st.session_state.get('password_correct'):
        account_now = accounts.get(st.session_state.get('actor'), {})
        expired = bool(accounts) and account_now.get('schema') != st.session_state.get('tenant_schema')
        expired |= now - st.session_state.get('last_activity', 0) > 1800
        expired |= now - st.session_state.get('login_time', 0) > 43200
        if not expired:
            st.session_state['last_activity'] = now
            return True
        st.session_state.clear()
        st.warning('Sua sessão expirou. Entre novamente.')
    if not accounts and not os.environ.get('APP_PASSWORD'):
        st.error('Configure o acesso antes de iniciar o app.')
        return False
    st.markdown('### 🔒 Acesso restrito')
    username = st.text_input('Usuário') if accounts else 'owner'
    password = st.text_input('Senha', type='password', key='login_password')
    if st.button('Entrar', type='primary'):
        attempts, lock = _login_attempts()
        with lock:
            recent = [t for t in attempts.get(username, []) if now-t < 300]
            if len(recent) >= 5:
                st.error('Muitas tentativas. Aguarde cinco minutos.')
                return False
            recent.append(now)
            attempts[username] = recent
        account = accounts.get(username, {})
        valid = verify_password(password, account.get('password_hash')) if accounts else hmac.compare_digest(password, os.environ.get('APP_PASSWORD',''))
        if valid:
            with lock: attempts.pop(username, None)
            st.session_state.clear()
            st.session_state.update(password_correct=True, actor=username, tenant_schema=account.get('schema','public'), last_activity=now, login_time=now)
            st.rerun()
        st.error('Usuário ou senha incorretos.')
    return False


def parse_valor(valor_str):
    try:
        return float(finance.parse_currency(valor_str))
    except (ValueError, ArithmeticError) as exc:
        st.error(str(exc))
        st.stop()


def resolver_valor_real(novo_pago, valor_planejado, valor_real_informado):
    """
    Regra do Fluxo: ao marcar como pago/recebido, valor real vazio/zero assume
    automaticamente o planejado. Se houver valor real informado, ele prevalece
    sem alterar o planejamento. Ao estornar, o realizado volta a zero.
    """
    if not bool(novo_pago):
        return 0.0
    planejado = max(float_seguro(valor_planejado), 0.0)
    real = float_seguro(valor_real_informado, 0.0)
    return planejado if valor_real_informado is None or pd.isna(valor_real_informado) else real


def flash(tipo, mensagem):
    """Guarda uma mensagem pra ser exibida DEPOIS do próximo rerun. st.rerun()
    interrompe a execução na hora, então um st.success() chamado bem antes de um
    st.rerun() na mesma linha nunca chega a ser visto na tela."""
    st.session_state['_flash'] = (tipo, mensagem)

def exibir_flash():
    if '_flash' in st.session_state:
        tipo, mensagem = st.session_state.pop('_flash')
        getattr(st, tipo)(mensagem)

# -----------------------------------------------------------------
# FEATURE 6 -- MESES DE SOBREVIVÊNCIA
# -----------------------------------------------------------------




# -----------------------------------------------------------------
# FEATURE 5 -- TRADUTOR DE DÍVIDA EM PLANTÃO
# -----------------------------------------------------------------

def calcular_valor_medio_plantao(hoje_ref, n_meses=6):
    """Valor médio de 1 plantão, com base no seu próprio histórico recente
    (não é um número fixo hardcoded) -- usado pra traduzir dívida em plantões."""
    data_inicio = hoje_ref - datetime.timedelta(days=30 * n_meses)
    df = fetch_dataframe(
        "SELECT valor FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE %s AND COALESCE(data_competencia,data_vencimento) >= %s",
        ('Plantão %', data_inicio)
    )
    if df.empty: return None, 0
    return float(df['valor'].astype(float).mean()), len(df)

# =================================================================
# 5. CONFIGURAÇÃO DA PÁGINA
# =================================================================

st.set_page_config(page_title="Gestão Financeira", layout="wide", page_icon="💰")
if not check_password(): st.stop()


def _banco_disponivel():
    try:
        with db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        # Uma segunda tentativa força pool totalmente novo.
        _fechar_pool_atual()
        try:
            with db_connection(autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True
        except Exception:
            return False


if not _banco_disponivel():
    st.error("Não foi possível conectar ao banco de dados agora. Seus dados não foram alterados.")
    st.caption("Isso costuma acontecer quando o provedor reinicia ou encerra conexões ociosas. Tente reconectar em alguns segundos.")
    if st.button("🔄 Tentar reconectar", type="primary"):
        _fechar_pool_atual()
        st.rerun()
    st.stop()

init_db()

def preferencia_get(chave, padrao=None):
    try:
        dfp = fetch_dataframe('SELECT valor FROM preferencias_app WHERE chave=%s', (chave,), silent=True)
        if dfp is not None and not dfp.empty:
            v = dfp.iloc[0].get('valor')
            if v is not None and str(v).strip() != '':
                return str(v)
    except Exception:
        pass
    return padrao

def preferencia_set(chave, valor):
    execute_query('''
        INSERT INTO preferencias_app (chave, valor, atualizado_em)
        VALUES (%s,%s,NOW())
        ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=NOW()
    ''', (str(chave), None if valor is None else str(valor)))

# =================================================================
# 5B. IDENTIDADE VISUAL
# =================================================================

aplicar_estilo_visual()


# =================================================================
# 6. ESTRUTURAS DINÂMICAS E CONSTANTES
# =================================================================

@st.cache_data(ttl=300, show_spinner=False)
def get_estrutura_dinamica(tenant_schema):
    estrutura = {"Entrada": {}, "Despesa": {}}
    try:
        df_custom = fetch_dataframe("SELECT tipo, categoria, subgrupo FROM categorias_personalizadas")
        if not df_custom.empty:
            for _, row in df_custom.iterrows():
                t, c, s = row['tipo'], row['categoria'], row['subgrupo']
                if t in estrutura:
                    if c not in estrutura[t]: estrutura[t][c] = []
                    if s and s not in estrutura[t][c]: estrutura[t][c].append(s)
    except Exception:
        st.error("Não foi possível carregar suas categorias. Tente novamente.")
        st.stop()
    return estrutura

def invalidar_caches_estruturais():
    """Chamar sempre que categorias forem criadas/editadas/excluídas: limpa o
    cache da estrutura e as guardas de recorrência, pra que uma categoria
    recorrente nova gere o lançamento do mês imediatamente."""
    get_estrutura_dinamica.clear()
    for k in [k for k in list(st.session_state.keys()) if str(k).startswith('rec_processado_')]:
        del st.session_state[k]

ESTRUTURA = get_estrutura_dinamica(st.session_state.get('tenant_schema', 'public'))
hoje = today_local()
meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
prioridades_map = {"Alta 🔴": 0, "Média 🟡": 1, "Baixa 🟢": 2}

# =================================================================
# 7. NAVEGAÇÃO, PERÍODO E HIERARQUIA DE USO
# =================================================================

# CSS complementar de UX: cards, hierarquia de seção e linha operacional.
style_block_565()


# Planejamento 2.0 — leitura rápida de plano x realizado, sem tabelas na visão principal.
style_block_781()

# Refinamento visual 2.0 — aproxima o shell e o Planejamento do mockup aprovado.
style_block_845()


# Rendas 2.0 — visual premium alinhado ao Planejamento 2.0.
style_block_1051()


def _nav_btn(rotulo, key, destino=None, container=None):
    alvo = container if container is not None else st.sidebar
    destino = destino or rotulo
    ativo = st.session_state.menu_atual == destino or (destino == '💰 Rendas' and st.session_state.menu_atual == '🏥 Escala de Plantões')
    if alvo.button(rotulo, key=key, type="primary" if ativo else "secondary", use_container_width=True):
        st.session_state.menu_atual = destino
        st.rerun()


def _mover_periodo(delta):
    mes = int(st.session_state.get('sb_mes', hoje.month)) - 1 + int(delta)
    ano = int(st.session_state.get('sb_ano', hoje.year)) + mes // 12
    mes = mes % 12 + 1
    st.session_state['sb_mes'] = mes
    st.session_state['sb_ano'] = ano


def _periodo_hoje():
    st.session_state['sb_mes'] = hoje.month
    st.session_state['sb_ano'] = hoje.year


def render_periodo_topo(chave):
    """Navegação rápida mês a mês, repetida nas telas que dependem do período."""
    c1, c2, c3, c4 = st.columns([.7, 3.2, .7, 1.2])
    c1.button("‹", key=f"period_prev_{chave}", on_click=_mover_periodo, args=(-1,), use_container_width=True)
    c2.markdown(
        f"<div class='ux-card' style='text-align:center;padding:.55rem .7rem;margin:0;'>"
        f"<b>{meses[mes_selecionado-1]} {ano_selecionado}</b></div>", unsafe_allow_html=True
    )
    c3.button("›", key=f"period_next_{chave}", on_click=_mover_periodo, args=(1,), use_container_width=True)
    c4.button("Hoje", key=f"period_today_{chave}", on_click=_periodo_hoje, use_container_width=True)


def cabecalho_pagina(titulo, subtitulo=None, chave_periodo=None):
    st.header(titulo)
    if subtitulo:
        st.markdown(f"<div class='ux-subtitle'>{subtitulo}</div>", unsafe_allow_html=True)
    if chave_periodo:
        render_periodo_topo(chave_periodo)


def render_kpi(rotulo, valor, nota=None, tom="neutral"):
    classe = {"positive":"ux-positive", "negative":"ux-negative", "accent":"ux-accent"}.get(tom, "")
    nota_html = f"<div class='ux-kpi-note'>{nota}</div>" if nota else ""
    st.markdown(
        f"<div class='ux-kpi'><div class='ux-kpi-label'>{rotulo}</div>"
        f"<div class='ux-kpi-value {classe}'>R$ {format_brl(valor)}</div>{nota_html}</div>",
        unsafe_allow_html=True,
    )


def render_empty_state(titulo, texto, icone="✓"):
    st.markdown(
        f"<div class='ux-empty'><div class='ux-empty-icon'>{icone}</div>"
        f"<b>{titulo}</b><br><span>{texto}</span></div>",
        unsafe_allow_html=True,
    )




def _render_tabela_escura(dataframe, currency_cols=None, numeric_cols=None, status_col=None):
    """Tabela somente-leitura com tema escuro consistente e scroll horizontal no mobile."""
    if dataframe is None or dataframe.empty:
        return
    currency_cols = set(currency_cols or [])
    numeric_cols = set(numeric_cols or []) | currency_cols

    cabecalho = []
    for col in dataframe.columns:
        classe = " class='ux-num'" if col in numeric_cols else ""
        cabecalho.append(f"<th{classe}>{html.escape(str(col))}</th>")

    linhas = []
    for _, row in dataframe.iterrows():
        classe_linha = ""
        if status_col and status_col in dataframe.columns:
            status = str(row.get(status_col, '') or '')
            if '✅' in status or status.startswith('🟢'):
                classe_linha = 'ux-row-paid'
            elif '⏳' in status:
                classe_linha = 'ux-row-pending'
            elif '🧮' in status:
                classe_linha = 'ux-row-budget'
            elif status.startswith('🔴'):
                classe_linha = 'ux-row-danger'
            elif status.startswith('🟡'):
                classe_linha = 'ux-row-warning'

        celulas = []
        for col in dataframe.columns:
            valor = row[col]
            classes = []
            if col in numeric_cols:
                classes.append('ux-num')
            if col == 'Pago/Real':
                classes.append('ux-real-strong')
            if col == 'Planejado':
                classes.append('ux-plan-muted')

            if col in currency_cols:
                texto = f"R$ {format_brl(valor)}"
            elif pd.isna(valor):
                texto = '—'
            else:
                texto = str(valor)
            class_attr = f" class='{' '.join(classes)}'" if classes else ''
            celulas.append(f"<td{class_attr}>{html.escape(texto)}</td>")

        tr_class = f" class='{classe_linha}'" if classe_linha else ''
        linhas.append(f"<tr{tr_class}>{''.join(celulas)}</tr>")

    tabela_html = (
        "<div class='ux-table-wrap'><table class='ux-dark-table'><thead><tr>"
        + ''.join(cabecalho)
        + "</tr></thead><tbody>"
        + ''.join(linhas)
        + "</tbody></table></div>"
    )
    st.markdown(tabela_html, unsafe_allow_html=True)






# Mais 2.0 — configurações organizadas em blocos, visual alinhado ao mockup.
style_block_1225()

st.sidebar.markdown(
    "<div class='brand2'>"
    "<div class='brand2-mark'>∿</div>"
    "<div><div class='brand2-name'>Meu Financeiro</div>"
    "<div class='brand2-sub'>Seu dinheiro, sem ruído.</div></div>"
    "</div>",
    unsafe_allow_html=True,
)
st.sidebar.markdown("<div class='brand2-version'>Versão 2.0 · Beta</div>", unsafe_allow_html=True)
st.sidebar.divider()

if "menu_atual" not in st.session_state:
    _paginas_inicio = {"Início":"🏠 Início", "Fluxo":"📊 Fluxo e Prioridades", "Planejamento":"📑 Demonstrativo", "Rendas":"💰 Rendas"}
    st.session_state.menu_atual = _paginas_inicio.get(preferencia_get('pagina_inicial','Início'), "🏠 Início")

_nav_btn("🏠 Início", "nav_inicio", "🏠 Início")
_nav_btn("📋 Fluxo", "nav_fluxo", "📊 Fluxo e Prioridades")
_nav_btn("💡 Planejamento", "nav_planejamento", "📑 Demonstrativo")
_nav_btn("💰 Rendas", "nav_rendas", "💰 Rendas")
_nav_btn("⚙️ Mais", "nav_mais", "⚙️ Mais")

menu = st.session_state.menu_atual

if "sb_mes" not in st.session_state: st.session_state["sb_mes"] = hoje.month
if "sb_ano" not in st.session_state: st.session_state["sb_ano"] = hoje.year

# No Planejamento 2.0 o período fica no cabeçalho, como no layout de produto.
# Nas demais telas o controle lateral permanece para manter navegação rápida.
if menu not in ("📑 Demonstrativo", "💰 Rendas"):
    st.sidebar.divider()
    st.sidebar.markdown("<div class='nav-eyebrow'>Período</div>", unsafe_allow_html=True)
    p1, p2, p3 = st.sidebar.columns([1, 3, 1])
    p1.button("‹", key="sb_prev", on_click=_mover_periodo, args=(-1,), use_container_width=True)
    p2.markdown(
        f"<div class='sidebar-period'>{meses[int(st.session_state['sb_mes'])-1][:3]} {st.session_state['sb_ano']}</div>",
        unsafe_allow_html=True,
    )
    p3.button("›", key="sb_next", on_click=_mover_periodo, args=(1,), use_container_width=True)
    st.sidebar.button("Ir para o mês atual", key="sb_today", on_click=_periodo_hoje, use_container_width=True)
    with st.sidebar.expander("Escolher outro período"):
        col_sb1, col_sb2 = st.columns(2)
        with col_sb1:
            st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], key="sb_mes")
        with col_sb2:
            st.selectbox("Ano", range(hoje.year-3, hoje.year+6), key="sb_ano")

mes_selecionado = int(st.session_state['sb_mes'])
ano_selecionado = int(st.session_state['sb_ano'])

def _df_raw(tabela):
    return fetch_dataframe(f"/* RAW */ SELECT * FROM {tabela}")


def exportar_backup_completo():
    from backup import export_snapshot
    with db_connection(autocommit=False) as conn:
        return export_snapshot(conn)


def validar_csv_lancamentos(df_imp):
    """Valida integralmente antes de iniciar qualquer restauração destrutiva."""
    problemas = []
    colunas_obrigatorias = ['tipo', 'categoria', 'descricao', 'valor', 'data_vencimento', 'compra_id']
    for col in colunas_obrigatorias:
        if col not in df_imp.columns:
            problemas.append(f"Coluna obrigatória '{col}' não existe no CSV.")
    if problemas:
        return problemas, None

    df_v = df_imp.copy()
    for col, default in [('parcela_atual', 1), ('total_parcelas', 1)]:
        if col not in df_v.columns:
            df_v[col] = default
        else:
            df_v[col] = pd.to_numeric(df_v[col], errors='coerce').fillna(default)

    if 'pago' not in df_v.columns:
        problemas.append("Coluna obrigatória 'pago' não existe no CSV.")
        return problemas, None
    df_v['pago'] = pd.to_numeric(df_v['pago'], errors='coerce').fillna(0)

    LIMITE_INT = 2_147_483_647
    for col in ['parcela_atual', 'total_parcelas', 'pago']:
        fora_do_limite = df_v[df_v[col].abs() > LIMITE_INT]
        for idx, row in fora_do_limite.iterrows():
            problemas.append(f"Linha {idx+2}: coluna '{col}' com valor {row[col]} fora do limite do banco.")

    for col in ['valor', 'valor_pago'] if 'valor_pago' in df_v.columns else ['valor']:
        nums = pd.to_numeric(df_v[col], errors='coerce')
        invalidos = df_v[nums.isna() | ~pd.Series(nums).apply(lambda x: pd.notna(x) and abs(x) != float('inf'))]
        for idx, row in invalidos.iterrows():
            problemas.append(f"Linha {idx+2}: coluna '{col}' com valor '{row[col]}' não é um número válido.")

    datas = pd.to_datetime(df_v['data_vencimento'], errors='coerce')
    for idx in df_v[datas.isna()].index:
        problemas.append(f"Linha {idx+2}: data_vencimento '{df_v.loc[idx, 'data_vencimento']}' inválida.")

    if 'data_competencia' not in df_v.columns:
        df_v['data_competencia'] = df_v['data_vencimento']
    if 'data_pagamento' not in df_v.columns:
        df_v['data_pagamento'] = df_v.apply(
            lambda r: r['data_vencimento'] if int_seguro(r.get('pago')) == 1 else None, axis=1
        )
    if 'eh_orcamento' not in df_v.columns:
        df_v['eh_orcamento'] = 0
    if 'valor_orcamento' not in df_v.columns:
        df_v['valor_orcamento'] = None
    if 'eh_estimativa' not in df_v.columns:
        df_v['eh_estimativa'] = 0

    return problemas, df_v


def _limpar_df_para_banco(df):
    if df is None:
        return None
    return df.astype(object).where(pd.notna(df), None)


def _insert_dataframe(cur, tabela, df, colunas_permitidas, on_conflict=''):
    if df is None or df.empty:
        return
    cols = [c for c in colunas_permitidas if c in df.columns]
    if not cols:
        return
    dados = _limpar_df_para_banco(df[cols])
    registros = [tuple(row[c] for c in cols) for _, row in dados.iterrows()]
    cols_sql = ','.join(cols)
    execute_values(cur, f"INSERT INTO {tabela} ({cols_sql}) VALUES %s {on_conflict}", registros)


def _restaurar_lancamentos_legacy(df_v):
    cols = [
        'compromisso_id','origem_evento','fonte_id','competencia_inferida','excecao_recorrencia','versao','id','tipo','categoria','subgrupo','descricao','valor','data_vencimento',
        'parcela_atual','total_parcelas','pago','compra_id','forma_pagamento',
        'prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento',
        'eh_orcamento','valor_orcamento'
    ]
    with transaction() as cur:
        cur.execute("TRUNCATE TABLE pagamentos, recorrencias_geradas, lancamentos RESTART IDENTITY CASCADE")
        _insert_dataframe(cur, 'lancamentos', df_v, cols)
        from agenda import adopt_legacy
        adopt_legacy(cur)
        from migrate import repair_recurrence_markers, migrate_envelopes
        if 'competencia_inferida' not in df_v.columns:
            cur.execute("UPDATE lancamentos SET competencia_inferida=TRUE WHERE data_competencia IS NULL OR (data_competencia=data_vencimento AND descricao NOT LIKE 'Plantão %%')")
        repair_recurrence_markers(cur)
        migrate_envelopes(cur)
        from recurrences import recover_manual_rules
        cur.execute('TRUNCATE series_manuais_geradas,series_manuais')
        recover_manual_rules(cur)
        cur.execute("SELECT setval(pg_get_serial_sequence('lancamentos','id'), COALESCE((SELECT MAX(id) FROM lancamentos),1), (SELECT COUNT(*)>0 FROM lancamentos))")
    return True


def importar_backup(arquivo):
    try:
        nome = getattr(arquivo, 'name', '').lower()
        if nome.endswith('.csv'):
            # Compatibilidade com backups antigos, que continham somente lançamentos.
            df_imp = pd.read_csv(arquivo)
            if 'forma_pagamento' not in df_imp.columns: df_imp['forma_pagamento'] = 'Outros'
            if 'prioridade' not in df_imp.columns: df_imp['prioridade'] = 'Baixa 🟢'
            if 'valor_pago' not in df_imp.columns: df_imp['valor_pago'] = df_imp['valor']
            problemas, df_v = validar_csv_lancamentos(df_imp)
            if problemas:
                raise ValueError('; '.join(problemas[:10]))
            _restaurar_lancamentos_legacy(df_v)
            return True, "Backup CSV legado restaurado. Categorias/configurações existentes foram preservadas."

        from backup import read_snapshot
        dfs = read_snapshot(arquivo)

        problemas, df_lanc = validar_csv_lancamentos(dfs['lancamentos.csv'])
        if problemas:
            raise ValueError('; '.join(problemas[:10]))

        cols_cat = ['id','tipo','categoria','subgrupo','valor_padrao','atraso_meses','dia_pagamento','is_recorrente','data_inicio','is_envelope','is_producao_variavel','modalidade_renda','data_fim','descricao_padrao','forma_padrao','prioridade_padrao']
        cols_lanc = ['compromisso_id','origem_evento','fonte_id','competencia_inferida','excecao_recorrencia','versao','requisicao_id','ajuste_pagamento_ids','fatura_id','id','tipo','categoria','subgrupo','descricao','valor','data_vencimento','parcela_atual','total_parcelas','pago','compra_id','forma_pagamento','prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento','eh_orcamento','valor_orcamento']
        cols_info = ['compra_id','credor','taxa_juros_mensal']
        cols_reserva = ['id','valor','atualizado_em']
        cols_pag = ['lancamento_id','valor','data_pagamento','origem','criado_em']
        cols_rec = ['categoria_id','competencia','criado_em']
        cols_orc = ['id','competencia','categoria','subgrupo','valor_planejado','origem','criado_em','atualizado_em']
        cols_pref = ['chave','valor','atualizado_em']

        with transaction() as cur:
            cur.execute("TRUNCATE TABLE agenda_eventos, series_manuais_geradas, series_manuais, auditoria, pagamentos, recorrencias_geradas, orcamentos_categorias, preferencias_app, info_dividas, reserva_emergencia, lancamentos, faturas, cartoes, categorias_personalizadas RESTART IDENTITY CASCADE")
            _insert_dataframe(cur, 'categorias_personalizadas', dfs['categorias_personalizadas.csv'], cols_cat)
            _insert_dataframe(cur, 'cartoes', dfs.get('cartoes.csv',pd.DataFrame()), ['id','nome','dia_fechamento','dia_vencimento'])
            _insert_dataframe(cur, 'faturas', dfs.get('faturas.csv',pd.DataFrame()), ['id','cartao_id','vencimento'])
            # Restore audit first, so new events receive fresh IDs.
            aud = dfs.get('auditoria.csv',pd.DataFrame()).copy()
            from psycopg2.extras import Json
            for col in ['anterior','posterior']:
                if col in aud: aud[col]=aud[col].map(lambda v: Json(v) if isinstance(v,dict) else None)
            _insert_dataframe(cur, 'auditoria', aud, ['id','entidade','operacao','anterior','posterior','ator','criado_em'])
            cur.execute("SELECT setval(pg_get_serial_sequence('auditoria','id'), COALESCE((SELECT MAX(id) FROM auditoria),1), (SELECT COUNT(*)>0 FROM auditoria))")
            _insert_dataframe(cur,'agenda_eventos',dfs.get('agenda_eventos.csv'),['id','titulo','inicio','fim','tipo','local','origem','chave_externa','status'])
            _insert_dataframe(cur, 'lancamentos', df_lanc, cols_lanc)
            from agenda import adopt_legacy
            adopt_legacy(cur)
            _insert_dataframe(cur, 'series_manuais', dfs.get('series_manuais.csv'), ['compra_id','tipo','categoria','subgrupo','descricao','valor','inicio','fim','dia','forma_pagamento','prioridade','atraso_meses'])
            _insert_dataframe(cur, 'series_manuais_geradas', dfs.get('series_manuais_geradas.csv'), ['compra_id','competencia'])
            _insert_dataframe(cur, 'info_dividas', dfs['info_dividas.csv'], cols_info, 'ON CONFLICT (compra_id) DO UPDATE SET credor=EXCLUDED.credor, taxa_juros_mensal=EXCLUDED.taxa_juros_mensal')
            _insert_dataframe(cur, 'reserva_emergencia', dfs['reserva_emergencia.csv'], cols_reserva, 'ON CONFLICT (id) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=EXCLUDED.atualizado_em')
            _insert_dataframe(cur, 'pagamentos', dfs['pagamentos.csv'], cols_pag, 'ON CONFLICT (lancamento_id, origem) DO UPDATE SET valor=EXCLUDED.valor, data_pagamento=EXCLUDED.data_pagamento')
            _insert_dataframe(cur, 'recorrencias_geradas', dfs['recorrencias_geradas.csv'], cols_rec, 'ON CONFLICT (categoria_id, competencia) DO NOTHING')
            _insert_dataframe(cur, 'orcamentos_categorias', dfs.get('orcamentos_categorias.csv', pd.DataFrame()), cols_orc, 'ON CONFLICT DO NOTHING')
            _insert_dataframe(cur, 'preferencias_app', dfs.get('preferencias_app.csv', pd.DataFrame()), cols_pref, 'ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=EXCLUDED.atualizado_em')
            cur.execute("INSERT INTO reserva_emergencia (id,valor,atualizado_em) VALUES (1,0,CURRENT_DATE) ON CONFLICT DO NOTHING")
            cur.execute("SELECT setval(pg_get_serial_sequence('categorias_personalizadas','id'), COALESCE((SELECT MAX(id) FROM categorias_personalizadas),1), (SELECT COUNT(*)>0 FROM categorias_personalizadas))")
            cur.execute("SELECT setval(pg_get_serial_sequence('lancamentos','id'), COALESCE((SELECT MAX(id) FROM lancamentos),1), (SELECT COUNT(*)>0 FROM lancamentos))")
            cur.execute("SELECT setval(pg_get_serial_sequence('pagamentos','id'), COALESCE((SELECT MAX(id) FROM pagamentos),1), (SELECT COUNT(*)>0 FROM pagamentos))")
            cur.execute("SELECT setval(pg_get_serial_sequence('orcamentos_categorias','id'), COALESCE((SELECT MAX(id) FROM orcamentos_categorias),1), (SELECT COUNT(*)>0 FROM orcamentos_categorias))")
            for table in ['cartoes','faturas']:
                cur.execute(sql.SQL("SELECT setval(pg_get_serial_sequence(%s,'id'), COALESCE((SELECT MAX(id) FROM {}),1), (SELECT COUNT(*)>0 FROM {}))").format(sql.Identifier(table),sql.Identifier(table)),(table,))
            from migrate import migrate_envelopes, repair_recurrence_markers
            migrate_envelopes(cur)
            if 'competencia_inferida' not in df_lanc.columns:
                cur.execute("UPDATE lancamentos SET competencia_inferida=TRUE WHERE data_competencia IS NULL OR (data_competencia=data_vencimento AND descricao NOT LIKE 'Plantão %%')")
            cur.execute("UPDATE lancamentos l SET fonte_id=c.id FROM categorias_personalizadas c WHERE l.compra_id='rec_'||c.id::text AND l.fonte_id IS NULL")
            repair_recurrence_markers(cur)
            from recurrences import recover_manual_rules
            recover_manual_rules(cur)

        return True, "Backup completo restaurado de forma atômica."
    except Exception as e:
        return False, str(e)


# A interface de backup foi movida para a página dedicada em Configurações.

periodos_recorrencia = [pd.Timestamp(ano_selecionado, mes_selecionado, 1) + pd.DateOffset(months=i) for i in range(3)]
processar_recorrencias_periodos([(p.month, p.year) for p in periodos_recorrencia])
dia_maximo_alvo = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
data_contexto_ativo = datetime.date(ano_selecionado, mes_selecionado, min(hoje.day, dia_maximo_alvo))
inicio_periodo, fim_periodo = limites_mes(mes_selecionado, ano_selecionado)

exibir_flash()

# =================================================================
# 7B. ONBOARDING 2.0 — 3 PASSOS, PROGRESSIVO E SEM BUROCRACIA
# =================================================================
if 'wizard_ativo' not in st.session_state:
    try:
        df_check_categorias = fetch_dataframe(
            "SELECT COUNT(*) as n FROM categorias_personalizadas",
            silent=True,
            raise_on_error=True,
        )
        if df_check_categorias.empty or 'n' not in df_check_categorias.columns:
            raise RuntimeError("Não foi possível confirmar o cadastro de categorias.")
        n_categorias_existentes = int(df_check_categorias.iloc[0]['n'])
    except Exception:
        st.error("Não foi possível verificar sua configuração porque o banco ficou indisponível. O onboarding não será aberto automaticamente.")
        if st.button("🔄 Reconectar ao banco", key="retry_onboarding_db"):
            _fechar_pool_atual()
            st.rerun()
        st.stop()
    st.session_state['wizard_ativo'] = (n_categorias_existentes == 0 and preferencia_get('onboarding_concluido') != '1')
    st.session_state['wizard_passo'] = 1

if 'wizard_rendas' not in st.session_state:
    st.session_state['wizard_rendas'] = []
    for h in list(st.session_state.get('wizard_hospitais') or []):
        st.session_state['wizard_rendas'].append({
            'modelo': 'Hospital', 'nome': str(h.get('nome') or '').strip(), 'valor': 0.0,
            'dia_recebimento': int_seguro(h.get('dia_pagamento'), 10),
            'modalidade': 'Variável', 'plantoes': True,
        })
if 'wizard_contas' not in st.session_state:
    st.session_state['wizard_contas'] = []
    for f in list(st.session_state.get('wizard_fixas') or []):
        st.session_state['wizard_contas'].append({
            'tipo_conta': 'Outro', 'nome': str(f.get('nome') or '').strip(),
            'valor': float_seguro(f.get('valor')), 'dia_vencimento': int_seguro(f.get('dia_vencimento'), 5),
        })
if 'wizard_orcamentos' not in st.session_state:
    st.session_state['wizard_orcamentos'] = list(st.session_state.get('wizard_envelopes') or [])

_WIZ_RENDA_MODELOS = ['Salário', 'Consultório', 'Hospital', 'Comissão', 'Freelance', 'Aluguel recebido', 'Outro']
_WIZ_CONTAS_MODELOS = ['Moradia', 'Energia', 'Internet', 'Escola', 'Plano de saúde', 'Financiamento', 'Outro']
_WIZ_ORCAMENTOS_MODELOS = ['Mercado', 'Lazer', 'Transporte', 'Farmácia', 'Cuidados pessoais', 'Pets', 'Outro']


def _wizard_css_v2():
    st.markdown(r'''
    <style>
    .onb-hero { margin:.15rem 0 1.35rem 0; }
    .onb-title { font-size:clamp(2rem,3.5vw,3.05rem); font-weight:800; letter-spacing:-.045em; line-height:1.02; color:#f5f8fb; }
    .onb-sub { margin-top:.45rem; color:#9aaabc; font-size:1.02rem; }
    .onb-steps { display:grid; grid-template-columns:1fr 1fr 1fr; gap:.65rem; margin:.55rem 0 1.15rem 0; }
    .onb-step { display:flex; gap:.7rem; align-items:center; border-bottom:2px solid #263747; padding:.3rem .15rem .7rem; color:#75879a; }
    .onb-step.active { color:#f5f8fb; border-color:#16d8cf; }
    .onb-step.done { color:#96b7b5; border-color:#287f7b; }
    .onb-step-num { width:2.15rem; height:2.15rem; border-radius:50%; border:1px solid #41566a; display:flex; align-items:center; justify-content:center; font-weight:800; flex:none; }
    .onb-step.active .onb-step-num { background:rgba(22,216,207,.13); border-color:#16d8cf; color:#38eee3; box-shadow:0 0 0 4px rgba(22,216,207,.06); }
    .onb-step.done .onb-step-num { background:rgba(22,216,207,.08); border-color:#287f7b; }
    .onb-step-label { font-weight:750; font-size:.98rem; line-height:1.1; }
    .onb-step-note { font-size:.78rem; color:#718497; margin-top:.18rem; }
    .onb-card-title { font-size:1.55rem; font-weight:800; letter-spacing:-.025em; color:#f4f7fb; }
    .onb-card-sub { color:#91a4b7; margin:.2rem 0 .9rem; }
    .onb-help { background:linear-gradient(150deg,rgba(12,35,49,.96),rgba(9,23,34,.95)); border:1px solid rgba(67,106,131,.35); border-radius:18px; padding:1.1rem 1.15rem; margin-bottom:.75rem; }
    .onb-help-title { font-weight:800; font-size:1.08rem; color:#eef7fb; margin-bottom:.7rem; }
    .onb-benefit { display:flex; gap:.7rem; padding:.72rem 0; border-top:1px solid rgba(92,119,137,.18); }
    .onb-benefit:first-of-type { border-top:0; }
    .onb-benefit-ico { width:2.15rem;height:2.15rem;border-radius:50%;display:flex;align-items:center;justify-content:center;background:rgba(18,207,194,.12);color:#35e1d7;font-size:1.05rem;flex:none; }
    .onb-benefit b { color:#edf5f8; font-size:.9rem; }
    .onb-benefit span { color:#8295a8; font-size:.78rem; display:block; margin-top:.1rem; }
    .onb-pending { background:#0b151e;border:1px solid rgba(83,111,129,.33);border-radius:14px;padding:.65rem .8rem;margin:.35rem 0;display:flex;justify-content:space-between;gap:.8rem;align-items:center; }
    .onb-pending-name { color:#ecf3f7;font-weight:700; }
    .onb-pending-meta { color:#7e91a3;font-size:.78rem;margin-top:.12rem; }
    .onb-footer-note { text-align:center;color:#6f8396;font-size:.78rem;padding-top:.5rem; }
    .onb-finish { text-align:center;padding:1.5rem .8rem; }
    .onb-finish-icon { width:4rem;height:4rem;margin:0 auto .8rem;border-radius:50%;display:flex;align-items:center;justify-content:center;background:rgba(22,216,207,.14);border:1px solid rgba(22,216,207,.38);font-size:1.65rem; }
    .onb-side-active { margin:.65rem 0 .45rem;padding:.7rem .75rem;border-radius:12px;background:linear-gradient(110deg,rgba(18,207,194,.2),rgba(13,81,91,.32));border:1px solid rgba(22,216,207,.32);color:#eafcfa;font-weight:800; }
    .onb-side-note { color:#73889b;font-size:.78rem;line-height:1.45;padding:.1rem .2rem; }
    /* Durante o onboarding a navegação normal sai de cena: menos escolhas, menos ruído. */
    section[data-testid="stSidebar"] div[data-testid="stButton"],
    section[data-testid="stSidebar"] details,
    section[data-testid="stSidebar"] .nav-eyebrow,
    section[data-testid="stSidebar"] .sidebar-period { display:none !important; }
    div[data-testid="stForm"] { border:1px solid rgba(80,109,128,.34) !important; border-radius:18px !important; padding:1rem 1rem .35rem !important; background:linear-gradient(160deg,rgba(15,28,39,.9),rgba(10,21,31,.82)) !important; }
    @media (max-width: 760px) {
      .onb-steps { grid-template-columns:1fr; gap:.1rem; }
      .onb-step { padding:.35rem .1rem; border-bottom:0; }
      .onb-step:not(.active) .onb-step-note { display:none; }
      .onb-title { font-size:2rem; }
    }
    </style>
    ''', unsafe_allow_html=True)


def _wizard_progress_v2(passo):
    passos = [(1,'Rendas','De onde vem seu dinheiro'),(2,'Contas','O que você precisa pagar'),(3,'Orçamentos','Defina seus limites')]
    blocos=[]
    for n,titulo,nota in passos:
        cls='active' if n==passo else ('done' if n<passo else '')
        blocos.append(f"<div class='onb-step {cls}'><div class='onb-step-num'>{'✓' if n<passo else n}</div><div><div class='onb-step-label'>{titulo}</div><div class='onb-step-note'>{nota}</div></div></div>")
    st.markdown("<div class='onb-steps'>"+''.join(blocos)+"</div>",unsafe_allow_html=True)


def _wizard_help_v2(passo):
    if passo==1:
        titulo='Como isso vai ajudar'; itens=[('▣','Organiza entradas por data','Você vê exatamente quando o dinheiro deve entrar.'),('↔','Relaciona contas com rendas','O app mostra se suas contas estão cobertas pelas entradas.'),('◫','Antecipa conflitos','Você vê o que vence antes da próxima entrada.')]
    elif passo==2:
        titulo='Comece pelo que pesa'; itens=[('⌂','Cadastre só as principais contas','Você pode completar despesas menores depois.'),('◷','Informe o vencimento','Isso permite organizar cada conta com a renda certa.'),('↻','O app repete por você','Contas mensais são criadas automaticamente nos próximos meses.')]
    else:
        titulo='Planeje sem complicar'; itens=[('◎','Orçamento não é uma conta','Ele serve como referência para acompanhar seus gastos.'),('↗','Realizado atualiza sozinho','Conforme você paga despesas, o uso da categoria aumenta.'),('✓','Esta etapa é opcional','Você pode começar sem definir nenhum orçamento.')]
    itens_html=''.join(f"<div class='onb-benefit'><div class='onb-benefit-ico'>{ico}</div><div><b>{html.escape(t)}</b><span>{html.escape(n)}</span></div></div>" for ico,t,n in itens)
    st.markdown(f"<div class='onb-help'><div class='onb-help-title'>{titulo}</div>{itens_html}</div>",unsafe_allow_html=True)


def _wizard_lista_v2(lista,chave,render):
    if not lista:
        st.caption('Nada adicionado ainda. Você pode continuar e completar depois.')
        return
    for i,item in enumerate(list(lista)):
        c1,c2=st.columns([8,1])
        nome,meta=render(item)
        c1.markdown(f"<div class='onb-pending'><div><div class='onb-pending-name'>{html.escape(nome)}</div><div class='onb-pending-meta'>{html.escape(meta)}</div></div></div>",unsafe_allow_html=True)
        if c2.button('×',key=f'{chave}_remove_{i}',help='Remover',use_container_width=True):
            st.session_state[chave].pop(i); st.rerun()


def _wizard_categoria_conta(tipo_conta):
    if tipo_conta in ('Moradia','Energia','Escola','Plano de saúde'): return 'Despesas Essenciais'
    return 'Despesas Recorrentes'


def _wizard_salvar_v2():
    """Persiste somente o que o usuário informou; nenhuma etapa é obrigatória."""
    rendas=list(st.session_state.get('wizard_rendas') or [])
    contas=list(st.session_state.get('wizard_contas') or [])
    orcamentos=list(st.session_state.get('wizard_orcamentos') or [])
    hoje_w=today_local(); competencia=datetime.date(hoje_w.year,hoje_w.month,1)
    try:
        with transaction() as cur:
            for r in rendas:
                modalidade=str(r.get('modalidade') or 'Variável'); valor=max(float_seguro(r.get('valor')),0.0); especial=bool(r.get('plantoes'))
                recorrente=1 if modalidade in ('Mensal','Variável') and valor>0 and not especial else 0
                cur.execute('''INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio,is_producao_variavel,modalidade_renda) VALUES ('Entrada','Rendas',%s,%s,0,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(r['nome'],valor if valor>0 else None,int(r['dia_recebimento']),recorrente,competencia,1 if especial else 0,modalidade))
                if not recorrente and valor > 0:
                    data_previsao = competencia.replace(day=min(int(r['dia_recebimento']),calendar.monthrange(competencia.year,competencia.month)[1]))
                    cur.execute("SELECT 1 FROM lancamentos WHERE tipo='Entrada' AND categoria='Rendas' AND subgrupo=%s AND data_vencimento=%s", (r['nome'],data_previsao))
                    if not cur.fetchone():
                        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,data_competencia,pago,valor_pago,forma_pagamento,compra_id,eh_estimativa)
                            VALUES ('Entrada','Rendas',%s,%s,%s,%s,%s,0,0,'Outros',%s,1)""",(r['nome'],r['nome']+' · previsão inicial',valor,data_previsao,competencia,'onboarding_'+str(uuid.uuid4())))

            for c in contas:
                cat=_wizard_categoria_conta(c.get('tipo_conta')); valor=max(float_seguro(c.get('valor')),0.0)
                cur.execute('''INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio,is_producao_variavel) VALUES ('Despesa',%s,%s,%s,0,%s,1,%s,0) ON CONFLICT DO NOTHING''',(cat,c['nome'],valor,int(c['dia_vencimento']),competencia))
            for o in orcamentos:
                nome=str(o.get('nome') or '').strip(); valor=max(float_seguro(o.get('valor')),0.0)
                if not nome or valor<=0.004: continue
                cat='Despesas Variáveis'
                cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,is_recorrente,data_inicio,is_producao_variavel) VALUES ('Despesa',%s,%s,0,%s,0) ON CONFLICT DO NOTHING",(cat,nome,competencia))
                cur.execute("UPDATE orcamentos_categorias SET valor_planejado=%s,origem='onboarding',atualizado_em=NOW() WHERE competencia=%s AND categoria=%s AND COALESCE(subgrupo,'')=%s",(valor,competencia,cat,nome))
                if cur.rowcount==0:
                    cur.execute("INSERT INTO orcamentos_categorias (competencia,categoria,subgrupo,valor_planejado,origem) VALUES (%s,%s,%s,%s,'onboarding')",(competencia,cat,nome,valor))
    except Exception as e:
        st.error(f'Não foi possível concluir a configuração. Nada foi salvo parcialmente: {e}'); return False
    invalidar_caches_estruturais()
    for chave in list(st.session_state.keys()):
        if str(chave).startswith('rec_processado_'): st.session_state.pop(chave,None)
    return True


def _wizard_encerrar_v2(mensagem='Tudo pronto. Seu mês já pode começar a ser organizado.'):
    if _wizard_salvar_v2():
        for k in ['wizard_rendas','wizard_contas','wizard_orcamentos','wizard_hospitais','wizard_fixas','wizard_dividas','wizard_envelopes']:
            if k in st.session_state: st.session_state[k]=[]
        st.session_state['wizard_ativo']=False; st.session_state['wizard_passo']=1; st.session_state['menu_atual']='🏠 Início'
        preferencia_set('onboarding_concluido','1')
        flash('success',f'✓ {mensagem}'); st.rerun()


def _wizard_nav_v2(passo,final=False):
    st.markdown("<div class='onb-footer-note'>Você pode completar ou alterar tudo depois.</div>",unsafe_allow_html=True)
    a,b,c=st.columns([1.15,1,1.45])
    if passo>1:
        if a.button('← Voltar',key=f'onb_back_{passo}',use_container_width=True): st.session_state['wizard_passo']=passo-1; st.rerun()
    else:
        if a.button('Pular por enquanto',key='onb_skip_all',use_container_width=True): _wizard_encerrar_v2('Configuração inicial encerrada. Você pode completar os dados a qualquer momento.')
    if passo>1:
        if b.button('Pular esta etapa',key=f'onb_skip_step_{passo}',use_container_width=True):
            if final: _wizard_encerrar_v2()
            else: st.session_state['wizard_passo']=passo+1; st.rerun()
    if c.button('Concluir →' if final else 'Continuar →',type='primary',key=f'onb_next_{passo}',use_container_width=True):
        if final: _wizard_encerrar_v2()
        else: st.session_state['wizard_passo']=passo+1; st.rerun()


def _wizard_passo1_rendas_v2():
    st.markdown("<div class='onb-card-title'>De onde vem seu dinheiro?</div><div class='onb-card-sub'>Adicione suas principais fontes. Uma só já é suficiente para começar.</div>",unsafe_allow_html=True)
    with st.form('onb_income_form',clear_on_submit=True):
        modelo=st.radio('Exemplo de fonte',_WIZ_RENDA_MODELOS,horizontal=True,key='onb_income_model')
        c1,c2=st.columns([1.55,1]); nome=c1.text_input('Nome da fonte',placeholder='Ex.: Hospital Help, Salário principal'); valor=c2.number_input('Valor esperado',min_value=0.0,step=100.0,format='%.2f')
        c3,c4=st.columns([1,1.45]); dia=c3.number_input('Dia aproximado do recebimento',min_value=1,max_value=31,value=10)
        sugestao={'Salário':'Mensal','Aluguel recebido':'Mensal','Comissão':'Variável','Consultório':'Variável','Hospital':'Variável','Freelance':'Eventual','Outro':'Variável'}.get(modelo,'Variável'); tipos=['Mensal','Variável','Eventual']
        modalidade=c4.selectbox('Tipo da renda',tipos,index=tipos.index(sugestao),help='Mensal é mais previsível; Variável se repete mas oscila; Eventual não tem recorrência confiável.')
        plantoes=st.checkbox('Usa plantões nessa fonte?',value=(modelo=='Hospital'),help='Ativa escala, produção e previsão de pagamento. O tipo da renda continua Mensal, Variável ou Eventual.')
        if st.form_submit_button('＋ Adicionar fonte',type='primary',use_container_width=True):
            nome_final=(nome.strip() or (modelo if modelo!='Outro' else ''))
            if not nome_final: st.error('Informe um nome para a fonte.')
            else:
                st.session_state['wizard_rendas'].append({'modelo':modelo,'nome':nome_final,'valor':float(valor),'dia_recebimento':int(dia),'modalidade':modalidade,'plantoes':bool(plantoes)}); st.rerun()
    _wizard_lista_v2(st.session_state['wizard_rendas'],'wizard_rendas',lambda r:(r['nome'],f"{r['modalidade']} · dia {r['dia_recebimento']} · R$ {format_brl(r['valor'])}"+(' · Plantões' if r.get('plantoes') else '')))
    _wizard_nav_v2(1)


def _wizard_passo2_contas_v2():
    st.markdown("<div class='onb-card-title'>Quais contas mais pesam no seu mês?</div><div class='onb-card-sub'>Cadastre apenas as principais. O restante pode ser adicionado aos poucos.</div>",unsafe_allow_html=True)
    with st.form('onb_bills_form',clear_on_submit=True):
        modelo=st.selectbox('Tipo de conta',_WIZ_CONTAS_MODELOS)
        c1,c2,c3=st.columns([1.6,1,1]); nome=c1.text_input('Nome da conta',placeholder='Ex.: Aluguel, Escola das crianças'); valor=c2.number_input('Valor aproximado',min_value=0.0,step=50.0,format='%.2f'); dia=c3.number_input('Vence dia',min_value=1,max_value=31,value=5)
        if st.form_submit_button('＋ Adicionar conta',type='primary',use_container_width=True):
            nome_final=(nome.strip() or (modelo if modelo!='Outro' else ''))
            if not nome_final or valor<=0: st.error('Informe o nome e um valor aproximado.')
            else:
                st.session_state['wizard_contas'].append({'tipo_conta':modelo,'nome':nome_final,'valor':float(valor),'dia_vencimento':int(dia)}); st.rerun()
    _wizard_lista_v2(st.session_state['wizard_contas'],'wizard_contas',lambda c:(c['nome'],f"R$ {format_brl(c['valor'])} · vence dia {c['dia_vencimento']} · mensal"))
    _wizard_nav_v2(2)


def _wizard_passo3_orcamentos_v2():
    st.markdown("<div class='onb-card-title'>Quer planejar alguns gastos?</div><div class='onb-card-sub'>Opcional. Defina apenas categorias que você realmente quer acompanhar de perto.</div>",unsafe_allow_html=True)
    with st.form('onb_budget_form',clear_on_submit=True):
        modelo=st.selectbox('Categoria',_WIZ_ORCAMENTOS_MODELOS)
        c1,c2=st.columns([1.6,1]); nome=c1.text_input('Nome do orçamento',placeholder='Ex.: Mercado, Lazer'); valor=c2.number_input('Orçamento do mês',min_value=0.0,step=50.0,format='%.2f')
        if st.form_submit_button('＋ Adicionar orçamento',type='primary',use_container_width=True):
            nome_final=(nome.strip() or (modelo if modelo!='Outro' else ''))
            if not nome_final or valor<=0: st.error('Informe uma categoria e um valor.')
            else:
                st.session_state['wizard_orcamentos'].append({'nome':nome_final,'valor':float(valor)}); st.rerun()
    _wizard_lista_v2(st.session_state['wizard_orcamentos'],'wizard_orcamentos',lambda o:(o['nome'],f"R$ {format_brl(o['valor'])} planejados para este mês"))
    st.markdown("<div class='onb-finish'><div class='onb-finish-icon'>✓</div><div class='onb-card-title' style='font-size:1.2rem'>Você já informou o essencial</div><div class='onb-card-sub'>Ao concluir, o app cria suas previsões e mostra as primeiras contas. Para fontes com plantões, substitua a previsão inicial ao cadastrar a produção.</div></div>",unsafe_allow_html=True)
    _wizard_nav_v2(3,final=True)


def renderizar_wizard_configuracao():
    _wizard_css_v2(); passo=max(1,min(int(st.session_state.get('wizard_passo',1)),3))
    st.sidebar.markdown("<div class='onb-side-active'>✦ Onboarding</div><div class='onb-side-note'>3 passos rápidos para o app entender suas rendas, contas e planejamento.</div>",unsafe_allow_html=True)
    st.markdown("<div class='onb-hero'><div class='onb-title'>Vamos organizar sua vida financeira</div><div class='onb-sub'>Em 3 passos rápidos o app já consegue organizar o seu mês.</div></div>",unsafe_allow_html=True)
    _wizard_progress_v2(passo)
    principal,ajuda=st.columns([2.05,1],gap='large')
    with principal:
        if passo==1: _wizard_passo1_rendas_v2()
        elif passo==2: _wizard_passo2_contas_v2()
        else: _wizard_passo3_orcamentos_v2()
    with ajuda: _wizard_help_v2(passo)

# =================================================================
# 8+. INTERFACE UX — USO DIÁRIO, ANÁLISE E CONFIGURAÇÕES
# =================================================================




def _sub_norm(v):
    return '' if pd.isna(v) else str(v).strip()


def _total_despesa_projetada(df):
    """Projeção baseada somente em despesas reais/previstas cadastradas."""
    if df is None or df.empty:
        return 0.0
    d = df[df['tipo'] == 'Despesa'].copy()
    if d.empty:
        return 0.0
    d['valor'] = pd.to_numeric(d['valor'], errors='coerce').fillna(0.0)
    d['valor_pago'] = pd.to_numeric(d['valor_pago'], errors='coerce').fillna(0.0)
    d['pago'] = pd.to_numeric(d['pago'], errors='coerce').fillna(0).astype(int)
    return float(d.apply(lambda r: float(r['valor_pago']) if int_seguro(r.get('pago')) == 1 else float(r['valor']), axis=1).sum())







def _inserir_plantoes(registros):
    with transaction() as cur:
        return insert_shifts(cur, registros)


def _referencia_cobertura():
    st.caption(f"Situação em {hoje:%d/%m/%Y} · Realizado: baixas confirmadas. Previsão: entradas e contas ainda pendentes.")
    st.caption("Considera as rendas e contas cadastradas; não representa seu saldo bancário.")


def _historico_baixas():
    with st.expander('Histórico de pagamentos e recebimentos'):
        df = fetch_dataframe("""SELECT criado_em AS data, ator,
            CASE WHEN anterior->>'pago'='1' AND COALESCE(posterior->>'pago','0')<>'1'
                 THEN 'Desfeito' WHEN posterior->>'pago'='1' THEN 'Confirmado' ELSE 'Alterado' END AS acao,
            COALESCE(posterior->>'tipo',anterior->>'tipo') AS tipo,
            COALESCE(posterior->>'descricao',anterior->>'descricao') AS descricao,
            anterior->>'valor_pago' AS valor_anterior, posterior->>'valor_pago' AS valor_atual
            FROM auditoria WHERE entidade='lancamentos'
            AND (anterior->>'pago'='1' OR posterior->>'pago'='1')
            ORDER BY criado_em DESC,id DESC LIMIT 100""")
        if df.empty: st.caption('Nenhuma baixa registrada no histórico.')
        else: st.dataframe(df, hide_index=True, use_container_width=True)
        st.caption('Últimas 100 alterações. Desfazer mantém o registro original no histórico.')


def _marcar_ids(ids, pago=True, data_pagamento=None):
    from payments import settle, reverse
    with transaction() as cur:
        if pago: settle(cur, ids, None, data_pagamento or hoje)
        else: reverse(cur, ids)

def _registrar_pagamento_ids(ids, valor_real_total=None, data_pagamento=None):
    from payments import settle
    with transaction() as cur:
        return settle(cur, ids, valor_real_total, data_pagamento or hoje)

def _estado_reorganizacao():
    key = f'cobertura_reorganizacao:{ano_selecionado:04d}-{mes_selecionado:02d}'
    df = fetch_dataframe("SELECT valor FROM preferencias_app WHERE chave=%s", (key,))
    return json.loads(df.iloc[0]['valor']) if not df.empty else {'ids':[]}


def _render_reorganizar_contas():
    from operations import reorganize_coverage
    state = _estado_reorganizacao()
    st.caption(f'Reorganizar apenas {mes_selecionado:02d}/{ano_selecionado}: desconsidera automaticamente as rendas do mês já marcadas como recebidas.')
    if st.button('Reorganizar contas pendentes', key='reorganizar_contas'):
        with transaction() as cur:
            reorganize_coverage(cur, ano_selecionado, mes_selecionado)
        flash('success','Contas do mês reorganizadas. Recebimentos preservados no histórico.')
        st.rerun()
    if state.get('ativo'):
        st.caption('Reorganização ativa apenas neste mês, pela data de vencimento. Outros meses não participam deste casamento.')
    if 'anterior' in state and st.button('Desfazer reorganização',key='desfazer_reorganizacao'):
        with transaction() as cur:
            reorganize_coverage(cur, ano_selecionado, mes_selecionado, undo=True)
        st.rerun()


def _consolidar_operacional(df, consolidar_cartao=False):
    state = _estado_reorganizacao()
    base = finance.coverage_month_scope(df, ano_selecionado, mes_selecionado, state)
    return finance._consolidar_operacional(base, consolidar_cartao=consolidar_cartao, hoje=hoje)









def _montar_plano_pagamentos(df_ops, ano, mes):
    return finance._montar_plano_pagamentos(df_ops, ano, mes, hoje=hoje)








def _fluxo2_resumo_proxima_renda(plano, ano, mes):
    return finance._fluxo2_resumo_proxima_renda(plano, ano, mes, hoje=hoje)


def _render_fluxo2_ponte(plano, ano, mes):
    _referencia_cobertura()
    resumo = _fluxo2_resumo_proxima_renda(plano, ano, mes)
    if not resumo:
        st.markdown(
            "<div class='flow2-bridge'><div class='flow2-bridge-grid'>"
            "<div><div class='flow2-bridge-label'>Próxima renda</div><div class='flow2-bridge-name'>Nenhuma renda futura neste período</div>"
            "<div class='flow2-bridge-meta'>O Fluxo continua mostrando as contas e recebimentos cadastrados.</div></div>"
            "<div></div><div><span class='flow2-pill ok'>Sem próxima renda</span></div></div></div>",
            unsafe_allow_html=True,
        )
        return

    fonte, total, risco = resumo['fonte'], resumo['total'], resumo['risco']
    if risco > 0.004:
        cls, pill_cls = 'danger', 'danger'
        status = f"Faltam R$ {format_brl(risco)}"
    else:
        cls, pill_cls = '', 'ok'
        status = '◷ Cobertura prevista' if resumo.get('prevista') else '✓ Recebimentos confirmados'
        pill_cls = 'warn' if resumo.get('prevista') else 'ok'
    st.markdown(
        f"<div class='flow2-bridge {cls}'><div class='flow2-bridge-grid'>"
        f"<div><div class='flow2-bridge-label'>Próxima renda</div>"
        f"<div class='flow2-bridge-name'>{html.escape(str(fonte['descricao']))}</div>"
        f"<div class='flow2-bridge-meta'>{fonte['data'].strftime('%d/%m')} · R$ {format_brl(fonte['valor'])}</div></div>"
        f"<div><div class='flow2-bridge-label'>Até lá vencem</div>"
        f"<div class='flow2-bridge-value'>R$ {format_brl(total)}</div>"
        f"<div class='flow2-bridge-meta'>{len(resumo['contas'])} conta(s)</div></div>"
        f"<div><span class='flow2-pill {pill_cls}'>{status}</span></div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )


def _fluxo2_pagar_lote_planejado(linhas, data_pagamento):
    from payments import settle
    with transaction() as cur:
        for r in linhas:
            settle(cur, r['ids'], None, data_pagamento)


def _fatura_detalhes(r, prefixo, controle=False):
    if not str(r.get('id_ui', '')).startswith('cartao_'): return
    key = f"{prefixo}_fatura_itens_{r['id_ui']}"
    if controle:
        st.toggle(f"Ver composição da fatura · {len(r['ids'])} lançamento(s)", key=key)
        return
    if not st.session_state.get(key): return
    ids = [int(i) for i in r['ids']]
    itens = fetch_dataframe("""SELECT id,descricao,categoria,subgrupo,valor,valor_pago,pago,
        parcela_atual,total_parcelas,data_competencia,data_vencimento,data_pagamento
        FROM lancamentos WHERE id=ANY(%s) ORDER BY data_competencia,id""", (ids,))
    with st.container(border=True):
        st.markdown(f"**Composição — {r['descricao']}**")
        st.caption('Somente os lançamentos que compõem esta linha da fatura. Outras parcelas e valores já baixados em outra linha não entram neste total.')
        if 'não identificado' in str(r['descricao']).lower():
            st.info('Estes lançamentos estão no crédito, mas ainda não têm um cartão identificado no cadastro.')
        if itens.empty:
            st.warning('Os lançamentos foram alterados. Atualize a página para conferir a fatura.')
            return
        linhas=[]
        total=money(0)
        for _, item in itens.iterrows():
            valor=money(_valor_operacional(item)); total+=valor
            n=int_seguro(item.get('total_parcelas'))
            parcela=f"{int_seguro(item.get('parcela_atual'))}/{n}" if 1<n<999 else 'À vista'
            data=pd.to_datetime(item.get('data_competencia'),errors='coerce')
            linhas.append({'Descrição':item['descricao'], 'Categoria':item['categoria'],
                'Referência':data.strftime('%d/%m/%Y') if pd.notna(data) else 'Não informada',
                'Parcela':parcela, 'Status':'Pago' if int_seguro(item['pago']) else 'Pendente',
                'Valor nesta fatura':f'R$ {format_brl(valor)}'})
        st.dataframe(pd.DataFrame(linhas),hide_index=True,use_container_width=True)
        st.markdown(f"**Total dos lançamentos: R$ {format_brl(total)}**")
        if abs(total-money(_valor_operacional(r)))>money('0.01'):
            st.warning('O total mudou desde a exibição da fatura. Atualize a página antes de pagar.')


def _render_fluxo2_timeline(df_visivel, df_todos, plano, prefixo='fluxo2'):
    if df_visivel.empty:
        render_empty_state("Nada por aqui", "Não há lançamentos que correspondam a este filtro.", "○")
        return

    contas_map = {str(c['id']): c for c in plano.get('contas', [])}
    fontes_map = {str(f['id']): f for f in plano.get('fontes', [])}

    # Barra de seleção: o total é uma simulação. Pagamento em lote só é oferecido
    # quando tudo selecionado são despesas pendentes.
    selecionados = []
    for _, r in df_todos.iterrows():
        key = f"{prefixo}_sel_{r['id_ui']}"
        if bool(st.session_state.get(key, False)):
            valor = _valor_operacional(r)
            selecionados.append((r, valor, key))

    if selecionados:
        total_sel = round(sum(v for _, v, _ in selecionados), 2)
        todos_pagaveis = all((r['tipo'] == 'Despesa') and int_seguro(r.get('pago')) == 0 for r, _, _ in selecionados)
        with st.container(border=True):
            st.markdown("<span class='flow2-selection'></span>", unsafe_allow_html=True)
            sc1, sc2, sc3 = st.columns([3.4, 1.05, 1.35])
            sc1.markdown(f"**{len(selecionados)} selecionado(s) · R$ {format_brl(total_sel)}**")
            sc1.caption("Seleção para organizar pagamentos; não representa saldo bancário.")
            if sc2.button("Limpar", key=f"{prefixo}_clear", use_container_width=True):
                for _, _, key in selecionados:
                    st.session_state[key] = False
                st.session_state.pop(f'{prefixo}_batch_open', None)
                st.rerun()
            if todos_pagaveis:
                if sc3.button("Pagar selecionadas", key=f"{prefixo}_batch", type="primary", use_container_width=True):
                    st.session_state[f'{prefixo}_batch_open'] = True
                    st.rerun()
            else:
                sc3.caption("Pagamento em lote só para despesas pendentes")

        if todos_pagaveis and st.session_state.get(f'{prefixo}_batch_open'):
            with st.container(border=True):
                st.markdown("<div class='flow2-batch-note'><b>Confirmar pagamento em lote</b><br>Será usado o valor planejado de cada conta. Se algum valor real for diferente, pague essa conta individualmente.</div>", unsafe_allow_html=True)
                with st.form(f"{prefixo}_batch_form"):
                    data_lote = st.date_input("Data dos pagamentos", value=hoje, format="DD/MM/YYYY")
                    bc1, bc2 = st.columns([1.4, 1])
                    ok_lote = bc1.form_submit_button("Confirmar pagamentos", type="primary", use_container_width=True)
                    cancel_lote = bc2.form_submit_button("Cancelar", use_container_width=True)
                if cancel_lote:
                    st.session_state.pop(f'{prefixo}_batch_open', None)
                    st.rerun()
                if ok_lote:
                    try:
                        _fluxo2_pagar_lote_planejado([r for r, _, _ in selecionados], data_lote)
                    except Exception as e:
                        st.error(f"Não foi possível concluir o pagamento em lote: {e}")
                    else:
                        for _, _, key in selecionados:
                            st.session_state[key] = False
                        st.session_state.pop(f'{prefixo}_batch_open', None)
                        flash('success', f"{len(selecionados)} contas marcadas como pagas.")
                        st.rerun()

    dados = df_visivel.sort_values(['data_vencimento', 'tipo', 'descricao']).copy()
    for data_ref, grupo in dados.groupby('data_vencimento', sort=True):
        data_ref = pd.to_datetime(data_ref).date()
        if data_ref == hoje:
            titulo_dia, dia_cls, dot_cls = f"HOJE · {data_ref.strftime('%d/%m')}", 'today', 'today'
        else:
            titulo_dia, dia_cls, dot_cls = data_ref.strftime('%d/%m'), '', ''
        st.markdown(f"<div class='flow2-day {dia_cls}'><span class='flow2-dot {dot_cls}'></span>{titulo_dia}</div>", unsafe_allow_html=True)

        for _, r in grupo.iterrows():
            pago = int_seguro(r.get('pago')) == 1
            atrasado = bool(r.get('atrasado'))
            planejado = float_seguro(r.get('valor'))
            realizado = float_seguro(r.get('valor_pago'))
            valor_exibir = realizado if pago else planejado
            id_ui = str(r['id_ui'])
            conta = contas_map.get(id_ui) if r['tipo'] == 'Despesa' else None
            fonte = fontes_map.get(id_ui) if r['tipo'] == 'Entrada' else None
            chave_acao = f"{prefixo}:{id_ui}"

            if pago:
                dt_pago = pd.to_datetime(r.get('data_pagamento'), errors='coerce')
                data_pago_txt = dt_pago.strftime('%d/%m') if pd.notna(dt_pago) else data_ref.strftime('%d/%m')
                status_meta = ("Pago" if r['tipo'] == 'Despesa' else "Recebido") + f" em {data_pago_txt}"
                meta_cls = ''
            elif atrasado:
                dias = max((hoje - data_ref).days, 1)
                status_meta = (f"Venceu há {dias} {'dia' if dias == 1 else 'dias'}" if r['tipo'] == 'Despesa' else f"Esperado há {dias} {'dia' if dias == 1 else 'dias'}")
                meta_cls = 'danger'
            elif data_ref == hoje:
                status_meta = "Vence hoje" if r['tipo'] == 'Despesa' else "Previsto hoje"
                meta_cls = 'warn' if r['tipo'] == 'Despesa' else ''
            elif data_ref == hoje + datetime.timedelta(days=1):
                status_meta = "Vence amanhã" if r['tipo'] == 'Despesa' else "Previsto amanhã"
                meta_cls = 'warn' if r['tipo'] == 'Despesa' else ''
            else:
                status_meta = ("Vence " if r['tipo'] == 'Despesa' else "Previsto para ") + data_ref.strftime('%d/%m')
                meta_cls = ''

            match_cls, match_text = ('', '')
            if (not pago) and r['tipo'] == 'Despesa':
                match_cls, match_text = _fluxo2_texto_cobertura(conta)
            elif (not pago) and r['tipo'] == 'Entrada' and fonte:
                futuros = [x for x in fonte.get('compromissos', []) if not x.get('pago')]
                comprometido = round(sum(float_seguro(x.get('valor')) for x in futuros), 2)
                match_text = f"R$ {format_brl(comprometido)} comprometidos" if comprometido > 0.004 else "Ainda sem contas atribuídas"

            with st.container(border=True):
                csel, cdesc, cvalor, caction, cextra = st.columns([.34, 4.25, 2.25, 1.25, .46])
                csel.checkbox(f"Selecionar {r['descricao']}", key=f"{prefixo}_sel_{id_ui}", label_visibility="collapsed")
                paid_cls = ' flow2-paid' if pago else ''
                cdesc.markdown(
                    f"<span class='flow2-row-anchor'></span><div class='{paid_cls.strip()}'>"
                    f"<div class='flow2-name'>{html.escape(str(r['descricao']))}</div>"
                    f"<div class='flow2-meta {meta_cls}'>{html.escape(status_meta)}</div></div>",
                    unsafe_allow_html=True,
                )

                with cdesc:
                    _fatura_detalhes(r, prefixo, controle=True)

                sinal = '+' if r['tipo'] == 'Entrada' else ''
                val_cls = 'ux-positive' if r['tipo'] == 'Entrada' else ('ux-negative' if not pago else '')
                diff = realizado - planejado if pago else 0.0
                diff_txt = ''
                if pago and abs(diff) > 0.004:
                    diff_txt = f"Planejado R$ {format_brl(planejado)}"
                elif match_text:
                    diff_txt = match_text
                cvalor.markdown(
                    f"<div class='{paid_cls.strip()}'><div class='flow2-amount {val_cls}'>{sinal}R$ {format_brl(valor_exibir)}</div>"
                    f"<div class='flow2-match {match_cls}'>{html.escape(diff_txt)}</div></div>",
                    unsafe_allow_html=True,
                )

                if pago:
                    caction.caption("Concluído")
                else:
                    rotulo = "Pagar" if r['tipo'] == 'Despesa' else "Receber"
                    if caction.button(rotulo, key=f"{prefixo}_act_{id_ui}", type="primary" if atrasado else "secondary", use_container_width=True):
                        st.session_state['_pagamento_aberto'] = chave_acao
                        st.rerun()

                if pago:
                    if cextra.button("↩", key=f"{prefixo}_undo_{id_ui}", help="Desfazer recebimento" if r['tipo']=='Entrada' else "Desfazer pagamento", use_container_width=True):
                        _marcar_ids(r['ids'], pago=False)
                        flash('success', 'Baixa desfeita. O planejado e o histórico foram preservados.')
                        st.rerun()
                elif r['tipo'] == 'Entrada' and fonte:
                    detalhe_key = f"{prefixo}_detail_{id_ui}"
                    if cextra.button("›", key=f"{prefixo}_detail_btn_{id_ui}", help="Ver contas ligadas a esta renda", use_container_width=True):
                        st.session_state[detalhe_key] = not bool(st.session_state.get(detalhe_key, False))
                        st.rerun()
                else:
                    cextra.write("")

            _fatura_detalhes(r, prefixo)

            if (not pago) and st.session_state.get('_pagamento_aberto') == chave_acao:
                acao_nome = 'pagamento' if r['tipo'] == 'Despesa' else 'recebimento'
                with st.container(border=True):
                    st.markdown(f"**Confirmar {acao_nome} · {r['descricao']}**")
                    st.caption(f"Planejado: R$ {format_brl(planejado)}")
                    if str(r.get("id_ui", "")).startswith("cartao_"):
                        st.caption("Diferenças serão registradas separadamente como encargos ou desconto da fatura, preservando as compras.")
                    with st.form(f"{prefixo}_pay_form_{id_ui}"):
                        f1, f2 = st.columns([1.35, 1])
                        valor_txt = f1.text_input(
                            "Quanto foi realmente pago?" if r['tipo'] == 'Despesa' else "Quanto foi realmente recebido?",
                            value=format_brl(planejado), key=f"{prefixo}_pay_value_{id_ui}",
                        )
                        data_real = f2.date_input("Data", value=hoje, format="DD/MM/YYYY", key=f"{prefixo}_pay_date_{id_ui}")
                        fb1, fb2 = st.columns([1.4, 1])
                        confirmar = fb1.form_submit_button("Confirmar pagamento" if r['tipo'] == 'Despesa' else "Confirmar recebimento", type="primary", use_container_width=True)
                        cancelar = fb2.form_submit_button("Cancelar", use_container_width=True)
                    if cancelar:
                        st.session_state.pop('_pagamento_aberto', None)
                        st.rerun()
                    if confirmar:
                        valor_informado = parse_valor(valor_txt)
                        try:
                            total_real = _registrar_pagamento_ids(r['ids'], valor_real_total=valor_informado, data_pagamento=data_real)
                        except Exception as e:
                            st.error(f"Não foi possível registrar o {acao_nome}: {e}")
                        else:
                            st.session_state.pop('_pagamento_aberto', None)
                            dif = total_real - planejado
                            if abs(dif) > 0.004:
                                sinal_dif = '+' if dif > 0 else '−'
                                flash('success', f"{acao_nome.capitalize()} registrado: R$ {format_brl(total_real)} ({sinal_dif} R$ {format_brl(abs(dif))} vs. planejado).")
                            else:
                                flash('success', f"{acao_nome.capitalize()} registrado por R$ {format_brl(total_real)}.")
                            st.rerun()

            detalhe_key = f"{prefixo}_detail_{id_ui}"
            if (not pago) and r['tipo'] == 'Entrada' and fonte and st.session_state.get(detalhe_key, False):
                compromissos = [x for x in fonte.get('compromissos', []) if not x.get('pago')]
                st.markdown("<div class='flow2-income-details'>", unsafe_allow_html=True)
                if compromissos:
                    for item in sorted(compromissos, key=lambda x: x['vencimento']):
                        st.markdown(
                            f"<div class='flow2-income-line'><span>{item['vencimento'].strftime('%d/%m')}</span>"
                            f"<span>{html.escape(str(item['descricao']))}</span><b>R$ {format_brl(item['valor'])}</b></div>",
                            unsafe_allow_html=True,
                        )
                    nao_comp = max(float_seguro(fonte.get('restante')), 0.0)
                    st.caption(f"Ainda não comprometido: R$ {format_brl(nao_comp)}")
                else:
                    st.caption("Nenhuma conta pendente foi atribuída a esta renda.")
                st.markdown("</div>", unsafe_allow_html=True)

def _render_linhas_operacionais(df_ops, prefixo, max_linhas=None, permitir_editar=False, permitir_selecao=False):
    if df_ops.empty:
        render_empty_state("Nada pendente aqui", "Não há lançamentos que correspondam a este filtro.")
        return

    dados = df_ops.head(max_linhas) if max_linhas else df_ops

    if permitir_selecao:
        selecionados = []
        for _, sr in dados.iterrows():
            skey = f"{prefixo}_sel_{sr['id_ui']}"
            if bool(st.session_state.get(skey, False)):
                s_pago = int_seguro(sr.get('pago')) == 1
                s_plan = float_seguro(sr.get('valor'))
                s_real = float_seguro(sr.get('valor_pago'))
                s_valor = s_real if s_pago and s_real > 0 else s_plan
                selecionados.append((sr, s_valor, skey))

        if selecionados:
            total_sel = sum(v for _, v, _ in selecionados)
            desp_sel = sum(v for rsel, v, _ in selecionados if rsel['tipo'] == 'Despesa')
            ent_sel = sum(v for rsel, v, _ in selecionados if rsel['tipo'] == 'Entrada')
            with st.container(border=True):
                rs1, rs2, rs3 = st.columns([1.1, 1.7, 1.1])
                rs1.metric("Selecionados", len(selecionados))
                rs2.metric("Total selecionado", f"R$ {format_brl(total_sel)}")
                if rs3.button("Limpar seleção", key=f"{prefixo}_limpar_selecao", use_container_width=True):
                    for _, _, chave_sel in selecionados:
                        st.session_state[chave_sel] = False
                    st.rerun()
                if desp_sel > 0 and ent_sel > 0:
                    st.caption(f"Despesas: R$ {format_brl(desp_sel)} · Entradas: R$ {format_brl(ent_sel)} · Diferença: R$ {format_brl(ent_sel - desp_sel)}")
                elif desp_sel > 0:
                    st.caption(f"Despesas selecionadas: R$ {format_brl(desp_sel)}")
                elif ent_sel > 0:
                    st.caption(f"Entradas selecionadas: R$ {format_brl(ent_sel)}")

    for i, r in dados.iterrows():
        atrasado = bool(r['atrasado'])
        pago = int_seguro(r.get('pago')) == 1
        data_txt = pd.to_datetime(r['data_vencimento']).strftime('%d/%m')
        planejado = float_seguro(r.get('valor'))
        realizado = float_seguro(r.get('valor_pago'))
        consolidado = bool(r.get('consolidado'))

        if permitir_selecao and permitir_editar:
            csel, c1, c2, c3, c4 = st.columns([.38, 4.42, 1.45, 1.25, .72])
        elif permitir_selecao:
            csel, c1, c2, c3 = st.columns([.38, 4.87, 1.50, 1.25])
            c4 = None
        elif permitir_editar:
            csel = None
            c1, c2, c3, c4 = st.columns([4.85, 1.48, 1.25, .72])
        else:
            csel = None
            c1, c2, c3 = st.columns([5.28, 1.55, 1.25])
            c4 = None

        if csel is not None:
            csel.checkbox(f"Selecionar {r['descricao']}", key=f"{prefixo}_sel_{r['id_ui']}", label_visibility="collapsed")

        categoria_txt = '' if pd.isna(r.get('categoria')) else str(r.get('categoria') or '')
        grupo_icon = "<span class='ux-group-icon' title='Agrupado'>▦</span>" if consolidado else ""
        estado_class = "ux-flow-paid" if pago else ("ux-flow-overdue" if atrasado else "ux-flow-pending")
        status_icon = "✓" if pago else ("●" if atrasado else "○")
        status_text = "Pago" if (pago and r['tipo']=='Despesa') else ("Recebido" if pago else ("Atrasado" if atrasado else "Pendente"))
        c1.markdown(
            f"<span class='ux-flow-row-anchor'></span><div class='{estado_class}'>"
            f"<div class='ux-flow-desc'>{grupo_icon}<span class='ux-flow-date'>{data_txt}</span>{html.escape(str(r['descricao']))}</div>"
            f"<div class='ux-flow-category'>{status_icon} {status_text}"
            + (f" · {html.escape(categoria_txt)}" if categoria_txt else "") + "</div></div>",
            unsafe_allow_html=True,
        )

        with c1:
            _fatura_detalhes(r, prefixo, controle=True)

        if pago:
            principal = realizado
            sub = f"Planejado R$ {format_brl(planejado)}" if abs(principal-planejado) > 0.004 else "Realizado"
            c2.markdown(
                f"<div class='ux-flow-paid' style='text-align:right;padding-top:.25rem;'>"
                f"<div class='ux-flow-value-main ux-positive'>R$ {format_brl(principal)}</div>"
                f"<div class='ux-flow-value-sub'>{sub}</div></div>", unsafe_allow_html=True
            )
        else:
            c2.markdown(
                f"<div style='text-align:right;padding-top:.25rem;'>"
                f"<div class='ux-flow-value-main'>R$ {format_brl(planejado)}</div>"
                f"<div class='ux-flow-value-sub'>Planejado</div></div>", unsafe_allow_html=True
            )

        _fatura_detalhes(r, prefixo)

        chave_acao = f"{prefixo}:{r['id_ui']}"
        if pago:
            if c3.button("↩ Desfazer recebimento" if r['tipo']=='Entrada' else "↩ Desfazer pagamento", key=f"{prefixo}_est_{i}_{r['id_ui']}", use_container_width=True):
                _marcar_ids(r['ids'], pago=False)
                if st.session_state.get('_pagamento_aberto') == chave_acao:
                    st.session_state.pop('_pagamento_aberto', None)
                flash('success', 'Baixa desfeita. O planejado e o histórico foram preservados.')
                st.rerun()
        else:
            rotulo = "Pagar" if r['tipo'] == 'Despesa' else "Receber"
            if c3.button(rotulo, key=f"{prefixo}_pay_{i}_{r['id_ui']}", type="primary" if atrasado else "secondary", use_container_width=True):
                st.session_state['_pagamento_aberto'] = chave_acao
                st.rerun()

        if c4 is not None:
            if consolidado:
                c4.caption("▦")
            elif c4.button("✎", key=f"{prefixo}_edit_{i}_{r['id_ui']}", use_container_width=True, help="Editar lançamento"):
                st.session_state['fluxo_editar_id'] = int(r['ids'][0])
                st.session_state['fluxo_editor_aberto'] = True
                st.rerun()

        if (not pago) and st.session_state.get('_pagamento_aberto') == chave_acao:
            acao_nome = "pagamento" if r['tipo'] == 'Despesa' else "recebimento"
            with st.container(border=True):
                st.markdown("<div class='ux-payment-box'>", unsafe_allow_html=True)
                st.markdown(f"**{r['descricao']}**")
                st.caption(f"Planejado: R$ {format_brl(planejado)}")
                if str(r.get("id_ui", "")).startswith("cartao_"):
                    st.caption("Diferenças geram encargos ou desconto separados; as compras são preservadas.")
                with st.form(f"form_pagamento_{prefixo}_{i}_{r['id_ui']}"):
                    f1, f2 = st.columns([1.5, 1])
                    valor_txt = f1.text_input(
                        "Valor real (opcional)", value="",
                        placeholder=f"Vazio = R$ {format_brl(planejado)}",
                        key=f"valor_pag_{prefixo}_{i}_{r['id_ui']}",
                        help="Se deixar vazio, o app considera que o valor real foi igual ao planejado."
                    )
                    data_real = f2.date_input("Data", value=hoje, format="DD/MM/YYYY", key=f"data_pag_{prefixo}_{i}_{r['id_ui']}")
                    b1, b2 = st.columns([1.4,1])
                    confirmar = b1.form_submit_button("Confirmar pagamento" if r['tipo']=='Despesa' else "Confirmar recebimento", type="primary", use_container_width=True)
                    cancelar = b2.form_submit_button("Cancelar", use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)

                if cancelar:
                    st.session_state.pop('_pagamento_aberto', None)
                    st.rerun()
                if confirmar:
                    valor_informado = parse_valor(valor_txt) if str(valor_txt).strip() else None
                    try:
                        total_real = _registrar_pagamento_ids(r['ids'], valor_real_total=valor_informado, data_pagamento=data_real)
                    except Exception as e:
                        st.error(f"Não foi possível registrar o {acao_nome}: {e}")
                    else:
                        st.session_state.pop('_pagamento_aberto', None)
                        diferenca = total_real - planejado
                        if abs(diferenca) > 0.004:
                            sinal = "+" if diferenca > 0 else "−"
                            msg = f"{acao_nome.capitalize()} registrado: R$ {format_brl(total_real)} ({sinal} R$ {format_brl(abs(diferenca))} em relação ao planejado)."
                        else:
                            msg = f"{acao_nome.capitalize()} registrado por R$ {format_brl(total_real)}."
                        flash('success', msg)
                        st.rerun()


def _dados_caixa():
    return fetch_dataframe("SELECT * FROM lancamentos WHERE pago=1 AND data_pagamento >= %s AND data_pagamento < %s AND data_pagamento <= %s", (inicio_periodo, fim_periodo, hoje))


def _dados_operacionais(dados_mes=None):
    if dados_mes is not None:
        return dados_mes.copy()
    return fetch_dataframe("""SELECT * FROM lancamentos
        WHERE data_vencimento >= %s AND data_vencimento < %s
        ORDER BY data_vencimento,id""", (inicio_periodo,fim_periodo))


def _dados_mes():
    df_mes_local = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento", (inicio_periodo, fim_periodo))
    if df_mes_local.empty and len(df_mes_local.columns) == 0:
        return pd.DataFrame(columns=['id','tipo','categoria','subgrupo','descricao','valor','data_vencimento','parcela_atual','total_parcelas','pago','compra_id','forma_pagamento','prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento','eh_orcamento','valor_orcamento'])
    return df_mes_local


def _orcamentos_mes(ano, mes):
    competencia = datetime.date(int(ano), int(mes), 1)
    df = fetch_dataframe('''
        SELECT id, competencia, categoria, subgrupo, valor_planejado, origem
        FROM orcamentos_categorias
        WHERE competencia=%s
        ORDER BY categoria, subgrupo
    ''', (competencia,))
    if df.empty:
        return pd.DataFrame(columns=['id','competencia','categoria','subgrupo','valor_planejado','origem','_sub'])
    df['valor_planejado'] = pd.to_numeric(df['valor_planejado'], errors='coerce').fillna(0.0)
    df['_sub'] = df['subgrupo'].apply(_sub_norm)
    return df


def _salvar_orcamento_categoria(ano, mes, categoria, subgrupo, valor):
    competencia = datetime.date(int(ano), int(mes), 1)
    sub = _sub_norm(subgrupo) or None
    valor = max(float_seguro(valor), 0.0)
    with transaction() as cur:
        if valor <= 0.004:
            cur.execute("DELETE FROM orcamentos_categorias WHERE competencia=%s AND categoria=%s AND COALESCE(subgrupo,'')=%s", (competencia, categoria, _sub_norm(sub)))
        else:
            cur.execute('''
                UPDATE orcamentos_categorias
                SET valor_planejado=%s, origem='manual', atualizado_em=NOW()
                WHERE competencia=%s AND categoria=%s AND COALESCE(subgrupo,'')=%s
            ''', (valor, competencia, categoria, _sub_norm(sub)))
            if cur.rowcount == 0:
                cur.execute('''
                    INSERT INTO orcamentos_categorias
                        (competencia,categoria,subgrupo,valor_planejado,origem)
                    VALUES (%s,%s,%s,%s,'manual')
                ''', (competencia, categoria, sub, valor))


def _planejamento_valores_item(item):
    if int_seguro(item.get('pago')) == 1:
        return float_seguro(item.get('valor_pago')), 0.0
    data=pd.to_datetime(item.get('data_competencia'),errors='coerce')
    if pd.isna(data): data=pd.to_datetime(item['data_vencimento'])
    if item.get('forma_pagamento')=='Crédito' and data.date()<=hoje:
        return float_seguro(item.get('valor')), 0.0
    return 0.0, float_seguro(item.get('valor'))


def _planejamento_detalhes(row):
    key=request_key('plan-details',[ano_selecionado,mes_selecionado,row['categoria'],row['subgrupo']])
    if not st.toggle(f"Ver custos de {row['nome']}",key=key): return
    itens=row.get('itens',[])
    if row['tem_orcamento']:
        st.caption(f"Planejado: orçamento definido de R$ {format_brl(row['planejado'])}. Esse limite não é a soma dos lançamentos.")
    else:
        st.caption('Planejado: soma dos valores previstos dos lançamentos abaixo.')
    st.caption('Itens do mês pela competência. Compras no crédito já realizadas entram no gasto mesmo antes de pagar a fatura; o pagamento da fatura não é somado novamente.')
    if not itens:
        st.info('Nenhum custo lançado nesta categoria no mês selecionado.')
        return
    linhas=[]
    for item in itens:
        realizado,pendente=_planejamento_valores_item(item)
        n=int_seguro(item.get('total_parcelas'))
        def data_txt(field):
            d=pd.to_datetime(item.get(field),errors='coerce')
            return d.strftime('%d/%m/%Y') if pd.notna(d) else '—'
        linhas.append({'Descrição':item['descricao'],
            'Competência':data_txt('data_competencia'),'Vencimento':data_txt('data_vencimento'),
            'Parcela':f"{int_seguro(item.get('parcela_atual'))}/{n}" if 1<n<999 else '—',
            'Pagamento':item.get('forma_pagamento') or 'Não informado',
            'Status':'Pago' if int_seguro(item.get('pago')) else ('Crédito a pagar' if item.get('forma_pagamento')=='Crédito' else 'Pendente'),
            'Previsto':f"R$ {format_brl(item['valor'])}",
            'No realizado':f'R$ {format_brl(realizado)}','Ainda previsto':f'R$ {format_brl(pendente)}'})
    st.dataframe(pd.DataFrame(linhas),hide_index=True,use_container_width=True)
    st.markdown(f"**Realizado: R$ {format_brl(row['realizado'])} · Ainda previsto: R$ {format_brl(row['comprometido'])}**")


def _planejamento_unidades(df, ano=None, mes=None):
    """Planejado x realizado por categoria/subgrupo, sem lançamentos de orçamento."""
    ano = int(ano if ano is not None else ano_selecionado)
    mes = int(mes if mes is not None else mes_selecionado)
    ini, fim = limites_mes(mes, ano)
    d = fetch_dataframe("SELECT * FROM lancamentos WHERE tipo='Despesa' AND COALESCE(data_competencia,data_vencimento) >= %s AND COALESCE(data_competencia,data_vencimento) < %s", (ini,fim))
    if not d.empty:
        d['valor'] = pd.to_numeric(d['valor'], errors='coerce').fillna(0.0)
        d['valor_pago'] = pd.to_numeric(d['valor_pago'], errors='coerce').fillna(0.0)
        d['pago'] = pd.to_numeric(d['pago'], errors='coerce').fillna(0).astype(int)
        d['_sub'] = d['subgrupo'].apply(_sub_norm)
        d = d[d['categoria'].fillna('') != 'Ajuste'].copy()
    orcs = _orcamentos_mes(ano, mes)
    cfg = fetch_dataframe("SELECT categoria,subgrupo FROM categorias_personalizadas WHERE tipo='Despesa' ORDER BY categoria,subgrupo")
    chaves = set()
    if not d.empty:
        chaves |= {(str(cat), _sub_norm(sub)) for cat, sub in d[['categoria','_sub']].itertuples(index=False, name=None)}
    if not orcs.empty:
        chaves |= {(str(cat), _sub_norm(sub)) for cat, sub in orcs[['categoria','_sub']].itertuples(index=False, name=None)}
    if not cfg.empty:
        chaves |= {(str(cat), _sub_norm(sub)) for cat, sub in cfg[['categoria','subgrupo']].itertuples(index=False, name=None)}
    rows = []
    for cat, sub in sorted(chaves):
        g = d[(d['categoria'] == cat) & (d['_sub'] == sub)] if not d.empty else pd.DataFrame()
        go = orcs[(orcs['categoria'] == cat) & (orcs['_sub'] == sub)] if not orcs.empty else pd.DataFrame()
        tem_orcamento = not go.empty
        planejado = float(pd.to_numeric(go['valor_planejado'], errors='coerce').fillna(0).sum()) if tem_orcamento else (float(pd.to_numeric(g['valor'], errors='coerce').fillna(0).sum()) if not g.empty else 0.0)
        realizado = 0.0
        comprometido = 0.0
        if not g.empty:
            for _, item in g.iterrows():
                gasto,pendente=_planejamento_valores_item(item)
                realizado += gasto
                comprometido += pendente
        diferenca = realizado - planejado
        percentual = (realizado / planejado * 100.0) if planejado > 0 else (100.0 if realizado > 0 else 0.0)
        rows.append({'categoria':cat,'subgrupo':sub,'nome':sub if sub else cat,'planejado':planejado,'realizado':realizado,'diferenca':diferenca,'percentual':percentual,'tem_orcamento':tem_orcamento,'comprometido':comprometido,'itens':g.to_dict('records') if not g.empty else []})
    return pd.DataFrame(rows, columns=['categoria','subgrupo','nome','planejado','realizado','diferenca','percentual','tem_orcamento','comprometido','itens'])


def _planejamento_resumo(df, ano=None, mes=None, unidades=None):
    vazio = {'receita_planejada':0.0,'receita_realizada':0.0,'despesa_planejada':0.0,'despesa_realizada':0.0,'resultado_planejado':0.0,'resultado_realizado':0.0}
    base = df.copy() if df is not None else pd.DataFrame()
    unidades = unidades if unidades is not None else _planejamento_unidades(base, ano, mes)
    desp_plan = float(unidades['planejado'].sum()) if not unidades.empty else 0.0
    if base.empty:
        base = pd.DataFrame(columns=['tipo','valor','valor_pago','pago'])
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)
    base['pago'] = pd.to_numeric(base['pago'], errors='coerce').fillna(0).astype(int)
    entradas = base[base['tipo'] == 'Entrada']
    despesas = base[base['tipo'] == 'Despesa']
    rec_plan = float(entradas['valor'].sum())
    caixa = _dados_caixa()
    rec_real = float(caixa.loc[caixa['tipo']=='Entrada','valor_pago'].sum())
    desp_real = float(unidades['realizado'].sum()) if not unidades.empty else 0.0
    return {'receita_planejada':rec_plan,'receita_realizada':rec_real,'despesa_planejada':desp_plan,'despesa_realizada':desp_real,'resultado_planejado':rec_plan-desp_plan,'resultado_realizado':rec_real-desp_real}


def _planejamento_orcamentos(df, ano=None, mes=None, unidades=None):
    unidades = unidades if unidades is not None else _planejamento_unidades(df, ano, mes)
    if unidades.empty:
        return pd.DataFrame(columns=['categoria','subgrupo','nome','orcamento','realizado','disponivel','percentual'])
    o = unidades[unidades['tem_orcamento']].copy()
    if o.empty:
        return pd.DataFrame(columns=['categoria','subgrupo','nome','orcamento','realizado','disponivel','percentual'])
    o['orcamento'] = o['planejado']
    o['disponivel'] = o['planejado'] - o['realizado'] - o['comprometido']
    return o[['categoria','subgrupo','nome','orcamento','realizado','comprometido','disponivel','percentual']]


def _planejamento_dividas():
    """Resumo das compras parceladas para a aba Planejamento > Dívidas."""
    dd = fetch_dataframe("""
        SELECT compra_id, MIN(categoria) categoria, MIN(subgrupo) subgrupo, MIN(descricao) descricao,
               SUM(valor) valor_total,
               SUM(CASE WHEN pago=0 THEN valor ELSE 0 END) saldo_aberto,
               COUNT(*) parcelas_cadastradas,
               SUM(CASE WHEN pago=0 THEN 1 ELSE 0 END) parcelas_abertas,
               SUM(CASE WHEN pago=1 THEN valor_pago ELSE 0 END) valor_pago_total,
               MAX(total_parcelas) total_parcelas,
               SUM(CASE WHEN pago=1 THEN 1 ELSE 0 END) parcelas_pagas,
               MIN(CASE WHEN pago=0 THEN data_vencimento END) proxima_parcela,
               MAX(data_vencimento) data_fim,
               MIN(CASE WHEN pago=0 THEN valor END) parcela_referencia
        FROM lancamentos
        WHERE tipo='Despesa' AND total_parcelas>1 AND total_parcelas!=999 AND compra_id IS NOT NULL
        GROUP BY compra_id
        ORDER BY data_fim
    """)
    if dd.empty:
        return dd
    info = fetch_dataframe('SELECT * FROM info_dividas')
    if not info.empty:
        dd = dd.merge(info, on='compra_id', how='left')
    else:
        dd['credor'] = None
        dd['taxa_juros_mensal'] = None
    for col in ['valor_total','valor_pago_total','parcela_referencia']:
        dd[col] = pd.to_numeric(dd[col], errors='coerce').fillna(0.0)
    for col in ['total_parcelas','parcelas_pagas']:
        dd[col] = pd.to_numeric(dd[col], errors='coerce').fillna(0).astype(int)
    dd['saldo'] = pd.to_numeric(dd['saldo_aberto'], errors='coerce').fillna(0).clip(lower=0.0)
    dd['parcelas_restantes'] = dd['parcelas_abertas'].astype(int)
    dd['progresso'] = dd.apply(lambda r: min(max(r['parcelas_pagas'] / r['parcelas_cadastradas'], 0.0), 1.0) if r['total_parcelas'] else 0.0, axis=1)
    dd['nome'] = dd.apply(lambda r: str(r['credor']).strip() if pd.notna(r.get('credor')) and str(r.get('credor')).strip() else str(r.get('descricao') or r.get('subgrupo') or 'Dívida'), axis=1)
    return dd


def _plan2_status(unidade):
    planejado = float(unidade.get('planejado', 0) or 0)
    realizado = float(unidade.get('realizado', 0) or 0)
    diferenca = realizado - planejado
    pct = (realizado / planejado * 100) if planejado > 0 else (100 if realizado > 0 else 0)
    if planejado > 0 and diferenca > 0.01:
        return 'bad', f"R$ {format_brl(diferenca)} acima"
    if planejado > 0 and pct >= 90:
        return 'warn', f"{pct:.0f}% utilizado"
    if planejado > 0 and realizado <= 0.004:
        return 'good', 'Ainda sem gasto'
    if planejado > 0:
        return 'good', f"R$ {format_brl(max(-diferenca, 0))} abaixo"
    if realizado > 0:
        return 'bad', 'Sem plano definido'
    return 'good', 'Dentro do plano'


def _render_plan2_unidade(row, mostrar_categoria=False):
    planejado = float(row.get('planejado', 0) or 0)
    realizado = float(row.get('realizado', 0) or 0)
    pct = (realizado / planejado * 100.0) if planejado > 0 else (100.0 if realizado > 0 else 0.0)
    width = min(max(pct, 0.0), 100.0)
    tom, status = _plan2_status(row)
    nome = html.escape(str(row.get('nome') or row.get('categoria') or 'Categoria'))
    subt = ''
    if mostrar_categoria and str(row.get('subgrupo') or '').strip():
        subt = f"<div class='plan2-name-sub'>{html.escape(str(row.get('categoria') or ''))}</div>"
    inicial = html.escape((nome[:1] if nome else '•').upper())
    st.markdown(
        f"<div class='plan2-row'>"
        f"<div class='plan2-name-cell'><span class='plan2-cat-icon'>{inicial}</span><div><div class='plan2-name'>{nome}</div>{subt}</div></div>"
        f"<div class='plan2-bar'><div class='plan2-fill {tom}' style='width:{width:.1f}%'></div></div>"
        f"<div class='plan2-values'><b>R$ {format_brl(realizado)}</b> gastos de R$ {format_brl(planejado)}<br>R$ {format_brl(row.get('comprometido',0))} comprometidos</div>"
        f"<div class='plan2-status {tom}'>{html.escape(status)}</div>"
        f"</div>", unsafe_allow_html=True,
    )


def _render_plan2_divida(row):
    nome = html.escape(str(row.get('nome') or 'Dívida'))
    saldo = float(row.get('saldo', 0) or 0)
    parcela = float(row.get('parcela_referencia', 0) or 0)
    restantes = int_seguro(row.get('parcelas_restantes'))
    progresso = float(row.get('progresso', 0) or 0)
    largura = max(0.0, min(progresso * 100.0, 100.0))
    prox = pd.to_datetime(row.get('proxima_parcela'), errors='coerce')
    prox_txt = prox.strftime('%d/%m') if pd.notna(prox) else '—'
    st.markdown(
        f"<div class='plan2-debt-row'><div class='plan2-debt-head'>"
        f"<div class='plan2-debt-name-wrap'><span class='plan2-debt-icon'>◇</span><div class='plan2-debt-name'>{nome}</div></div>"
        f"<div class='plan2-debt-balance'>R$ {format_brl(saldo)}</div></div>"
        f"<div class='plan2-debt-meta'><span>Parcela R$ {format_brl(parcela)}</span><span>{restantes} parcela(s)</span><span>Próxima {prox_txt}</span></div>"
        f"<div class='plan2-debt-progress'><span style='width:{largura:.1f}%'></span></div></div>",
        unsafe_allow_html=True,
    )


def _renda_fonte_key(categoria, subgrupo):
    return (str(categoria or '').strip().casefold(), str(subgrupo or '').strip().casefold())


def _renda_fonte_nome(categoria, subgrupo):
    cat = str(categoria or '').strip(); sub = str(subgrupo or '').strip()
    return sub or cat or 'Renda'


def _renda_modalidade(def_row):
    """Tipo da renda; Plantões é um recurso independente, não um tipo."""
    if def_row is None:
        return 'Variável'
    explicita = str(def_row.get('modalidade_renda') or '').strip()
    if explicita in ('Mensal', 'Variável', 'Eventual'):
        return explicita
    if explicita == 'Plantões':  # compatibilidade com bases ainda não migradas
        return 'Variável'
    # Fontes antigas sem classificação recebem um padrão neutro; o usuário pode
    # definir conscientemente Mensal ou Eventual ao editar a fonte.
    return 'Variável'


def _rendas_fontes_periodo(df_mes, ano, mes):
    defs = fetch_dataframe('''
        SELECT id,tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,
               is_recorrente,data_inicio,is_producao_variavel,modalidade_renda
        FROM categorias_personalizadas WHERE tipo='Entrada'
        ORDER BY categoria,subgrupo
    ''')
    removed=fetch_dataframe("SELECT valor FROM preferencias_app WHERE chave LIKE 'fonte_excluida:%%'")
    removed_keys={_renda_fonte_key(v.get('categoria'),v.get('subgrupo')) for v in (json.loads(x) for x in removed.get('valor',[]))}
    entradas = df_mes[df_mes['tipo']=='Entrada'].copy() if df_mes is not None and not df_mes.empty else pd.DataFrame()
    entradas=finance.align_shift_sources(entradas,defs)
    if not entradas.empty:
        entradas['valor']=pd.to_numeric(entradas['valor'],errors='coerce').fillna(0.0)
        entradas['valor_pago']=pd.to_numeric(entradas['valor_pago'],errors='coerce').fillna(0.0)
        entradas['pago']=pd.to_numeric(entradas['pago'],errors='coerce').fillna(0).astype(int)
        entradas['data_vencimento']=pd.to_datetime(entradas['data_vencimento'],errors='coerce')
    fontes={}
    if not defs.empty:
        for _,r in defs.iterrows():
            k=_renda_fonte_key(r.get('categoria'),r.get('subgrupo'))
            fontes[k]={'key':k,'id':int_seguro(r.get('id')) or None,'categoria':str(r.get('categoria') or ''),'subgrupo':str(r.get('subgrupo') or ''),'nome':_renda_fonte_nome(r.get('categoria'),r.get('subgrupo')),'valor_padrao':float_seguro(r.get('valor_padrao')),'dia_pagamento':int_seguro(r.get('dia_pagamento')),'atraso_meses':int_seguro(r.get('atraso_meses')),'is_recorrente':int_seguro(r.get('is_recorrente')),'modalidade_renda':str(r.get('modalidade_renda') or '').strip(),'especializada':(int_seguro(r.get('is_producao_variavel'))==1),'data_inicio':r.get('data_inicio'),'def_row':r}
    if not entradas.empty:
        for _,r in entradas.iterrows():
            k=_renda_fonte_key(r.get('categoria'),r.get('subgrupo'))
            if k not in fontes and k not in removed_keys:
                fontes[k]={'key':k,'id':None,'categoria':str(r.get('categoria') or ''),'subgrupo':str(r.get('subgrupo') or ''),'nome':_renda_fonte_nome(r.get('categoria'),r.get('subgrupo')),'valor_padrao':0.0,'dia_pagamento':0,'atraso_meses':0,'is_recorrente':0,'modalidade_renda':'','especializada':False,'data_inicio':None,'def_row':None}
    # Média histórica: usa somente meses FECHADOS anteriores ao período selecionado
    # e somente valores efetivamente recebidos. Previsões do mês atual não entram.
    comp=datetime.date(int(ano),int(mes),1)
    hist_ini=(pd.Timestamp(comp)-pd.DateOffset(months=6)).date()
    hist_fim=min(comp,hoje.replace(day=1))
    hist=fetch_dataframe(
        '''SELECT tipo,descricao,categoria,subgrupo,valor_pago,pago,data_pagamento,data_vencimento
           FROM lancamentos
           WHERE tipo='Entrada'
             AND pago=1
             AND COALESCE(valor_pago,0) > 0
             AND COALESCE(data_pagamento,data_vencimento) >= %s
             AND COALESCE(data_pagamento,data_vencimento) < %s''',
        (hist_ini,hist_fim)
    )
    hist=finance.align_shift_sources(hist,defs)
    medias={}
    media_meses={}
    if not hist.empty:
        hist['valor_pago']=pd.to_numeric(hist['valor_pago'],errors='coerce').fillna(0.0)
        hist['_data_real']=pd.to_datetime(hist['data_pagamento'],errors='coerce').fillna(pd.to_datetime(hist['data_vencimento'],errors='coerce'))
        hist=hist[hist['_data_real'].notna() & (hist['valor_pago']>0)].copy()
        if not hist.empty:
            hist['_mes']=hist['_data_real'].dt.to_period('M').astype(str)
            hist['_key']=hist.apply(lambda r:_renda_fonte_key(r.get('categoria'),r.get('subgrupo')),axis=1)
            by=hist.groupby(['_key','_mes'])['valor_pago'].sum().reset_index()
            for k,grp in by.groupby('_key'):
                inicio_fonte = pd.Period(grp['_mes'].min(), freq='M')
                periodo = pd.period_range(inicio_fonte, pd.Period(hist_fim,freq='M')-1, freq='M')
                serie = grp.set_index('_mes')['valor_pago'].reindex(periodo.astype(str), fill_value=0)
                meses_validos=len(serie)
                media_meses[k]=meses_validos
                medias[k]=float(serie.mean()) if meses_validos >= 2 else 0.0
    caixa_rendas = finance.align_shift_sources(_dados_caixa(),defs)
    saida=[]
    for k,f in fontes.items():
        if not entradas.empty:
            mask=entradas.apply(lambda r:_renda_fonte_key(r.get('categoria'),r.get('subgrupo'))==k,axis=1); grp=entradas[mask].copy()
        else: grp=pd.DataFrame()
        esperado=float(grp['valor'].sum()) if not grp.empty else 0.0; realizado=float(grp.loc[grp['pago']==1,'valor_pago'].sum()) if not grp.empty else 0.0; pendente=float(grp.loc[grp['pago']==0,'valor'].sum()) if not grp.empty else 0.0
        recebimentos_fonte = caixa_rendas[(caixa_rendas['tipo']=='Entrada') & (caixa_rendas['categoria']==f['categoria']) & (caixa_rendas['subgrupo'].fillna('')==f['subgrupo'])]
        realizado = float(recebimentos_fonte['valor_pago'].sum())
        f['quantidade_plantoes']=int(grp['descricao'].str.match(r'(?i)^plant[ãa]o\s',na=False).sum()) if not grp.empty else 0
        prox=None
        if not grp.empty:
            fut=grp[grp['pago']==0].sort_values('data_vencimento')
            if not fut.empty and pd.notna(fut.iloc[0]['data_vencimento']): prox=fut.iloc[0]['data_vencimento'].date()
        if prox is None and f['dia_pagamento']>0 and pendente>0.004: prox=datetime.date(int(ano),int(mes),min(f['dia_pagamento'],calendar.monthrange(int(ano),int(mes))[1]))
        modalidade=_renda_modalidade(f['def_row'])
        media=float(medias.get(k,0.0) or 0.0)
        meses_media=int(media_meses.get(k,0) or 0)
        f.update({'esperado':round(esperado,2),'realizado':round(realizado,2),'pendente':round(pendente,2),'media':round(media,2),'media_meses':meses_media,'proxima_data':prox,'modalidade':modalidade}); saida.append(f)
    saida.sort(key=lambda x:(x['proxima_data'] or datetime.date.max,-x['esperado'],x['nome'].casefold()))
    return saida


def _rendas_recebimentos_janela(ano, mes):
    ini=datetime.date(int(ano),int(mes),1); fim=(pd.Timestamp(ini)+pd.DateOffset(months=1)).date()+datetime.timedelta(days=15)
    df=fetch_dataframe('''SELECT * FROM lancamentos WHERE tipo='Entrada' AND data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento,id''',(ini,fim))
    if df.empty: return []
    df['valor']=pd.to_numeric(df['valor'],errors='coerce').fillna(0.0); df['valor_pago']=pd.to_numeric(df['valor_pago'],errors='coerce').fillna(0.0); df['pago']=pd.to_numeric(df['pago'],errors='coerce').fillna(0).astype(int); df['data_vencimento']=pd.to_datetime(df['data_vencimento'],errors='coerce')
    df['_key']=df.apply(lambda r:_renda_fonte_key(r.get('categoria'),r.get('subgrupo')),axis=1); df['_nome']=df.apply(lambda r:_renda_fonte_nome(r.get('categoria'),r.get('subgrupo')),axis=1)
    rows=[]
    for (k,dt),grp in df.groupby(['_key','data_vencimento'],dropna=False):
        if pd.isna(dt): continue
        pagos=int(grp['pago'].sum()); total=len(grp)
        if pagos==total: status='Recebido'; valor=float(grp['valor_pago'].sum()); classe='received'
        elif pagos>0: status='Parcial'; valor=float(grp['valor_pago'].sum()+grp.loc[grp['pago']==0,'valor'].sum()); classe='partial'
        else: status='Previsto'; valor=float(grp['valor'].sum()); classe='expected'
        rows.append({'data':pd.to_datetime(dt).date(),'nome':str(grp.iloc[0]['_nome']),'valor':valor,'status':status,'classe':classe})
    rows.sort(key=lambda x:x['data']); return rows


def _renda_badge_class(modalidade):
    return {'Plantões':'blue','Mensal':'purple','Variável':'teal','Eventual':'amber'}.get(modalidade,'teal')


if st.session_state.get('wizard_ativo'):
    renderizar_wizard_configuracao()

# Build UX 2.0: onboarding-v2-v21
# -----------------------------------------------------------------
# INÍCIO
# -----------------------------------------------------------------
elif menu == "🏠 Início":
    _referencia_cobertura()
    df_mes = _dados_mes()
    df_home = _dados_operacionais(df_mes)

    # Cabeçalho orientado a contexto, não a análise.
    st.markdown(
        f"<div class='home2-head'><div class='home2-hello'>Olá 👋</div>"
        f"<div class='home2-sub'>{meses[mes_selecionado-1]} de {ano_selecionado} · veja o que precisa da sua atenção agora.</div></div>",
        unsafe_allow_html=True,
    )

    if df_mes.empty and df_home.empty:
        render_empty_state("Vamos organizar seu primeiro ciclo financeiro", "Comece informando de onde vem sua renda. Você pode completar o restante depois.", "＋")
        c1, c2 = st.columns([1.2, 1])
        if c1.button("💰 Adicionar minha primeira renda", type="primary", use_container_width=True):
            st.session_state["novo_tipo"] = "Entrada"
            st.session_state["novo_pago_imediato"] = False
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
        if c2.button("＋ Registrar uma conta", use_container_width=True):
            st.session_state["novo_tipo"] = "Despesa"
            st.session_state["novo_pago_imediato"] = False
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
    else:
        # Base operacional: a Home trabalha apenas com entradas e despesas reais.
        df_mes['valor'] = pd.to_numeric(df_mes['valor'], errors='coerce').fillna(0.0)
        df_mes['valor_pago'] = pd.to_numeric(df_mes['valor_pago'], errors='coerce').fillna(0.0)
        df_real = df_mes.copy()

        caixa = _dados_caixa()
        ent_caixa = caixa[caixa['tipo']=='Entrada']
        desp_caixa = caixa[caixa['tipo']=='Despesa']
        ent = df_real[df_real['tipo']=='Entrada']
        desp = df_real[df_real['tipo']=='Despesa']
        recebido = float(ent_caixa['valor_pago'].sum())
        a_receber = float(ent[pd.to_numeric(ent['pago'], errors='coerce').fillna(0).astype(int)==0]['valor'].sum())
        pago = float(desp_caixa['valor_pago'].sum())
        a_pagar = float(desp[pd.to_numeric(desp['pago'], errors='coerce').fillna(0).astype(int)==0]['valor'].sum())
        resultado_atual = recebido - pago

        # Orientação e alertas usam o mesmo mês selecionado dos lançamentos.
        data_ref = data_contexto_ativo
        df_janela = df_home

        ops_home = _consolidar_operacional(df_janela, consolidar_cartao=True) if not df_janela.empty else pd.DataFrame()
        plano_home = _montar_plano_pagamentos(ops_home, ano_selecionado, mes_selecionado)

        # ---------------------------------------------------------
        # 1. PRÓXIMA RENDA + PONTE ATÉ ELA
        # ---------------------------------------------------------
        fontes_futuras = [f for f in plano_home['fontes'] if (not f['recebido']) and f['data'] >= data_ref]
        proxima_renda = min(fontes_futuras, key=lambda f: (f['data'], f['descricao'])) if fontes_futuras else None
        pendentes = [c for c in plano_home['contas'] if not c['pago']]

        if proxima_renda:
            contas_ate = [c for c in pendentes if c['vencimento'] <= proxima_renda['data']]
            total_ate = round(sum(c['valor'] for c in contas_ate), 2)
            risco_ate = round(sum(c['risco_valor'] for c in contas_ate), 2)
            qtd_ate = len(contas_ate)
            dias_renda = (proxima_renda['data'] - data_ref).days
            quando = "hoje" if dias_renda == 0 else ("amanhã" if dias_renda == 1 else proxima_renda['data'].strftime('%d/%m'))
            if risco_ate <= 0.004:
                hero_cls, status_cls = "", "ok"
                depende_previsao = any(not a.get('recebido') for c in contas_ate for a in c.get('alocacoes', []))
                status_titulo = '◷ Cobertura prevista' if depende_previsao else '✓ Coberto com recebimentos confirmados'
                status_texto = 'Depende da entrada das rendas previstas.' if depende_previsao else 'Considerando os recebimentos e pagamentos registrados no app.'
                status_cls = 'warn' if depende_previsao else 'ok'
            else:
                hero_cls, status_cls = "danger", "danger"
                status_titulo = f"⚠ Faltam R$ {format_brl(risco_ate)}"
                status_texto = "Há contas que vencem antes de existir cobertura suficiente registrada."

            st.markdown(
                f"<div class='home2-hero {hero_cls}'><div class='home2-hero-grid'><div>"
                f"<div class='home2-eyebrow'>Próxima renda</div>"
                f"<div class='home2-income-name'>{html.escape(proxima_renda['descricao'])}</div>"
                f"<div class='home2-income-value'>R$ {format_brl(proxima_renda['valor'])}</div>"
                f"<div class='home2-income-date'>previstos em {proxima_renda['data'].strftime('%d/%m/%Y')} · {quando}</div></div>"
                f"<div class='home2-hero-side'><div class='home2-eyebrow'>Contas pendentes até essa data</div>"
                f"<div class='home2-bridge-value'>R$ {format_brl(total_ate)}</div>"
                f"<div class='home2-income-date'>{qtd_ate} conta(s)</div>"
                f"<div class='home2-status {status_cls}'><b>{status_titulo}</b><br>{status_texto}</div></div></div></div>",
                unsafe_allow_html=True,
            )
            if st.button("Ver contas até essa renda →", key="home2_ver_ponte", use_container_width=True):
                st.session_state.menu_atual = "📊 Fluxo e Prioridades"
                st.session_state['fluxo_rapido_status'] = 'A pagar'
                st.session_state['fluxo_ate'] = proxima_renda['data']
                st.rerun()
        else:
            risco_total = round(sum(c['risco_valor'] for c in plano_home['risco_contas']), 2)
            st.markdown(
                f"<div class='home2-hero {'danger' if pendentes else ''}'>"
                f"<div class='home2-eyebrow'>Próxima renda</div>"
                f"<div class='home2-income-name'>Nenhuma renda futura cadastrada neste período</div>"
                f"<div class='home2-income-date'>{'Ainda há R$ ' + format_brl(a_pagar) + ' a pagar.' if a_pagar > 0 else 'Não há contas pendentes registradas.'}</div>"
                + (f"<div class='home2-status danger'><b>⚠ R$ {format_brl(risco_total)} sem cobertura no prazo</b><br>Cadastre ou confirme uma próxima renda para organizar essas contas.</div>" if risco_total > 0 else "")
                + "</div>",
                unsafe_allow_html=True,
            )

        # ---------------------------------------------------------
        # 2. PRECISA DA SUA ATENÇÃO — no máximo 3 itens
        # ---------------------------------------------------------
        if not ops_home.empty:
            ops_home = ops_home.copy()
            ops_home['data_vencimento'] = pd.to_datetime(ops_home['data_vencimento'], errors='coerce').dt.date
            ops_pend = ops_home[pd.to_numeric(ops_home['pago'], errors='coerce').fillna(0).astype(int) == 0].copy()
            ops_pend['dias'] = ops_pend['data_vencimento'].apply(lambda d: (d - data_ref).days if pd.notna(d) else 9999)
            atras_desp = ops_pend[(ops_pend['tipo']=='Despesa') & (ops_pend['dias'] < 0)].sort_values(['dias','data_vencimento'])
            atras_ent = ops_pend[(ops_pend['tipo']=='Entrada') & (ops_pend['dias'] < 0)].sort_values(['dias','data_vencimento'])
            proxim_desp = ops_pend[(ops_pend['tipo']=='Despesa') & (ops_pend['dias'] >= 0) & (ops_pend['dias'] <= 2)].sort_values(['dias','data_vencimento'])
            partes = [x for x in [atras_desp, atras_ent, proxim_desp] if not x.empty]
            atencao = pd.concat(partes, ignore_index=False).drop_duplicates(subset=['id_ui']).head(3) if partes else pd.DataFrame()

            if not atencao.empty:
                st.markdown("<div class='ux-section-title'>Precisa da sua atenção</div>", unsafe_allow_html=True)
                _render_linhas_operacionais(atencao, 'home2_atencao', max_linhas=3)
                if len(ops_pend) > len(atencao):
                    if st.button(f"Ver todos os pendentes ({len(ops_pend)}) →", key="home2_ver_todos", use_container_width=True):
                        st.session_state["fluxo_rapido_status"] = "Pendentes"
                        st.session_state.pop("fluxo_ate", None)
                        st.session_state.menu_atual = "📊 Fluxo e Prioridades"
                        st.rerun()

        # ---------------------------------------------------------
        # 3. SEU MÊS — 3 números e previsão secundária
        # ---------------------------------------------------------
        st.markdown("<div class='ux-section-title'>Seu mês</div>", unsafe_allow_html=True)
        sm1, sm2, sm3, sm4 = st.columns([1,1,1,1.08])
        sm1.markdown(f"<div class='home2-month-card green'><div class='home2-month-value ux-positive'>R$ {format_brl(recebido)}</div><div class='home2-month-label'>Recebido até agora</div></div>", unsafe_allow_html=True)
        sm2.markdown(f"<div class='home2-month-card red'><div class='home2-month-value ux-negative'>R$ {format_brl(pago)}</div><div class='home2-month-label'>Pago até agora</div></div>", unsafe_allow_html=True)
        sm3.markdown(f"<div class='home2-month-card blue'><div class='home2-month-value {'ux-positive' if resultado_atual >= 0 else 'ux-negative'}'>R$ {format_brl(resultado_atual)}</div><div class='home2-month-label'>Resultado até agora</div></div>", unsafe_allow_html=True)
        sm4.markdown(
            f"<div class='home2-forecast'><div class='home2-forecast-title'>Ainda previsto</div>"
            f"<div class='home2-forecast-line ux-positive'>↑ + R$ {format_brl(a_receber)} a receber</div>"
            f"<div class='home2-forecast-line ux-negative'>↓ − R$ {format_brl(a_pagar)} a pagar</div></div>",
            unsafe_allow_html=True,
        )

        # ---------------------------------------------------------
        # 4. PRÓXIMOS ACONTECIMENTOS — timeline curta
        # ---------------------------------------------------------
        st.markdown("<div class='ux-section-title'>Seu mês daqui para frente</div>", unsafe_allow_html=True)
        eventos = []
        if not ops_home.empty:
            futuros_ops = ops_home[(pd.to_numeric(ops_home['pago'], errors='coerce').fillna(0).astype(int)==0) & (ops_home['data_vencimento'] >= data_ref)].copy()
            futuros_ops = futuros_ops.sort_values(['data_vencimento','tipo']).head(3)
            for _, r in futuros_ops.iterrows():
                eventos.append({
                    'data': r['data_vencimento'], 'tipo': r['tipo'], 'descricao': str(r['descricao']),
                    'valor': float_seguro(r.get('valor')),
                })

        cards = [
            f"<div class='home2-event today'><div class='home2-event-date'>Hoje</div><div class='home2-event-name'>{data_ref.strftime('%d/%m/%Y')}</div><div class='home2-event-value ux-accent'>Você está aqui</div></div>"
        ]
        for ev in eventos:
            classe = 'in' if ev['tipo']=='Entrada' else 'out'
            sinal = '+' if ev['tipo']=='Entrada' else '−'
            valor_cls = 'ux-positive' if ev['tipo']=='Entrada' else 'ux-negative'
            cards.append(
                f"<div class='home2-event {classe}'><div class='home2-event-date'>{ev['data'].strftime('%d/%m')}</div>"
                f"<div class='home2-event-name'>{html.escape(ev['descricao'])}</div>"
                f"<div class='home2-event-value {valor_cls}'>{sinal} R$ {format_brl(ev['valor'])}</div></div>"
            )
        while len(cards) < 4:
            cards.append("<div class='home2-event'><div class='home2-event-date'>Depois</div><div class='home2-event-name'>Sem outro evento próximo</div></div>")
        st.markdown("<div class='home2-timeline'>" + ''.join(cards[:4]) + "</div>", unsafe_allow_html=True)
        if st.button("Ver no Fluxo →", key="home2_ver_fluxo", use_container_width=True):
            st.session_state.menu_atual = "📊 Fluxo e Prioridades"
            st.rerun()

        # ---------------------------------------------------------
        # 5. REGISTRAR — poucos atalhos, linguagem comum
        # ---------------------------------------------------------
        st.markdown("<div class='ux-section-title'>Registrar novo lançamento</div>", unsafe_allow_html=True)
        q1,q2,q3,q4 = st.columns(4)
        if q1.button("↓ Gastei", key="home2_gastei", use_container_width=True):
            st.session_state['novo_tipo'] = 'Despesa'
            st.session_state['novo_home_origem'] = 'gastei'
            st.session_state['novo_pago_imediato'] = True
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
        if q2.button("↑ Recebi", key="home2_recebi", type="primary", use_container_width=True):
            st.session_state['novo_tipo'] = 'Entrada'
            st.session_state['novo_home_origem'] = 'recebi'
            st.session_state['novo_pago_imediato'] = True
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
        if q3.button("◷ Conta futura", key="home2_conta", use_container_width=True):
            st.session_state['novo_tipo'] = 'Despesa'
            st.session_state['novo_home_origem'] = 'futuro'
            st.session_state['novo_pago_imediato'] = False
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
        if q4.button("💰 Renda futura", key="home2_renda", use_container_width=True):
            st.session_state['novo_tipo'] = 'Entrada'
            st.session_state['novo_home_origem'] = 'futuro'
            st.session_state['novo_pago_imediato'] = False
            st.session_state.menu_atual = "📝 Lançamentos"
            st.rerun()
        st.markdown("<div class='home2-tip'>💡 Dica: cadastre contas e rendas recorrentes uma vez para reduzir o trabalho nos próximos meses.</div>", unsafe_allow_html=True)

# -----------------------------------------------------------------
# NOVO LANÇAMENTO
# -----------------------------------------------------------------
elif menu == "📝 Lançamentos":
    cabecalho_pagina("Novo lançamento", "Informe o essencial. A categoria pode ficar para depois.")
    tipo = st.radio("O que aconteceu?", ["Despesa","Entrada"], horizontal=True, key="novo_tipo")
    descricao = st.text_input("Descrição", placeholder="Ex.: Mercado, escola ou Hospital Help")
    c1,c2 = st.columns(2)
    valor_input = c1.text_input("Valor (R$)", value="0,00")
    data_ref = c2.date_input("Data da compra" if tipo=='Despesa' else "Data prevista", value=hoje, format="DD/MM/YYYY")
    forma_pgto = st.radio("Como foi a compra?", ["À vista","Crédito"], horizontal=True) if tipo=='Despesa' else 'Outros'
    cartao = None
    nome_cartao = ''
    if forma_pgto == 'Crédito':
        cartoes = fetch_dataframe('SELECT * FROM cartoes ORDER BY nome')
        opcoes = cartoes['nome'].tolist() + ['＋ Cadastrar cartão']
        escolha = st.selectbox('Cartão', opcoes)
        if escolha == '＋ Cadastrar cartão':
            nome_cartao = st.text_input('Nome do cartão', placeholder='Ex.: Nubank')
            cc1,cc2 = st.columns(2)
            fechamento = cc1.number_input('Dia de fechamento',min_value=1,max_value=31,value=25)
            vencimento = cc2.number_input('Dia de vencimento',min_value=1,max_value=31,value=5)
        else:
            cartao = cartoes[cartoes['nome']==escolha].iloc[0]
            fechamento, vencimento = int(cartao['dia_fechamento']),int(cartao['dia_vencimento'])
        dt_vencimento = st.date_input('Vencimento da primeira fatura',value=finance.invoice_due(data_ref,fechamento,vencimento),format='DD/MM/YYYY')
        st.caption('A compra entra no orçamento na data da compra e será paga na fatura. Confira o vencimento sugerido.')
        pago_imediato = False
        data_pgto = None
    else:
        dt_vencimento = data_ref
        pago_imediato = st.checkbox('Já foi pago' if tipo=='Despesa' else 'Já recebi', key='novo_pago_imediato')
        data_pgto = st.date_input('Data efetiva', value=hoje, format='DD/MM/YYYY') if pago_imediato else None
    with st.expander('Categoria e outros detalhes'):
        categorias = ['Sem categoria'] + list(ESTRUTURA.get(tipo,{}).keys())
        categoria = st.selectbox('Categoria (opcional)', list(dict.fromkeys(categorias)))
        subs = ESTRUTURA.get(tipo,{}).get(categoria,[])
        subgrupo = st.selectbox('Subgrupo (opcional)', ['']+subs)
        prioridade = st.radio('Prioridade', ['Baixa 🟢','Média 🟡','Alta 🔴'],horizontal=True)
        rec_label = st.radio('Repetição',['Uma vez','Parcelada','Repete todo mês'] if forma_pgto!='Crédito' else ['Uma vez','Parcelada'],horizontal=True)
        parcelas = st.number_input('Número de parcelas',min_value=2,max_value=240,value=2) if rec_label=='Parcelada' else 1
        base_valor = st.radio('O valor informado é', ['Total da compra','Valor de cada parcela'],horizontal=True) if rec_label=='Parcelada' else 'Valor de cada parcela'
    rotulo = 'Registrar compra no cartão' if forma_pgto=='Crédito' else ('Registrar pagamento' if pago_imediato and tipo=='Despesa' else 'Registrar recebimento' if pago_imediato else 'Agendar conta' if tipo=='Despesa' else 'Agendar renda')
    # Stable for retries of the same draft; a deliberate new draft gets a new nonce.
    if st.button('Novo lançamento', key='novo_rascunho'):
        st.session_state['novo_nonce'] = str(uuid.uuid4())
    nonce = st.session_state.setdefault('novo_nonce', str(uuid.uuid4()))
    requisicao = request_key(nonce, [tipo, descricao.strip(), valor_input, data_ref,
        forma_pgto, int(cartao['id']) if cartao is not None else nome_cartao,
        dt_vencimento, pago_imediato, data_pgto, categoria, subgrupo, prioridade,
        rec_label, parcelas, base_valor])
    if st.button(rotulo,type='primary',use_container_width=True):
        val = parse_valor(valor_input)
        if not descricao.strip() or val<=0:
            st.error('Informe uma descrição e um valor maior que zero.')
        elif forma_pgto=='Crédito' and cartao is None and not nome_cartao.strip():
            st.error('Informe o nome do cartão.')
        elif data_pgto and data_pgto > hoje:
            st.error('Uma data futura deve ser agendada, não marcada como realizada.')
        else:
            comp_id = requisicao
            quantidade = 1 if rec_label=='Repete todo mês' else int(parcelas)
            valores = split_total(val,quantidade) if base_valor=='Total da compra' else [money(val)]*quantidade
            try:
                with transaction() as cur:
                    cartao_id = int(cartao['id']) if cartao is not None else None
                    if forma_pgto=='Crédito' and cartao_id is None:
                        cur.execute('INSERT INTO cartoes(nome,dia_fechamento,dia_vencimento) VALUES (%s,%s,%s) ON CONFLICT(nome) DO UPDATE SET nome=EXCLUDED.nome RETURNING id',(nome_cartao.strip(),fechamento,vencimento))
                        cartao_id = cur.fetchone()[0]
                    if rec_label=='Repete todo mês':
                        cur.execute('''INSERT INTO series_manuais(compra_id,tipo,categoria,subgrupo,descricao,valor,inicio,dia,forma_pagamento,prioridade)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                            (comp_id,tipo,categoria,subgrupo or None,descricao.strip(),money(val),dt_vencimento,dt_vencimento.day,forma_pgto,prioridade))
                        cur.execute('INSERT INTO series_manuais_geradas(compra_id,competencia) VALUES (%s,%s) ON CONFLICT DO NOTHING',(comp_id,dt_vencimento.replace(day=1)))
                    for i,v in enumerate(valores):
                        data_v = (pd.Timestamp(dt_vencimento)+pd.DateOffset(months=i)).date()
                        competencia = data_ref if forma_pgto=='Crédito' else (pd.Timestamp(data_ref)+pd.DateOffset(months=i)).date()
                        fatura_id = None
                        if cartao_id is not None:
                            cur.execute('INSERT INTO faturas(cartao_id,vencimento) VALUES (%s,%s) ON CONFLICT(cartao_id,vencimento) DO UPDATE SET vencimento=EXCLUDED.vencimento RETURNING id',(cartao_id,data_v))
                            fatura_id = cur.fetchone()[0]
                        pago = int(bool(pago_imediato) and i==0)
                        cur.execute("""INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia,data_pagamento,fatura_id,requisicao_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                            (tipo,categoria,subgrupo or None,descricao.strip(),v,data_v,i+1,999 if rec_label=='Repete todo mês' else quantidade,pago,comp_id,forma_pgto,prioridade,v if pago else 0,competencia,data_pgto if pago else None,fatura_id,requisicao))
                flash('success','Lançamento registrado. Para repetir os mesmos dados intencionalmente, toque em Novo lançamento.'); st.rerun()
            except Exception:
                st.error('Não foi possível registrar. Confira os dados e tente novamente.')

# 11. MÓDULO 2: FLUXO E PRIORIDADES
# =================================================================

elif menu == "📊 Fluxo e Prioridades":
    cabecalho_pagina("📋 Fluxo", "O que entra, o que sai e quando — com a renda que cobre cada conta.", "fluxo")
    st.caption("Uma agenda financeira simples: vencimento, valor, status e de onde vem o dinheiro.")
    _render_reorganizar_contas()
    _historico_baixas()
    df_todos_fluxo = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento ASC", (inicio_periodo, fim_periodo))
    df = df_todos_fluxo.copy()

    # A linha do tempo respeita o mês selecionado, inclusive após reorganizar.
    df_contexto_fluxo = _dados_operacionais(df)
    tab_fluxo = st.container()

    with tab_fluxo:
        if df.empty and df_contexto_fluxo.empty:
            render_empty_state("Nenhuma conta ou entrada neste mês", "Registre uma conta ou renda para começar a organizar o fluxo.", "○")
        else:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)
            df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce').fillna(0.0)

            df_ui = df_contexto_fluxo if not df_contexto_fluxo.empty else df
            df_ui['valor'] = pd.to_numeric(df_ui['valor'], errors='coerce').fillna(0.0)
            df_ui['valor_pago'] = pd.to_numeric(df_ui['valor_pago'], errors='coerce').fillna(0.0)
            ops_rapido = _consolidar_operacional(df_ui, consolidar_cartao=True)
            plano_fluxo = _montar_plano_pagamentos(ops_rapido, ano_selecionado, mes_selecionado)
            _render_fluxo2_ponte(plano_fluxo, ano_selecionado, mes_selecionado)

            filtro_rapido = st.radio(
                "Mostrar", ["Todos", "Pendentes", "A pagar", "A receber", "Pagos", "Atrasados"],
                horizontal=True, label_visibility="collapsed", key="fluxo_rapido_status"
            )
            tipos_rapidos, cats_sel_rapidas = [], []
            cats_rapidas = sorted([x for x in ops_rapido['categoria'].dropna().unique().tolist() if x]) if not ops_rapido.empty else []
            with st.expander("Filtros", expanded=False):
                fr1, fr2 = st.columns(2)
                tipos_rapidos = fr1.multiselect("Entradas ou despesas", ["Despesa", "Entrada"], placeholder="Todos", key="fluxo_rapido_tipos")
                cats_sel_rapidas = fr2.multiselect("Categoria", cats_rapidas, placeholder="Todas", key="fluxo_rapido_cats")

            vis_rapida = ops_rapido.copy()
            if tipos_rapidos:
                vis_rapida = vis_rapida[vis_rapida['tipo'].isin(tipos_rapidos)]
            if filtro_rapido == "Pendentes":
                vis_rapida = vis_rapida[vis_rapida['pago'] == 0]
            elif filtro_rapido == "A pagar":
                vis_rapida = vis_rapida[(vis_rapida['tipo'] == 'Despesa') & (vis_rapida['pago'] == 0)]
            elif filtro_rapido == "A receber":
                vis_rapida = vis_rapida[(vis_rapida['tipo'] == 'Entrada') & (vis_rapida['pago'] == 0)]
            elif filtro_rapido == "Pagos":
                vis_rapida = vis_rapida[vis_rapida['pago'] == 1]
            elif filtro_rapido == "Atrasados":
                vis_rapida = vis_rapida[vis_rapida['atrasado']]
            if cats_sel_rapidas:
                vis_rapida = vis_rapida[vis_rapida['categoria'].isin(cats_sel_rapidas)]

            if st.session_state.get('fluxo_ate'):
                ate = st.session_state['fluxo_ate']
                st.caption(f"Contas até {ate.strftime('%d/%m/%Y')}, incluindo atrasadas")
                vis_rapida = vis_rapida[vis_rapida['data_vencimento'] <= ate]
                if st.button('Mostrar todas as datas'):
                    st.session_state.pop('fluxo_ate',None)
                    st.rerun()
            _render_fluxo2_timeline(vis_rapida, ops_rapido, plano_fluxo, prefixo='fluxo2')

            st.caption("Edição estrutural, séries, exclusões e ferramentas técnicas ficam fora do uso diário.")

            with st.expander("••• Ferramentas avançadas", expanded=False):
                st.caption('Registros individuais. Faturas são pagas na linha do tempo; encargos e descontos pertencem à baixa.')
                st.subheader("🔍 Filtros")
                c_filt1, c_filt2 = st.columns(2)
                tipos_disp = df['tipo'].unique().tolist()
                with c_filt1: sel_tipo = st.multiselect("Filtrar por Tipo", tipos_disp, placeholder="Todos os Tipos")
                tipos_filtro = sel_tipo if sel_tipo else tipos_disp
                cat_disp = df[df['tipo'].isin(tipos_filtro)]['categoria'].unique().tolist()
                with c_filt2: sel_cat = st.multiselect("Filtrar por Categoria", cat_disp, placeholder="Todas as Categorias")
                cat_filtro = sel_cat if sel_cat else cat_disp
                df_view = df[df['tipo'].isin(tipos_filtro) & df['categoria'].isin(cat_filtro)].copy()

                df_view['id'] = df_view['id'].astype(str)
                df_view['ordem_pri'] = df_view['prioridade'].map(prioridades_map).fillna(2)
                df_view = df_view.sort_values(['data_vencimento', 'ordem_pri']).reset_index(drop=True)
                df_view['Pago'] = df_view['pago'].astype(bool)
                df_view.loc[df_view['pago'] == 0, 'valor_pago'] = float('nan')
                df_view['Data'] = pd.to_datetime(df_view['data_vencimento']).dt.date
                df_view['Data Pagamento'] = pd.to_datetime(df_view['data_pagamento'], errors='coerce').dt.date

                def calcular_alerta_atraso(row):
                    if not row['Pago'] and row['Data'] < hoje:
                        dias = (hoje - row['Data']).days
                        return f"🔴 Atrasado há {dias} dias"
                    return "🟢 Em dia"
                df_view['Alerta'] = df_view.apply(calcular_alerta_atraso, axis=1)

                def format_desc(row):
                    if pd.notna(row.get('total_parcelas')) and row['total_parcelas'] > 1 and row['total_parcelas'] != 999:
                        return f"{row['descricao']} ({int_seguro(row.get('parcela_atual'), 1)}/{int_seguro(row.get('total_parcelas'), 1)})"
                    return row['descricao']

                df_view['Desc. Exibição'] = df_view.apply(format_desc, axis=1)
                df_view.insert(0, '🗑️ Excluir', "")

                st.markdown(
                    "*Edite **Planejado** e **Pago/Recebido** separadamente. "
                    "Ao marcar **Pago**, o campo real vazio usa o Planejado; zero digitado é preservado. "
                    "Se o real for diferente, informe o valor recebido/pago e o Planejado será preservado.*"
                )
                edit_df = st.data_editor(
                    df_view[['🗑️ Excluir', 'Data', 'Data Pagamento', 'Alerta', 'prioridade', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago']],
                    use_container_width=True, hide_index=True,
                    column_config={
                        "🗑️ Excluir": st.column_config.SelectboxColumn("Excluir", options=["", "Este", "Este e Futuros"], width="small"),
                        "Data": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY", required=True),
                        "Data Pagamento": st.column_config.DateColumn("Pago em", format="DD/MM/YYYY"),
                        "Alerta": st.column_config.TextColumn("Status", disabled=True),
                        "valor": st.column_config.NumberColumn("Planejado", format="%.2f", min_value=0.0, required=True),
                        "valor_pago": st.column_config.NumberColumn("Pago/Recebido", format="%.2f"),
                        "prioridade": st.column_config.SelectboxColumn("Prioridade", options=["Alta 🔴", "Média 🟡", "Baixa 🟢"]),
                        "Desc. Exibição": st.column_config.TextColumn("Descrição", disabled=False)
                    }
                )

                edit_df['tipo'] = df_view['tipo'].values
                edit_df['ordem_pri'] = df_view['ordem_pri'].values

                if st.button("Salvar Alterações Rápidas", type="primary"):
                    try:
                        with transaction() as cur:
                            for i, row in edit_df.iterrows():
                                orig_row = df_view.loc[i]
                                novo_pago = 1 if bool(row['Pago']) else 0
                                if pd.isna(row.get('valor')):
                                    raise ValueError('Informe o valor planejado; use zero somente se for intencional.')
                                novo_valor = float(finance.money(row['valor']))
                                novo_valor_pago = resolver_valor_real(
                                    novo_pago,
                                    novo_valor,
                                    row.get('valor_pago')
                                )
                                orig_valor = float_seguro(orig_row.get('valor'))
                                orig_valor_pago = float_seguro(orig_row.get('valor_pago'))

                                orig_data_pgto = None
                                if pd.notna(orig_row.get('data_pagamento')):
                                    orig_data_pgto = pd.to_datetime(orig_row['data_pagamento']).date()
                                nova_data_pgto = row['Data Pagamento'] if pd.notna(row['Data Pagamento']) else None
                                if novo_pago == 1 and nova_data_pgto is None:
                                    nova_data_pgto = orig_data_pgto or hoje
                                if novo_pago == 0:
                                    nova_data_pgto = None

                                desc_editada = str(row['Desc. Exibição']) != str(orig_row['Desc. Exibição'])
                                nova_desc = ('' if pd.isna(row['Desc. Exibição']) else str(row['Desc. Exibição']).strip()) if desc_editada else orig_row['descricao']
                                excluir_futuros = row['🗑️ Excluir'] == "Este e Futuros"
                                excluir_algo = row['🗑️ Excluir'] in ("Este", "Este e Futuros")
                                data_pgto_mudou = nova_data_pgto != orig_data_pgto
                                mudou = (
                                    excluir_algo
                                    or novo_pago != int_seguro(orig_row.get('pago'))
                                    or abs(novo_valor - orig_valor) > 0.004
                                    or abs(novo_valor_pago - orig_valor_pago) > 0.004
                                    or str(row['prioridade']) != str(orig_row['prioridade'])
                                    or desc_editada
                                    or row['Data'] != orig_row['Data']
                                    or data_pgto_mudou
                                )
                                if not mudou:
                                    continue
                                from operations import quick_edit
                                quick_edit(cur, orig_row, {
                                    'pago': novo_pago, 'valor': novo_valor,
                                    'valor_pago': novo_valor_pago, 'data_pagamento': nova_data_pgto,
                                    'data_vencimento': row['Data'], 'descricao': nova_desc,
                                    'prioridade': row['prioridade'],
                                }, delete=excluir_algo, future=excluir_futuros)
                    except Exception as e:
                        st.error(f"Alterações canceladas; nenhuma edição parcial foi aplicada: {e}")
                    else:
                        flash("success", "✅ Alterações salvas em uma única transação!")
                        st.rerun()

                st.divider()

                if st.toggle("📱 Mostrar despesas pendentes para copiar", value=False, key="fluxo_texto_whatsapp"):
                    df_despesas_pendentes = edit_df[(edit_df['tipo'] == 'Despesa') & (~edit_df['Pago'])].sort_values(['ordem_pri', 'Data'])

                    if df_despesas_pendentes.empty:
                        st.info("Nenhuma despesa pendente identificada para este período.")
                    else:
                        texto_wpp = f"*Despesas Pendentes ({meses[mes_selecionado-1]}/{ano_selecionado})*\n\n"
                        t_wpp = 0.0

                        for _, r in df_despesas_pendentes.iterrows():
                            d_s = pd.to_datetime(r['Data']).strftime('%d/%m')
                            v_num = float(r['valor'])
                            texto_wpp += f"{d_s} - {r['Desc. Exibição']}: R$ {format_brl(v_num)}\n"
                            t_wpp += v_num

                        texto_wpp += f"\n*Total Pendente:* R$ {format_brl(t_wpp)}"
                        st.code(texto_wpp, language="markdown")

                st.divider()
                st.subheader("✏️ Alterar lançamento e série")
                mask_individuais = (~df['forma_pagamento'].isin(['Crédito'])) & (~df['descricao'].str.match(r'(?i)^plant[ãa]o\s', na=False)) & (df['pago'] == 0)
                df_edit = df[mask_individuais].copy() if not df.empty else df
                opcoes = {r['id']: f"{pd.to_datetime(r['data_vencimento']).strftime('%d/%m/%Y')} | {r['descricao']} (R$ {format_brl(r['valor'])})" for _, r in df_edit.iterrows()}
                sel_id = st.selectbox("Lançamento:", options=[None] + list(opcoes.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes[x])
                if sel_id:
                    r_sel = df[df['id'] == sel_id].iloc[0]
                    with st.container(border=True):
                        c_ed1, c_ed2 = st.columns(2)
                        with c_ed1:
                            e_tipo = st.radio("Tipo", ["Despesa", "Entrada"], index=0 if r_sel['tipo'] == 'Despesa' else 1, horizontal=True)
                            e_desc = st.text_input("Descrição", value=r_sel['descricao'])
                            e_val = st.text_input("Novo Valor (R$)", value=str(r_sel['valor']).replace('.', ','))
                            e_data = st.date_input("Nova Data de Vencimento", value=pd.to_datetime(r_sel['data_vencimento']).date(), format="DD/MM/YYYY")
                            opcoes_forma = ["À vista", "Crédito", "Outros"]
                            idx_forma = opcoes_forma.index(r_sel['forma_pagamento']) if r_sel['forma_pagamento'] in opcoes_forma else 2
                            e_forma = st.selectbox("Forma de Pagamento", opcoes_forma, index=idx_forma)
                        with c_ed2:
                            cat_options = list(ESTRUTURA[e_tipo].keys())
                            idx_cat = cat_options.index(r_sel['categoria']) if r_sel['categoria'] in cat_options else 0
                            e_cat = st.selectbox("Categoria", cat_options, index=idx_cat)
                            subs_disp = ESTRUTURA[e_tipo][e_cat] if e_cat in ESTRUTURA[e_tipo] else []
                            idx_sub = subs_disp.index(r_sel['subgrupo']) if r_sel['subgrupo'] in subs_disp else 0
                            e_sub = st.selectbox("Subgrupo", subs_disp, index=idx_sub)
                            e_escopo = st.radio("Aplicar alteração estrutural em:", ["Apenas neste lançamento", "Neste e em todos os futuros da mesma compra"])

                        if st.button("💾 Salvar alteração", type="primary"):
                            v_final = parse_valor(e_val)
                            if v_final <= 0:
                                st.error("O valor deve ser maior que zero.")
                            else:
                                try:
                                    with transaction() as cur:
                                        from operations import lock_entry, change_series
                                        current = lock_entry(cur, int(sel_id), int(r_sel['versao']))
                                        if e_forma != r_sel['forma_pagamento'] and (e_forma == 'Crédito' or r_sel['forma_pagamento'] == 'Crédito'):
                                            raise ValueError('Cadastre compras no cartão em Novo lançamento, vinculando o cartão e a fatura.')
                                        change_series(cur, current, {
                                            'tipo':e_tipo, 'categoria':e_cat, 'subgrupo':e_sub,
                                            'descricao':e_desc, 'valor':v_final, 'data_vencimento':e_data,
                                            'forma_pagamento':e_forma,
                                        }, future=e_escopo != 'Apenas neste lançamento')
                                except Exception as e:
                                    st.error(f"Mudança estrutural cancelada; nenhuma alteração parcial foi aplicada: {e}")
                                else:
                                    flash("success", "Lançamento atualizado de forma atômica!"); st.rerun()


    # =================================================================
# 12. MÓDULO 3: DEMONSTRATIVO (COM ANALÍTICO DE PROVISÕES)
# =================================================================

elif menu == "📑 Demonstrativo":
    # Cabeçalho compacto: título à esquerda e seletor de período à direita,
    # reproduzindo a hierarquia visual do mockup aprovado.
    ph1, ph2 = st.columns([4.7, 1.35], vertical_alignment="top")
    with ph1:
        st.markdown(
            "<div class='plan2-shell-head'><div class='plan2-head'><div class='plan2-title'>Planejamento</div>"
            "<div class='plan2-sub'>Acompanhe se sua vida financeira está seguindo o plano.</div></div></div>",
            unsafe_allow_html=True,
        )
    with ph2:
        st.markdown("<span class='plan2-period-anchor'></span>", unsafe_allow_html=True)
        periodos_plan = [(a, m) for a in range(hoje.year-3, hoje.year+6) for m in range(1, 13)]
        periodo_atual = (ano_selecionado, mes_selecionado)
        if st.session_state.get('plan2_period_picker') not in periodos_plan:
            st.session_state['plan2_period_picker'] = periodo_atual
        # Sincroniza quando o período foi alterado fora desta tela.
        if st.session_state.get('_plan2_last_period') != periodo_atual:
            st.session_state['plan2_period_picker'] = periodo_atual
            st.session_state['_plan2_last_period'] = periodo_atual
        periodo_novo = st.selectbox(
            "Período",
            periodos_plan,
            format_func=lambda x: f"▣  {meses[x[1]-1]} de {x[0]}",
            key='plan2_period_picker',
            label_visibility='collapsed',
        )
        if periodo_novo != periodo_atual:
            st.session_state['sb_ano'], st.session_state['sb_mes'] = int(periodo_novo[0]), int(periodo_novo[1])
            st.session_state['_plan2_last_period'] = periodo_novo
            st.rerun()

    st.caption('Receitas realizadas: recebidas no mês. Despesas: gastos pela competência, incluindo compras no crédito; pagamento de fatura não duplica o consumo.')
    df = _dados_mes()
    unidades = _planejamento_unidades(df, ano_selecionado, mes_selecionado)
    resumo = _planejamento_resumo(df, ano_selecionado, mes_selecionado, unidades=unidades)
    orcamentos_plan = _planejamento_orcamentos(df, ano_selecionado, mes_selecionado, unidades=unidades)
    dividas_plan = _planejamento_dividas()

    tab_visao, tab_cat, tab_div = st.tabs(["Visão geral", "Categorias", "Dívidas"])

    with tab_visao:
        # Três respostas apenas: o que planejei, o que aconteceu e a diferença.
        cards = [
            ("Receitas", "↗", "in", resumo['receita_planejada'], resumo['receita_realizada'], resumo['receita_realizada'] - resumo['receita_planejada'], 'receita'),
            ("Despesas", "↓", "out", resumo['despesa_planejada'], resumo['despesa_realizada'], resumo['despesa_realizada'] - resumo['despesa_planejada'], 'despesa'),
            ("Resultado", "▥", "result", resumo['resultado_planejado'], resumo['resultado_realizado'], resumo['resultado_realizado'] - resumo['resultado_planejado'], 'resultado'),
        ]
        cols = st.columns(3)
        for col, (nome, icone, classe_icon, plan, real, delta, natureza) in zip(cols, cards):
            card_class = {'in':'income','out':'outcome','result':'result-card'}.get(classe_icon, '')
            if abs(delta) <= 0.004:
                tom = 'neutral'; delta_txt = 'Em linha com o planejado'
            else:
                # Em despesas, gastar menos é favorável; nos demais, resultado maior é favorável.
                favoravel = (delta < 0) if natureza == 'despesa' else (delta > 0)
                tom = 'good' if favoravel else 'bad'
                sinal = '+' if delta > 0 else '−'
                delta_txt = f"{sinal} R$ {format_brl(abs(delta))}"
            col.markdown(
                f"<div class='plan2-summary {card_class}'><div class='plan2-summary-top'>"
                f"<div class='plan2-icon {classe_icon}'>{icone}</div><div class='plan2-summary-name'>{nome}</div></div>"
                f"<div class='plan2-pair'><div><div class='plan2-small-label'>Planejado</div><div class='plan2-big'>R$ {format_brl(plan)}</div></div>"
                f"<div><div class='plan2-small-label'>Realizado</div><div class='plan2-big'>R$ {format_brl(real)}</div></div></div>"
                f"<div class='plan2-delta {tom}'>{delta_txt}</div></div>",
                unsafe_allow_html=True,
            )

        if df.empty and unidades.empty:
            render_empty_state("Ainda não há dados para comparar", "Registre suas rendas e contas; o planejamento aparece automaticamente.", "○")
        else:
            # Prioriza estouros, categorias perto do limite e depois maiores diferenças absolutas.
            if not unidades.empty:
                desvios = unidades.copy()
                # A visão geral não chama de "desvio" uma categoria que ainda nem teve gasto.
                # Priorizamos o que já está acontecendo: estouros, proximidade do orçamento e uso relevante.
                desvios = desvios[(desvios['realizado'] > 0.01) | (desvios['percentual'] >= 80)].copy()
                desvios['_overspend'] = (desvios['diferenca'] > 0.01).astype(int)
                desvios['_near'] = ((desvios['percentual'] >= 80) & (desvios['diferenca'] <= 0.01)).astype(int)
                desvios['_abs'] = desvios['diferenca'].abs()
                desvios = desvios.sort_values(['_overspend','_near','percentual','realizado'], ascending=[False,False,False,False]).head(4)
                with st.container(border=True):
                    st.markdown("<span class='plan2-panel-anchor'></span>", unsafe_allow_html=True)
                    st.markdown("<div class='plan2-panel-head'><div class='plan2-panel-title'>Onde você está desviando</div><div class='plan2-panel-note'>Realizado até agora × plano do mês</div></div>", unsafe_allow_html=True)
                    if desvios.empty:
                        st.markdown("<div class='plan2-empty-inline'>Nenhum desvio relevante até agora.</div>", unsafe_allow_html=True)
                    else:
                        for _, r in desvios.iterrows():
                            _render_plan2_unidade(r, mostrar_categoria=True)

            c_orc, c_div = st.columns(2)
            with c_orc:
                with st.container(border=True):
                    st.markdown("<span class='plan2-panel-anchor'></span>", unsafe_allow_html=True)
                    st.markdown("<div class='plan2-panel-head'><div class='plan2-panel-title'>Orçamentos do mês</div><div class='plan2-panel-note'>Gasto realizado</div></div>", unsafe_allow_html=True)
                    if orcamentos_plan.empty:
                        st.markdown("<div class='plan2-panel-note'>Nenhum orçamento mensal definido.</div>", unsafe_allow_html=True)
                    else:
                        for _, r in orcamentos_plan.sort_values('percentual', ascending=False).head(5).iterrows():
                            pct = float(r['percentual'] or 0)
                            width = min(max(pct,0),100)
                            tom = 'bad' if pct > 100 else ('warn' if pct >= 90 else 'good')
                            st.markdown(
                                f"<div class='plan2-limit-row'><div><div class='plan2-name'>{html.escape(str(r['nome']))}</div>"
                                f"<div class='plan2-name-sub'>{html.escape(str(r['categoria'])) if str(r['nome']) != str(r['categoria']) else ''}</div></div>"
                                f"<div class='plan2-bar'><div class='plan2-fill {tom}' style='width:{width:.1f}%'></div></div>"
                                f"<div class='plan2-percent'>{pct:.0f}%</div>"
                                f"<div class='plan2-values'><b>R$ {format_brl(r['realizado'])}</b> de R$ {format_brl(r['orcamento'])}</div></div>",
                                unsafe_allow_html=True,
                            )

            with c_div:
                with st.container(border=True):
                    st.markdown("<span class='plan2-panel-anchor'></span>", unsafe_allow_html=True)
                    st.markdown("<div class='plan2-panel-head'><div class='plan2-panel-title'>Dívidas</div><div class='plan2-panel-note'>Parcelamentos ativos</div></div>", unsafe_allow_html=True)
                    ativas = dividas_plan[dividas_plan['saldo'] > 0.01] if not dividas_plan.empty else pd.DataFrame()
                    if ativas.empty:
                        st.markdown("<div class='plan2-panel-note'>Nenhuma dívida parcelada ativa.</div>", unsafe_allow_html=True)
                    else:
                        for _, r in ativas.sort_values('saldo', ascending=False).head(4).iterrows():
                            _render_plan2_divida(r)

    with tab_cat:
        st.markdown("### Categorias")
        st.caption("Acompanhe todas as categorias. Quando você define um orçamento mensal, o app compara automaticamente planejado e realizado.")
        if unidades.empty:
            render_empty_state("Nenhuma categoria para analisar", "As categorias aparecerão aqui conforme você registrar despesas.", "○")
        else:
            f1, f2 = st.columns([1.2, 1])
            somente_atencao = f1.checkbox("Mostrar só o que precisa de atenção", value=False, key="plan2_so_atencao")
            busca = f2.text_input("Buscar categoria", placeholder="Ex.: mercado", key="plan2_busca_categoria")
            view = unidades.copy()
            if somente_atencao:
                view = view[(view['diferenca'] > 0.01) | (view['percentual'] >= 90)]
            if busca.strip():
                q = busca.strip().lower()
                view = view[view.apply(lambda r: q in str(r['nome']).lower() or q in str(r['categoria']).lower(), axis=1)]
            view['_rank'] = view.apply(lambda r: 2 if r['diferenca'] > .01 else (1 if r['percentual'] >= 90 else 0), axis=1)
            view = view.sort_values(['_rank','percentual','realizado'], ascending=[False,False,False])
            with st.container(border=True):
                st.markdown("<span class='plan2-panel-anchor'></span>", unsafe_allow_html=True)
                if view.empty:
                    st.markdown("<div class='plan2-panel-note'>Nada corresponde a este filtro.</div>", unsafe_allow_html=True)
                else:
                    for _, r in view.iterrows():
                        _render_plan2_unidade(r, mostrar_categoria=True)
                        _planejamento_detalhes(r)

            with st.expander("✏️ Definir orçamento deste mês", expanded=False):
                st.caption("Opcional: defina quanto pretende gastar em uma categoria. Isso não cria conta nem lançamento.")
                cfg_desp = fetch_dataframe("SELECT categoria,subgrupo FROM categorias_personalizadas WHERE tipo='Despesa' ORDER BY categoria,subgrupo")
                opcoes = set()
                if not cfg_desp.empty:
                    opcoes |= {(str(r['categoria']), _sub_norm(r.get('subgrupo'))) for _, r in cfg_desp.iterrows()}
                if not unidades.empty:
                    opcoes |= {(str(r['categoria']), _sub_norm(r.get('subgrupo'))) for _, r in unidades.iterrows()}
                opcoes = sorted(opcoes)
                if not opcoes:
                    st.info("Crie uma categoria de despesa primeiro.")
                else:
                    escolha = st.selectbox("Categoria", opcoes, format_func=lambda x: f"{x[0]} · {x[1] or 'Geral'}", key="plan2_orc_categoria")
                    atual_df = _orcamentos_mes(ano_selecionado, mes_selecionado)
                    atual = 0.0
                    if not atual_df.empty:
                        mm = atual_df[(atual_df['categoria']==escolha[0]) & (atual_df['_sub']==escolha[1])]
                        if not mm.empty:
                            atual = float(mm.iloc[0]['valor_planejado'])
                    valor_orc = st.number_input("Orçamento do mês", min_value=0.0, step=50.0, value=float(atual), key=f"plan2_orc_val_{ano_selecionado}_{mes_selecionado}_{escolha[0]}_{escolha[1]}")
                    st.caption("Use R$ 0 para remover o orçamento. O realizado continuará sendo acompanhado normalmente.")
                    if st.button("Salvar orçamento", type="primary", key="plan2_orc_salvar"):
                        _salvar_orcamento_categoria(ano_selecionado, mes_selecionado, escolha[0], escolha[1], valor_orc)
                        flash('success', 'Orçamento do mês atualizado.')
                        st.rerun()

            if st.button("⚙️ Gerenciar categorias", key="plan2_editar_categorias"):
                st.session_state.menu_atual = "⚙️ Gerenciar Categorias"
                st.rerun()

    with tab_div:
        st.markdown("### Dívidas")
        st.caption("Acompanhe quanto falta, a parcela atual e o progresso dos seus parcelamentos.")
        if dividas_plan.empty:
            render_empty_state("Nenhuma dívida parcelada", "Parcelamentos aparecerão aqui automaticamente.", "✓")
        else:
            ativas = dividas_plan[dividas_plan['saldo'] > 0.01].copy()
            quitadas = dividas_plan[dividas_plan['saldo'] <= 0.01].copy()
            saldo_total = float(ativas['saldo'].sum()) if not ativas.empty else 0.0
            parcela_total = float(ativas['parcela_referencia'].sum()) if not ativas.empty else 0.0
            d1, d2, d3 = st.columns(3)
            with d1:
                render_kpi("Saldo restante", saldo_total, f"{len(ativas)} dívida(s) ativa(s)", "negative")
            with d2:
                render_kpi("Parcelas atuais", parcela_total, "soma aproximada das próximas parcelas")
            with d3:
                st.markdown(
                    f"<div class='ux-kpi'><div class='ux-kpi-label'>Parcelas restantes</div>"
                    f"<div class='ux-kpi-value ux-accent'>{int(ativas['parcelas_restantes'].sum()) if not ativas.empty else 0}</div>"
                    f"<div class='ux-kpi-note'>em todos os parcelamentos ativos</div></div>", unsafe_allow_html=True,
                )

            with st.container(border=True):
                st.markdown("<span class='plan2-panel-anchor'></span>", unsafe_allow_html=True)
                for _, r in ativas.sort_values('saldo', ascending=False).iterrows():
                    _render_plan2_divida(r)
                if ativas.empty:
                    st.markdown("<div class='plan2-panel-note'>Você não tem parcelamentos ativos.</div>", unsafe_allow_html=True)

            with st.expander("✏️ Credor e taxa de juros (opcional)"):
                op = {r['compra_id']: r['nome'] for _, r in dividas_plan.iterrows()}
                sel = st.selectbox('Dívida', [None] + list(op), format_func=lambda z: 'Selecione...' if z is None else op[z], key='plan2_div_sel')
                if sel:
                    lr = dividas_plan[dividas_plan['compra_id'] == sel].iloc[0]
                    cred = st.text_input('Credor', value=lr['credor'] if pd.notna(lr.get('credor')) else '', key=f"plan2_div_credor_{sel}")
                    taxa = st.number_input('Taxa mensal (%)', min_value=0.0, step=.1, value=float(lr['taxa_juros_mensal']) if pd.notna(lr.get('taxa_juros_mensal')) else 0.0, key=f"plan2_div_taxa_{sel}")
                    if st.button('Salvar informações', type='primary', key='plan2_div_save'):
                        execute_query("INSERT INTO info_dividas (compra_id,credor,taxa_juros_mensal) VALUES (%s,%s,%s) ON CONFLICT (compra_id) DO UPDATE SET credor=EXCLUDED.credor,taxa_juros_mensal=EXCLUDED.taxa_juros_mensal", (sel, cred.strip() or None, taxa if taxa > 0 else None))
                        flash('success', 'Informações salvas.')
                        st.rerun()

            if not quitadas.empty:
                with st.expander(f"Ver {len(quitadas)} dívida(s) quitada(s)"):
                    for _, r in quitadas.iterrows():
                        st.markdown(f"✓ **{html.escape(str(r['nome']))}** · quitada", unsafe_allow_html=True)

# =================================================================
# -----------------------------------------------------------------
# BALANÇO ANUAL
# -----------------------------------------------------------------
elif menu == "📈 Balanço Anual":
    cabecalho_pagina("📈 Balanço Anual", "Veja evolução mensal e compare com o ano anterior.")
    anos=fetch_dataframe("SELECT DISTINCT EXTRACT(YEAR FROM COALESCE(data_pagamento,data_vencimento))::int ano FROM lancamentos ORDER BY ano DESC")
    if anos.empty: render_empty_state('Ainda não há histórico anual', 'Registre movimentações em mais períodos para comparar o ano.', '○')
    else:
        lista=anos['ano'].astype(int).tolist(); ano_balanco=st.selectbox('Ano',lista,index=0)
        processar_recorrencias_periodos([(m,ano_balanco) for m in range(1,13)])
        ia,fa=limites_ano(ano_balanco)
        dfy=fetch_dataframe("SELECT * FROM lancamentos WHERE (pago=1 AND data_pagamento <= CURRENT_DATE AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ia,fa,ia,fa))
        if dfy.empty: render_empty_state('Nenhuma movimentação neste ano', 'Escolha outro ano ou registre novos lançamentos.', '○')
        else:
            dfy['valor']=pd.to_numeric(dfy['valor'],errors='coerce').fillna(0); dfy['valor_pago']=pd.to_numeric(dfy['valor_pago'],errors='coerce').fillna(0)
            dfy['data_h']=dfy.apply(lambda r:r['data_pagamento'] if int_seguro(r.get('pago'))==1 and pd.notna(r['data_pagamento']) else r['data_vencimento'],axis=1)
            dfy['mes_num']=pd.to_datetime(dfy['data_h']).dt.month
            def _valor_hibrido_ano(r):
                return float(r['valor_pago']) if int_seguro(r.get('pago')) == 1 else float(r['valor'])
            dfy['h']=dfy.apply(_valor_hibrido_ano,axis=1)
            mens=dfy.groupby(['mes_num','tipo'])['h'].sum().unstack(fill_value=0).reindex(range(1,13),fill_value=0).reset_index()
            for c in ['Entrada','Despesa']:
                if c not in mens: mens[c]=0.0
            mens['Resultado']=mens['Entrada']-mens['Despesa']; mens['Mês']=mens['mes_num'].apply(lambda m:meses[m-1][:3])
            te=float(mens['Entrada'].sum()); td=float(mens['Despesa'].sum()); res=te-td; margem=res/te*100 if te else 0
            # comparação ano anterior
            ip,fp=limites_ano(ano_balanco-1); prev=fetch_dataframe("SELECT tipo,valor,valor_pago,pago FROM lancamentos WHERE (pago=1 AND data_pagamento <= CURRENT_DATE AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ip,fp,ip,fp))
            pe=pdv=pr=0.0
            if not prev.empty:
                prev['valor']=pd.to_numeric(prev['valor'],errors='coerce').fillna(0); prev['valor_pago']=pd.to_numeric(prev['valor_pago'],errors='coerce').fillna(0)
                # Comparativo usa a mesma lógica de projeção, evitando somar limite + compras do limite duas vezes.
                prev_ent=prev[prev['tipo']=='Entrada']; pe=float(prev_ent.apply(lambda r:float(r['valor_pago']) if int_seguro(r.get('pago'))==1 else float(r['valor']),axis=1).sum()) if not prev_ent.empty else 0.0
                pdv=_total_despesa_projetada(prev); pr=pe-pdv
            def delta(cur,ant): return f"{((cur/ant)-1)*100:+.1f}% vs {ano_balanco-1}" if ant else None
            q1,q2,q3,q4=st.columns(4); q1.metric('Receita',f"R$ {format_brl(te)}",delta(te,pe)); q2.metric('Despesa',f"R$ {format_brl(td)}",delta(td,pdv),delta_color='inverse'); q3.metric('Resultado',f"R$ {format_brl(res)}",delta(res,pr)); q4.metric('Margem',f"{margem:.1f}%")
            st.subheader('Resultado mês a mês'); fig=px.bar(mens,x='Mês',y='Resultado',labels={'Resultado':'R$'}); st.plotly_chart(aplicar_tema_grafico(fig),use_container_width=True,key="finance_chart_1")
            st.subheader('Receitas x despesas'); fig2=px.bar(mens,x='Mês',y=['Entrada','Despesa'],barmode='group'); st.plotly_chart(aplicar_tema_grafico(fig2),use_container_width=True,key="finance_chart_2")
            mens['Acumulado'] = mens['Resultado'].cumsum()
            with st.expander('📈 Análises anuais adicionais'):
                st.subheader('Fluxo acumulado')
                fig_acum=px.area(mens,x='Mês',y='Acumulado',markers=True,labels={'Acumulado':'R$'})
                st.plotly_chart(aplicar_tema_grafico(fig_acum),use_container_width=True,key="finance_chart_3")
                ca1,ca2=st.columns(2)
                gasto_cat=dfy[dfy['tipo']=='Despesa'].groupby('categoria')['h'].sum().sort_values().reset_index()
                with ca1:
                    st.subheader('Distribuição por categoria')
                    if not gasto_cat.empty:
                        fig_cat=px.bar(gasto_cat,x='h',y='categoria',orientation='h',labels={'h':'R$','categoria':''})
                        st.plotly_chart(aplicar_tema_grafico(fig_cat),use_container_width=True,key="finance_chart_4")
                with ca2:
                    st.subheader('Maiores centros de custo (subgrupos)')
                    gasto_sub=dfy[dfy['tipo']=='Despesa'].groupby('subgrupo',dropna=False)['h'].sum().sort_values().tail(12).reset_index()
                    gasto_sub['subgrupo']=gasto_sub['subgrupo'].fillna('Geral')
                    if not gasto_sub.empty:
                        fig_sub=px.bar(gasto_sub,x='h',y='subgrupo',orientation='h',labels={'h':'R$','subgrupo':''})
                        st.plotly_chart(aplicar_tema_grafico(fig_sub),use_container_width=True,key="finance_chart_5")
            gasto=dfy[dfy['tipo']=='Despesa'].groupby('categoria')['h'].sum().sort_values().tail(12).reset_index();
            if not gasto.empty:
                st.subheader('Maiores centros de custo'); fig3=px.bar(gasto,x='h',y='categoria',orientation='h',labels={'h':'R$','categoria':''}); st.plotly_chart(aplicar_tema_grafico(fig3),use_container_width=True,key="finance_chart_6")

# -----------------------------------------------------------------
# DÍVIDAS
# -----------------------------------------------------------------
elif menu == "💳 Dívidas":
    cabecalho_pagina("💳 Dívidas", "Quanto falta, quanto pesa neste mês e quantos plantões isso representa.", "dividas")
    dd=_planejamento_dividas()
    if dd.empty: st.info('Nenhuma dívida parcelada encontrada.')
    else:
        total=float(dd['saldo'].clip(lower=0).sum()); ativos=int((dd['saldo']>.01).sum()); rest=int(dd['parcelas_restantes'].sum()); vm,np=calcular_valor_medio_plantao(hoje)
        mesdf=_dados_mes(); renda=float(mesdf[mesdf['tipo']=='Entrada'].apply(lambda r:float(r['valor_pago']) if int_seguro(r.get('pago'))==1 else float(r['valor']),axis=1).sum()) if not mesdf.empty else 0
        parcela_mes=float(mesdf[(mesdf['tipo']=='Despesa')&(mesdf['total_parcelas']>1)&(mesdf['total_parcelas']!=999)].apply(lambda r:float(r['valor_pago']) if int_seguro(r.get('pago'))==1 else float(r['valor']),axis=1).sum()) if not mesdf.empty else 0
        compromet=parcela_mes/renda*100 if renda else 0
        a,b,c,d=st.columns(4); a.metric('Saldo total',f"R$ {format_brl(total)}"); b.metric('Parcelas neste mês',f"R$ {format_brl(parcela_mes)}"); c.metric('Comprometimento da renda',f"{compromet:.1f}%" if renda else '—'); d.metric('Equivale a',f"{total/vm:.1f} plantões" if vm else '—')
        st.caption(f"{ativos} dívida(s) ativa(s) · {rest} parcela(s) restante(s) no total")
        for _,x in dd.sort_values('saldo',ascending=False).iterrows():
            nome=x['credor'] if pd.notna(x.get('credor')) and str(x['credor']).strip() else x['descricao']; tp=int(x['total_parcelas']); pp=int(x['parcelas_pagas']); prog=min(pp/tp,1)
            with st.container(border=True):
                c1,c2=st.columns([3,1.3]); c1.markdown(f"**{nome}**"); c1.caption(f"{x['categoria']} · {x['subgrupo'] or 'Geral'}"); c2.markdown(f"<div style='text-align:right'><b>R$ {format_brl(max(float(x['saldo']),0))}</b><br><span class='ux-muted'>restantes</span></div>",unsafe_allow_html=True)
                st.progress(prog); st.caption(f"{pp} de {tp} parcelas pagas")
                t1,t2,t3=st.columns(3); t1.caption(f"Próxima: {pd.to_datetime(x['proxima_parcela']).strftime('%d/%m/%Y') if pd.notna(x['proxima_parcela']) else '—'}"); t2.caption(f"Termina: {meses[pd.to_datetime(x['data_fim']).month-1]} {pd.to_datetime(x['data_fim']).year}"); t3.caption(f"≈ {(x['valor_total']/tp)/vm:.1f} plantão/mês" if vm else '')
        with st.expander('✏️ Credor e taxa (opcional)'):
            op={r['compra_id']:(r['credor'] if pd.notna(r.get('credor')) and str(r.get('credor')).strip() else r['descricao']) for _,r in dd.iterrows()}; sel=st.selectbox('Dívida',[None]+list(op),format_func=lambda z:'Selecione...' if z is None else op[z])
            if sel:
                lr=dd[dd['compra_id']==sel].iloc[0]; cred=st.text_input('Credor',value=lr['credor'] if pd.notna(lr.get('credor')) else ''); taxa=st.number_input('Taxa mensal (%)',min_value=0.0,step=.1,value=float(lr['taxa_juros_mensal']) if pd.notna(lr.get('taxa_juros_mensal')) else 0.0)
                if st.button('Salvar informações',type='primary'):
                    execute_query("INSERT INTO info_dividas (compra_id,credor,taxa_juros_mensal) VALUES (%s,%s,%s) ON CONFLICT (compra_id) DO UPDATE SET credor=EXCLUDED.credor,taxa_juros_mensal=EXCLUDED.taxa_juros_mensal",(sel,cred.strip() or None,taxa if taxa>0 else None)); flash('success','Informações salvas.'); st.rerun()

# -----------------------------------------------------------------
# RENDAS 2.0
# -----------------------------------------------------------------
elif menu == "💰 Rendas":
    rh1,rh2=st.columns([4.7,1.35],vertical_alignment='top')
    with rh1:
        st.markdown("<div class='income2-head'><div class='income2-title'>Rendas</div><div class='income2-sub'>Veja de onde vem seu dinheiro, quanto já entrou e o que ainda está previsto.</div></div>",unsafe_allow_html=True)
    with rh2:
        st.markdown("<span class='income2-period-anchor'></span>",unsafe_allow_html=True)
        periodos_renda=[(a,m) for a in range(hoje.year-3,hoje.year+6) for m in range(1,13)]; atual=(ano_selecionado,mes_selecionado)
        if st.session_state.get('income2_period_picker') not in periodos_renda: st.session_state['income2_period_picker']=atual
        if st.session_state.get('_income2_last_period') != atual: st.session_state['income2_period_picker']=atual; st.session_state['_income2_last_period']=atual
        novo=st.selectbox('Período',periodos_renda,format_func=lambda x:f"▣  {meses[x[1]-1]} de {x[0]}",key='income2_period_picker',label_visibility='collapsed')
        if novo != atual: st.session_state['sb_ano'],st.session_state['sb_mes']=int(novo[0]),int(novo[1]); st.session_state['_income2_last_period']=novo; st.rerun()

    df_rendas_mes=_dados_mes(); fontes=_rendas_fontes_periodo(df_rendas_mes,ano_selecionado,mes_selecionado); recebimentos=_rendas_recebimentos_janela(ano_selecionado,mes_selecionado)
    esperado=sum(f['esperado'] for f in fontes); recebido=sum(f['realizado'] for f in fontes); previsto=sum(f['pendente'] for f in fontes); nfontes=len(fontes)
    pct_receb=(recebido/esperado*100) if esperado>0 else (100 if recebido>0 else 0); pct_prev=(previsto/esperado*100) if esperado>0 else 0
    kc=st.columns(4); kpis=[('↗','green','Esperado no mês',f"R$ {format_brl(esperado)}",'Total previsto de todas as fontes',None),('▣','blue','Recebido até agora',f"R$ {format_brl(recebido)}",f"{pct_receb:.0f}% do esperado",pct_receb),('◷','amber','Ainda previsto',f"R$ {format_brl(previsto)}",f"{pct_prev:.0f}% do esperado",pct_prev),('◇','purple','Fontes ativas',str(nfontes),'Fontes de renda cadastradas',None)]
    for col,(ico,cor,rot,val,nota,pct) in zip(kc,kpis):
        with col:
            prog='' if pct is None else f"<div class='income2-progress {'amber' if cor=='amber' else ''}'><span style='width:{min(max(pct,0),100):.1f}%'></span></div>"
            st.markdown(f"<div class='income2-kpi'><div class='income2-kpi-top'><div class='income2-kpi-icon {cor}'>{ico}</div><div><div class='income2-kpi-label'>{rot}</div><div class='income2-kpi-value'>{val}</div></div></div>{prog}<div class='income2-kpi-note'>{nota}</div></div>",unsafe_allow_html=True)

    left,right=st.columns([1.45,1],gap='medium')
    with left:
        with st.container(border=True):
            st.markdown("<span class='income2-panel-anchor'></span>",unsafe_allow_html=True); h1,h2=st.columns([3,1.15],vertical_alignment='center')
            h1.markdown("<div class='income2-panel-title'>Fontes de renda</div><div class='income2-panel-note'>Uma visão simples das origens do seu dinheiro.</div>",unsafe_allow_html=True)
            if h2.button('＋ Adicionar fonte',key='income2_add_btn',use_container_width=True): st.session_state['income2_add_open']=not st.session_state.get('income2_add_open',False)
            if st.session_state.get('income2_add_open'):
                especial=st.checkbox('Ativar gestão de plantões',value=False,key='income2_new_shifts',help='A renda será calculada automaticamente pela agenda.')
                with st.form('income2_add_form',clear_on_submit=False):
                    a1,a2=st.columns([1.6,1])
                    nome=a1.text_input('Nome da fonte',placeholder='Ex.: Consultório, Hospital Help')
                    modalidade=a2.selectbox('Tipo da renda',['Mensal','Variável','Eventual'])
                    b1,b2,b3=st.columns(3)
                    valor=b1.number_input('Calculado pela agenda' if especial else 'Valor esperado',min_value=0.0,step=100.0,format='%.2f',disabled=especial)
                    dia=b2.number_input('Dia de recebimento',min_value=1,max_value=31,value=10)
                    atraso=b3.number_input('Meses até receber',min_value=0,max_value=6,value=0)
                    if st.form_submit_button('Salvar fonte',type='primary',use_container_width=True):
                        if not nome.strip():
                            st.error('Informe um nome para a fonte.')
                        else:
                            rec=1 if modalidade in ('Mensal','Variável') and valor>0 and not especial else 0
                            execute_query('''INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio,is_producao_variavel,modalidade_renda) VALUES ('Entrada',%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',('Rendas',nome.strip(),valor if valor>0 and not especial else None,int(atraso),int(dia),rec,datetime.date(ano_selecionado,mes_selecionado,1),1 if especial else 0,modalidade))
                            invalidar_caches_estruturais()
                            st.session_state.pop(f"rec_processado_{mes_selecionado}_{ano_selecionado}",None)
                            st.session_state['income2_add_open']=False
                            flash('success','Fonte de renda adicionada.')
                            st.rerun()
            if not fontes: st.markdown("<div class='plan2-empty-inline'>Nenhuma fonte cadastrada. Adicione a primeira para organizar seus recebimentos.</div>",unsafe_allow_html=True)
            for idx,f in enumerate(fontes):
                with st.container(border=True):
                    st.markdown("<span class='income2-source-anchor'></span>",unsafe_allow_html=True)
                    c1,c2,c3=st.columns([3.1,1.5,.95],vertical_alignment='center')
                    badge=_renda_badge_class(f['modalidade'])
                    media_txt=''
                    if f.get('media',0)>0.004 and int(f.get('media_meses',0) or 0)>=2:
                        media_txt=f"<div class='income2-source-meta'>Média histórica ({int(f['media_meses'])} meses): R$ {format_brl(f['media'])}</div>"
                    especial_txt="<span class='income2-badge blue'>Plantões ativos</span>" if f['especializada'] else ''
                    c1.markdown(f"<div class='income2-source-name'>{html.escape(f['nome'])}<span class='income2-badge {badge}'>{html.escape(f['modalidade'])}</span>{especial_txt}</div><div class='income2-source-money'>Esperado neste mês <b>R$ {format_brl(f['esperado'])}</b></div>"+media_txt+(f"<div class='income2-source-meta'>▣ Normalmente recebe dia {f['dia_pagamento']}</div>" if f['dia_pagamento'] else "<div class='income2-source-meta'>Sem recorrência fixa</div>"),unsafe_allow_html=True)
                    if f['especializada'] or f.get('quantidade_plantoes',0):
                        c1.caption(f"Calculado pela agenda · {f.get('quantidade_plantoes',0)} plantão(ões) com recebimento previsto neste mês")
                        if c1.button('Ver plantões',key=f"income2_agenda_{idx}"):
                            st.session_state['rendas_fonte_filtro']=f['nome']
                            st.session_state.menu_atual='🏥 Escala de Plantões'
                            st.rerun()
                    prox=f['proxima_data'].strftime('%d/%m') if f['proxima_data'] else '—'
                    sit='Previsto' if f['pendente']>0.004 else ('Recebido' if f['realizado']>0.004 else 'Sem previsão')
                    c2.markdown(f"<div class='income2-source-meta'>Próximo recebimento</div><div class='income2-source-money'><b>{prox}</b></div><div class='income2-source-meta'>{sit} · R$ {format_brl(f['pendente'] if f['pendente']>0.004 else f['realizado'])}</div>",unsafe_allow_html=True)
                    if f['id']:
                        if c3.button('Editar ›',key=f"income2_edit_{idx}",use_container_width=True):
                            atual=st.session_state.get('income2_edit_id')
                            st.session_state['income2_edit_id']=None if atual==f['id'] else f['id']
                            st.rerun()
                    else:
                        c3.markdown("<div style='text-align:right'><span class='income2-status expected'>Importada</span></div>",unsafe_allow_html=True)

                    delete_key=f"income2_delete_{f['key']}"
                    if c3.button('Excluir fonte',key=delete_key):
                        st.session_state['income2_delete_key']=f['key']
                    if st.session_state.get('income2_delete_key')==f['key']:
                        st.warning(f'Excluir esta fonte também apaga da agenda os plantões de {hoje:%d/%m/%Y} em diante. Plantões anteriores e lançamentos que não são plantões serão preservados. Recebimentos confirmados serão preservados. A exclusão ficará no histórico.')
                        yes,no=st.columns(2)
                        if yes.button('Confirmar exclusão da fonte',key=delete_key+'_confirm'):
                            from operations import delete_income_source
                            with ui_transaction() as cur:
                                deleted=delete_income_source(cur,f['categoria'],f['subgrupo'],hoje)
                            invalidar_caches_estruturais()
                            st.session_state.pop('income2_delete_key',None)
                            flash('success',f'Fonte excluída e {deleted} plantão(ões) removido(s) a partir de hoje. Plantões anteriores preservados.')
                            st.rerun()
                        if no.button('Cancelar exclusão',key=delete_key+'_cancel'):
                            st.session_state.pop('income2_delete_key',None)
                            st.rerun()

                    if st.session_state.get('income2_edit_id')==f.get('id') and f.get('id'):
                        tipos=['Mensal','Variável','Eventual']
                        tipo_atual=f['modalidade'] if f['modalidade'] in tipos else 'Variável'
                        ee=st.checkbox('Gestão de plantões',value=bool(f['especializada']),key=f"income2_edit_shifts_{f['id']}",help='O valor da renda vem exclusivamente da agenda.')
                        with st.form(f"income2_edit_form_{f['id']}"):
                            st.caption('Alterar o dia, o prazo ou o valor padrão atualiza as previsões do mês atual e seguintes. Recebidos, meses anteriores e exceções individuais são preservados.')
                            e1,e2=st.columns([1.2,1])
                            em=e1.selectbox('Tipo da renda',tipos,index=tipos.index(tipo_atual))
                            e3,e4,e5=st.columns(3)
                            ev=e3.number_input('Calculado pela agenda' if ee else 'Valor esperado',min_value=0.0,value=float(f['esperado'] if ee else (f['valor_padrao'] or 0)),step=100.0,format='%.2f',disabled=ee)
                            if ee: st.caption('Para alterar a renda, edite os plantões na agenda.')
                            ed=e4.number_input('Dia de recebimento',min_value=1,max_value=31,value=int(f['dia_pagamento'] or 10))
                            ea=e5.number_input('Meses até receber',min_value=0,max_value=6,value=int(f['atraso_meses'] or 0))
                            sb1,sb2=st.columns(2)
                            salvar=sb1.form_submit_button('Salvar alterações',type='primary',use_container_width=True)
                            cancelar=sb2.form_submit_button('Cancelar',use_container_width=True)
                            if salvar:
                                rec=1 if em in ('Mensal','Variável') and ev>0 and not ee else 0
                                from operations import edit_income_source
                                with ui_transaction() as cur:
                                    reagendados=edit_income_source(cur,int(f['id']),ev if ev>0 else None,int(ed),int(ea),rec,1 if ee else 0,em,hoje)
                                invalidar_caches_estruturais()
                                st.session_state.pop(f"rec_processado_{mes_selecionado}_{ano_selecionado}",None)
                                st.session_state.pop('income2_edit_id',None)
                                flash('success',f'Fonte atualizada. {reagendados} recebimento(s) pendente(s) reagendado(s) no mês atual e seguintes.')
                                st.rerun()
                            if cancelar:
                                st.session_state.pop('income2_edit_id',None)
                                st.rerun()

    with right:
        with st.container(border=True):
            st.markdown("<span class='income2-panel-anchor'></span><div class='income2-panel-title'>Próximos recebimentos</div><div class='income2-panel-note'>O mês selecionado e os primeiros dias do próximo.</div>",unsafe_allow_html=True)
            if not recebimentos: st.markdown("<div class='plan2-empty-inline'>Nenhum recebimento previsto neste período.</div>",unsafe_allow_html=True)
            for r in recebimentos[:6]: st.markdown(f"<div class='income2-timeline-row'><div class='income2-date'>{r['data'].strftime('%d/%m')}</div><div class='income2-name'>{html.escape(r['nome'])}</div><div class='income2-value'>R$ {format_brl(r['valor'])}</div><div><span class='income2-status {r['classe']}'>{r['status']}</span></div></div>",unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown("<span class='income2-panel-anchor'></span><div class='income2-panel-title'>Visão por fonte</div><div class='income2-panel-note'>Quanto do esperado já foi recebido.</div>",unsafe_allow_html=True)
            for f in sorted(fontes,key=lambda x:x['esperado'],reverse=True)[:6]:
                pct=(f['realizado']/f['esperado']*100) if f['esperado']>0 else (100 if f['realizado']>0 else 0); st.markdown(f"<div class='income2-progress-row'><div class='income2-name'>{html.escape(f['nome'])}</div><div class='income2-mini-bar'><span style='width:{min(max(pct,0),100):.1f}%'></span></div><div class='income2-mini-values'><b>R$ {format_brl(f['realizado'])}</b> de R$ {format_brl(f['esperado'])}</div></div>",unsafe_allow_html=True)

    with st.container(border=True):
        st.markdown("<span class='income2-panel-anchor'></span><div class='income2-panel-title'>Modo especializado</div><div class='income2-panel-note'>Plantões é um recurso adicional. A fonte continua sendo Mensal, Variável ou Eventual.</div>",unsafe_allow_html=True)
        configuradas=[f for f in fontes if f.get('id')]
        if not configuradas:
            st.markdown("<div class='plan2-empty-inline'>Cadastre uma fonte para ativar recursos especializados.</div>",unsafe_allow_html=True)
        for i,f in enumerate(configuradas):
            x1,x2,x3=st.columns([4.2,1.1,1.2],vertical_alignment='center')
            estado='Plantões ativos' if f['especializada'] else 'Modo padrão'
            x1.markdown(f"<div class='income2-special-copy'><b>{html.escape(f['nome'])}</b><span>{html.escape(f['modalidade'])} · {estado}</span></div>",unsafe_allow_html=True)
            desejado=x2.toggle('Plantões',value=bool(f['especializada']),key=f"income2_special_toggle_{f['id']}")
            if desejado!=bool(f['especializada']):
                rec=1 if f['modalidade'] in ('Mensal','Variável') and float_seguro(f['valor_padrao'])>0 and not desejado else 0
                execute_query('UPDATE categorias_personalizadas SET is_producao_variavel=%s,is_recorrente=%s WHERE id=%s',(1 if desejado else 0,rec,int(f['id'])))
                invalidar_caches_estruturais()
                st.session_state.pop(f"rec_processado_{mes_selecionado}_{ano_selecionado}",None)
                flash('success','Gestão de plantões atualizada.')
                st.rerun()
            if f['especializada']:
                if x3.button('Gerenciar ›',key=f'income2_special_{i}',use_container_width=True):
                    st.session_state['rendas_fonte_filtro']=f['nome']
                    st.session_state.menu_atual='🏥 Escala de Plantões'
                    st.rerun()
            else:
                x3.caption('Opcional')

# -----------------------------------------------------------------
# PLANTÕES — modo especializado de Rendas
# -----------------------------------------------------------------
elif menu == "🏥 Escala de Plantões":
    from agenda_page import render_agenda
    from shift_page import render_shift_page
    render_agenda(ano_selecionado,mes_selecionado,hoje)
    with st.expander('Plantões e previsões de recebimento'):
        render_shift_page(ano_selecionado,mes_selecionado,hoje,data_contexto_ativo,meses,render_periodo_topo,flash,parse_valor,aplicar_tema_grafico)

# -----------------------------------------------------------------
# MAIS — concentra recursos que não precisam competir na navegação diária
# -----------------------------------------------------------------
elif menu == "⚙️ Mais":
    nome_exibicao = preferencia_get('nome_exibicao', 'Conta pessoal') or 'Conta pessoal'
    st.markdown(
        f"<div class='more2-head'><div><div class='more2-title'>Mais</div>"
        f"<div class='more2-sub'>Configurações e ferramentas para organizar seu financeiro.</div></div>"
        f"<div class='more2-profile'><div class='more2-profile-name'>{html.escape(nome_exibicao)}</div>"
        f"<div class='more2-profile-sub'>Conta pessoal · configurações</div></div></div>",
        unsafe_allow_html=True,
    )

    def _more2_go(label, key, destino, icon, title, desc, tone='blue', extra=None):
        with st.container(border=True):
            st.markdown("<span class='more2-card-anchor'></span>", unsafe_allow_html=True)
            ccopy, cbtn = st.columns([6.4, .8], vertical_alignment='center')
            with ccopy:
                st.markdown(
                    f"<div class='more2-card-copy'><div class='more2-card-icon {tone}'>{icon}</div>"
                    f"<div><div class='more2-card-title'>{html.escape(title)}</div>"
                    f"<div class='more2-card-desc'>{html.escape(desc)}</div></div></div>",
                    unsafe_allow_html=True,
                )
            with cbtn:
                if st.button('›', key=key, use_container_width=True, help=label):
                    if extra:
                        for k,v in extra.items(): st.session_state[k]=v
                    st.session_state.menu_atual = destino
                    st.rerun()

    def _more2_section(icon, title, subtitle, cards):
        with st.container(border=True):
            st.markdown("<span class='more2-section-anchor'></span>", unsafe_allow_html=True)
            st.markdown(
                f"<div class='more2-section-head'><div class='more2-section-icon'>{icon}</div>"
                f"<div><div class='more2-section-title'>{html.escape(title)}</div>"
                f"<div class='more2-section-sub'>{html.escape(subtitle)}</div></div></div>",
                unsafe_allow_html=True,
            )
            for i in range(0, len(cards), 2):
                cols = st.columns(2)
                for col, card in zip(cols, cards[i:i+2]):
                    with col: _more2_go(**card)

    _more2_section('▰', 'Organização', 'Gerencie as informações principais do app.', [
        dict(label='Categorias',key='m2_cat',destino='⚙️ Gerenciar Categorias',icon='▣',title='Categorias',desc='Crie e edite categorias de receitas e despesas.',tone='blue'),
        dict(label='Recorrências',key='m2_rec',destino='🔄 Recorrências',icon='◫',title='Recorrências',desc='Veja contas e receitas geradas automaticamente.',tone='green'),
        dict(label='Fontes de renda',key='m2_fontes',destino='💰 Rendas',icon='◉',title='Fontes de renda',desc='Adicione e edite suas fontes de renda.',tone='purple'),
        dict(label='Orçamentos',key='m2_orc',destino='📑 Demonstrativo',icon='◎',title='Orçamentos',desc='Defina e acompanhe orçamentos por categoria.',tone='pink'),
    ])

    _more2_section('▤', 'Dados', 'Faça backup, restaure ou exporte seus dados.', [
        dict(label='Backup',key='m2_backup',destino='💾 Backup e Restauração',icon='⇧',title='Backup',desc='Crie uma cópia de segurança completa.',tone='green'),
        dict(label='Restaurar backup',key='m2_restore',destino='💾 Backup e Restauração',icon='↥',title='Restaurar backup',desc='Restaure seus dados a partir de ZIP ou CSV legado.',tone='purple'),
        dict(label='Exportar dados',key='m2_export',destino='📤 Exportar Dados',icon='⇩',title='Exportar dados',desc='Baixe lançamentos, categorias e orçamentos em CSV.',tone='amber'),
        dict(label='Importar dados',key='m2_import',destino='💾 Backup e Restauração',icon='↧',title='Importar dados',desc='Importe um backup completo ou arquivo legado.',tone='orange'),
    ])

    _more2_section('●', 'Conta e preferências', 'Ajuste o app ao seu jeito.', [
        dict(label='Perfil',key='m2_perfil',destino='👤 Perfil e Preferências',icon='○',title='Perfil',desc='Nome de exibição e informações básicas.',tone='pink',extra={'config_tab':'Perfil'}),
        dict(label='Aparência',key='m2_apar',destino='👤 Perfil e Preferências',icon='◌',title='Aparência',desc='Veja o tema visual ativo do app.',tone='blue',extra={'config_tab':'Aparência'}),
        dict(label='Preferências',key='m2_pref',destino='👤 Perfil e Preferências',icon='⚙',title='Preferências do app',desc='Escolha onde o app abre por padrão.',tone='green',extra={'config_tab':'Preferências'}),
        dict(label='Segurança',key='m2_sec',destino='👤 Perfil e Preferências',icon='▢',title='Segurança',desc='Veja o modo de acesso e encerre a sessão.',tone='purple',extra={'config_tab':'Segurança'}),
    ])

    _more2_section('◆', 'Avançado', 'Ferramentas adicionais e manutenção.', [
        dict(label='Manutenção',key='m2_maint',destino='🧰 Manutenção e Diagnóstico',icon='⌕',title='Manutenção',desc='Rotinas de correção e limpeza de dados.',tone='amber'),
        dict(label='Diagnóstico',key='m2_diag',destino='🩺 Diagnóstico',icon='⌁',title='Diagnóstico',desc='Verifique banco, dados e status do app.',tone='green'),
        dict(label='Ferramentas técnicas',key='m2_tools',destino='🧰 Manutenção e Diagnóstico',icon='>_',title='Ferramentas técnicas',desc='Acesse ferramentas administrativas avançadas.',tone='pink'),
        dict(label='Refazer configuração inicial',key='m2_onb',destino='⚙️ Mais',icon='↶',title='Refazer configuração inicial',desc='Abra o onboarding novamente sem apagar seus dados.',tone='purple',extra={'wizard_ativo':True,'wizard_passo':1,'wizard_rendas':[],'wizard_contas':[],'wizard_orcamentos':[]}),
    ])

# -----------------------------------------------------------------
# RECORRÊNCIAS — visão simples; edição estrutural continua em Categorias
# -----------------------------------------------------------------
elif menu == "🔄 Recorrências":
    cabecalho_pagina("🔄 Recorrências", "Contas e receitas que o app gera automaticamente a cada mês.")
    if st.button("← Voltar para Mais", key="rec_back"): st.session_state.menu_atual='⚙️ Mais'; st.rerun()
    rec = fetch_dataframe("""SELECT id,tipo,categoria,subgrupo,valor_padrao,dia_pagamento,atraso_meses,data_inicio
                              FROM categorias_personalizadas WHERE COALESCE(is_recorrente,0)=1 AND (data_fim IS NULL OR data_fim>=CURRENT_DATE)
                              UNION ALL SELECT NULL,tipo,categoria,COALESCE(subgrupo,descricao),valor,dia,0,inicio FROM series_manuais
                              WHERE fim IS NULL OR fim>=CURRENT_DATE ORDER BY tipo,categoria,subgrupo""")
    if rec.empty:
        render_empty_state("Nenhuma recorrência ativa", "Quando uma conta ou renda se repetir todo mês, ela aparecerá aqui.", "↻")
    else:
        for _,r in rec.iterrows():
            nome = str(r.get('subgrupo') or r.get('categoria') or 'Recorrência')
            tipo = str(r.get('tipo') or '')
            valor = float_seguro(r.get('valor_padrao'))
            detalhe = f"R$ {format_brl(valor)} por mês" if valor>0 else "Valor variável"
            if tipo=='Entrada' and int_seguro(r.get('dia_pagamento'))>0: detalhe += f" · normalmente dia {int_seguro(r.get('dia_pagamento'))}"
            st.markdown(f"<div class='ux-card'><b>{'↗' if tipo=='Entrada' else '↘'} {html.escape(nome)}</b><br><span class='ux-muted'>{html.escape(detalhe)}</span></div>",unsafe_allow_html=True)
    if st.button("Gerenciar categorias e recorrências", use_container_width=True): st.session_state.menu_atual='⚙️ Gerenciar Categorias'; st.rerun()

# -----------------------------------------------------------------
# PERFIL E PREFERÊNCIAS
# -----------------------------------------------------------------
elif menu == "👤 Perfil e Preferências":
    cabecalho_pagina("Conta e preferências", "Ajustes leves da experiência. Dados financeiros continuam separados destas preferências.")
    if st.button("← Voltar para Mais", key="pref_back"): st.session_state.menu_atual='⚙️ Mais'; st.rerun()
    tabs = ['Perfil','Aparência','Preferências','Segurança']
    inicial = st.session_state.get('config_tab','Perfil')
    try: idx = tabs.index(inicial)
    except ValueError: idx = 0
    # st.tabs não permite seleção programática portátil; mostramos o alvo primeiro para manter o clique útil.
    ordered = [tabs[idx]] + [x for x in tabs if x != tabs[idx]]
    t1,t2,t3,t4 = st.tabs(ordered)
    for tab,nome_tab in zip([t1,t2,t3,t4],ordered):
        with tab:
            if nome_tab=='Perfil':
                nome_atual=preferencia_get('nome_exibicao','') or ''
                nome_novo=st.text_input('Nome de exibição',value=nome_atual,placeholder='Como você quer aparecer no app?')
                st.caption('Esta preferência é apenas visual e não altera autenticação ou dados financeiros.')
                if st.button('Salvar perfil',type='primary',key='save_profile'):
                    preferencia_set('nome_exibicao',nome_novo.strip()); flash('success','Perfil atualizado.'); st.rerun()
            elif nome_tab=='Aparência':
                st.markdown('**Tema atual: Escuro refinado**')
                st.caption('A versão 2.0 usa uma linguagem visual única para manter consistência entre Home, Fluxo, Planejamento e Rendas.')
                st.info('Outros temas ficam fora desta versão para evitar fragmentar a experiência visual.')
            elif nome_tab=='Preferências':
                atual=preferencia_get('pagina_inicial','Início') or 'Início'
                op=['Início','Fluxo','Planejamento','Rendas']
                escolha=st.selectbox('Abrir o app em',op,index=op.index(atual) if atual in op else 0)
                if st.button('Salvar preferência',type='primary',key='save_pref'):
                    preferencia_set('pagina_inicial',escolha); flash('success','Preferência salva. Ela vale para a próxima sessão.'); st.rerun()
            else:
                protegido=bool(os.environ.get('APP_PASSWORD') or os.environ.get('APP_USERS_JSON'))
                st.markdown(f"**Acesso por senha:** {'Ativo' if protegido else 'Não configurado'}")
                st.caption('A senha continua sendo definida no ambiente de deploy; esta versão não altera o modelo de autenticação.')
                if st.button('Encerrar sessão',key='logout_app'):
                    st.session_state.clear()
                    st.rerun()

# -----------------------------------------------------------------
# EXPORTAÇÃO CSV
# -----------------------------------------------------------------
elif menu == "📤 Exportar Dados":
    cabecalho_pagina("📤 Exportar dados", "Baixe cópias legíveis dos principais dados sem alterar o banco.")
    if st.button("← Voltar para Mais", key="exp_back"): st.session_state.menu_atual='⚙️ Mais'; st.rerun()
    export_sets = [
        ('Lançamentos','lancamentos.csv',_df_raw('lancamentos')),
        ('Categorias','categorias.csv',fetch_dataframe('SELECT * FROM categorias_personalizadas ORDER BY tipo,categoria,subgrupo')),
        ('Orçamentos','orcamentos.csv',fetch_dataframe('SELECT * FROM orcamentos_categorias ORDER BY competencia,categoria,subgrupo')),
        ('Dívidas','dividas.csv',fetch_dataframe('SELECT * FROM info_dividas ORDER BY compra_id')),
    ]
    for titulo,nome,dfx in export_sets:
        with st.container(border=True):
            c1,c2=st.columns([4,1],vertical_alignment='center')
            c1.markdown(f"**{titulo}**")
            c1.caption(f"{len(dfx)} registro(s)")
            c2.download_button('Baixar CSV',data=dfx.to_csv(index=False).encode('utf-8-sig'),file_name=nome,mime='text/csv',key=f'dl_{nome}',use_container_width=True)
    st.divider()
    if st.button('Preparar backup completo ZIP',type='primary'):
        st.session_state['_backup_blob']=exportar_backup_completo(); st.session_state['_backup_nome']=f"backup_completo_{hoje.strftime('%d_%m_%Y')}.zip"
    if st.session_state.get('_backup_blob') is not None:
        st.download_button('Baixar backup completo',data=st.session_state['_backup_blob'],file_name=st.session_state.get('_backup_nome','backup_completo.zip'),mime='application/zip')

# -----------------------------------------------------------------
# DIAGNÓSTICO — somente leitura
# -----------------------------------------------------------------
elif menu == "🩺 Diagnóstico":
    cabecalho_pagina("🩺 Diagnóstico", "Uma checagem rápida do banco e da consistência dos dados.")
    if st.button("← Voltar para Mais", key="diag_back"): st.session_state.menu_atual='⚙️ Mais'; st.rerun()
    ok_db=_banco_disponivel()
    counts={}
    for nome,tabela in [('Lançamentos','lancamentos'),('Categorias','categorias_personalizadas'),('Orçamentos','orcamentos_categorias'),('Pagamentos','pagamentos')]:
        try:
            d=fetch_dataframe(f'SELECT COUNT(*) n FROM {tabela}',silent=True); counts[nome]=int(d.iloc[0]['n']) if not d.empty else 0
        except Exception: counts[nome]=None
    d1,d2,d3=st.columns(3)
    d1.metric('Banco','Online' if ok_db else 'Indisponível')
    d2.metric('Lançamentos',counts.get('Lançamentos') if counts.get('Lançamentos') is not None else '—')
    d3.metric('Categorias',counts.get('Categorias') if counts.get('Categorias') is not None else '—')
    st.caption(f"Build atual: {APP_BUILD}")
    if ok_db: st.success('Conexão com o banco respondendo normalmente.')
    else: st.error('O banco não respondeu ao teste de conexão.')
    st.markdown('**Estruturas da versão 2.0**')
    st.write(f"Orçamentos mensais: {counts.get('Orçamentos','—')} · Pagamentos registrados: {counts.get('Pagamentos','—')}")
    if st.button('Abrir manutenção avançada',use_container_width=True): st.session_state.menu_atual='🧰 Manutenção e Diagnóstico'; st.rerun()

# -----------------------------------------------------------------
# CATEGORIAS E AUTOMAÇÕES
# -----------------------------------------------------------------
elif menu == "⚙️ Gerenciar Categorias":
    cabecalho_pagina("⚙️ Categorias e Automações", "Veja primeiro o que existe; edite somente quando precisar.")
    cfg=fetch_dataframe('SELECT * FROM categorias_personalizadas ORDER BY tipo,categoria,subgrupo')
    if cfg.empty: st.info('Nenhuma categoria cadastrada.')
    else:
        for tipo,icon in [('Despesa','🔴'),('Entrada','🟢')]:
            st.subheader(f'{icon} {tipo}s')
            bloco=cfg[cfg['tipo']==tipo]
            if bloco.empty: st.caption('Nenhuma.')
            for cat in bloco['categoria'].dropna().unique():
                with st.container(border=True):
                    st.markdown(f'**{cat}**')
                    for _,r in bloco[bloco['categoria']==cat].iterrows():
                        tags=[]
                        if int_seguro(r.get('is_recorrente'))==1: tags.append(f"🔄 Repete todo mês · R$ {format_brl(float_seguro(r.get('valor_padrao')))}")
                        elif tipo=='Entrada' and pd.notna(r.get('dia_pagamento')): tags.append(f"recebe dia {int(r['dia_pagamento'])}")
                        st.markdown(f"• **{r['subgrupo'] if pd.notna(r['subgrupo']) and str(r['subgrupo']).strip() else 'Geral'}** <span class='ux-muted'>· {' · '.join(tags) if tags else 'manual'}</span>",unsafe_allow_html=True)
    tab_new,tab_edit=st.tabs(['＋ Nova categoria','✏️ Editar / excluir'])
    with tab_new:
        ntipo=st.radio('Tipo',['Despesa','Entrada'],horizontal=True,key='cat_new_tipo'); c1,c2=st.columns(2); ncat=c1.text_input('Categoria',key='cat_new_cat'); nsub=c2.text_input('Subgrupo (opcional)',key='cat_new_sub')
        n_rec=st.checkbox('🔄 Repete todo mês',key='cat_new_rec'); rec=n_rec
        val=0.0; atraso=0; dia=10; inicio=data_contexto_ativo
        if ntipo=='Entrada' or rec:
            x1,x2,x3=st.columns(3); val=x1.number_input('Valor padrão',min_value=0.0,step=50.0,key='cat_new_val'); atraso=x2.number_input('Atraso em meses',0,6,1 if ntipo=='Entrada' else 0,key='cat_new_atraso'); dia=x3.number_input('Dia pagamento/vencimento',1,31,10,key='cat_new_dia')
            if rec: inicio=st.date_input('Começar em',value=data_contexto_ativo,key='cat_new_inicio')
        if st.button('Salvar categoria',type='primary',key='cat_new_save'):
            if not ncat.strip(): st.error('Categoria é obrigatória.')
            else:
                execute_query("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(ntipo,ncat.strip(),nsub.strip() or None,val if val>0 else None,atraso,dia,1 if rec else 0,inicio if rec else None)); invalidar_caches_estruturais(); flash('success','Categoria criada.'); st.rerun()
    with tab_edit:
        if cfg.empty: st.info('Nada para editar.')
        else:
            op={int(r['id']):f"{r['tipo']} · {r['categoria']} · {r['subgrupo'] if pd.notna(r['subgrupo']) and str(r['subgrupo']).strip() else 'Geral'}" for _,r in cfg.iterrows()}; sel=st.selectbox('Escolha o item',[None]+list(op),format_func=lambda x:'Selecione...' if x is None else op[x],key='cat_edit_sel')
            if sel:
                r=cfg[cfg['id']==sel].iloc[0]; e1,e2=st.columns(2); cat=e1.text_input('Categoria',value=r['categoria'],key=f"cat_edit_cat_{sel}"); sub=e2.text_input('Subgrupo',value=r['subgrupo'] if pd.notna(r['subgrupo']) else '',key=f"cat_edit_sub_{sel}"); rec=st.checkbox('🔄 Repete todo mês',value=bool(r['is_recorrente']==1),key=f"cat_edit_rec_{sel}"); efet=rec
                val=float(r['valor_padrao']) if pd.notna(r['valor_padrao']) else 0.0; atraso=int(r['atraso_meses']) if pd.notna(r['atraso_meses']) else 0; dia=int(r['dia_pagamento']) if pd.notna(r['dia_pagamento']) else 10
                if r['tipo']=='Entrada' or efet:
                    z1,z2,z3=st.columns(3); val=z1.number_input('Valor padrão',value=val,key=f"cat_edit_val_{sel}"); atraso=z2.number_input('Atraso em meses',0,6,atraso,key=f"cat_edit_atraso_{sel}"); dia=z3.number_input('Dia pagamento/vencimento',1,31,dia,key=f"cat_edit_dia_{sel}")
                st.caption('Mudanças valem para novos lançamentos e recorrências; o histórico anterior é preservado.')
                b1,b2=st.columns(2)
                if b1.button('Salvar alterações',type='primary',use_container_width=True):
                    from operations import edit_category
                    with ui_transaction() as cur:
                        edit_category(cur, int(sel), cat.strip(), sub.strip() or None, val if val>0 else None, atraso, dia, int(efet), hoje)
                    invalidar_caches_estruturais(); flash('success','Categoria e previsões pendentes atualizadas.'); st.rerun()
                confirmar=b2.checkbox('Confirmar exclusão',key='cat_del_confirm')
                if b2.button('Excluir',disabled=not confirmar,use_container_width=True):
                    from operations import delete_income_source
                    with ui_transaction() as cur:
                        if r['tipo']=='Entrada': delete_income_source(cur,r['categoria'],r['subgrupo'],hoje)
                        else: cur.execute('DELETE FROM categorias_personalizadas WHERE id=%s',(int(sel),))
                    invalidar_caches_estruturais(); flash('success','Categoria excluída.'); st.rerun()

# -----------------------------------------------------------------
# BACKUP
# -----------------------------------------------------------------
elif menu == "💾 Backup e Restauração":
    cabecalho_pagina("💾 Backup e Restauração", "Ferramentas administrativas ficam fora do uso diário.")
    st.subheader('Criar backup completo')
    st.caption('Inclui lançamentos, categorias, orçamentos mensais, dívidas, reserva, pagamentos e controle de recorrências.')
    if st.button('📦 Preparar backup ZIP',type='primary'):
        try: st.session_state['_backup_blob']=exportar_backup_completo(); st.session_state['_backup_nome']=f"backup_completo_{hoje.strftime('%d_%m_%Y')}.zip"
        except Exception as e: st.error(f'Falha ao preparar backup: {e}')
    if st.session_state.get('_backup_blob') is not None:
        st.download_button('⬇️ Baixar backup',data=st.session_state['_backup_blob'],file_name=st.session_state.get('_backup_nome','backup_completo.zip'),mime='application/zip')
    st.divider(); st.subheader('Restaurar backup'); st.warning('A restauração substitui o estado do banco. Prepare e guarde uma cópia de segurança antes de substituir os dados.')
    if st.button('1. Preparar cópia de segurança antes de restaurar'):
        try:
            st.session_state['_pre_restore_blob']=exportar_backup_completo()
            st.session_state['_pre_restore_nome']=f"antes_da_restauracao_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            st.session_state.pop('backup_restore_confirm',None)
        except Exception:
            st.error('Não foi possível preparar a cópia. A restauração permanece bloqueada.')
    if st.session_state.get('_pre_restore_blob') is not None:
        st.download_button('2. Baixar cópia de segurança',data=st.session_state['_pre_restore_blob'],file_name=st.session_state['_pre_restore_nome'],mime='application/zip')
    up=st.file_uploader('3. Selecione o backup ZIP ou CSV legado',type=['zip','csv'],key='backup_restore_file')
    conf=st.checkbox('Guardei a cópia de segurança e confirmo a substituição dos dados',key='backup_restore_confirm')
    if up is not None and st.button('Restaurar',type='primary',disabled=not (conf and st.session_state.get('_pre_restore_blob'))):
        ok,msg=importar_backup(up)
        if ok:
            invalidar_caches_estruturais()
            for key in list(st.session_state):
                if key.startswith(('rec_processado_','fluxo2_','wizard_')): st.session_state.pop(key,None)
            st.session_state.pop('_backup_blob',None)
            flash('success',msg); st.rerun()
        else: st.error(f'Restauração cancelada. {msg}')

# -----------------------------------------------------------------
# MANUTENÇÃO E DIAGNÓSTICO
# -----------------------------------------------------------------
elif menu == "🧰 Manutenção e Diagnóstico":
    cabecalho_pagina("🧰 Manutenção e diagnóstico", "Ferramentas técnicas e correções históricas — fora do fluxo normal.")
    with st.expander('🔍 Verificar orçamentos do mês', expanded=True):
        conc=fetch_dataframe("""
            SELECT o.categoria,o.subgrupo,o.valor_planejado,
                   COALESCE(SUM(CASE WHEN l.tipo='Despesa' AND l.pago=1 THEN l.valor_pago ELSE 0 END),0) realizado
            FROM orcamentos_categorias o
            LEFT JOIN lancamentos l ON l.categoria=o.categoria
              AND COALESCE(l.subgrupo,'')=COALESCE(o.subgrupo,'')
              AND l.data_vencimento >= o.competencia
              AND l.data_vencimento < (o.competencia + INTERVAL '1 month')
            WHERE o.competencia=%s
            GROUP BY o.categoria,o.subgrupo,o.valor_planejado
            ORDER BY o.categoria,o.subgrupo
        """, (datetime.date(ano_selecionado,mes_selecionado,1),))
        if conc.empty: st.info('Nenhum orçamento definido neste mês.')
        else:
            conc['disponivel']=pd.to_numeric(conc['valor_planejado'],errors='coerce').fillna(0)-pd.to_numeric(conc['realizado'],errors='coerce').fillna(0)
            _render_tabela_escura(conc.rename(columns={'categoria':'Categoria','subgrupo':'Subgrupo','valor_planejado':'Planejado','realizado':'Realizado','disponivel':'Disponível'}),currency_cols={'Planejado','Realizado','Disponível'})
    with st.expander("🧹 Limpar lançamentos antigos com tag 'Provisão'"):
        prov=fetch_dataframe("SELECT id,tipo,categoria,subgrupo,descricao,valor,data_vencimento,pago FROM lancamentos WHERE descricao ILIKE %s ORDER BY data_vencimento",('%(Provisão)%',))
        if prov.empty: st.success('Nenhum lançamento antigo encontrado.')
        else:
            st.dataframe(prov,hide_index=True,use_container_width=True); c=st.checkbox('Confirmo a exclusão permanente destes itens',key='maint_prov_conf')
            if st.button('Apagar itens listados',disabled=not c,key='maint_prov_del'):
                ids=prov['id'].astype(int).tolist();
                with ui_transaction() as cur: cur.execute('DELETE FROM lancamentos WHERE id = ANY(%s)',(ids,))
                flash('success',f'{len(ids)} item(ns) apagado(s).'); st.rerun()
    with st.expander('🧹 Corrigir descrições antigas duplicadas'):
        dup=fetch_dataframe(r"SELECT id,descricao FROM lancamentos WHERE descricao ~ '\(\d+/\d+\) \(\d+/\d+\)$' ORDER BY id")
        if dup.empty: st.success('Nenhuma descrição duplicada encontrada.')
        else:
            prev=dup.copy(); prev['corrigida']=prev['descricao'].apply(lambda d:re.sub(r'(\s*\(\d+/\d+\))+$','',d).strip()); st.dataframe(prev,hide_index=True,use_container_width=True)
            if st.button('Corrigir todas',type='primary'):
                with ui_transaction() as cur:
                    for _,r in dup.iterrows(): cur.execute('UPDATE lancamentos SET descricao=%s WHERE id=%s',(re.sub(r'(\s*\(\d+/\d+\))+$','',r['descricao']).strip(),int(r['id'])))
                flash('success','Descrições corrigidas.'); st.rerun()
    with st.expander('🚨 Apagar histórico global de plantões'):
        st.error('Ação irreversível. Use apenas se realmente quiser remover todos os plantões do banco.')
        conf=st.checkbox('Confirmo que quero apagar TODO o histórico de plantões',key='maint_purge_plant')
        if st.button('Purgar histórico de plantões',disabled=not conf,key='maint_purge_btn'):
            execute_query("DELETE FROM lancamentos WHERE tipo='Entrada' AND descricao LIKE 'Plantão %'"); flash('success','Histórico de plantões apagado.'); st.rerun()
