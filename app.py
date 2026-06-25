import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import plotly.express as px
import datetime
import calendar
import uuid
import os
import io

# =================================================================
# 1. INFRAESTRUTURA E CONEXÃO (SINGLETON ROBUSTO E ANTI-DDoS)
# =================================================================

@st.cache_resource(ttl=3600)
def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL não configurada na variável de ambiente.")
        st.stop()

    db_url = db_url.replace(":6543/", ":5432/")
    if "sslmode=require" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"

    try:
        conn = psycopg2.connect(
            db_url,
            options="-c client_encoding=utf8",
            connect_timeout=10
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"Falha Crítica de Conexão com o PostgreSQL: {e}")
        st.stop()

def execute_query(query, params=None, fetch=False):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch: return cur.fetchall()
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_connection.clear()
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch: return cur.fetchall()
        except Exception as e:
            st.error(f"Erro de Banco de Dados: {e}")
            return [] if fetch else None

def execute_values_query(query, params_list):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            execute_values(cur, query, params_list)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_connection.clear()
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                execute_values(cur, query, params_list)
        except Exception as e:
            st.error(f"Erro de Inserção Múltipla: {e}")

def fetch_dataframe(query, params=None):
    try:
        conn = get_connection()
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        get_connection.clear()
        try:
            conn = get_connection()
            return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            st.error(f"Erro de Leitura de Dados: {e}")
            return pd.DataFrame()

@st.cache_resource
def init_db():
    execute_query('''
        CREATE TABLE IF NOT EXISTS categorias_personalizadas (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT,
            valor_padrao NUMERIC,
            atraso_meses INTEGER,
            dia_pagamento INTEGER,
            is_recorrente INTEGER DEFAULT 0,
            data_inicio DATE,
            is_envelope INTEGER DEFAULT 0
        );
    ''')
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS valor_padrao NUMERIC;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS atraso_meses INTEGER;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS dia_pagamento INTEGER;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_recorrente INTEGER DEFAULT 0;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS data_inicio DATE;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_envelope INTEGER DEFAULT 0;")

    execute_query('''
        CREATE TABLE IF NOT EXISTS lancamentos (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT,
            descricao TEXT,
            valor NUMERIC,
            data_vencimento DATE,
            parcela_atual INTEGER,
            total_parcelas INTEGER,
            pago INTEGER DEFAULT 0,
            compra_id TEXT,
            forma_pagamento TEXT DEFAULT 'Outros',
            prioridade TEXT DEFAULT 'Baixa 🟢',
            valor_pago NUMERIC DEFAULT 0.0
        );
    ''')
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS forma_pagamento TEXT DEFAULT 'Outros';")
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS prioridade TEXT DEFAULT 'Baixa 🟢';")
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS valor_pago NUMERIC DEFAULT 0.0;")

# =================================================================
# 2. MOTOR DE GERAÇÃO LAZY / RECORRÊNCIAS CONTRATUAIS
# =================================================================

def processar_recorrencias_lazy(mes, ano):
    df_contratos = fetch_dataframe("SELECT * FROM categorias_personalizadas WHERE is_recorrente = 1")
    if df_contratos.empty: return

    for _, contrato in df_contratos.iterrows():
        dt_inicio = pd.to_datetime(contrato['data_inicio']).date() if pd.notna(contrato['data_inicio']) else datetime.date(ano, mes, 1)
        dt_limite_alvo = datetime.date(ano, mes, min(int(contrato['dia_pagamento'] or 1), calendar.monthrange(ano, mes)[1]))

        if dt_limite_alvo < dt_inicio: continue

        compra_id_contrato = f"rec_{contrato['id']}"
        check_exist = fetch_dataframe(
            "SELECT id FROM lancamentos WHERE compra_id = %s AND EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s LIMIT 1",
            (compra_id_contrato, mes, ano)
        )

        if check_exist.empty:
            val_p = float(contrato['valor_padrao'] or 0.0)
            desc_c = f"{contrato['categoria']} - {contrato['subgrupo'] or ''} (Recorrente)"
            execute_query('''
                INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago)
                VALUES (%s, %s, %s, %s, %s, %s, 1, 1, 0, %s, 'Outros', 'Média 🟡', 0.0)
            ''', (contrato['tipo'], contrato['categoria'], contrato['subgrupo'], desc_c, val_p, dt_limite_alvo, compra_id_contrato))

# =================================================================
# 3. MOTOR DE ABATIMENTO AUTOMÁTICO DE ENVELOPES (BACKEND)
# =================================================================

def executar_abatimento_envelope(categoria, subgrupo, valor_gasto, mes, ano):
    """
    Deduz dinamicamente o valor de uma despesa real realizada do teto orçamentário
    (pago=0) da mesma categoria+subgrupo no mês vigente, caso a categoria esteja
    configurada como Envelope Virtual (is_envelope=1) em '⚙️ Gerenciar Categorias'.

    O saldo pode ficar NEGATIVO de propósito (sem clamp em 0), pra preservar o
    invariante "orçamento original = realizado + disponível" mesmo estourando o teto.
    """
    df_cat = fetch_dataframe("SELECT is_envelope FROM categorias_personalizadas WHERE categoria = %s AND tipo = 'Despesa' LIMIT 1", (categoria,))
    if not df_cat.empty and int(df_cat.iloc[0]['is_envelope'] or 0) == 1:
        execute_query('''
            UPDATE lancamentos
            SET valor = valor - %s
            WHERE pago = 0
              AND tipo = 'Despesa'
              AND categoria = %s
              AND subgrupo = %s
              AND EXTRACT(MONTH FROM data_vencimento) = %s
              AND EXTRACT(YEAR FROM data_vencimento) = %s
        ''', (valor_gasto, categoria, subgrupo, mes, ano))

# =================================================================
# 4. SISTEMA DE SEGURANÇA E AUXILIARES
# =================================================================

def check_password():
    if "password_correct" not in st.session_state: st.session_state["password_correct"] = False
    if st.session_state["password_correct"]: return True
    st.markdown("### 🔒 Acesso Restrito")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar", type="primary"):
        if senha == os.environ.get("APP_PASSWORD"):
            st.session_state["password_correct"] = True
            st.rerun()
        else: st.error("Senha incorreta.")
    return False

def parse_valor(valor_str):
    if isinstance(valor_str, (float, int)): return float(valor_str)
    clean_val = str(valor_str).replace('.', '').replace(',', '.')
    try: return float(clean_val)
    except ValueError: return 0.0

def format_brl(valor):
    if pd.isna(valor): return "0,00"
    return f"{float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def ordenar_categorias_com_prioridade(categorias, prioridade="despesas essenciais"):
    """Ordena uma lista de categorias colocando a categoria prioritária primeiro
    (comparação sem diferenciar maiúsculas/minúsculas), e o resto em ordem alfabética."""
    cats = list(categorias)
    match = next((c for c in cats if str(c).strip().lower() == prioridade), None)
    resto = sorted([c for c in cats if c != match], key=lambda x: str(x).lower())
    return ([match] if match else []) + resto

def flash(tipo, mensagem):
    """Guarda uma mensagem pra ser exibida DEPOIS do próximo rerun. st.rerun()
    interrompe a execução na hora, então um st.success() chamado bem antes de um
    st.rerun() na mesma linha nunca chega a ser visto na tela."""
    st.session_state['_flash'] = (tipo, mensagem)

def exibir_flash():
    if '_flash' in st.session_state:
        tipo, mensagem = st.session_state.pop('_flash')
        getattr(st, tipo)(mensagem)

# =================================================================
# 5. CONFIGURAÇÃO DA PÁGINA
# =================================================================

st.set_page_config(page_title="Gestão Financeira", layout="wide", page_icon="💰")
if not check_password(): st.stop()
init_db()

# =================================================================
# 5B. IDENTIDADE VISUAL
# =================================================================
def aplicar_estilo_visual():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    :root, .stApp {
        --background-color: #12161B !important;
        --secondary-background-color: #1B2127 !important;
        --text-color: #E8EAED !important;
        --primary-color: #3FAE8D !important;
    }
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"],
    [data-testid="stHeader"],
    .main {
        background-color: #12161B !important;
    }
    [data-testid="stHeader"] {
        background-color: rgba(0,0,0,0) !important;
    }
    .stApp {
        color: #E8EAED;
    }

    html, body, [class*="css"] {
        font-family: 'Manrope', sans-serif;
    }
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Manrope', sans-serif !important;
        font-weight: 700 !important;
        color: #E8EAED !important;
        letter-spacing: -0.01em;
    }

    div[data-testid="stMetric"], div[data-testid="metric-container"] {
        background: #1B2127 !important;
        border: 1px solid #2A3138;
        border-radius: 14px;
        padding: 0.9rem 1.1rem 0.8rem 1.1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.35);
    }
    div[data-testid="stMetricValue"] {
        font-family: 'IBM Plex Mono', monospace !important;
        font-weight: 600 !important;
        color: #E8EAED !important;
    }
    div[data-testid="stMetricLabel"] {
        font-weight: 600 !important;
        color: #8B94A0 !important;
        font-size: 0.78rem !important;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    div[data-testid="stMetricLabel"] p {
        color: #8B94A0 !important;
    }

    .stApp label, .stApp .stMarkdown, .stApp .stMarkdown p,
    .stApp [data-testid="stWidgetLabel"] p,
    .stApp [data-testid="stWidgetLabel"] {
        color: #E8EAED !important;
    }
    .stApp [data-testid="stCaptionContainer"] {
        color: #8B94A0 !important;
    }

    section[data-testid="stSidebar"] {
        background: #0E1216 !important;
        border-right: 1px solid #2A3138;
    }
    section[data-testid="stSidebar"] * {
        color: #E8EAED !important;
    }

    .stButton button,
    .stButton button[kind="secondary"],
    .stButton button:not([kind="primary"]) {
        background-color: #1B2127 !important;
        border: 1px solid #2A3138 !important;
        color: #E8EAED !important;
    }
    .stButton button *,
    .stButton button[kind="secondary"] *,
    .stButton button:not([kind="primary"]) * {
        color: #E8EAED !important;
    }
    .stButton button:hover,
    .stButton button:not([kind="primary"]):hover {
        background-color: #20262C !important;
        border-color: #3FAE8D !important;
        color: #E8EAED !important;
    }

    .stButton button[kind="primary"] {
        background-color: #3FAE8D !important;
        border: 1px solid #3FAE8D !important;
        color: #0E1216 !important;
    }
    .stButton button[kind="primary"] * {
        color: #0E1216 !important;
    }
    .stButton button[kind="primary"]:hover {
        background-color: #379A7C !important;
        border-color: #379A7C !important;
        color: #0E1216 !important;
    }
    .stButton button[kind="primary"]:hover * {
        color: #0E1216 !important;
    }

    section[data-testid="stSidebar"] .stButton button {
        width: 100%;
        text-align: left;
        justify-content: flex-start;
        border-radius: 8px;
        font-weight: 500;
        padding: 0.45rem 0.8rem;
    }

    .nav-eyebrow {
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #8B94A0 !important;
        margin: 1.1rem 0 0.4rem 0.15rem;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 600;
        color: #8B94A0 !important;
    }
    .stTabs [data-baseweb="tab"] p {
        color: #8B94A0 !important;
    }
    .stTabs [aria-selected="true"] {
        color: #3FAE8D !important;
    }
    .stTabs [aria-selected="true"] p {
        color: #3FAE8D !important;
    }

    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        border-radius: 10px;
        overflow: hidden;
        border: 1px solid #2A3138;
    }

    div[data-testid="stExpander"], div[data-testid="stForm"] {
        border: 1px solid #2A3138 !important;
        border-radius: 12px !important;
        background: #1B2127 !important;
    }
    </style>
    """, unsafe_allow_html=True)

aplicar_estilo_visual()

def aplicar_tema_grafico(fig):
    fig.update_layout(
        paper_bgcolor="#1B2127",
        plot_bgcolor="#1B2127",
        font=dict(family="Manrope, sans-serif", color="#E8EAED"),
        legend=dict(font=dict(color="#E8EAED")),
        xaxis=dict(gridcolor="#2A3138", linecolor="#2A3138", color="#8B94A0"),
        yaxis=dict(gridcolor="#2A3138", linecolor="#2A3138", color="#8B94A0"),
    )
    return fig

# =================================================================
# 6. ESTRUTURAS DINÂMICAS E CONSTANTES
# =================================================================

def get_estrutura_dinamica():
    estrutura = {"Entrada": {}, "Despesa": {}}
    try:
        df_custom = fetch_dataframe("SELECT tipo, categoria, subgrupo FROM categorias_personalizadas")
        if not df_custom.empty:
            for _, row in df_custom.iterrows():
                t, c, s = row['tipo'], row['categoria'], row['subgrupo']
                if t in estrutura:
                    if c not in estrutura[t]: estrutura[t][c] = []
                    if s and s not in estrutura[t][c]: estrutura[t][c].append(s)
    except Exception: pass
    return estrutura

ESTRUTURA = get_estrutura_dinamica()
hoje = datetime.date.today()
meses = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
prioridades_map = {"Alta 🔴": 0, "Média 🟡": 1, "Baixa 🟢": 2}

# =================================================================
# 7. SIDEBAR E FILTROS GLOBAL
# =================================================================

st.sidebar.markdown(
    "<div style='font-weight:800; font-size:1.05rem; color:#E8EAED; margin-bottom:0.2rem;'>💰 Gestão Financeira</div>"
    "<div style='font-size:0.8rem; color:#8B94A0; margin-bottom:0.4rem;'>Painel de controle financeiro</div>",
    unsafe_allow_html=True
)
st.sidebar.divider()

if "menu_atual" not in st.session_state:
    st.session_state.menu_atual = "🏠 Início"

def _nav_btn(label, key):
    ativo = st.session_state.menu_atual == label
    if st.sidebar.button(label, key=key, type="primary" if ativo else "secondary", use_container_width=True):
        st.session_state.menu_atual = label
        st.rerun()

st.sidebar.markdown("<div class='nav-eyebrow'>Visão Geral</div>", unsafe_allow_html=True)
_nav_btn("🏠 Início", "nav_inicio")
_nav_btn("📑 Demonstrativo", "nav_demonstrativo")
_nav_btn("📈 Balanço Anual", "nav_balanco")

st.sidebar.markdown("<div class='nav-eyebrow'>Lançar e Organizar</div>", unsafe_allow_html=True)
_nav_btn("📝 Lançamentos", "nav_lancamentos")
_nav_btn("📊 Fluxo e Prioridades", "nav_fluxo")

st.sidebar.markdown("<div class='nav-eyebrow'>Plantões</div>", unsafe_allow_html=True)
_nav_btn("🏥 Escala de Plantões", "nav_escala")

st.sidebar.markdown("<div class='nav-eyebrow'>Configuração</div>", unsafe_allow_html=True)
_nav_btn("⚙️ Gerenciar Categorias", "nav_categorias")

menu = st.session_state.menu_atual
st.sidebar.divider()

st.sidebar.markdown("<div class='nav-eyebrow'>Período Ativo</div>", unsafe_allow_html=True)
col_sb1, col_sb2 = st.sidebar.columns(2)
with col_sb1: mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1, key="sb_mes")
with col_sb2: ano_selecionado = st.selectbox("Ano", range(hoje.year-2, hoje.year+5), index=2, key="sb_ano")

st.sidebar.divider()
st.sidebar.markdown("<div class='nav-eyebrow'>Backup</div>", unsafe_allow_html=True)

def exportar_csv():
    df = fetch_dataframe("SELECT * FROM lancamentos")
    return df.to_csv(index=False).encode('utf-8') if not df.empty else None

def importar_csv(arquivo):
    try:
        df_imp = pd.read_csv(arquivo)
        if 'forma_pagamento' not in df_imp.columns: df_imp['forma_pagamento'] = 'Outros'
        if 'prioridade' not in df_imp.columns: df_imp['prioridade'] = 'Baixa 🟢'
        if 'valor_pago' not in df_imp.columns: df_imp['valor_pago'] = df_imp['valor']

        execute_query("TRUNCATE TABLE lancamentos RESTART IDENTITY")
        registros = [(
            r['tipo'], r['categoria'], r['subgrupo'], r['descricao'], r['valor'],
            r['data_vencimento'], r['parcela_atual'], r['total_parcelas'], r['pago'],
            r['compra_id'], r['forma_pagamento'], r['prioridade'], r['valor_pago']
        ) for _, r in df_imp.iterrows()]

        execute_values_query('''
            INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago)
            VALUES %s
        ''', registros)
        return True
    except Exception as e:
        st.error(f"Erro Crítico de Restauração: {e}")
        return False

c_data = exportar_csv()
if c_data: st.sidebar.download_button("📥 Baixar CSV", data=c_data, file_name=f"backup_{hoje.strftime('%d_%m_%Y')}.csv", mime="text/csv")
a_up = st.sidebar.file_uploader("Restaurar CSV", type="csv")
if a_up and st.sidebar.button("🚀 Confirmar Restauração"):
    if importar_csv(a_up):
        flash("success", "📥 Backup restaurado com sucesso!")
        st.rerun()

processar_recorrencias_lazy(mes_selecionado, ano_selecionado)
dia_maximo_alvo = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
data_contexto_ativo = datetime.date(ano_selecionado, mes_selecionado, min(hoje.day, dia_maximo_alvo))

exibir_flash()

# =================================================================
# 8. MÓDULO: TELA INICIAL
# =================================================================

if menu == "🏠 Início":
    st.header("🏠 Painel Executivo Imediato")

    dt_limite = hoje + datetime.timedelta(days=7)
    df_7d = fetch_dataframe("SELECT data_vencimento, tipo, descricao, valor, pago FROM lancamentos WHERE data_vencimento BETWEEN %s AND %s ORDER BY data_vencimento ASC", (hoje, dt_limite))
    df_mes_atual = fetch_dataframe("SELECT tipo, valor, valor_pago, pago FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (hoje.month, hoje.year))

    c_inc1, c_inc2, c_inc3 = st.columns(3)
    if not df_mes_atual.empty:
        df_mes_atual['valor'] = df_mes_atual['valor'].astype(float)
        df_mes_atual['valor_pago'] = df_mes_atual['valor_pago'].astype(float)

        ent_confirmadas = df_mes_atual[(df_mes_atual['tipo'] == 'Entrada') & (df_mes_atual['pago'] == 1)]['valor_pago'].sum()
        ent_projetadas = df_mes_atual[(df_mes_atual['tipo'] == 'Entrada') & (df_mes_atual['pago'] == 0)]['valor'].sum()
        desp_totais = df_mes_atual[df_mes_atual['tipo'] == 'Despesa']['valor'].sum()

        c_inc1.metric("📥 Entradas Confirmadas (Mês)", f"R$ {format_brl(ent_confirmadas)}")
        c_inc2.metric("⏳ Entradas a Receber (Projetado)", f"R$ {format_brl(ent_projetadas)}")
        c_inc3.metric("⚖️ Sobra Projetada", f"R$ {format_brl((ent_confirmadas + ent_projetadas) - desp_totais)}")
    else:
        c_inc1.metric("📥 Entradas Confirmadas (Mês)", "R$ 0,00")
        c_inc2.metric("⏳ Entradas a Receber (Projetado)", "R$ 0,00")
        c_inc3.metric("⚖️ Sobra Projetada", "R$ 0,00")

    st.divider()
    st.subheader("🗓️ Agenda de Vencimentos (Próximos 7 dias)")
    if df_7d.empty:
        st.success("Nenhuma conta vencendo ou receita prevista para os próximos 7 dias! 🎉")
    else:
        df_7d['valor'] = df_7d['valor'].astype(float)
        df_7d['Status'] = df_7d['pago'].apply(lambda x: '✅ Pago' if x == 1 else '⏳ Pendente')
        df_7d['Data'] = pd.to_datetime(df_7d['data_vencimento']).dt.strftime('%d/%m/%Y')

        def _cor_linha_status(row):
            if row['Status'] == '✅ Pago':
                return ['background-color: #17241E; color: #E8EAED'] * len(row)
            return ['background-color: #29241A; color: #E8EAED'] * len(row)

        df_7d_view = df_7d[['Data', 'tipo', 'descricao', 'valor', 'Status']].rename(
            columns={'tipo': 'Tipo', 'descricao': 'Descrição', 'valor': 'Valor'}
        )
        estilo_7d = df_7d_view.style.apply(_cor_linha_status, axis=1).format({'Valor': lambda v: f"R$ {format_brl(v)}"})
        st.dataframe(estilo_7d, use_container_width=True, hide_index=True)

# =================================================================
# 9. MÓDULO: GERENCIAR CATEGORIAS E RECORRÊNCIAS
# =================================================================

elif menu == "⚙️ Gerenciar Categorias":
    st.header("⚙️ Gerenciar Categorias e Contratos Recorrentes")
    df_custom_global = fetch_dataframe("SELECT * FROM categorias_personalizadas")
    tab_add, tab_edit, tab_del = st.tabs(["➕ Adicionar", "✏️ Editar", "🗑️ Excluir"])

    with tab_add:
        c_add1, c_add2 = st.columns(2)
        with c_add1:
            ntipo = st.radio("Para qual tipo?", ["Despesa", "Entrada"], horizontal=True, key="add_tipo")
            ncat = st.text_input("Nome da Categoria (Nova ou Existente)", placeholder="Ex: Valores Fixos", key="add_cat_input")
            n_rec = st.checkbox("🔄 Contrato fixo/recorrente? (Autogeração Mensal)", key="add_rec_check")
        with c_add2:
            nsub = st.text_input("Nome do Subgrupo (Opcional)", placeholder="Ex: Hospital Trauma", key="add_sub_input")
            if ntipo == "Despesa":
                n_env = st.checkbox("⚖️ Tornar esta categoria um 'Envelope Virtual' (Teto para despesas variáveis)", key="add_env_check")
            else:
                n_env = False
            n_rec_efetivo = n_rec or n_env
            if n_rec_efetivo: n_dt_start = st.date_input("Data de Início do Contrato/Teto", value=data_contexto_ativo, key="add_dt_input")

        if n_env:
            st.caption("💡 Envelope Virtual precisa de um teto mensal recorrente para funcionar — por isso a recorrência foi ativada automaticamente.")

        if ntipo == "Entrada" or n_rec_efetivo:
            st.markdown("---")
            st.markdown("##### ⚙️ Parâmetros de Padrão e Recorrência")
            c_opt1, c_opt2, c_opt3 = st.columns(3)
            v_opt = c_opt1.number_input("Valor Padrão (R$)", min_value=0.0, step=50.0, value=0.0, key="add_vopt_num")
            a_opt = c_opt2.number_input("Atraso (Meses) - Útil p/ Plantões", min_value=0, max_value=6, value=1 if ntipo=="Entrada" else 0, key="add_aopt_num")
            d_opt = c_opt3.number_input("Dia de Pagamento/Vencimento", min_value=1, max_value=31, value=10, key="add_dopt_num")
        else:
            v_opt, a_opt, d_opt = 0.0, 0, 10

        if st.button("Salvar Nova Categoria/Subgrupo", type="primary", key="add_save_btn"):
            if not ncat.strip(): st.error("O nome da Categoria é obrigatório.")
            else:
                is_rec_val = 1 if n_rec_efetivo else 0
                is_env_val = 1 if n_env else 0
                dt_start_val = n_dt_start if n_rec_efetivo else None
                execute_query("INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento, is_recorrente, data_inicio, is_envelope) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                              (ntipo, ncat.strip(), nsub.strip(), v_opt if v_opt > 0 else None, a_opt, d_opt, is_rec_val, dt_start_val, is_env_val))
                flash("success", "Categoria adicionada com sucesso!"); st.rerun()

    with tab_edit:
        if not df_custom_global.empty:
            opcoes_edit_local = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom_global.iterrows()}
            sel_edit = st.selectbox("Selecione o item para editar:", options=[None] + list(opcoes_edit_local.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_edit_local[x], key="edit_select_target")
            if sel_edit:
                nó = df_custom_global[df_custom_global['id'] == sel_edit].iloc[0]
                c_ed_n1, c_ed_n2 = st.columns(2)
                with c_ed_n1: new_cat = st.text_input("Nova Categoria", value=nó['categoria'], key="edit_cat_input")
                with c_ed_n2: new_sub = st.text_input("Novo Subgrupo", value=nó['subgrupo'] if pd.notna(nó['subgrupo']) else "", key="edit_sub_input")

                e_env = st.checkbox("⚖️ Tornar esta categoria um 'Envelope Virtual'", value=bool(nó['is_envelope'] == 1), key="edit_env_check") if nó['tipo'] == 'Despesa' else False
                e_rec = st.checkbox("🔄 Contrato fixo/recorrente? (Autogeração Mensal)", value=bool(nó['is_recorrente'] == 1) or e_env, key="edit_rec_check", disabled=e_env)
                e_rec_efetivo = e_rec or e_env
                if e_env:
                    st.caption("💡 Envelope Virtual precisa de recorrência para gerar o teto mensal — por isso essa opção está travada como ativa.")

                if nó['tipo'] == "Entrada" or e_rec_efetivo:
                    st.markdown("---")
                    st.markdown("##### ⚙️ Parâmetros de Padrão e Recorrência")
                    c_opt_e1, c_opt_e2, c_opt_e3 = st.columns(3)
                    v_edit = c_opt_e1.number_input("Valor Padrão (R$)", value=float(nó['valor_padrao']) if pd.notna(nó['valor_padrao']) else 0.0, key="edit_vopt_num")
                    a_edit = c_opt_e2.number_input("Atraso (Meses)", value=int(nó['atraso_meses']) if pd.notna(nó['atraso_meses']) else (1 if nó['tipo']=="Entrada" else 0), key="edit_aopt_num")
                    d_edit = c_opt_e3.number_input("Dia Pagamento", value=int(nó['dia_pagamento']) if pd.notna(nó['dia_pagamento']) else 10, key="edit_dopt_num")
                else:
                    v_edit = float(nó['valor_padrao']) if pd.notna(nó['valor_padrao']) else 0.0
                    a_edit = int(nó['atraso_meses']) if pd.notna(nó['atraso_meses']) else 0
                    d_edit = int(nó['dia_pagamento']) if pd.notna(nó['dia_pagamento']) else 10

                if st.button("💾 Confirmar Edição", type="primary", key="edit_save_btn"):
                    execute_query("UPDATE categorias_personalizadas SET categoria=%s, subgrupo=%s, valor_padrao=%s, atraso_meses=%s, dia_pagamento=%s, is_recorrente=%s, is_envelope=%s WHERE id=%s",
                                  (new_cat, new_sub, v_edit if v_edit > 0 else None, a_edit, d_edit, 1 if e_rec_efetivo else 0, 1 if e_env else 0, sel_edit))
                    execute_query("UPDATE lancamentos SET categoria=%s, subgrupo=%s WHERE tipo=%s AND categoria=%s AND subgrupo=%s", (new_cat, new_sub, nó['tipo'], nó['categoria'], nó['subgrupo']))
                    flash("success", "Categoria atualizada com sucesso!"); st.rerun()
        else: st.info("Nenhuma categoria encontrada.")

    with tab_del:
        if not df_custom_global.empty:
            opcoes_del_local = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom_global.iterrows()}
            sel_del = st.selectbox("Selecione o item para excluir:", options=[None] + list(opcoes_del_local.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_del_local[x], key="del_select_target")
            if sel_del and st.button("🗑️ Excluir Selecionado", type="primary", key="del_save_btn"):
                execute_query("DELETE FROM categorias_personalizadas WHERE id = %s", (sel_del,))
                flash("success", "Categoria excluída com sucesso!"); st.rerun()

# =================================================================
# 10. MÓDULO 1: LANÇAMENTOS
# =================================================================

elif menu == "📝 Lançamentos":
    st.header(f"📝 Novo Lançamento ({meses[mes_selecionado-1]}/{ano_selecionado})")

    col1, col2 = st.columns(2)
    with col1:
        tipo = st.radio("Tipo", ["Despesa", "Entrada"], horizontal=True, key="lanc_tipo")
        forma_pgto = st.selectbox("Forma de Pagamento", ["À vista", "Crédito", "Outros"], index=0 if tipo == "Entrada" else 1)
        descricao = st.text_input("Descrição")
        valor_input = st.text_input("Valor Planejado (R$)", value="0,00")
        prioridade = st.radio("Prioridade", ["Baixa 🟢", "Média 🟡", "Alta 🔴"], index=0, horizontal=True)

        pago_imediato = st.checkbox("Marcar como Pago/Efetivado imediatamente")
        if pago_imediato:
            st.caption("💡 Em compras parceladas, só a 1ª parcela é marcada como paga agora — as futuras continuam pendentes.")
    with col2:
        if not ESTRUTURA[tipo]:
            st.error("Não há categorias ativas. Crie uma no módulo '⚙️ Gerenciar Categorias'.")
            categoria, subgrupo = None, None
        else:
            categoria = st.selectbox("Categoria", list(ESTRUTURA[tipo].keys()))
            subgrupos_disp = ESTRUTURA[tipo][categoria] if categoria in ESTRUTURA[tipo] else []
            subgrupo = st.selectbox("Subgrupo", subgrupos_disp)

        data_venc_base = st.date_input("Data Referência", value=data_contexto_ativo, format="DD/MM/YYYY")

        parcelas = 1
        tipo_rec = st.radio("Recorrência", ["Única", "Parcelada", "Fixa/Contínua"], horizontal=True)
        if tipo_rec == "Parcelada": parcelas = st.number_input("Parcelas", min_value=2, value=2)
        elif tipo_rec == "Fixa/Contínua": parcelas = 60

    if st.button("Registrar Lançamento", type="primary") and categoria:
        val_f = parse_valor(valor_input)
        if val_f <= 0: st.error("O valor deve ser maior que zero.")
        else:
            comp_id = str(uuid.uuid4())
            registros = []
            tot_p = 999 if tipo_rec == "Fixa/Contínua" else parcelas

            # "Pago imediato" só faz sentido pra esta ocorrência específica (i==0).
            # Parcelas/meses futuros de uma compra parcelada/contínua ainda não venceram,
            # então nunca devem nascer já marcadas como pagas.
            for i in range(parcelas):
                m_f = data_venc_base.month - 1 + i
                a_f = data_venc_base.year + m_f // 12
                m_f = m_f % 12 + 1
                d_p = datetime.date(a_f, m_f, min(data_venc_base.day, calendar.monthrange(a_f, m_f)[1]))

                pago_atual = 1 if (pago_imediato and i == 0) else 0
                v_pago_atual = val_f if (pago_imediato and i == 0) else 0.0

                registros.append((tipo, categoria, subgrupo, descricao, val_f, d_p, i+1, tot_p, pago_atual, comp_id, forma_pgto, prioridade, v_pago_atual))

            execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago) VALUES %s''', registros)

            # Abatimento automático de envelope: só dispara se a 1ª ocorrência já nasceu paga.
            if tipo == "Despesa" and pago_imediato:
                executar_abatimento_envelope(categoria, subgrupo, val_f, data_venc_base.month, data_venc_base.year)

            flash("success", "✅ Lançamento registrado com sucesso!"); st.rerun()

# =================================================================
# 11. MÓDULO 2: FLUXO E PRIORIDADES
# =================================================================

elif menu == "📊 Fluxo e Prioridades":
    st.header("📊 Fluxo e Prioridades")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s ORDER BY data_vencimento ASC", (mes_selecionado, ano_selecionado))

    if df.empty: st.warning("Sem dados.")
    else:
        df['valor'] = df['valor'].astype(float)
        df['valor_pago'] = df['valor_pago'].fillna(0.0).astype(float)

        st.subheader("🔍 Filtros")
        c_filt1, c_filt2 = st.columns(2)
        tipos_disp = df['tipo'].unique().tolist()
        with c_filt1: sel_tipo = st.multiselect("Filtrar por Tipo", tipos_disp, placeholder="Todos os Tipos")
        tipos_filtro = sel_tipo if sel_tipo else tipos_disp
        cat_disp = df[df['tipo'].isin(tipos_filtro)]['categoria'].unique().tolist()
        with c_filt2: sel_cat = st.multiselect("Filtrar por Categoria", cat_disp, placeholder="Todas as Categorias")
        cat_filtro = sel_cat if sel_cat else cat_disp

        df_view = df[(df['tipo'].isin(tipos_filtro)) & (df['categoria'].isin(cat_filtro))].copy()
        df_view['ids_alvo'] = df_view['id'].astype(str)

        mask_cred = df_view['forma_pagamento'] == 'Crédito'
        if mask_cred.any():
            sum_cred = df_view[mask_cred]['valor'].sum()
            sum_pago_cred = df_view[mask_cred]['valor_pago'].sum()
            all_paid = (df_view[mask_cred]['pago'] == 1).all()
            ids_lote_credito = ','.join(df_view[mask_cred]['id'].astype(str))

            dummy_credito = pd.DataFrame([{
                'id': '-1', 'tipo': 'Despesa', 'categoria': 'N/A', 'subgrupo': '',
                'descricao': '💳 Cartão de Crédito (Fatura Consolidada)', 'valor': sum_cred,
                'valor_pago': sum_pago_cred, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10),
                'pago': 1 if all_paid else 0, 'compra_id': 'cartao_dummy',
                'forma_pagamento': 'Crédito', 'prioridade': 'Alta 🔴', 'ids_alvo': ids_lote_credito
            }])
            df_view = df_view[~mask_cred].copy()
            df_view = pd.concat([df_view, dummy_credito], ignore_index=True)

        mask_plantoes = (df_view['tipo'] == 'Entrada') & df_view['descricao'].str.contains('plant', case=False, na=False)
        if mask_plantoes.any():
            df_plantoes = df_view[mask_plantoes].copy()
            df_view = df_view[~mask_plantoes].copy()
            for nome_grupo, grupo in df_plantoes.groupby(['subgrupo', 'data_vencimento']):
                subg_nome, dt_venc = nome_grupo
                sum_pago_plantao = grupo['valor_pago'].sum()
                status_lote = 1 if (grupo['pago'] == 1).all() else 0
                ids_lote_plantao = ','.join(grupo['id'].astype(str))

                dummy_plantao = pd.DataFrame([{
                    'id': f'plantao_{subg_nome}', 'tipo': 'Entrada', 'categoria': grupo.iloc[0]['categoria'],
                    'subgrupo': subg_nome, 'descricao': f'🏥 Plantões {subg_nome} (Consolidado do Mês)',
                    'valor': grupo['valor'].sum(), 'valor_pago': sum_pago_plantao,
                    'data_vencimento': dt_venc, 'pago': status_lote, 'compra_id': 'plantao_dummy',
                    'forma_pagamento': 'Outros', 'prioridade': 'Baixa 🟢', 'ids_alvo': ids_lote_plantao
                }])
                df_view = pd.concat([df_view, dummy_plantao], ignore_index=True)

        df_view['id'] = df_view['id'].astype(str)
        df_view['ordem_pri'] = df_view['prioridade'].map(prioridades_map).fillna(2)
        df_view = df_view.sort_values(['data_vencimento', 'ordem_pri']).reset_index(drop=True)
        df_view['Pago'] = df_view['pago'].astype(bool)
        df_view['Data'] = pd.to_datetime(df_view['data_vencimento']).dt.date

        def calcular_alerta_atraso(row):
            if not row['Pago'] and row['Data'] < hoje:
                dias = (hoje - row['Data']).days
                return f"🔴 Atrasado há {dias} dias"
            return "🟢 Em dia"
        df_view['Alerta'] = df_view.apply(calcular_alerta_atraso, axis=1)

        def format_desc(row):
            if pd.notna(row.get('total_parcelas')) and row['total_parcelas'] > 1 and row['total_parcelas'] != 999:
                return f"{row['descricao']} ({int(row['parcela_atual'])}/{int(row['total_parcelas'])})"
            return row['descricao']

        df_view['Desc. Exibição'] = df_view.apply(format_desc, axis=1)
        df_view.insert(0, '🗑️ Este', False)
        df_view.insert(1, '🗑️ Futuros', False)

        st.markdown("*(Dica: Modificar o 'Valor Real' preserva 100% o seu planejamento na coluna anterior).*")
        edit_df = st.data_editor(
            df_view[['🗑️ Este', '🗑️ Futuros', 'Data', 'Alerta', 'prioridade', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago']],
            use_container_width=True, hide_index=True,
            column_config={
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "Alerta": st.column_config.TextColumn("Status", disabled=True),
                "valor": st.column_config.NumberColumn("Valor Previsto", format="%.2f"),
                "valor_pago": st.column_config.NumberColumn("Valor Pago/Real", format="%.2f"),
                "prioridade": st.column_config.SelectboxColumn("Prioridade", options=["Alta 🔴", "Média 🟡", "Baixa 🟢"]),
                "Desc. Exibição": st.column_config.TextColumn("Descrição", disabled=False)
            }
        )

        edit_df['tipo'] = df_view['tipo'].values
        edit_df['ordem_pri'] = df_view['ordem_pri'].values

        if st.button("Salvar Alterações Rápidas", type="primary"):
            for i, row in edit_df.iterrows():
                orig_row = df_view.loc[i]
                id_s = str(orig_row['id'])
                novo_pago = 1 if row['Pago'] else 0

                novo_valor = float(row['valor'])
                novo_valor_pago = float(row['valor_pago']) if pd.notna(row['valor_pago']) else 0.0

                orig_valor = float(orig_row['valor'])
                orig_valor_pago = float(orig_row['valor_pago'])

                delta = novo_valor - orig_valor
                delta_pago = novo_valor_pago - orig_valor_pago

                if novo_pago == 1 and novo_valor_pago == 0.0:
                    novo_valor_pago = novo_valor
                    delta_pago = novo_valor - orig_valor_pago
                elif novo_pago == 0:
                    novo_valor_pago = 0.0
                    delta_pago = 0.0 - orig_valor_pago

                nova_desc = row['Desc. Exibição'].split(' (')[0]
                tupla_ids_reais = tuple(map(int, orig_row['ids_alvo'].split(',')))

                if row['🗑️ Este'] or row['🗑️ Futuros']:
                    if id_s == '-1': st.warning("Cartões consolidados não podem ser apagados aqui.")
                    elif id_s.startswith('plantao_'): execute_query("DELETE FROM lancamentos WHERE id IN %s", (tupla_ids_reais,))
                    else: execute_query("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s" if row['🗑️ Futuros'] else "DELETE FROM lancamentos WHERE id = %s", (orig_row['compra_id'], orig_row['data_vencimento']) if row['🗑️ Futuros'] else (tupla_ids_reais[0],))
                else:
                    if id_s == '-1':
                        execute_query("UPDATE lancamentos SET pago=%s WHERE id IN %s", (novo_pago, tupla_ids_reais))
                        if novo_pago == 1: execute_query("UPDATE lancamentos SET valor_pago=valor WHERE id IN %s AND valor_pago=0", (tupla_ids_reais,))
                        elif novo_pago == 0: execute_query("UPDATE lancamentos SET valor_pago=0 WHERE id IN %s", (tupla_ids_reais,))
                        if delta != 0 or delta_pago != 0: execute_query("INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, valor_pago, data_vencimento, pago, forma_pagamento, prioridade) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('Despesa', 'Ajuste', '', '💳 Ajuste de Fatura Consolidada', delta, delta_pago, row['Data'], novo_pago, 'Outros', row['prioridade']))
                    elif id_s.startswith('plantao_'):
                        subg = id_s.replace('plantao_', '')
                        execute_query("UPDATE lancamentos SET pago=%s WHERE id IN %s", (novo_pago, tupla_ids_reais))
                        if novo_pago == 1: execute_query("UPDATE lancamentos SET valor_pago=valor WHERE id IN %s AND valor_pago=0", (tupla_ids_reais,))
                        elif novo_pago == 0: execute_query("UPDATE lancamentos SET valor_pago=0 WHERE id IN %s", (tupla_ids_reais,))
                        if delta != 0 or delta_pago != 0: execute_query("INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, valor_pago, data_vencimento, pago, forma_pagamento, prioridade) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('Entrada', 'Ajuste', subg, f'🏥 Ajuste de Plantão {subg}', delta, delta_pago, row['Data'], novo_pago, 'Outros', row['prioridade']))
                    else:
                        execute_query("UPDATE lancamentos SET pago=%s, prioridade=%s, descricao=%s, valor=%s, valor_pago=%s, data_vencimento=%s WHERE id=%s", (novo_pago, row['prioridade'], nova_desc, novo_valor, novo_valor_pago, row['Data'], tupla_ids_reais[0]))

                        if orig_row['tipo'] == 'Despesa' and novo_pago == 1 and orig_row['pago'] == 0:
                            executar_abatimento_envelope(orig_row['categoria'], orig_row['subgrupo'], novo_valor_pago, mes_selecionado, ano_selecionado)
            flash("success", "✅ Alterações salvas com sucesso!")
            st.rerun()

        st.divider()

        with st.expander("📱 Despesas Pendentes para WhatsApp (Copiar)", expanded=False):
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
        st.subheader("✏️ Edição Estrutural Avançada")
        mask_individuais = (~df['forma_pagamento'].isin(['Crédito'])) & (~(df['tipo'] == 'Entrada') & ~df['descricao'].str.contains('Plantão', na=False))
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

                if st.button("💾 Confirmar Mudança Estrutural", type="primary"):
                    v_final = parse_valor(e_val)
                    if e_escopo == "Apenas neste lançamento":
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s WHERE id=%s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                    else:
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s WHERE id=%s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, forma_pagamento=%s WHERE compra_id=%s AND data_vencimento > %s AND id != %s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_forma, r_sel['compra_id'], r_sel['data_vencimento'], int(sel_id)))
                    flash("success", "Lançamento atualizado com sucesso!"); st.rerun()

# =================================================================
# 12. MÓDULO 3: DEMONSTRATIVO (COM ANALÍTICO DE PROVISÕES)
# =================================================================

elif menu == "📑 Demonstrativo":
    st.header("📑 Demonstrativo Financeiro")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))

    tab_dem, tab_env = st.tabs(["📊 Balanço Mensal", "⚖️ Provisões Orçado vs Realizado"])

    with tab_dem:
        if not df.empty:
            df['valor'] = df['valor'].astype(float)
            df['valor_pago'] = df['valor_pago'].fillna(0.0).astype(float)
            df['Data BR'] = pd.to_datetime(df['data_vencimento']).dt.strftime('%d/%m/%Y')
            df_e, df_d = df[df['tipo'] == 'Entrada'], df[df['tipo'] == 'Despesa']

            c_m1, c_m2, c_m3 = st.columns(3)
            c_m1.metric("Receita Total (Planejada)", f"R$ {format_brl(df_e['valor'].sum())}")
            c_m2.metric("Despesa Total (Planejada)", f"R$ {format_brl(df_d['valor'].sum())}")
            c_m3.metric("Orçamento Base-Zero (ZBB)", f"R$ {format_brl(df_e['valor'].sum() - df_d['valor'].sum())}")

            falta_receber = df_e['valor'].sum() - df_e[df_e['pago'] == 1]['valor'].sum()
            falta_pagar = df_d['valor'].sum() - df_d[df_d['pago'] == 1]['valor'].sum()

            c_res1, c_res2 = st.columns(2)
            c_res1.metric("⏳ Restante a Receber (Efetivo)", f"R$ {format_brl(falta_receber)}")
            c_res2.metric("🚨 Restante a Pagar (Efetivo)", f"R$ {format_brl(falta_pagar)}")

            # "Ajuste" é um lançamento de apoio interno (gerado ao editar cartão/plantão
            # consolidado em Fluxo e Prioridades) -- os valores acima já o incluem
            # corretamente, mas ele fica oculto das listagens por categoria abaixo.
            df_e_visivel = df_e[df_e['categoria'] != 'Ajuste']
            df_d_visivel = df_d[df_d['categoria'] != 'Ajuste']

            st.divider()
            st.subheader("📊 Distribuição de Despesas")
            if not df_d_visivel.empty:
                df_grp = df_d_visivel.groupby('categoria')['valor'].sum().reset_index()
                fig = px.pie(df_grp, values='valor', names='categoria', hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(aplicar_tema_grafico(fig), use_container_width=True)

            def exibir_demonstrativo(dataframe):
                if dataframe.empty: return
                dataframe['Desc. Exibição'] = dataframe.apply(lambda r: f"{r['descricao']} ({int(r['parcela_atual'])}/{int(r['total_parcelas'])})" if pd.notna(r.get('total_parcelas')) and r['total_parcelas'] > 1 and r['total_parcelas'] != 999 else r['descricao'], axis=1)
                dataframe['Status'] = dataframe['pago'].apply(lambda x: '✅ Pago' if x == 1 else '⏳ Pendente')

                tabela = dataframe[['Data BR', 'Desc. Exibição', 'valor', 'valor_pago', 'prioridade', 'Status']].rename(
                    columns={'Data BR': 'Data', 'Desc. Exibição': 'Descrição', 'valor': 'Planejado', 'valor_pago': 'Pago/Real', 'prioridade': 'Prioridade'}
                )

                def _cor_linha_demonstrativo(row):
                    if row['Status'] == '✅ Pago':
                        return ['background-color: #17241E; color: #E8EAED'] * len(row)
                    return ['background-color: #29241A; color: #E8EAED'] * len(row)

                estilo = tabela.style.apply(_cor_linha_demonstrativo, axis=1).format({
                    'Planejado': lambda v: f"R$ {format_brl(v)}",
                    'Pago/Real': lambda v: f"R$ {format_brl(v)}"
                })
                st.dataframe(estilo, hide_index=True, use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🟢 Entradas Detalhadas")
                for cat in sorted(df_e_visivel['categoria'].unique(), key=lambda x: str(x).lower()):
                    df_c = df_e_visivel[df_e_visivel['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            if df_s.empty: continue
                            st.markdown(f"**🔹 {sub if sub else 'Geral'}**")
                            exibir_demonstrativo(df_s)
            with c2:
                st.subheader("🔴 Despesas Detalhadas")
                for cat in ordenar_categorias_com_prioridade(df_d_visivel['categoria'].unique()):
                    df_c = df_d_visivel[df_d_visivel['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            if df_s.empty: continue
                            st.markdown(f"**🔹 {sub if sub else 'Geral'}**")
                            exibir_demonstrativo(df_s)
        else:
            st.info("Sem lançamentos neste período.")

    with tab_env:
        st.subheader("⚖️ Acompanhamento de Envelopes (Despesas Variáveis)")
        st.markdown("Comparação em tempo real entre o teu teto orçamentário e o que já foi gasto.")

        df_envelopes_config = fetch_dataframe("SELECT categoria, subgrupo FROM categorias_personalizadas WHERE is_envelope = 1 AND tipo = 'Despesa'")

        if df_envelopes_config.empty:
            st.info("Nenhuma categoria está configurada como 'Envelope Virtual' atualmente. Vá a '⚙️ Gerenciar Categorias' para ativar.")
        elif df.empty:
            st.info("Sem transações no período ativo.")
        else:
            matriz_envelopes = []
            for _, combo in df_envelopes_config.drop_duplicates().iterrows():
                cat, sub = combo['categoria'], combo['subgrupo']
                df_pago = df[(df['categoria'] == cat) & (df['subgrupo'] == sub) & (df['pago'] == 1)]
                df_teto = df[(df['categoria'] == cat) & (df['subgrupo'] == sub) & (df['pago'] == 0)]

                realizado = float(df_pago['valor_pago'].sum())
                disponivel = float(df_teto['valor'].sum())  # pode ser negativo se estourou o teto
                orcamento_inicial = realizado + disponivel

                if realizado == 0 and disponivel == 0:
                    continue

                if disponivel > 0 and orcamento_inicial > 0:
                    percent_livre = (disponivel / orcamento_inicial) * 100
                    status_txt = f"🟢 {percent_livre:.1f}% disponível"
                elif disponivel == 0:
                    status_txt = "🟡 Limite exato atingido"
                else:
                    status_txt = f"🔴 Estourado em R$ {format_brl(abs(disponivel))}"

                matriz_envelopes.append({
                    "Categoria": cat,
                    "Subgrupo": sub if sub else "Geral",
                    "Orçamento Inicial (Teto)": orcamento_inicial,
                    "Gasto Realizado (Acumulado)": realizado,
                    "Saldo Restante Livre": disponivel,
                    "Métrica de Saúde": status_txt
                })

            if matriz_envelopes:
                df_matriz = pd.DataFrame(matriz_envelopes)

                def _cor_linha_envelope(row):
                    if row['Métrica de Saúde'].startswith('🔴'):
                        return ['background-color: #2A1B19; color: #E8EAED'] * len(row)
                    if row['Métrica de Saúde'].startswith('🟡'):
                        return ['background-color: #29241A; color: #E8EAED'] * len(row)
                    return ['background-color: #17241E; color: #E8EAED'] * len(row)

                estilo_env = df_matriz.style.apply(_cor_linha_envelope, axis=1).format({
                    'Orçamento Inicial (Teto)': lambda v: f"R$ {format_brl(v)}",
                    'Gasto Realizado (Acumulado)': lambda v: f"R$ {format_brl(v)}",
                    'Saldo Restante Livre': lambda v: f"R$ {format_brl(v)}"
                })
                st.dataframe(estilo_env, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum lançamento encontrado para os envelopes configurados neste mês.")

# =================================================================
# 13. MÓDULO: BALANÇO ANUAL
# =================================================================

elif menu == "📈 Balanço Anual":
    st.header("📈 Balanço Financeiro Anual")
    anos_disp = fetch_dataframe("SELECT DISTINCT EXTRACT(YEAR FROM data_vencimento) as ano FROM lancamentos ORDER BY ano DESC")
    if anos_disp.empty:
        st.info("Sem dados suficientes para gerar balanço anual.")
    else:
        ano_balanco = st.selectbox("Ano de Referência", anos_disp['ano'].astype(int).tolist(), index=0)
        for m in range(1, 13): processar_recorrencias_lazy(m, ano_balanco)

        df_ano = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(YEAR FROM data_vencimento) = %s", (ano_balanco,))
        if df_ano.empty:
            st.warning("Sem dados.")
        else:
            df_ano['valor'] = df_ano['valor'].astype(float)
            df_ano['valor_pago'] = df_ano['valor_pago'].fillna(0.0).astype(float)
            df_ano['mes_num'] = pd.to_datetime(df_ano['data_vencimento']).dt.month

            df_ano['hibrido_fpa'] = df_ano.apply(lambda r: float(r['valor_pago']) if r['pago'] == 1 else float(r['valor']), axis=1)
            mensal = df_ano.groupby(['mes_num', 'tipo'])['hibrido_fpa'].sum().unstack(fill_value=0.0)
            mensal = mensal.reindex(range(1, 13), fill_value=0.0).reset_index()

            for col in ['Entrada', 'Despesa']:
                if col not in mensal.columns: mensal[col] = 0.0
            mensal['Saldo'] = mensal['Entrada'] - mensal['Despesa']
            mensal['Mes'] = mensal['mes_num'].apply(lambda x: meses[x-1])
            mensal['Acumulado'] = mensal['Saldo'].cumsum()

            tot_ent = mensal['Entrada'].sum()
            tot_des = mensal['Despesa'].sum()
            lucro_ano = tot_ent - tot_des
            margem = (lucro_ano / tot_ent * 100) if tot_ent > 0 else 0

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Faturamento Anual", f"R$ {format_brl(tot_ent)}")
            c2.metric("Despesa Anual", f"R$ {format_brl(tot_des)}")
            c3.metric("Resultado Líquido Anual", f"R$ {format_brl(lucro_ano)}")
            c4.metric("Margem", f"{margem:.1f}%")

            st.divider()
            tab_graf1, tab_graf2 = st.tabs(["📊 Evolução Mensal", "🗂️ Composição de Gastos"])

            with tab_graf1:
                fig_evol = px.bar(mensal, x='Mes', y=['Entrada', 'Despesa'],
                                  barmode='group', title="Balanço FP&A Híbrido (Realizado + Projetado)",
                                  color_discrete_map={'Entrada': '#3FAE8D', 'Despesa': '#E0695C'},
                                  labels={'value': 'Valor (R$)', 'variable': 'Fluxo'})
                fig_evol.update_layout(legend_title_text='Fluxo')
                st.plotly_chart(aplicar_tema_grafico(fig_evol), use_container_width=True)

                fig_acum = px.area(mensal, x='Mes', y='Acumulado', title="Fluxo de Caixa Acumulado (Híbrido)",
                                   color_discrete_sequence=['#3FAE8D'], markers=True)
                st.plotly_chart(aplicar_tema_grafico(fig_acum), use_container_width=True)

            with tab_graf2:
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.subheader("Distribuição por Categoria")
                    df_desp_ano = df_ano[df_ano['tipo'] == 'Despesa'].groupby('categoria')['hibrido_fpa'].sum().reset_index()
                    fig_pie_d = px.pie(df_desp_ano, values='hibrido_fpa', names='categoria', hole=0.5)
                    fig_pie_d.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(aplicar_tema_grafico(fig_pie_d), use_container_width=True)
                with col_d2:
                    st.subheader("Maiores Centros de Custo (Subgrupos)")
                    df_sub_ano = df_ano[df_ano['tipo'] == 'Despesa'].groupby('subgrupo')['hibrido_fpa'].sum().sort_values(ascending=False).head(12).reset_index()
                    fig_sub = px.bar(df_sub_ano, x='hibrido_fpa', y='subgrupo', orientation='h',
                                     title="Top 12 Centros de Custo do Ano",
                                     color='hibrido_fpa', color_continuous_scale=['#29241A', '#DDA251', '#E0695C'])
                    fig_sub.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(aplicar_tema_grafico(fig_sub), use_container_width=True)

# =================================================================
# 14. MÓDULO: ESCALA VISUAL DE PLANTÕES
# =================================================================

elif menu == "🏥 Escala de Plantões":
    st.header("🏥 Escala Visual de Plantões")
    c_m, c_a = st.columns(2)
    with c_m: cal_mes = st.selectbox("Mês do Calendário", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1)
    with c_a: cal_ano = st.selectbox("Ano do Calendário", range(hoje.year-1, hoje.year+2), index=1)
    st.divider()
    df_t = fetch_dataframe("SELECT * FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'")
    df_m_cal = pd.DataFrame()
    if not df_t.empty:
        df_t['d_p_str'] = df_t['descricao'].str.extract(r'\((.*?)\)')
        df_t['d_p'] = pd.to_datetime(df_t['d_p_str'], format='%d/%m/%Y', errors='coerce').dt.date
        df_m_cal = df_t[(pd.to_datetime(df_t['d_p']).dt.month == cal_mes) & (pd.to_datetime(df_t['d_p']).dt.year == cal_ano)].copy()

    cols = st.columns(7)
    for i, dia in enumerate(["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]): cols[i].markdown(f"<div style='text-align: center; font-weight: 700; padding: 6px; font-family: Manrope, sans-serif; color:#8B94A0; font-size:0.78rem; text-transform:uppercase; letter-spacing:0.04em; border-bottom: 2px solid #3FAE8D;'>{dia}</div>", unsafe_allow_html=True)
    for week in calendar.monthcalendar(cal_ano, cal_mes):
        w_cols = st.columns(7)
        for i, day in enumerate(week):
            with w_cols[i]:
                if day != 0:
                    cd = datetime.date(cal_ano, cal_mes, day)
                    bg = "background-color: rgba(63, 174, 141, 0.14);" if cd == hoje else "background-color: #1B2127;"
                    brdr = "border: 2px solid #3FAE8D;" if cd == hoje else "border: 1px solid #2A3138;"
                    html = f"<div style='{bg} {brdr} border-radius: 10px; padding: 6px; min-height: 90px; margin-top: 6px;'><div style='text-align:right; font-weight:600; font-family: Manrope, sans-serif; color:#E8EAED; font-size:0.85rem;'>{day}</div>"
                    if not df_m_cal.empty:
                        for _, s in df_m_cal[df_m_cal['d_p'] == cd].iterrows(): html += f"<div style='background-color:#3FAE8D; color:#0E1216; font-size:10px; font-weight:600; padding:2px 4px; border-radius:4px; margin-top:2px; white-space:nowrap; overflow:hidden;'>🏥 {s['subgrupo']}</div>"
                    st.markdown(html + "</div>", unsafe_allow_html=True)

    st.divider()
    st.subheader("📋 Gerenciar Escala Deste Mês")
    if not df_m_cal.empty:
        locais_disp = df_m_cal['subgrupo'].unique().tolist()
        sel_locais = st.multiselect("Filtrar por Hospital", locais_disp, placeholder="Todos os Hospitais")
        df_geren = df_m_cal[df_m_cal['subgrupo'].isin(sel_locais)] if sel_locais else df_m_cal
        df_geren = df_geren.sort_values('d_p').reset_index(drop=True)
        df_geren.insert(0, '🗑️ Apagar', False)
        df_geren['Data do Plantão'] = pd.to_datetime(df_geren['d_p']).dt.strftime('%d/%m/%Y')
        edit_esc = st.data_editor(df_geren[['🗑️ Apagar', 'Data do Plantão', 'subgrupo', 'valor']], use_container_width=True, hide_index=True)

        confirm_del_lote = st.checkbox("⚠️ Confirmo que quero apagar os plantões selecionados na planilha acima")
        c_b1, c_b2 = st.columns(2)
        with c_b1:
            if st.button("💾 Salvar Exclusões Selecionadas", disabled=not confirm_del_lote):
                n_apagados = int(edit_esc['🗑️ Apagar'].sum())
                for i, r in edit_esc.iterrows():
                    if r['🗑️ Apagar']: execute_query("DELETE FROM lancamentos WHERE id = %s", (int(df_geren.loc[i, 'id']),))
                flash("success", f"🗑️ {n_apagados} plantão(ões) apagado(s) com sucesso!")
                st.rerun()
        with c_b2:
            if st.button("🚨 Apagar TUDO o que está listado acima", disabled=not confirm_del_lote):
                ids = tuple(df_geren['id'].tolist())
                if ids:
                    if len(ids) == 1: execute_query("DELETE FROM lancamentos WHERE id = %s", (ids[0],))
                    else: execute_query("DELETE FROM lancamentos WHERE id IN %s", (ids,))
                    flash("success", f"🗑️ {len(ids)} plantão(ões) apagado(s) com sucesso!")
                    st.rerun()
    else: st.info("Sem plantões registrados.")

    st.divider()
    st.subheader("🗑️ Limpeza de Histórico de Plantões")
    confirm_purgar_global = st.checkbox("🚨 Confirmo que quero APAGAR O HISTÓRICO GLOBAL e irreversível de plantões do banco de dados")
    if st.button("🚨 Purgar Histórico Global de Plantões", type="primary", disabled=not confirm_purgar_global):
        execute_query("DELETE FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'")
        flash("success", "Histórico de plantões purgado."); st.rerun()

    st.divider()
    st.subheader("➕ Adicionar à Escala")
    modo = st.radio("Modo", ["Dia Específico", "Plantões Fixos na Semana"], horizontal=True)
    locais_dyn = list(set([item for sublist in ESTRUTURA["Entrada"].values() for item in sublist]))
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            loc_p = st.selectbox("🏥 Local", locais_dyn if locais_dyn else ["Vazio"])
            default_vals = {"v": 1000.0, "m": 1, "d": 10}
            if loc_p != "Vazio":
                res = fetch_dataframe("SELECT valor_padrao, atraso_meses, dia_pagamento FROM categorias_personalizadas WHERE subgrupo = %s AND tipo = 'Entrada' LIMIT 1", (loc_p,))
                if not res.empty:
                    if pd.notna(res.iloc[0]['valor_padrao']): default_vals["v"] = float(res.iloc[0]['valor_padrao'])
                    if pd.notna(res.iloc[0]['atraso_meses']): default_vals["m"] = int(res.iloc[0]['atraso_meses'])
                    if pd.notna(res.iloc[0]['dia_pagamento']): default_vals["d"] = int(res.iloc[0]['dia_pagamento'])
            if modo == "Dia Específico": d_p = st.date_input("Data", value=data_contexto_ativo)
            else: dias_s = st.multiselect("Dias", options=[0,1,2,3,4,5,6], format_func=lambda x: ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"][x])
        with c2:
            v_t = st.number_input("Valor (R$)", value=default_vals["v"])
            reg_m = st.number_input("Atraso (Meses)", min_value=0, max_value=6, value=default_vals["m"])
            reg_d = st.number_input("Dia Pagto", min_value=1, max_value=31, value=default_vals["d"])
            if modo != "Dia Específico": m_rec = st.number_input("Repetir por meses", min_value=1, value=6)

        if st.button("🚀 Registrar Plantão", type="primary") and loc_p != "Vazio":
            cat_escolhida = next((c for c, subs in ESTRUTURA.get("Entrada", {}).items() if loc_p in subs), "N/A")
            regs = []
            if modo == "Dia Específico":
                m_f = (d_p.month + reg_m - 1) % 12 + 1
                a_f = d_p.year + (d_p.month + reg_m - 1) // 12
                regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({d_p.strftime('%d/%m/%Y')})", v_t, datetime.date(a_f, m_f, reg_d), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢", 0.0))
            elif dias_s:
                for off in range(m_rec):
                    m_a, a_a = (mes_selecionado + off - 1) % 12 + 1, ano_selecionado + (mes_selecionado + off - 1) // 12
                    m_p, a_p = (m_a + reg_m - 1) % 12 + 1, a_a + (m_a + reg_m - 1) // 12
                    for d in range(1, calendar.monthrange(a_a, m_a)[1] + 1):
                        curr = datetime.date(a_a, m_a, d)
                        if curr.weekday() in dias_s: regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({curr.strftime('%d/%m/%Y')})", v_t, datetime.date(a_p, m_p, reg_d), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢", 0.0))
            if regs:
                execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago) VALUES %s''', regs)
                flash("success", f"✅ {len(regs)} plantão(ões) registrado(s) com sucesso!")
                st.rerun()
