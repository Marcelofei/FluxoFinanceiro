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
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_producao_variavel INTEGER DEFAULT 0;")

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
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS eh_estimativa INTEGER DEFAULT 0;")

    # Tabela leve só para enriquecer dívidas detectadas automaticamente (nome do
    # credor e taxa, ambos opcionais e puramente informativos). O painel de
    # Dívidas funciona sem nenhuma linha aqui -- isso só adiciona contexto.
    execute_query('''
        CREATE TABLE IF NOT EXISTS info_dividas (
            compra_id TEXT PRIMARY KEY,
            credor TEXT,
            taxa_juros_mensal NUMERIC
        );
    ''')

# =================================================================
# 2. MOTOR DE GERAÇÃO LAZY / RECORRÊNCIAS CONTRATUAIS
# =================================================================

def processar_recorrencias_lazy(mes, ano):
    """
    OTIMIZADO: (1) roda no máximo 1 vez por mês/ano por sessão (guarda em
    session_state) em vez de a cada clique; (2) verifica todos os contratos
    existentes numa ÚNICA query em vez de 1 query por contrato (era N+1);
    (3) insere tudo que faltar em um único lote.
    A guarda é invalidada quando você cria/edita categorias, então uma
    categoria recorrente nova gera o teto do mês na hora, sem reiniciar.
    """
    guarda = f"rec_processado_{mes}_{ano}"
    if st.session_state.get(guarda): return

    df_contratos = fetch_dataframe("SELECT * FROM categorias_personalizadas WHERE is_recorrente = 1")
    if df_contratos.empty:
        st.session_state[guarda] = True
        return

    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]

    # UMA query pra descobrir quais contratos já geraram lançamento neste mês
    df_exist = fetch_dataframe(
        "SELECT DISTINCT compra_id FROM lancamentos WHERE compra_id LIKE 'rec\\_%%' AND EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s",
        (mes, ano)
    )
    ja_gerados = set(df_exist['compra_id'].tolist()) if not df_exist.empty else set()

    registros = []
    for _, contrato in df_contratos.iterrows():
        compra_id_contrato = f"rec_{contrato['id']}"
        if compra_id_contrato in ja_gerados: continue

        dt_inicio = pd.to_datetime(contrato['data_inicio']).date() if pd.notna(contrato['data_inicio']) else datetime.date(ano, mes, 1)

        # Envelope sempre nasce no último dia do mês (como Provisão funcionava antes),
        # independente do campo "Dia de Pagamento" -- esse campo só vale pra contratos
        # fixos normais (Aluguel, Internet, etc).
        eh_envelope = int(contrato.get('is_envelope') or 0) == 1
        dia_alvo = ultimo_dia_mes if eh_envelope else min(int(contrato['dia_pagamento'] or 1), ultimo_dia_mes)
        dt_limite_alvo = datetime.date(ano, mes, dia_alvo)

        if dt_limite_alvo < dt_inicio: continue

        val_p = float(contrato['valor_padrao'] or 0.0)
        sufixo = "(Envelope do Mês)" if eh_envelope else "(Recorrente)"
        desc_c = f"{contrato['categoria']} - {contrato['subgrupo'] or ''} {sufixo}"
        registros.append((contrato['tipo'], contrato['categoria'], contrato['subgrupo'], desc_c, val_p,
                          dt_limite_alvo, 1, 1, 0, compra_id_contrato, 'Outros', 'Média 🟡', 0.0))

    if registros:
        execute_values_query('''
            INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago)
            VALUES %s
        ''', registros)

    st.session_state[guarda] = True

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

    /* -----------------------------------------------------------
       RESPONSIVO PARA MOBILE.
       st.columns() do Streamlit não empilha sozinho em tela estreita --
       fica tudo espremido lado a lado. Essa regra força empilhamento
       vertical abaixo de 640px (celular; não afeta tablet/desktop),
       e ajusta espaçamento/tamanho de fonte pra caber melhor.
       Não alcança a GRADE INTERNA do st.data_editor/st.dataframe --
       isso é limite real do componente, não tem CSS que resolva.
       ----------------------------------------------------------- */
    @media (max-width: 640px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
        }
        [data-testid="stHorizontalBlock"] > div {
            width: 100% !important;
            min-width: 100% !important;
            flex: 1 1 100% !important;
        }
        .block-container {
            padding-left: 0.9rem !important;
            padding-right: 0.9rem !important;
            padding-top: 1.2rem !important;
        }
        div[data-testid="stMetricValue"] { font-size: 1.25rem !important; }
        h1 { font-size: 1.3rem !important; }
        h2 { font-size: 1.15rem !important; }
        h3 { font-size: 1.05rem !important; }
        .stButton button {
            min-height: 2.6rem;
            font-size: 0.92rem !important;
        }
        section[data-testid="stSidebar"] .stButton button {
            min-height: 2.4rem;
        }
        div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
            font-size: 0.85rem !important;
        }
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

@st.cache_data(ttl=300, show_spinner=False)
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

def invalidar_caches_estruturais():
    """Chamar sempre que categorias forem criadas/editadas/excluídas: limpa o
    cache da estrutura e as guardas de recorrência, pra que uma categoria
    recorrente nova gere o lançamento do mês imediatamente."""
    get_estrutura_dinamica.clear()
    for k in [k for k in list(st.session_state.keys()) if str(k).startswith('rec_processado_')]:
        del st.session_state[k]

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

def _nav_btn(label, key, container=None):
    alvo = container if container is not None else st.sidebar
    ativo = st.session_state.menu_atual == label
    if alvo.button(label, key=key, type="primary" if ativo else "secondary", use_container_width=True):
        st.session_state.menu_atual = label
        st.rerun()

# Navegação organizada por frequência de uso: o dia a dia fica sempre à vista,
# telas de consulta ocasional ficam recolhidas -- menos opções na tela, mesma função.
st.sidebar.markdown("<div class='nav-eyebrow'>Dia a Dia</div>", unsafe_allow_html=True)
_nav_btn("🏠 Início", "nav_inicio")
_nav_btn("📊 Fluxo e Prioridades", "nav_fluxo")
_nav_btn("📝 Lançamentos", "nav_lancamentos")

st.sidebar.markdown("<div class='nav-eyebrow'>Análise</div>", unsafe_allow_html=True)
_nav_btn("📑 Demonstrativo", "nav_demonstrativo")
_nav_btn("💳 Dívidas", "nav_dividas")

_relatorios_aberto = st.session_state.menu_atual in ("📈 Balanço Anual", "🏥 Escala de Plantões", "⚙️ Gerenciar Categorias")
with st.sidebar.expander("📂 Relatórios e Configuração", expanded=_relatorios_aberto):
    _nav_btn("📈 Balanço Anual", "nav_balanco", container=st)
    _nav_btn("🏥 Escala de Plantões", "nav_escala", container=st)
    _nav_btn("⚙️ Gerenciar Categorias", "nav_categorias", container=st)

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

def validar_csv_lancamentos(df_imp):
    """
    Valida TODAS as linhas ANTES de qualquer coisa tocar o banco. Se algo
    estiver errado, retorna a lista de problemas (linha + coluna + motivo)
    e a restauração inteira é cancelada -- a tabela antiga nunca chega a
    ser apagada. Isso resolve o problema de fundo do 'integer out of
    range': antes, um valor ruim só era descoberto DEPOIS do TRUNCATE já
    ter se efetivado (autocommit), sem chance de desfazer.
    """
    problemas = []
    colunas_obrigatorias = ['tipo', 'categoria', 'descricao', 'valor', 'data_vencimento', 'compra_id']
    for col in colunas_obrigatorias:
        if col not in df_imp.columns:
            problemas.append(f"Coluna obrigatória '{col}' não existe no CSV.")
    if problemas: return problemas, None

    df_v = df_imp.copy()

    # parcela_atual / total_parcelas / pago são INTEGER no banco -- NaN vira
    # erro de tipo lá (as 48 linhas de 'Ajuste' antigas nascem sem esses
    # campos preenchidos). Aqui a gente já resolve isso ANTES de mandar pro
    # Postgres: vira 1/1/pago conforme o próprio valor de 'pago' da linha.
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
        fora_do_limite = df_v[(df_v[col].abs() > LIMITE_INT)]
        for idx, row in fora_do_limite.iterrows():
            problemas.append(f"Linha {idx+2}: coluna '{col}' com valor {row[col]} -- fora do limite do banco (máx {LIMITE_INT}).")

    # valor / valor_pago precisam ser números finitos (não texto, não infinito)
    for col in ['valor', 'valor_pago'] if 'valor_pago' in df_v.columns else ['valor']:
        nums = pd.to_numeric(df_v[col], errors='coerce')
        invalidos = df_v[nums.isna() | ~pd.Series(nums).apply(lambda x: pd.notna(x) and abs(x) != float('inf'))]
        for idx, row in invalidos.iterrows():
            problemas.append(f"Linha {idx+2}: coluna '{col}' com valor '{row[col]}' não é um número válido.")

    # data_vencimento precisa ser uma data reconhecível
    datas = pd.to_datetime(df_v['data_vencimento'], errors='coerce')
    for idx in df_v[datas.isna()].index:
        problemas.append(f"Linha {idx+2}: coluna 'data_vencimento' com valor '{df_v.loc[idx, 'data_vencimento']}' não é uma data válida.")

    return problemas, df_v

def importar_csv(arquivo):
    try:
        df_imp = pd.read_csv(arquivo)
        if 'forma_pagamento' not in df_imp.columns: df_imp['forma_pagamento'] = 'Outros'
        if 'prioridade' not in df_imp.columns: df_imp['prioridade'] = 'Baixa 🟢'
        if 'valor_pago' not in df_imp.columns: df_imp['valor_pago'] = df_imp['valor']

        problemas, df_v = validar_csv_lancamentos(df_imp)
        if problemas:
            st.error(f"❌ Restauração cancelada ANTES de tocar no banco -- {len(problemas)} problema(s) encontrado(s). "
                     "Seus dados atuais continuam intactos.")
            with st.expander("Ver detalhes dos problemas", expanded=True):
                for p in problemas[:50]:
                    st.write(f"• {p}")
                if len(problemas) > 50:
                    st.caption(f"... e mais {len(problemas) - 50} problema(s).")
            return False

        registros = [(
            r['tipo'], r['categoria'], r['subgrupo'], r['descricao'], r['valor'],
            r['data_vencimento'], int(r['parcela_atual']), int(r['total_parcelas']), int(r['pago']),
            r['compra_id'], r['forma_pagamento'], r['prioridade'], r['valor_pago']
        ) for _, r in df_v.iterrows()]

        # TRANSAÇÃO ATÔMICA DE VERDADE: TRUNCATE e INSERT agora vivem na MESMA
        # transação. Antes, com autocommit=True, o TRUNCATE já se efetivava
        # sozinho antes do INSERT ser tentado -- se o INSERT falhasse, a
        # tabela ficava vazia sem chance de desfazer. Agora, se qualquer
        # coisa falhar aqui dentro, TUDO volta ao estado anterior.
        conn = get_connection()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE lancamentos RESTART IDENTITY")
                execute_values(cur, '''
                    INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago)
                    VALUES %s
                ''', registros)
            conn.commit()
        except Exception as e:
            conn.rollback()
            st.error(f"Erro Crítico de Restauração (revertido -- seus dados antigos foram preservados): {e}")
            return False
        finally:
            conn.autocommit = True
        return True
    except Exception as e:
        st.error(f"Erro Crítico de Restauração: {e}")
        return False

# Backup sob demanda: o SELECT da tabela inteira só roda quando você pede,
# não a cada interação com o app (antes, esse custo era pago em TODO clique).
if st.sidebar.button("📥 Preparar backup (CSV)", key="btn_prep_backup"):
    st.session_state['_backup_csv'] = exportar_csv()
if st.session_state.get('_backup_csv') is not None:
    st.sidebar.download_button("⬇️ Baixar backup pronto", data=st.session_state['_backup_csv'],
                               file_name=f"backup_{hoje.strftime('%d_%m_%Y')}.csv", mime="text/csv")
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

    # CORREÇÃO de consistência: as métricas agora seguem o Período Ativo da
    # sidebar (antes usavam sempre o mês corrente, ignorando sua seleção).
    st.caption(f"Métricas de {meses[mes_selecionado-1]}/{ano_selecionado} · agenda sempre dos próximos 7 dias")
    dt_limite = hoje + datetime.timedelta(days=7)
    cols_lanc = "id, data_vencimento, tipo, categoria, subgrupo, descricao, valor, pago, forma_pagamento"
    df_atraso = fetch_dataframe(
        f"SELECT {cols_lanc} FROM lancamentos WHERE pago = 0 AND data_vencimento < %s "
        f"AND EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s "
        f"ORDER BY data_vencimento ASC, tipo",
        (hoje, mes_selecionado, ano_selecionado)
    )
    df_7d = fetch_dataframe(f"SELECT {cols_lanc} FROM lancamentos WHERE data_vencimento BETWEEN %s AND %s ORDER BY data_vencimento ASC, tipo", (hoje, dt_limite))
    df_mes_atual = fetch_dataframe("SELECT tipo, valor, valor_pago, pago FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))

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

    # -----------------------------------------------------------------------
    # CONSOLIDAÇÃO: mesma ideia do Fluxo e Prioridades -- compras de cartão
    # e plantões viram 1 linha só, pra uma fatura com várias compras não
    # tomar a tela inteira e esconder os outros vencimentos. Isso é só visual/
    # de conveniência aqui (não substitui o Fluxo e Prioridades, que é o
    # lugar certo pra dar baixa detalhada e reconciliar mês inteiro).
    # Reaproveitada tanto pros Atrasados quanto pra Agenda de 7 dias.
    # -----------------------------------------------------------------------
    def _consolidar_lancamentos(df):
        df = df.copy()
        df['valor'] = df['valor'].astype(float)
        mask_cred = (df['tipo'] == 'Despesa') & (df['forma_pagamento'] == 'Crédito')
        linhas = []
        if mask_cred.any():
            grp = df[mask_cred]
            linhas.append({
                'descricao': f"💳 Cartão de Crédito ({len(grp)} compra{'s' if len(grp)>1 else ''})",
                'tipo': 'Despesa', 'valor': grp['valor'].sum(),
                'pago': 1 if (grp['pago'] == 1).all() else 0,
                'data_vencimento': grp['data_vencimento'].min(),
                'ids': grp['id'].astype(int).tolist(), 'categoria': None, 'subgrupo': None,
            })

        mask_plant = (df['tipo'] == 'Entrada') & df['descricao'].str.contains('plant', case=False, na=False)
        if mask_plant.any():
            for subg_nome, grp in df[mask_plant].groupby('subgrupo'):
                linhas.append({
                    'descricao': f"🏥 Plantões {subg_nome} ({len(grp)})",
                    'tipo': 'Entrada', 'valor': grp['valor'].sum(),
                    'pago': 1 if (grp['pago'] == 1).all() else 0,
                    'data_vencimento': grp['data_vencimento'].min(),
                    'ids': grp['id'].astype(int).tolist(), 'categoria': None, 'subgrupo': None,
                })

        for _, r in df[~mask_cred & ~mask_plant].iterrows():
            linhas.append({
                'descricao': r['descricao'], 'tipo': r['tipo'], 'valor': r['valor'], 'pago': int(r['pago']),
                'data_vencimento': r['data_vencimento'], 'ids': [int(r['id'])],
                'categoria': r['categoria'], 'subgrupo': r['subgrupo'],
            })
        return sorted(linhas, key=lambda x: (x['data_vencimento'], x['tipo']))

    def _exibir_linhas_com_acao(linhas, prefixo_key, atrasado=False):
        # Ação em 1 clique: dá baixa direto daqui (individual OU em lote, se
        # for uma linha consolidada), sem precisar navegar até Fluxo e Prioridades.
        for idx, r in enumerate(linhas):
            eh_pago = r['pago'] == 1
            eh_despesa = r['tipo'] == 'Despesa'
            dt_str = pd.to_datetime(r['data_vencimento']).strftime('%d/%m')
            icone = "📤" if eh_despesa else "📥"
            cor_data = "#E0695C" if atrasado else "inherit"

            c_lin1, c_lin2, c_lin3 = st.columns([5.2, 1.6, 1.4])
            with c_lin1:
                st.markdown(f"{icone} <span style='color:{cor_data}; font-weight:600;'>{dt_str}</span> · {r['descricao']}", unsafe_allow_html=True)
            with c_lin2:
                st.markdown(f"<div style='text-align:right; font-family: IBM Plex Mono, monospace;'>R$ {format_brl(r['valor'])}</div>", unsafe_allow_html=True)
            with c_lin3:
                if eh_pago:
                    st.markdown("✅ <span style='color:#8B94A0;'>Pago</span>" if eh_despesa else "✅ <span style='color:#8B94A0;'>Recebido</span>", unsafe_allow_html=True)
                else:
                    rotulo_acao = "✓ Pagar" if eh_despesa else "✓ Receber"
                    if st.button(rotulo_acao, key=f"{prefixo_key}_{idx}_{'_'.join(map(str, r['ids']))}", use_container_width=True):
                        ids_tupla = tuple(r['ids'])
                        if len(ids_tupla) == 1:
                            execute_query("UPDATE lancamentos SET pago=1, valor_pago=valor WHERE id=%s", (ids_tupla[0],))
                        else:
                            execute_query("UPDATE lancamentos SET pago=1, valor_pago=valor WHERE id IN %s", (ids_tupla,))
                        if eh_despesa and r['categoria'] is not None:
                            dt_v = pd.to_datetime(r['data_vencimento'])
                            executar_abatimento_envelope(r['categoria'], r['subgrupo'], float(r['valor']), int(dt_v.month), int(dt_v.year))
                        flash("success", f"✅ '{r['descricao']}' marcado como {'pago' if eh_despesa else 'recebido'}!")
                        st.rerun()

    st.divider()
    if not df_atraso.empty:
        linhas_atraso = _consolidar_lancamentos(df_atraso)
        st.subheader(f"🚨 Atrasados em {meses[mes_selecionado-1]}/{ano_selecionado} ({len(linhas_atraso)})")
        st.caption("Vencidos dentro do mês selecionado na sidebar e ainda não marcados como pagos/recebidos.")
        _exibir_linhas_com_acao(linhas_atraso, "quickpay_atraso", atrasado=True)
        st.divider()

    st.subheader("🗓️ Agenda de Vencimentos (Próximos 7 dias)")
    if df_7d.empty:
        st.success("Nenhuma conta vencendo ou receita prevista para os próximos 7 dias! 🎉")
    else:
        linhas_7d = _consolidar_lancamentos(df_7d)
        _exibir_linhas_com_acao(linhas_7d, "quickpay_7d")

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
            st.caption("💡 Envelope Virtual sempre nasce no último dia do mês com o valor total planejado, e vai sendo abatido conforme você lança despesas pagas nessa categoria/subgrupo — o campo 'Dia de Pagamento' abaixo não é usado neste caso.")

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
                invalidar_caches_estruturais(); flash("success", "Categoria adicionada com sucesso!"); st.rerun()

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
                    st.caption("💡 Envelope Virtual sempre nasce no último dia do mês com o valor total planejado, e vai sendo abatido conforme você lança despesas pagas nessa categoria/subgrupo — o campo 'Dia de Pagamento' abaixo não é usado neste caso.")

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
                    invalidar_caches_estruturais(); flash("success", "Categoria atualizada com sucesso!"); st.rerun()
        else: st.info("Nenhuma categoria encontrada.")

    with tab_del:
        if not df_custom_global.empty:
            opcoes_del_local = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom_global.iterrows()}
            sel_del = st.selectbox("Selecione o item para excluir:", options=[None] + list(opcoes_del_local.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_del_local[x], key="del_select_target")
            if sel_del and st.button("🗑️ Excluir Selecionado", type="primary", key="del_save_btn"):
                execute_query("DELETE FROM categorias_personalizadas WHERE id = %s", (sel_del,))
                invalidar_caches_estruturais(); flash("success", "Categoria excluída com sucesso!"); st.rerun()

    st.divider()
    with st.expander("🧹 Limpeza de Lançamentos Antigos (tag 'Provisão')"):
        st.caption("Itens lançados quando a Provisão ainda existia em 'Lançamentos'. Busca direto pela tag no banco, "
                   "independente de em qual aba eles aparecem hoje.")
        df_provisao_antiga = fetch_dataframe("SELECT id, tipo, categoria, subgrupo, descricao, valor, data_vencimento, pago FROM lancamentos WHERE descricao ILIKE %s ORDER BY data_vencimento", ('%(Provisão)%',))
        if df_provisao_antiga.empty:
            st.success("Nenhum lançamento com a tag 'Provisão' encontrado.")
        else:
            df_provisao_antiga['valor'] = df_provisao_antiga['valor'].astype(float)
            st.warning(f"Encontrados {len(df_provisao_antiga)} lançamento(s), somando R$ {format_brl(df_provisao_antiga['valor'].sum())}.")
            st.dataframe(df_provisao_antiga[['data_vencimento', 'tipo', 'categoria', 'subgrupo', 'descricao', 'valor', 'pago']], use_container_width=True, hide_index=True)
            confirm_limpeza_prov = st.checkbox("⚠️ Confirmo que quero apagar TODOS os lançamentos listados acima, permanentemente", key="confirm_limpeza_prov")
            if st.button("🚨 Apagar Todos os Lançamentos 'Provisão' Listados", type="primary", disabled=not confirm_limpeza_prov, key="btn_limpeza_prov"):
                ids_apagar = tuple(df_provisao_antiga['id'].tolist())
                if len(ids_apagar) == 1:
                    execute_query("DELETE FROM lancamentos WHERE id = %s", (ids_apagar[0],))
                else:
                    execute_query("DELETE FROM lancamentos WHERE id IN %s", (ids_apagar,))
                flash("success", f"🧹 {len(ids_apagar)} lançamento(s) antigo(s) apagado(s)."); st.rerun()

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

        # -----------------------------------------------------------
        # CONSOLIDAÇÃO (feita sobre TODO o mês, ANTES de qualquer filtro).
        #
        # CORREÇÃO: antes, a consolidação de Cartão de Crédito/Plantões
        # rodava sobre o resultado JÁ FILTRADO por Tipo/Categoria. Se você
        # deixasse (mesmo sem querer) um filtro de categoria ativo, marcar
        # a linha "Fatura Consolidada" como paga só dava baixa nas compras
        # daquela categoria filtrada -- as demais compras de crédito
        # continuavam pago=0 no banco, e reapareciam como pendentes no
        # Demonstrativo (que não tem filtro nenhum), mesmo você tendo
        # "marcado tudo como pago" aqui. Agora a consolidação usa SEMPRE
        # o mês inteiro (df_base), então marcar Pago sempre baixa 100%
        # das compras reais, e o filtro só decide o que aparece na TELA.
        # -----------------------------------------------------------
        df_base = df.copy()
        df_base['ids_alvo'] = df_base['id'].astype(str)

        mask_cred_full = df_base['forma_pagamento'] == 'Crédito'
        dummy_credito = None
        if mask_cred_full.any():
            sum_cred = df_base[mask_cred_full]['valor'].sum()
            sum_pago_cred = df_base[mask_cred_full]['valor_pago'].sum()
            all_paid = (df_base[mask_cred_full]['pago'] == 1).all()
            ids_lote_credito = ','.join(df_base[mask_cred_full]['id'].astype(str))

            dummy_credito = pd.DataFrame([{
                'id': '-1', 'tipo': 'Despesa', 'categoria': 'N/A', 'subgrupo': '',
                'descricao': '💳 Cartão de Crédito (Fatura Consolidada)', 'valor': sum_cred,
                'valor_pago': sum_pago_cred, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10),
                'pago': 1 if all_paid else 0, 'compra_id': 'cartao_dummy',
                'forma_pagamento': 'Crédito', 'prioridade': 'Alta 🔴', 'ids_alvo': ids_lote_credito
            }])
        df_base_sem_cred = df_base[~mask_cred_full].copy()

        mask_plantoes_full = (df_base_sem_cred['tipo'] == 'Entrada') & df_base_sem_cred['descricao'].str.contains('plant', case=False, na=False)
        dummies_plantao = []
        if mask_plantoes_full.any():
            df_plantoes_full = df_base_sem_cred[mask_plantoes_full].copy()
            for nome_grupo, grupo in df_plantoes_full.groupby(['subgrupo', 'data_vencimento']):
                subg_nome, dt_venc = nome_grupo
                sum_pago_plantao = grupo['valor_pago'].sum()
                status_lote = 1 if (grupo['pago'] == 1).all() else 0
                ids_lote_plantao = ','.join(grupo['id'].astype(str))

                dummies_plantao.append({
                    'id': f'plantao_{subg_nome}', 'tipo': 'Entrada', 'categoria': grupo.iloc[0]['categoria'],
                    'subgrupo': subg_nome, 'descricao': f'🏥 Plantões {subg_nome} (Consolidado do Mês)',
                    'valor': grupo['valor'].sum(), 'valor_pago': sum_pago_plantao,
                    'data_vencimento': dt_venc, 'pago': status_lote, 'compra_id': 'plantao_dummy',
                    'forma_pagamento': 'Outros', 'prioridade': 'Baixa 🟢', 'ids_alvo': ids_lote_plantao
                })
        df_individuais = df_base_sem_cred[~mask_plantoes_full].copy()

        df_consolidado = df_individuais.copy()
        if dummy_credito is not None:
            df_consolidado = pd.concat([df_consolidado, dummy_credito], ignore_index=True)
        if dummies_plantao:
            df_consolidado = pd.concat([df_consolidado, pd.DataFrame(dummies_plantao)], ignore_index=True)

        # -----------------------------------------------------------
        # FILTROS (aplicados por cima do dataframe já consolidado).
        # As linhas consolidadas (Fatura/Plantões) ficam ISENTAS do filtro
        # de categoria -- elas representam várias categorias ao mesmo tempo,
        # então filtrar por categoria não deveria fazê-las sumir da tela
        # (o que também contribuía pra confusão de "sumiu, então já paguei
        # tudo"). Elas continuam respeitando o filtro de Tipo normalmente.
        # -----------------------------------------------------------
        st.subheader("🔍 Filtros")
        c_filt1, c_filt2 = st.columns(2)
        tipos_disp = df_individuais['tipo'].unique().tolist()
        with c_filt1: sel_tipo = st.multiselect("Filtrar por Tipo", tipos_disp, placeholder="Todos os Tipos")
        tipos_filtro = sel_tipo if sel_tipo else tipos_disp
        cat_disp = df_individuais[df_individuais['tipo'].isin(tipos_filtro)]['categoria'].unique().tolist()
        with c_filt2: sel_cat = st.multiselect("Filtrar por Categoria", cat_disp, placeholder="Todas as Categorias")
        cat_filtro = sel_cat if sel_cat else cat_disp

        eh_dummy = df_consolidado['id'].astype(str).isin(['-1']) | df_consolidado['id'].astype(str).str.startswith('plantao_')
        mask_individuais_filtro = (~eh_dummy) & df_consolidado['tipo'].isin(tipos_filtro) & df_consolidado['categoria'].isin(cat_filtro)
        mask_dummy_filtro = eh_dummy & df_consolidado['tipo'].isin(tipos_filtro)
        df_view = df_consolidado[mask_individuais_filtro | mask_dummy_filtro].copy()

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
        df_view.insert(0, '🗑️ Excluir', "")

        st.markdown("*(Dica: Modificar o 'Valor Real' preserva 100% o seu planejamento na coluna anterior).*")
        edit_df = st.data_editor(
            df_view[['🗑️ Excluir', 'Data', 'Alerta', 'prioridade', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago']],
            use_container_width=True, hide_index=True,
            column_config={
                "🗑️ Excluir": st.column_config.SelectboxColumn("Excluir", options=["", "Este", "Este e Futuros"], width="small"),
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

                # Só reescreve a descrição se você editou o texto de fato -- isso
                # também corrige um bug latente em que salvar qualquer coisa cortava
                # o sufixo "(1/12)" / "(Recorrente)" de TODAS as linhas, mesmo intocadas.
                desc_editada = str(row['Desc. Exibição']) != str(orig_row['Desc. Exibição'])
                nova_desc = row['Desc. Exibição'].split(' (')[0] if desc_editada else orig_row['descricao']
                tupla_ids_reais = tuple(map(int, orig_row['ids_alvo'].split(',')))
                excluir_futuros = row['🗑️ Excluir'] == "Este e Futuros"
                excluir_algo = row['🗑️ Excluir'] in ("Este", "Este e Futuros")

                # OTIMIZAÇÃO: só toca no banco nas linhas que realmente mudaram.
                # Antes, marcar 1 checkbox rodava 1 UPDATE pra CADA linha da tabela.
                mudou = (
                    excluir_algo
                    or novo_pago != int(orig_row['pago'])
                    or abs(novo_valor - orig_valor) > 0.004
                    or abs(novo_valor_pago - orig_valor_pago) > 0.004
                    or str(row['prioridade']) != str(orig_row['prioridade'])
                    or desc_editada
                    or row['Data'] != orig_row['Data']
                )
                if not mudou:
                    continue

                if excluir_algo:
                    if id_s == '-1': st.warning("Cartões consolidados não podem ser apagados aqui.")
                    elif id_s.startswith('plantao_'): execute_query("DELETE FROM lancamentos WHERE id IN %s", (tupla_ids_reais,))
                    else: execute_query("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s" if excluir_futuros else "DELETE FROM lancamentos WHERE id = %s", (orig_row['compra_id'], orig_row['data_vencimento']) if excluir_futuros else (tupla_ids_reais[0],))
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

            # -----------------------------------------------------------
            # CORREÇÃO: 'Ajuste' contava nas métricas acima (falta_pagar/
            # falta_receber) mas ficava invisível na lista por categoria,
            # o que podia parecer "o total não bate com o que vejo pra
            # marcar". Agora, se existir algum Ajuste PENDENTE (pago=0),
            # ele aparece aqui, separado, pra você conseguir reconciliar.
            # -----------------------------------------------------------
            df_ajustes_pend_despesa = df_d[(df_d['categoria'] == 'Ajuste') & (df_d['pago'] == 0)]
            df_ajustes_pend_entrada = df_e[(df_e['categoria'] == 'Ajuste') & (df_e['pago'] == 0)]
            if not df_ajustes_pend_despesa.empty or not df_ajustes_pend_entrada.empty:
                total_ajustes = df_ajustes_pend_despesa['valor'].sum() + df_ajustes_pend_entrada['valor'].sum()
                with st.expander(f"🔧 Ajustes pendentes não categorizados — R$ {format_brl(total_ajustes)} (incluído nos totais acima, mas fora das categorias abaixo)", expanded=True):
                    st.caption("Esses lançamentos nascem quando você edita o VALOR (não só o 'Pago') de uma linha "
                              "consolidada de Cartão/Plantão em '📊 Fluxo e Prioridades'.")
                    df_ajustes_tudo = pd.concat([df_ajustes_pend_despesa, df_ajustes_pend_entrada])
                    st.dataframe(
                        df_ajustes_tudo[['Data BR', 'tipo', 'descricao', 'valor']].rename(
                            columns={'Data BR': 'Data', 'tipo': 'Tipo', 'descricao': 'Descrição', 'valor': 'Valor'}
                        ).style.format({'Valor': lambda v: f"R$ {format_brl(v)}"}),
                        hide_index=True, use_container_width=True
                    )

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

        st.divider()
        with st.expander("🔍 Conciliação (verificação de integridade dos envelopes)"):
            st.caption("Confere se 'Gasto Realizado + Saldo Restante' ainda bate com o Valor Padrão configurado na "
                      "categoria. Diferenças podem ser normais (ex: você mudou o Valor Padrão depois que o teto do "
                      "mês já tinha sido gerado) — isso só te avisa pra você decidir se é esperado ou não.")

            df_conciliacao = fetch_dataframe('''
                WITH envelopes AS (
                    SELECT categoria, subgrupo, COALESCE(valor_padrao, 0) as valor_padrao
                    FROM categorias_personalizadas
                    WHERE is_envelope = 1 AND tipo = 'Despesa'
                ),
                realizado_mes AS (
                    SELECT categoria, subgrupo, SUM(valor_pago) as realizado
                    FROM lancamentos
                    WHERE tipo = 'Despesa' AND pago = 1 AND descricao NOT ILIKE %s
                      AND EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s
                    GROUP BY categoria, subgrupo
                ),
                teto_mes AS (
                    SELECT categoria, subgrupo, SUM(valor) as saldo_atual, COUNT(*) as qtd_linhas_teto
                    FROM lancamentos
                    WHERE tipo = 'Despesa' AND pago = 0 AND descricao ILIKE %s
                      AND EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s
                    GROUP BY categoria, subgrupo
                )
                SELECT
                    e.categoria, e.subgrupo, e.valor_padrao,
                    COALESCE(r.realizado, 0) as realizado,
                    COALESCE(t.saldo_atual, 0) as saldo_atual,
                    COALESCE(t.qtd_linhas_teto, 0) as qtd_linhas_teto
                FROM envelopes e
                LEFT JOIN realizado_mes r ON r.categoria = e.categoria AND r.subgrupo = e.subgrupo
                LEFT JOIN teto_mes t ON t.categoria = e.categoria AND t.subgrupo = e.subgrupo
            ''', ('%(Recorrente)%', mes_selecionado, ano_selecionado, '%(Recorrente)%', mes_selecionado, ano_selecionado))

            if df_conciliacao.empty:
                st.info("Nenhuma categoria de envelope configurada ainda.")
            else:
                df_conciliacao['valor_padrao'] = df_conciliacao['valor_padrao'].astype(float)
                df_conciliacao['realizado'] = df_conciliacao['realizado'].astype(float)
                df_conciliacao['saldo_atual'] = df_conciliacao['saldo_atual'].astype(float)
                df_conciliacao['diferenca'] = df_conciliacao['valor_padrao'] - (df_conciliacao['realizado'] + df_conciliacao['saldo_atual'])

                problemas = df_conciliacao[(df_conciliacao['diferenca'].abs() > 0.01) | (df_conciliacao['qtd_linhas_teto'] > 1) | (df_conciliacao['qtd_linhas_teto'] == 0)]

                if problemas.empty:
                    st.success("✅ Tudo conciliado — nenhuma divergência encontrada nos envelopes deste mês.")
                else:
                    st.warning(f"⚠️ {len(problemas)} item(ns) pra revisar:")
                    for _, p in problemas.iterrows():
                        motivos = []
                        if p['qtd_linhas_teto'] == 0:
                            motivos.append("nenhum teto gerado pra este mês ainda (recorrência pode não ter rodado)")
                        if p['qtd_linhas_teto'] > 1:
                            motivos.append(f"{int(p['qtd_linhas_teto'])} linhas de teto simultâneas (deveria ter só 1)")
                        if abs(p['diferenca']) > 0.01:
                            motivos.append(f"diferença de R$ {format_brl(abs(p['diferenca']))} entre o Valor Padrão e (realizado + saldo)")
                        st.markdown(f"**{p['categoria']} → {p['subgrupo'] or 'Geral'}** — {'; '.join(motivos)}")

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
# 14. MÓDULO: PAINEL DE DÍVIDAS
# =================================================================

elif menu == "💳 Dívidas":
    st.header("💳 Painel de Dívidas")
    st.caption("Detectado automaticamente a partir de despesas lançadas como 'Parcelada' em '📝 Lançamentos'. "
              "Compras de parcela única ou recorrências 'Fixa/Contínua' não entram aqui, porque não têm data de término.")

    df_dividas = fetch_dataframe('''
        SELECT
            compra_id, categoria, subgrupo,
            MIN(descricao) as descricao,
            SUM(valor) as valor_total,
            SUM(CASE WHEN pago = 1 THEN valor_pago ELSE 0 END) as valor_pago_total,
            MAX(total_parcelas) as total_parcelas,
            SUM(CASE WHEN pago = 1 THEN 1 ELSE 0 END) as parcelas_pagas,
            MIN(data_vencimento) as data_inicio,
            MAX(data_vencimento) as data_fim,
            MIN(CASE WHEN pago = 0 THEN data_vencimento END) as proxima_parcela
        FROM lancamentos
        WHERE tipo = 'Despesa' AND total_parcelas > 1 AND total_parcelas != 999 AND compra_id IS NOT NULL
        GROUP BY compra_id, categoria, subgrupo
        ORDER BY data_fim ASC
    ''')

    if df_dividas.empty:
        st.info("Nenhuma despesa parcelada com mais de 1 parcela encontrada ainda. Lance uma dívida/financiamento "
                "em '📝 Lançamentos' com Recorrência = 'Parcelada' e ela aparece aqui automaticamente.")
    else:
        st.caption("💡 'Parcelas pagas' conta só o que foi marcado como pago em '📝 Lançamentos' ou '📊 Fluxo e "
                  "Prioridades' — não assume que uma parcela vencida foi paga, porque atraso pode acontecer.")

        df_info = fetch_dataframe("SELECT * FROM info_dividas")
        df_dividas = df_dividas.merge(df_info, on='compra_id', how='left')
        df_dividas['valor_total'] = df_dividas['valor_total'].astype(float)
        df_dividas['valor_pago_total'] = df_dividas['valor_pago_total'].astype(float)
        df_dividas['saldo_devedor'] = df_dividas['valor_total'] - df_dividas['valor_pago_total']

        total_divida_geral = float(df_dividas['saldo_devedor'].clip(lower=0).sum())
        n_dividas_ativas = int((df_dividas['saldo_devedor'] > 0.01).sum())
        n_parcelas_restantes = int((df_dividas['total_parcelas'] - df_dividas['parcelas_pagas']).clip(lower=0).sum())

        c1, c2, c3 = st.columns(3)
        c1.metric("💰 Saldo Devedor Total", f"R$ {format_brl(total_divida_geral)}")
        c2.metric("📋 Dívidas Ativas", str(n_dividas_ativas))
        c3.metric("📅 Parcelas Restantes (todas)", str(n_parcelas_restantes))

        st.divider()

        for _, d in df_dividas.sort_values('saldo_devedor', ascending=False).iterrows():
            credor_label = d['credor'] if pd.notna(d.get('credor')) and str(d.get('credor')).strip() else d['descricao']
            total_parc = int(d['total_parcelas']) if pd.notna(d['total_parcelas']) and d['total_parcelas'] > 0 else 1
            parc_pagas = int(d['parcelas_pagas'])
            progresso = min(parc_pagas / total_parc, 1.0)

            with st.container(border=True):
                c_a, c_b = st.columns([3, 1.4])
                with c_a:
                    st.markdown(f"**{credor_label}**")
                    st.caption(f"{d['categoria']} → {d['subgrupo'] or 'Geral'}")
                with c_b:
                    if d['saldo_devedor'] <= 0.01:
                        st.success("✅ Quitada")
                    else:
                        st.markdown(f"<div style='text-align:right; font-family: IBM Plex Mono, monospace; font-weight:600; font-size:1.1rem;'>R$ {format_brl(d['saldo_devedor'])}</div>", unsafe_allow_html=True)
                        st.caption("Saldo devedor")

                st.progress(progresso)
                st.caption(f"{parc_pagas}/{total_parc} parcelas pagas")

                c_x, c_y, c_z = st.columns(3)
                c_x.caption(f"📆 Início: {pd.to_datetime(d['data_inicio']).strftime('%d/%m/%Y')}")
                if pd.notna(d['proxima_parcela']):
                    c_y.caption(f"⏳ Próxima: {pd.to_datetime(d['proxima_parcela']).strftime('%d/%m/%Y')}")
                else:
                    c_y.caption("⏳ Sem parcelas pendentes")
                c_z.caption(f"🏁 Término previsto: {pd.to_datetime(d['data_fim']).strftime('%d/%m/%Y')}")

                if pd.notna(d.get('taxa_juros_mensal')):
                    st.caption(f"📊 Taxa informada: {float(d['taxa_juros_mensal']):.2f}% a.m. (apenas referência, não usada em cálculo)")

        st.divider()
        with st.expander("✏️ Adicionar nome do credor / taxa (opcional)"):
            st.caption("Isso é só pra exibição — não muda nenhum valor ou parcela já lançada.")
            opcoes_divida = {r['compra_id']: (r['credor'] if pd.notna(r.get('credor')) and str(r.get('credor')).strip() else r['descricao']) for _, r in df_dividas.iterrows()}
            sel_divida = st.selectbox("Selecione a dívida:", options=[None] + list(opcoes_divida.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_divida[x])
            if sel_divida:
                linha_atual = df_dividas[df_dividas['compra_id'] == sel_divida].iloc[0]
                credor_input = st.text_input("Nome do credor", value=linha_atual['credor'] if pd.notna(linha_atual.get('credor')) else "")
                taxa_input = st.number_input("Taxa de juros mensal (%)", min_value=0.0, step=0.1,
                                             value=float(linha_atual['taxa_juros_mensal']) if pd.notna(linha_atual.get('taxa_juros_mensal')) else 0.0)
                if st.button("💾 Salvar Informações", type="primary"):
                    execute_query('''
                        INSERT INTO info_dividas (compra_id, credor, taxa_juros_mensal) VALUES (%s, %s, %s)
                        ON CONFLICT (compra_id) DO UPDATE SET credor = EXCLUDED.credor, taxa_juros_mensal = EXCLUDED.taxa_juros_mensal
                    ''', (sel_divida, credor_input.strip() or None, taxa_input if taxa_input > 0 else None))
                    flash("success", "Informações da dívida salvas!"); st.rerun()

# =================================================================
# 15. MÓDULO: ESCALA VISUAL DE PLANTÕES
# =================================================================

elif menu == "🏥 Escala de Plantões":
    st.header("🏥 Escala Visual de Plantões")

    with st.expander("📥 Importar Plantões via CSV (não mexe em mais nada do banco)"):
        st.caption(
            "Diferente do 'Restaurar CSV' da sidebar (que APAGA a tabela inteira e recoloca do zero), "
            "esta importação só ADICIONA plantões novos -- todo o resto do seu banco (despesas, outras "
            "entradas, dívidas) fica intocado. Plantões que já existem (mesmo local + mesma data) são "
            "detectados e ignorados, então pode importar o mesmo arquivo mais de uma vez sem duplicar."
        )
        st.markdown(
            "**Formato esperado do CSV** (cabeçalho na 1ª linha, sem acento obrigatório):\n"
            "- `data` — data do plantão, formato `DD/MM/AAAA`\n"
            "- `local` — precisa bater com um Subgrupo já cadastrado em '⚙️ Gerenciar Categorias' (ex: Trauma, Unimed, HELP)\n"
            "- `valor` — opcional; se ausente, usa o Valor Padrão cadastrado para aquele local"
        )
        csv_plantoes = st.file_uploader("Arquivo CSV de plantões", type="csv", key="upload_plantoes_csv")
        if csv_plantoes is not None:
            try:
                df_imp_plant = pd.read_csv(csv_plantoes)
                df_imp_plant.columns = [c.strip().lower() for c in df_imp_plant.columns]
                col_data = next((c for c in df_imp_plant.columns if c in ('data', 'data_plantao', 'date')), None)
                col_local = next((c for c in df_imp_plant.columns if c in ('local', 'hospital', 'subgrupo')), None)
                col_valor = next((c for c in df_imp_plant.columns if c in ('valor', 'value')), None)

                if not col_data or not col_local:
                    st.error("O CSV precisa ter pelo menos as colunas 'data' e 'local'.")
                else:
                    df_defaults = fetch_dataframe("SELECT categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento FROM categorias_personalizadas WHERE tipo = 'Entrada'")
                    df_existentes = fetch_dataframe("SELECT descricao FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'")
                    descricoes_existentes = set(df_existentes['descricao'].tolist()) if not df_existentes.empty else set()

                    novos, ignorados_dup, sem_local = [], [], []
                    for _, row in df_imp_plant.iterrows():
                        try:
                            data_plantao = pd.to_datetime(str(row[col_data]).strip(), format='%d/%m/%Y').date()
                        except Exception:
                            sem_local.append(f"{row[col_data]!r} (data inválida)")
                            continue

                        local_str = str(row[col_local]).strip()
                        info_local = df_defaults[df_defaults['subgrupo'].str.strip().str.lower() == local_str.lower()]
                        if info_local.empty:
                            sem_local.append(f"{local_str} ({data_plantao.strftime('%d/%m/%Y')})")
                            continue
                        info_local = info_local.iloc[0]

                        descricao_nova = f"Plantão {info_local['subgrupo']} ({data_plantao.strftime('%d/%m/%Y')})"
                        if descricao_nova in descricoes_existentes:
                            ignorados_dup.append(descricao_nova)
                            continue

                        if col_valor and pd.notna(row.get(col_valor)):
                            valor_final = parse_valor(row[col_valor])
                        elif pd.notna(info_local['valor_padrao']):
                            valor_final = float(info_local['valor_padrao'])
                        else:
                            sem_local.append(f"{descricao_nova} (sem valor e sem Valor Padrão cadastrado)")
                            continue

                        atraso_m = int(info_local['atraso_meses']) if pd.notna(info_local['atraso_meses']) else 1
                        dia_pgto = int(info_local['dia_pagamento']) if pd.notna(info_local['dia_pagamento']) else 10
                        m_f = (data_plantao.month + atraso_m - 1) % 12 + 1
                        a_f = data_plantao.year + (data_plantao.month + atraso_m - 1) // 12
                        dia_pgto_ajustado = min(dia_pgto, calendar.monthrange(a_f, m_f)[1])
                        data_vencto = datetime.date(a_f, m_f, dia_pgto_ajustado)

                        novos.append((
                            'Entrada', info_local['categoria'], info_local['subgrupo'], descricao_nova,
                            valor_final, data_vencto, 1, 1, 0, str(uuid.uuid4()), 'Outros', 'Baixa 🟢', 0.0
                        ))
                        descricoes_existentes.add(descricao_nova)  # evita duplicata dentro do próprio arquivo

                    st.divider()
                    c_res1, c_res2, c_res3 = st.columns(3)
                    c_res1.metric("✅ Novos a importar", len(novos))
                    c_res2.metric("↩️ Já existiam (ignorados)", len(ignorados_dup))
                    c_res3.metric("⚠️ Com problema", len(sem_local))

                    if sem_local:
                        with st.container(border=True):
                            st.caption("Linhas com problema (local não encontrado, data inválida, ou sem valor):")
                            for s in sem_local: st.write(f"• {s}")

                    if novos and st.button(f"➕ Confirmar Importação de {len(novos)} Plantão(ões) Novo(s)", type="primary"):
                        execute_values_query('''
                            INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago)
                            VALUES %s
                        ''', novos)
                        flash("success", f"✅ {len(novos)} plantão(ões) importado(s) — nenhum outro dado foi alterado.")
                        st.rerun()
            except Exception as e:
                st.error(f"Erro ao ler o CSV: {e}")

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
