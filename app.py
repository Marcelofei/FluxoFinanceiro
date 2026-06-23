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
        st.cache_resource.clear()
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
        st.cache_resource.clear()
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
        st.cache_resource.clear()
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

def executar_abatimento_envelope(categoria, valor_gasto, mes, ano):
    """
    Deduz dinamicamente o valor de uma despesa real realizada do teto orçamentário (pago=0)
    caso a categoria esteja configurada como um Envelope Virtual.
    """
    df_cat = fetch_dataframe("SELECT is_envelope FROM categorias_personalizadas WHERE categoria = %s AND tipo = 'Despesa' LIMIT 1", (categoria,))
    if not df_cat.empty and int(df_cat.iloc[0]['is_envelope'] or 0) == 1:
        execute_query('''
            UPDATE lancamentos 
            SET valor = GREATEST(0, valor - %s)
            WHERE pago = 0 
              AND tipo = 'Despesa' 
              AND categoria = %s 
              AND EXTRACT(MONTH FROM data_vencimento) = %s 
              AND EXTRACT(YEAR FROM data_vencimento) = %s
        ''', (valor_gasto, categoria, mes, ano))

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

# =================================================================
# 5. CONFIGURAÇÃO DA PÁGINA
# =================================================================

st.set_page_config(page_title="Gestão Financeira", layout="wide")
if not check_password(): st.stop()
init_db()

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

st.sidebar.title("Navegação")
menu = st.sidebar.radio("Módulo:", [
    "🏠 Início",
    "📝 Lançamentos", 
    "⚙️ Gerenciar Categorias",
    "📊 Fluxo e Prioridades", 
    "📑 Demonstrativo", 
    "📈 Balanço Anual",
    "🔀 Otimização de Pagamentos", 
    "🏥 Escala de Plantões"
])
st.sidebar.divider()

st.sidebar.markdown("### 📅 Período Ativo")
col_sb1, col_sb2 = st.sidebar.columns(2)
with col_sb1: mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1, key="sb_mes")
with col_sb2: ano_selecionado = st.selectbox("Ano", range(hoje.year-2, hoje.year+5), index=2, key="sb_ano")

st.sidebar.divider()
st.sidebar.subheader("🛡️ Backup")

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
    if importar_csv(a_up): st.rerun()

processar_recorrencias_lazy(mes_selecionado, ano_selecionado)
dia_maximo_alvo = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
data_contexto_ativo = datetime.date(ano_selecionado, mes_selecionado, min(hoje.day, dia_maximo_alvo))

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
        st.dataframe(df_7d[['Data', 'tipo', 'descricao', 'valor', 'Status']], use_container_width=True, hide_index=True)

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
            ncat = st.text_input("Nome da Categoria (Nova ou Existente)", placeholder="Ex: Valores Fixos")
            n_rec = st.checkbox("🔄 Contrato fixo/recorrente? (Autogeração Mensal)")
        with c_add2:
            nsub = st.text_input("Nome do Subgrupo (Opcional)", placeholder="Ex: Hospital Trauma")
            if ntipo == "Despesa":
                n_env = st.checkbox("⚖️ Tornar esta categoria um 'Envelope Virtual' (Teto para despesas variáveis)")
            else:
                n_env = False
            if n_rec: n_dt_start = st.date_input("Data de Início do Contrato", value=data_contexto_ativo)
        
        if ntipo == "Entrada" or n_rec or n_env:
            st.markdown("---")
            st.markdown("##### ⚙️ Parâmetros de Padrão e Recorrência")
            c_opt1, c_opt2, c_opt3 = st.columns(3)
            v_opt = c_opt1.number_input("Valor Padrão (R$)", min_value=0.0, step=50.0, value=0.0)
            a_opt = c_opt2.number_input("Atraso (Meses) - Útil p/ Plantões", min_value=0, max_value=6, value=1 if ntipo=="Entrada" else 0)
            d_opt = c_opt3.number_input("Dia de Pagamento/Vencimento", min_value=1, max_value=31, value=10)
        else:
            v_opt, a_opt, d_opt = 0.0, 0, 10

        if st.button("Salvar Nova Categoria/Subgrupo", type="primary"):
            if not ncat.strip(): st.error("O nome da Categoria é obrigatório.")
            else:
                is_rec_val = 1 if n_rec else 0
                is_env_val = 1 if n_env else 0
                dt_start_val = n_dt_start if n_rec else None
                execute_query("INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento, is_recorrente, data_inicio, is_envelope) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                              (ntipo, ncat.strip(), nsub.strip(), v_opt if v_opt > 0 else None, a_opt, d_opt, is_rec_val, dt_start_val, is_env_val))
                st.success("Adicionado com sucesso!"); st.rerun()
                
    with tab_edit:
        if not df_custom_global.empty:
            opcoes_edit_local = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom_global.iterrows()}
            sel_edit = st.selectbox("Selecione o item para editar:", options=[None] + list(opcoes_edit_local.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_edit_local[x])
            if sel_edit:
                nó = df_custom_global[df_custom_global['id'] == sel_edit].iloc[0]
                c_ed_n1, c_ed_n2 = st.columns(2)
                with c_ed_n1: new_cat = st.text_input("Nova Categoria", value=nó['categoria'])
                with c_ed_n2: new_sub = st.text_input("Novo Subgrupo", value=nó['subgrupo'] if pd.notna(nó['subgrupo']) else "")
                
                e_rec = st.checkbox("🔄 Contrato fixo/recorrente? (Autogeração Mensal)", value=bool(nó['is_recorrente'] == 1))
                e_env = st.checkbox("⚖️ Tornar esta categoria um 'Envelope Virtual'", value=bool(nó['is_envelope'] == 1)) if nó['tipo'] == 'Despesa' else False
                
                if nó['tipo'] == "Entrada" or e_rec or e_env:
                    st.markdown("---")
                    st.markdown("##### ⚙️ Parâmetros de Padrão e Recorrência")
                    c_opt_e1, c_opt_e2, c_opt_e3 = st.columns(3)
                    v_edit = c_opt_e1.number_input("Valor Padrão (R$)", value=float(nó['valor_padrao']) if pd.notna(nó['valor_padrao']) else 0.0)
                    a_edit = c_opt_e2.number_input("Atraso (Meses)", value=int(nó['atraso_meses']) if pd.notna(nó['atraso_meses']) else (1 if nó['tipo']=="Entrada" else 0))
                    d_edit = c_opt_e3.number_input("Dia Pagamento", value=int(nó['dia_pagamento']) if pd.notna(nó['dia_pagamento']) else 10)
                else:
                    v_edit = float(nó['valor_padrao']) if pd.notna(nó['valor_padrao']) else 0.0
                    a_edit = int(nó['atraso_meses']) if pd.notna(nó['atraso_meses']) else 0
                    d_edit = int(nó['dia_pagamento']) if pd.notna(nó['dia_pagamento']) else 10

                if st.button("💾 Confirmar Edição", type="primary"):
                    execute_query("UPDATE categorias_personalizadas SET categoria=%s, subgrupo=%s, valor_padrao=%s, atraso_meses=%s, dia_pagamento=%s, is_recorrente=%s, is_envelope=%s WHERE id=%s", 
                                  (new_cat, new_sub, v_edit if v_edit > 0 else None, a_edit, d_edit, 1 if e_rec else 0, 1 if e_env else 0, sel_edit))
                    execute_query("UPDATE lancamentos SET categoria=%s, subgrupo=%s WHERE tipo=%s AND categoria=%s AND subgrupo=%s", (new_cat, new_sub, nó['tipo'], nó['categoria'], nó['subgrupo']))
                    st.success("Atualizado."); st.rerun()
        else: st.info("Nenhuma categoria encontrada.")

    with tab_del:
        if not df_custom_global.empty:
            opcoes_del_local = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom_global.iterrows()}
            sel_del = st.selectbox("Selecione o item para excluir:", options=[None] + list(opcoes_del_local.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_del_local[x])
            if sel_del and st.button("🗑️ Excluir Selecionado", type="primary"):
                execute_query("DELETE FROM categorias_personalizadas WHERE id = %s", (sel_del,))
                st.success("Excluído!"); st.rerun()

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
    with col2:
        if not ESTRUTURA[tipo]:
            st.error("Não há categorias ativas. Crie uma no módulo '⚙️ Gerenciar Categorias'.")
            categoria, subgrupo = None, None
        else:
            categoria = st.selectbox("Categoria", list(ESTRUTURA[tipo].keys()))
            subgrupos_disp = ESTRUTURA[tipo][categoria] if categoria in ESTRUTURA[tipo] else []
            subgrupo = st.selectbox("Subgrupo", subgrupos_disp)
        
        gasto_continuo = st.checkbox("🗓️ Provisão (Mês todo)")
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
            desc_final = f"{descricao} (Provisão)" if gasto_continuo else descricao
            
            pago_val = 1 if pago_imediato else 0
            v_pago_val = val_f if pago_imediato else 0.0
            
            for i in range(parcelas):
                m_f = data_venc_base.month - 1 + i
                a_f = data_venc_base.year + m_f // 12
                m_f = m_f % 12 + 1
                d_p = datetime.date(a_f, m_f, calendar.monthrange(a_f, m_f)[1]) if gasto_continuo else datetime.date(a_f, m_f, min(data_venc_base.day, calendar.monthrange(a_f, m_f)[1]))
                registros.append((tipo, categoria, subgrupo, desc_final, val_f, d_p, i+1, tot_p, pago_val, comp_id, forma_pgto, prioridade, v_pago_val))
                
            execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago) VALUES %s''', registros)
            
            # Executa abatimento automático se for um envelope pago imediato
            if tipo == "Despesa" and pago_imediato:
                executar_abatimento_envelope(categoria, val_f, data_venc_base.month, data_venc_base.year)
                
            st.success("Salvo com sucesso!"); st.rerun()

# =================================================================
# 11. MÓDULO 2: FLUXO E PRIORIDADES (COM PROJETOR DE ENVELOPES)
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
                        
                        # Executa o abatimento automático em lote caso tenha sido marcado como pago manualmente agora
                        if orig_row['tipo'] == 'Despesa' and novo_pago == 1 and orig_row['pago'] == 0:
                            executar_abatimento_envelope(orig_row['categoria'], novo_valor_pago, mes_selecionado, ano_selecionado)
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

# =================================================================
# 12. MÓDULO 3: DEMONSTRATIVO (COM ANALÍTICO DE ENVELOPES)
# =================================================================

elif menu == "📑 Demonstrativo":
    st.header("📑 Demonstrativo Financeiro")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))
    
    tab_dem, tab_env = st.tabs(["📊 Balanço Mensal", "⚖️ Envelopes Orçado vs Realizado"])
    
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

            st.divider()
            st.subheader("📊 Distribuição de Despesas")
            if not df_d.empty:
                df_grp = df_d.groupby('categoria')['valor'].sum().reset_index()
                fig = px.pie(df_grp, values='valor', names='categoria', hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)

            def exibir_demonstrativo(dataframe):
                if dataframe.empty: return
                dataframe['Desc. Exibição'] = dataframe.apply(lambda r: f"{r['descricao']} ({int(r['parcela_atual'])}/{int(r['total_parcelas'])})" if pd.notna(r.get('total_parcelas')) and r['total_parcelas'] > 1 and r['total_parcelas'] != 999 else r['descricao'], axis=1)
                dataframe['Status'] = dataframe['pago'].apply(lambda x: '✅ Pago' if x == 1 else '⏳ Pendente')
                st.dataframe(
                    dataframe[['Data BR', 'Desc. Exibição', 'valor', 'valor_pago', 'prioridade', 'Status']],
                    hide_index=True, use_container_width=True,
                    column_config={"valor": st.column_config.NumberColumn("Planejado", format="%.2f"), "valor_pago": st.column_config.NumberColumn("Pago/Real", format="%.2f")}
                )

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🟢 Entradas Detalhadas")
                for cat in df_e['categoria'].unique():
                    df_c = df_e[df_e['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            exibir_demonstrativo(df_s)
            with c2:
                st.subheader("🔴 Despesas Detalhadas")
                for cat in df_d['categoria'].unique():
                    df_c = df_d[df_d['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            exibir_demonstrativo(df_s)

    with tab_env:
        st.subheader("⚖️ Acompanhamento Clínico de Envelopes (Despesas Variáveis)")
        st.markdown("Mapeamento em tempo real do teu teto orçamentário inicial contra os teus gastos fragmentados de rotina.")
        
        df_envelopes_config = fetch_dataframe("SELECT categoria FROM categorias_personalizadas WHERE is_envelope = 1 AND tipo = 'Despesa'")
        
        if df_envelopes_config.empty:
            st.info("Nenhuma categoria está configurada como 'Envelope Virtual' atualmente. Vá a '⚙️ Gerenciar Categorias' para ativar.")
        elif df.empty:
            st.info("Sem transações no período ativo.")
        else:
            matriz_envelopes = []
            for cat in df_envelopes_config['categoria'].unique():
                df_pago = df[(df['categoria'] == cat) & (df['pago'] == 1)]
                df_teto = df[(df['categoria'] == cat) & (df['pago'] == 0)]
                
                realizado = float(df_pago['valor_pago'].sum())
                disponivel = float(df_teto['valor'].sum())
                orcamento_inicial = realizado + disponivel
                
                if orcamento_inicial > 0:
                    percent_livre = (disponivel / orcamento_inicial) * 100
                    if disponivel > 0:
                        status_txt = f"🟢 +{percent_livre:.1f}% disponível"
                    else:
                        status_txt = f"🔴 Estourado"
                        
                    matriz_envelopes.append({
                        "Categoria": cat,
                        "Orçamento Inicial (Teto)": orcamento_inicial,
                        "Gasto Realizado (Acumulado)": realizado,
                        "Saldo Restante Livre": disponivel,
                        "Métrica de Saúde": status_txt
                    })
            
            if matriz_envelopes:
                df_matriz = pd.DataFrame(matriz_envelopes)
                st.dataframe(
                    df_matriz, use_container_width=True, hide_index=True,
                    column_config={
                        "Orçamento Inicial (Teto)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Gasto Realizado (Acumulado)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Saldo Restante Livre": st.column_config.NumberColumn(format="R$ %.2f")
                    }
                )
            else:
                st.info("Nenhum lançamento encontrado para os envelopes configurados neste mês.")

# =================================================================
# 12. MÓDULO: BALANÇO ANUAL
# =================================================================

elif menu == "📈 Balanço Anual":
    st.header("📈 Balanço Financeiro Anual")
    anos_disp = fetch_dataframe("SELECT DISTINCT EXTRACT(YEAR FROM data_vencimento) as ano FROM lancamentos ORDER BY ano DESC")
    if anos_disp.empty: st.info("Sem dados suficientes.")
    else:
        ano_balanco = st.selectbox("Ano de Referência", anos_disp['ano'].astype(int).tolist(), index=0)
        for m in range(1, 13): processar_recorrencias_lazy(m, ano_balanco)
            
        df_ano = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(YEAR FROM data_vencimento) = %s", (ano_balanco,))
        if df_ano.empty: st.warning("Sem dados.")
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
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Faturamento Anual", f"R$ {format_brl(tot_ent)}")
            c2.metric("Despesa Anual", f"R$ {format_brl(tot_des)}")
            c3.metric("Resultado Líquido Anual", f"R$ {format_brl(tot_ent - tot_des)}")
            
            st.divider()
            fig_evol = px.bar(mensal, x='Mes', y=['Entrada', 'Despesa'], barmode='group', title="Balanço FP&A Híbrido")
            st.plotly_chart(fig_evol, use_container_width=True)

# =================================================================
# 13. MÓDULO 4: OTIMIZAÇÃO DE PAGAMENTOS
# =================================================================

elif menu == "🔀 Otimização de Pagamentos":
    st.header("🔀 Otimização de Pagamentos")
    df_mes = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))
    
    if not df_mes.empty:
        df_e, df_d = df_mes[df_mes['tipo'] == 'Entrada'].copy(), df_mes[df_mes['tipo'] == 'Despesa'].copy()
        mask_p = df_e['descricao'].str.contains('Plantão', na=False)
        if mask_p.any():
            df_plantoes = df_e[mask_p].copy()
            df_e = df_e[~mask_p].copy()
            for (sub, data), group in df_plantoes.groupby(['subgrupo', 'data_vencimento']):
                df_e = pd.concat([df_e, pd.DataFrame([{'descricao': f'🏥 Plantões {sub}', 'valor': group['valor'].sum(), 'data_vencimento': data, 'subgrupo': sub, 'prioridade': 'Baixa 🟢'}])], ignore_index=True)

        df_e['is_prov_hr'] = df_e['descricao'].str.contains(r'Produção\s+(?:Radioclim|Humana)', case=False, na=False)
        df_hr, df_outras = df_e[df_e['is_prov_hr']].copy(), df_e[~df_e['is_prov_hr']].copy()
        
        st.subheader("🗓️ Fluxo Geral: Alocações Prontas")
        if not df_outras.empty:
            ag_e = df_outras.groupby('data_vencimento').agg({'valor':'sum', 'descricao': lambda x: ' + '.join(pd.Series(x).unique().astype(str))}).reset_index()
            for _, e in ag_e.iterrows():
                st.markdown(f"### 📥 Recebimento Previsto: {e['descricao']} | R$ {format_brl(e['valor'])}")
                st.divider()

# =================================================================
# 14. MÓDULO 5: ESCALA VISUAL DE PLANTÕES
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
    for i, dia in enumerate(["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]): cols[i].markdown(f"<div style='text-align: center; font-weight: bold; padding: 5px; border-bottom: 2px solid #4CAF50;'>{dia}</div>", unsafe_allow_html=True)
    for week in calendar.monthcalendar(cal_ano, cal_mes):
        w_cols = st.columns(7)
        for i, day in enumerate(week):
            with w_cols[i]:
                if day != 0:
                    cd = datetime.date(cal_ano, cal_mes, day)
                    bg = "background-color: rgba(76, 175, 80, 0.1);" if cd == hoje else ""
                    brdr = "border: 2px solid #4CAF50;" if cd == hoje else "border: 1px solid rgba(150, 150, 150, 0.3);"
                    html = f"<div style='{bg} {brdr} border-radius: 5px; padding: 5px; min-height: 90px; margin-top: 5px;'><div style='text-align:right; font-weight:bold;'>{day}</div>"
                    if not df_m_cal.empty:
                        for _, s in df_m_cal[df_m_cal['d_p'] == cd].iterrows(): html += f"<div style='background-color:#4CAF50; color:white; font-size:10px; padding:2px; margin-top:2px; white-space:nowrap; overflow:hidden;'>🏥 {s['subgrupo']}</div>"
                    st.markdown(html + "</div>", unsafe_allow_html=True)

    st.divider()
    st.subheader("📋 Gerenciar Escala Deste Mês")
    if not df_m_cal.empty:
        edit_esc = st.data_editor(df_m_cal[['id', 'subgrupo', 'valor']], use_container_width=True, hide_index=True)
        confirm_del_lote = st.checkbox("⚠️ Confirmo a purga dos plantões selecionados")
        if st.button("🗑️ Eliminar Plantões", disabled=not confirm_del_lote):
            execute_query("DELETE FROM lancamentos WHERE id = %s", (int(edit_esc.iloc[0]['id']),))
            st.rerun()
    else: st.info("Sem plantões registrados.")
