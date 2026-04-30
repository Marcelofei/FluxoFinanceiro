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
# 1. INFRAESTRUTURA E CONEXÃO (SINGLETON ROBUSTO)
# =================================================================

@st.cache_resource(ttl=3600)
def get_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL não configurada.")
        st.stop()
    try:
        conn = psycopg2.connect(
            db_url, 
            options="-c client_encoding=utf8", 
            connect_timeout=10
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"Falha Crítica de Conexão: {e}")
        st.stop()

def execute_query(query, params=None, fetch=False):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch: return cur.fetchall()
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        st.cache_resource.clear()
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch: return cur.fetchall()

def execute_values_query(query, params_list):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            execute_values(cur, query, params_list)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        st.cache_resource.clear()
        conn = get_connection()
        with conn.cursor() as cur:
            execute_values(cur, query, params_list)

def fetch_dataframe(query, params=None):
    try:
        conn = get_connection()
        return pd.read_sql_query(query, conn, params=params)
    except Exception:
        st.cache_resource.clear()
        conn = get_connection()
        return pd.read_sql_query(query, conn, params=params)

@st.cache_resource
def init_db():
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
            forma_pagamento TEXT DEFAULT 'Outros'
        );
    ''')
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS forma_pagamento TEXT DEFAULT 'Outros';")
    execute_query('''
        CREATE TABLE IF NOT EXISTS categorias_personalizadas (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT
        );
    ''')

# =================================================================
# 2. SISTEMA DE SEGURANÇA E AUXILIARES
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
# 3. CONFIGURAÇÃO DA PÁGINA (OBRIGATÓRIO SER O PRIMEIRO)
# =================================================================

st.set_page_config(page_title="Gestão Financeira", layout="wide")

if not check_password():
    st.stop()

init_db()

# =================================================================
# 4. ESTRUTURAS E CONSTANTES
# =================================================================

ESTRUTURA_DEFAULT = {
    "Entrada": {
        "Valores Fixos": ["Trauma", "Trauma FDS", "USG HT", "HuniCG Not", "HuniCG Diu", "HELP", "HC", "Radioclim", "Humana", "HECI", "Wanderley", "Outros"],
        "Valores Variáveis": ["HELP", "Radioclim", "Humana", "Unimed", "HECI", "Wanderley", "Outros"],
        "Outros": ["Outros"]
    },
    "Despesa": {
        "Despesas Essenciais": ["Moradia", "Educação", "Transporte", "Saúde", "Compras", "Outros"],
        "Despesas Mensais": ["Canil", "Sítio", "Assinaturas", "PJ", "Ajuda Família", "Lazer", "Outros"],
        "Despesas Variáveis": ["Geral", "Outros"],
        "Reserva": ["Caixa", "Investimentos"],
        "Outros": ["Outros"]
    }
}

VALORES_PLANTAO = {
    "Trauma": 1160.00, "Trauma FDS": 1305.00, "USG HT": 942.50,
    "HuniCG Not": 1008.00, "HuniCG Diu": 840.00, "HELP": 1260.00,
    "HC": 471.25, "Radioclim": 1200.00, "Humana": 800.00, "HECI": 25.00,
    "Wanderley": 1000.00
}

REGRAS_PAGAMENTO = {
    "HC": {"meses": 2, "dia": 1},
    "Trauma": {"meses": 2, "dia": 1},
    "Trauma FDS": {"meses": 2, "dia": 1},
    "USG HT": {"meses": 2, "dia": 1},
    "HuniCG Not": {"meses": 2, "dia": 25},
    "HuniCG Diu": {"meses": 2, "dia": 25},
    "HECI": {"meses": 1, "dia": 15},
    "HELP": {"meses": 1, "dia": 15},
    "Humana": {"meses": 1, "dia": 10},
    "Radioclim": {"meses": 1, "dia": 10},
    "Wanderley": {"meses": 1, "dia": 10}
}

def get_estrutura_dinamica():
    estrutura = {"Entrada": {}, "Despesa": {}}
    for t, cats in ESTRUTURA_DEFAULT.items():
        for c, subs in cats.items():
            estrutura[t][c] = subs.copy()
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

# =================================================================
# 5. SIDEBAR E FILTROS
# =================================================================

st.sidebar.title("Navegação")
menu = st.sidebar.radio("Módulo:", [
    "📝 Lançamentos", 
    "📊 Fluxo e Prioridades", 
    "📑 Demonstrativo", 
    "🔀 Otimização de Pagamentos", 
    "🏥 Escala de Plantões"
])

st.sidebar.divider()
st.sidebar.subheader("🛡️ Backup")

def exportar_csv():
    df = fetch_dataframe("SELECT * FROM lancamentos")
    return df.to_csv(index=False).encode('utf-8') if not df.empty else None

def importar_csv(arquivo):
    try:
        df_imp = pd.read_csv(arquivo)
        if 'forma_pagamento' not in df_imp.columns: df_imp['forma_pagamento'] = 'Outros'
        execute_query("TRUNCATE TABLE lancamentos")
        registros = [
            (r['tipo'], r['categoria'], r['subgrupo'], r['descricao'], r['valor'], r['data_vencimento'], r['parcela_atual'], r['total_parcelas'], r['pago'], r['compra_id'], r['forma_pagamento'])
            for _, r in df_imp.iterrows()
        ]
        execute_values_query('''
            INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento)
            VALUES %s
        ''', registros)
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False

c_data = exportar_csv()
if c_data: st.sidebar.download_button("📥 Baixar CSV", data=c_data, file_name=f"backup_{hoje.strftime('%d_%m_%Y')}.csv", mime="text/csv")
a_up = st.sidebar.file_uploader("Restaurar CSV", type="csv")
if a_up and st.sidebar.button("🚀 Confirmar Restauração"): 
    if importar_csv(a_up): st.rerun()

st.markdown("### 📅 Filtro de Período")
col_top1, col_top2 = st.columns(2)
with col_top1:
    mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1)
with col_top2:
    ano_selecionado = st.selectbox("Ano", range(hoje.year-2, hoje.year+5), index=2)

# =================================================================
# 6. MÓDULO 1: LANÇAMENTOS
# =================================================================

if menu == "📝 Lançamentos":
    st.header("📝 Novo Lançamento")
    col1, col2 = st.columns(2)
    with col1:
        tipo = st.radio("Tipo", ["Despesa", "Entrada"], horizontal=True)
        forma_pgto = st.selectbox("Forma de Pagamento", ["À vista", "Crédito", "Outros"], index=0 if tipo == "Entrada" else 1)
        descricao = st.text_input("Descrição")
        valor_input = st.text_input("Valor (R$)", value="0,00")
        gasto_continuo = st.checkbox("🗓️ Provisão (Mês todo)")
        data_venc_base = st.date_input("Data Referência", value=hoje, format="DD/MM/YYYY")
    with col2:
        categoria = st.selectbox("Categoria", list(ESTRUTURA[tipo].keys()))
        subgrupos_disp = ESTRUTURA[tipo][categoria] if categoria in ESTRUTURA[tipo] else []
        subgrupo = st.selectbox("Subgrupo", subgrupos_disp)
        
        parcelas = 1
        if tipo == "Despesa":
            tipo_rec = st.radio("Recorrência", ["Única", "Parcelada", "Fixa/Contínua"], horizontal=True)
            if tipo_rec == "Parcelada": parcelas = st.number_input("Parcelas", min_value=2, value=2)
            elif tipo_rec == "Fixa/Contínua": parcelas = 60
        else:
            tipo_rec = st.radio("Recorrência", ["Única", "Fixa/Contínua"], horizontal=True)
            if tipo_rec == "Fixa/Contínua": parcelas = 60

    if st.button("Registrar Lançamento", type="primary"):
        val_f = parse_valor(valor_input)
        if val_f <= 0: st.error("Valor inválido.")
        else:
            comp_id = str(uuid.uuid4())
            registros = []
            
            tot_p = 999 if tipo_rec == "Fixa/Contínua" else parcelas
            desc_final = f"{descricao} (Provisão)" if (tipo == "Despesa" and gasto_continuo) else descricao
            
            for i in range(parcelas):
                m_f = data_venc_base.month - 1 + i
                a_f = data_venc_base.year + m_f // 12
                m_f = m_f % 12 + 1
                d_p = datetime.date(a_f, m_f, calendar.monthrange(a_f, m_f)[1]) if gasto_continuo else datetime.date(a_f, m_f, min(data_venc_base.day, calendar.monthrange(a_f, m_f)[1]))
                registros.append((tipo, categoria, subgrupo, desc_final, val_f, d_p, i+1, tot_p, 0, comp_id, forma_pgto))
            
            execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento) VALUES %s''', registros)
            st.success("Salvo!"); st.rerun()

    st.divider()
    
    with st.expander("⚙️ Gerenciar Categorias e Subgrupos Personalizados"):
        tab_add, tab_del = st.tabs(["➕ Adicionar", "🗑️ Excluir"])
        
        with tab_add:
            st.markdown("Crie novas ramificações para classificar seus lançamentos.")
            c_add1, c_add2 = st.columns(2)
            with c_add1:
                ntipo = st.radio("Para qual tipo?", ["Despesa", "Entrada"], horizontal=True, key="add_tipo")
                ncat = st.text_input("Nome da Categoria (Nova ou Existente)", placeholder="Ex: Viagens")
            with c_add2:
                nsub = st.text_input("Nome do Subgrupo (Opcional)", placeholder="Ex: Hotel")
            
            if st.button("Salvar Nova Categoria/Subgrupo", type="primary"):
                if ncat.strip() == "":
                    st.error("O nome da Categoria é obrigatório.")
                else:
                    execute_query(
                        "INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo) VALUES (%s, %s, %s)", 
                        (ntipo, ncat.strip(), nsub.strip())
                    )
                    st.success(f"Adicionado com sucesso! Agora você pode usar em novos lançamentos.")
                    st.rerun()
                    
        with tab_del:
            st.markdown("Apague categorias ou subgrupos que você criou. *(As originais do sistema não podem ser apagadas)*")
            df_custom = fetch_dataframe("SELECT id, tipo, categoria, subgrupo FROM categorias_personalizadas")
            if df_custom.empty:
                st.info("Nenhuma categoria personalizada encontrada. Crie uma na aba ao lado.")
            else:
                opcoes_del = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo'] if r['subgrupo'] else '(Apenas categoria principal)'}" for _, r in df_custom.iterrows()}
                sel_del = st.selectbox("Selecione o item para excluir:", options=[None] + list(opcoes_del.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_del[x])
                
                if sel_del and st.button("🗑️ Excluir Selecionado", type="primary"):
                    execute_query("DELETE FROM categorias_personalizadas WHERE id = %s", (sel_del,))
                    st.success("Excluído com sucesso do banco de dados!")
                    st.rerun()

# =================================================================
# 7. MÓDULO 2: FLUXO E PRIORIDADES
# =================================================================

elif menu == "📊 Fluxo e Prioridades":
    st.header("📊 Fluxo e Prioridades")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s ORDER BY data_vencimento ASC", (mes_selecionado, ano_selecionado))
    
    if df.empty: st.warning("Sem dados.")
    else:
        df['valor'] = df['valor'].astype(float)
        
        st.subheader("🔍 Filtros")
        c_filt1, c_filt2 = st.columns(2)
        
        tipos_disp = df['tipo'].unique().tolist()
        with c_filt1:
            sel_tipo = st.multiselect("Filtrar por Tipo", tipos_disp, placeholder="Todos os Tipos")
        
        tipos_filtro = sel_tipo if sel_tipo else tipos_disp
        cat_disp = df[df['tipo'].isin(tipos_filtro)]['categoria'].unique().tolist()
        
        with c_filt2:
            sel_cat = st.multiselect("Filtrar por Categoria", cat_disp, placeholder="Todas as Categorias")

        cat_filtro = sel_cat if sel_cat else cat_disp

        df_view = df[(df['tipo'].isin(tipos_filtro)) & (df['categoria'].isin(cat_filtro))].copy()
        
        # 1. AGLUTINAÇÃO DO CARTÃO DE CRÉDITO
        mask_cred = df_view['forma_pagamento'] == 'Crédito'
        if mask_cred.any():
            sum_cred = df_view[mask_cred]['valor'].sum()
            all_paid = (df_view[mask_cred]['pago'] == 1).all()
            dummy_credito = pd.DataFrame([{
                'id': '-1', 'tipo': 'Despesa', 'categoria': 'Despesas Essenciais', 'subgrupo': '',
                'descricao': '💳 Cartão de Crédito (Fatura Consolidada)', 'valor': sum_cred,
                'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10),
                'pago': 1 if all_paid else 0, 'compra_id': 'cartao_dummy', 'forma_pagamento': 'Crédito'
            }])
            df_view = df_view[~mask_cred].copy()
            df_view = pd.concat([df_view, dummy_credito], ignore_index=True)
            
        # 2. AGLUTINAÇÃO DOS PLANTÕES (LIMPEZA VISUAL)
        mask_plantoes = (df_view['tipo'] == 'Entrada') & df_view['descricao'].str.contains('Plantão', na=False)
        if mask_plantoes.any():
            df_plantoes = df_view[mask_plantoes].copy()
            df_view = df_view[~mask_plantoes].copy()
            
            agrupado_plantoes = df_plantoes.groupby(['subgrupo', 'data_vencimento'])
            for nome_grupo, grupo in agrupado_plantoes:
                subg_nome, dt_venc = nome_grupo
                soma_valor = grupo['valor'].sum()
                todos_pagos = (grupo['pago'] == 1).all()
                
                dummy_plantao = pd.DataFrame([{
                    'id': f'plantao_{subg_nome}', 
                    'tipo': 'Entrada', 
                    'categoria': grupo.iloc[0]['categoria'], 
                    'subgrupo': subg_nome,
                    'descricao': f'🏥 Plantões {subg_nome} (Consolidado do Mês)', 
                    'valor': soma_valor,
                    'data_vencimento': dt_venc,
                    'pago': 1 if todos_pagos else 0, 
                    'compra_id': 'plantao_dummy', 
                    'forma_pagamento': 'Outros'
                }])
                df_view = pd.concat([df_view, dummy_plantao], ignore_index=True)
        
        df_view['id'] = df_view['id'].astype(str)
        df_view = df_view.sort_values('data_vencimento').reset_index(drop=True)
        df_view['Pago'] = df_view['pago'].astype(bool)
        df_view['Data'] = pd.to_datetime(df_view['data_vencimento']).dt.date
        
        df_view.insert(0, '🗑️ Este', False)
        df_view.insert(1, '🗑️ Futuros', False)

        edit_df = st.data_editor(
            df_view[['🗑️ Este', '🗑️ Futuros', 'Data', 'descricao', 'valor', 'Pago']], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                "valor": st.column_config.NumberColumn("Valor", format="%.2f")
            }
        )
        
        if st.button("Salvar Alterações Rápidas"):
            for i, row in edit_df.iterrows():
                id_s = str(df_view.loc[i, 'id'])
                novo_pago = 1 if row['Pago'] else 0
                
                if row['🗑️ Este'] or row['🗑️ Futuros']:
                    if id_s == '-1':
                        pass
                    elif id_s.startswith('plantao_'):
                        subg = id_s.replace('plantao_', '')
                        execute_query("DELETE FROM lancamentos WHERE tipo='Entrada' AND subgrupo=%s AND data_vencimento=%s AND descricao LIKE 'Plantão %%'", (subg, row['Data']))
                    else:
                        if row['🗑️ Futuros']:
                            execute_query("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s", (df_view.loc[i, 'compra_id'], df_view.loc[i, 'data_vencimento']))
                        else:
                            execute_query("DELETE FROM lancamentos WHERE id = %s", (int(id_s),))
                else:
                    if id_s == '-1':
                        execute_query("UPDATE lancamentos SET pago=%s WHERE forma_pagamento='Crédito' AND EXTRACT(MONTH FROM data_vencimento)=%s AND EXTRACT(YEAR FROM data_vencimento)=%s", (novo_pago, mes_selecionado, ano_selecionado))
                    elif id_s.startswith('plantao_'):
                        subg = id_s.replace('plantao_', '')
                        execute_query("UPDATE lancamentos SET pago=%s WHERE tipo='Entrada' AND subgrupo=%s AND data_vencimento=%s AND descricao LIKE 'Plantão %%'", (novo_pago, subg, row['Data']))
                    else:
                        execute_query("UPDATE lancamentos SET pago=%s, descricao=%s, valor=%s, data_vencimento=%s WHERE id=%s", (novo_pago, row['descricao'], float(row['valor']), row['Data'], int(id_s)))
            st.rerun()

        st.divider()

        # =========================================================
        # MÓDULO DO WHATSAPP (ATUALIZADO PARA CONTAS PAGAS/PENDENTES)
        # =========================================================
        st.subheader("📱 Compartilhar no WhatsApp")
        st.markdown("Gere um resumo rápido das contas do mês para copiar e enviar (inclui as pagas e as pendentes).")
        
        if st.button("Gerar Lista para WhatsApp", type="secondary"):
            df_despesas_wpp = df_view[df_view['tipo'] == 'Despesa'].copy()
            df_despesas_wpp = df_despesas_wpp.sort_values('data_vencimento')
            
            if df_despesas_wpp.empty:
                st.success("Nenhuma despesa registrada para este mês! 🎉")
            else:
                texto_wpp = f"*Resumo de Contas - {meses[mes_selecionado-1]}/{ano_selecionado}*\n\n"
                total_wpp = 0.0
                
                for _, row in df_despesas_wpp.iterrows():
                    data_str = pd.to_datetime(row['data_vencimento']).strftime('%d/%m')
                    if row['Pago']:
                        texto_wpp += f"✅ ~{data_str} - {row['descricao']}: R$ {format_brl(row['valor'])}~\n"
                    else:
                        texto_wpp += f"⏳ {data_str} - {row['descricao']}: R$ {format_brl(row['valor'])}\n"
                        total_wpp += float(row['valor'])
                
                texto_wpp += f"\n*Total Restante a Pagar:* R$ {format_brl(total_wpp)}"
                
                st.info("Copie o texto abaixo clicando no botão no canto superior direito da caixa:")
                st.code(texto_wpp, language="markdown")
                
        st.divider()
        
        st.subheader("✏️ Edição Estrutural Avançada")
        st.markdown("Selecione um lançamento abaixo para alterar suas propriedades essenciais. *(Faturas consolidadas e Pacotes de Plantões não aparecem aqui; edite os itens individuais)*")
        
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
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    e_escopo = st.radio("Aplicar alteração estrutural em:", ["Apenas neste lançamento", "Neste e em todos os futuros da mesma compra"])
                    
                if st.button("💾 Confirmar Mudança Estrutural", type="primary"):
                    v_final = parse_valor(e_val)
                    
                    if e_escopo == "Apenas neste lançamento":
                        execute_query("""
                            UPDATE lancamentos 
                            SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s 
                            WHERE id=%s
                        """, (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                    else:
                        execute_query("""
                            UPDATE lancamentos 
                            SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s 
                            WHERE id=%s
                        """, (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                        
                        execute_query("""
                            UPDATE lancamentos 
                            SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, forma_pagamento=%s 
                            WHERE compra_id=%s AND data_vencimento > %s AND id != %s
                        """, (e_tipo, e_cat, e_sub, e_desc, v_final, e_forma, r_sel['compra_id'], r_sel['data_vencimento'], int(sel_id)))
                        
                    st.success("Lançamento(s) atualizado(s) com sucesso!")
                    st.rerun()

# =================================================================
# 8. MÓDULO 3: DEMONSTRATIVO
# =================================================================

elif menu == "📑 Demonstrativo":
    st.header("📑 Demonstrativo Financeiro")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))
    if not df.empty:
        df['valor'] = df['valor'].astype(float)
        df['Data BR'] = pd.to_datetime(df['data_vencimento']).dt.strftime('%d/%m/%Y')
        
        df_entradas = df[df['tipo'] == 'Entrada']
        df_despesas = df[df['tipo'] == 'Despesa']
        
        total_entradas = df_entradas['valor'].sum()
        total_despesas = df_despesas['valor'].sum()
        saldo_zbb = total_entradas - total_despesas
        
        entradas_recebidas = df_entradas[df_entradas['pago'] == 1]['valor'].sum()
        falta_receber = total_entradas - entradas_recebidas
        
        despesas_pagas = df_despesas[df_despesas['pago'] == 1]['valor'].sum()
        falta_pagar = total_despesas - despesas_pagas
        
        st.subheader("📈 Resumo Geral")
        c_met1, c_met2, c_met3 = st.columns(3)
        c_met1.metric("Receita Total", f"R$ {format_brl(total_entradas)}")
        c_met2.metric("Despesa Total", f"R$ {format_brl(total_despesas)}")
        c_met3.metric("Orçamento Base-Zero (ZBB)", f"R$ {format_brl(saldo_zbb)}")
        
        c_res1, c_res2 = st.columns(2)
        c_res1.metric("⏳ Restante a Receber", f"R$ {format_brl(falta_receber)}")
        c_res2.metric("🚨 Restante a Pagar", f"R$ {format_brl(falta_pagar)}")
        
        st.divider()
        
        st.subheader("📊 Distribuição de Despesas")
        if not df_despesas.empty:
            df_grp = df_despesas.groupby('categoria')['valor'].sum().reset_index()
            fig = px.pie(df_grp, values='valor', names='categoria', hole=0.4)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
        st.divider()

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🟢 Entradas Detalhadas")
            for cat in ESTRUTURA["Entrada"].keys():
                df_c = df_entradas[df_entradas['categoria'] == cat]
                if not df_c.empty:
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            df_s['Status'] = df_s['pago'].apply(lambda x: '✅' if x == 1 else '⏳')
                            st.markdown(f"**{sub}** (R$ {format_brl(df_s['valor'].sum())})")
                            st.dataframe(df_s[['Data BR', 'descricao', 'valor', 'forma_pagamento', 'Status']], hide_index=True)
        with c2:
            st.subheader("🔴 Despesas Detalhadas")
            for cat in ESTRUTURA["Despesa"].keys():
                df_c = df_despesas[df_despesas['categoria'] == cat]
                if not df_c.empty:
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            df_s['Status'] = df_s['pago'].apply(lambda x: '✅' if x == 1 else '⏳')
                            st.markdown(f"**{sub}** (R$ {format_brl(df_s['valor'].sum())})")
                            st.dataframe(df_s[['Data BR', 'descricao', 'valor', 'forma_pagamento', 'Status']], hide_index=True)

        st.divider()
        st.subheader("💳 Detalhamento e Edição de Faturas (Crédito)")
        df_credito = df[df['forma_pagamento'] == 'Crédito'].copy()
        if not df_credito.empty:
            df_credito = df_credito.sort_values('data_vencimento').reset_index(drop=True)
            df_credito['Data'] = pd.to_datetime(df_credito['data_vencimento']).dt.date
            df_credito['Pago'] = df_credito['pago'].astype(bool)
            df_credito.insert(0, '🗑️ Apagar', False)
            
            edit_credito = st.data_editor(
                df_credito[['🗑️ Apagar', 'Data', 'categoria', 'subgrupo', 'descricao', 'valor', 'Pago']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"),
                    "valor": st.column_config.NumberColumn("Valor", format="%.2f"),
                    "categoria": st.column_config.Column("Categoria", disabled=True),
                    "subgrupo": st.column_config.Column("Subgrupo", disabled=True)
                }
            )
            
            if st.button("💾 Salvar Alterações do Cartão", type="primary"):
                for i, row in edit_credito.iterrows():
                    id_s = int(df_credito.loc[i, 'id'])
                    if row['🗑️ Apagar']:
                        execute_query("DELETE FROM lancamentos WHERE id = %s", (id_s,))
                    else:
                        novo_pago = 1 if row['Pago'] else 0
                        execute_query("UPDATE lancamentos SET pago=%s, descricao=%s, valor=%s, data_vencimento=%s WHERE id=%s", 
                                      (novo_pago, row['descricao'], float(row['valor']), row['Data'], id_s))
                st.rerun()
        else:
            st.info("Nenhum lançamento no crédito encontrado para este período.")

# =================================================================
# 9. MÓDULO 4: OTIMIZAÇÃO DE PAGAMENTOS
# =================================================================

elif menu == "🔀 Otimização de Pagamentos":
    st.header("🔀 Otimização de Pagamentos")
    df_mes = fetch_dataframe("SELECT * FROM lancamentos WHERE EXTRACT(MONTH FROM data_vencimento) = %s AND EXTRACT(YEAR FROM data_vencimento) = %s", (mes_selecionado, ano_selecionado))
    
    if not df_mes.empty:
        df_entradas = df_mes[df_mes['tipo'] == 'Entrada'].copy()
        
        # --- AGLUTINAÇÃO DOS PLANTÕES NA OTIMIZAÇÃO (Limpeza de Títulos) ---
        mask_plantoes = df_entradas['descricao'].str.contains('Plantão', na=False)
        if mask_plantoes.any():
            df_plantoes = df_entradas[mask_plantoes].copy()
            df_entradas = df_entradas[~mask_plantoes].copy()
            
            agrupado_plantoes = df_plantoes.groupby(['subgrupo', 'data_vencimento'])
            for nome_grupo, grupo in agrupado_plantoes:
                subg_nome, dt_venc = nome_grupo
                soma_valor = grupo['valor'].sum()
                
                dummy_plantao = pd.DataFrame([{
                    'id': f'plantao_{subg_nome}', 
                    'tipo': 'Entrada', 
                    'categoria': grupo.iloc[0]['categoria'], 
                    'subgrupo': subg_nome,
                    'descricao': f'🏥 Plantões {subg_nome}',
                    'valor': soma_valor,
                    'data_vencimento': dt_venc,
                    'pago': 0,
                    'compra_id': 'plantao_dummy', 
                    'forma_pagamento': 'Outros'
                }])
                df_entradas = pd.concat([df_entradas, dummy_plantao], ignore_index=True)

        df_saidas = df_mes[df_mes['tipo'] == 'Despesa'].copy()
        
        mask_cred = df_saidas['forma_pagamento'] == 'Crédito'
        if mask_cred.any():
            sum_cred = df_saidas[mask_cred]['valor'].sum()
            df_saidas = df_saidas[~mask_cred].copy()
            dummy = pd.DataFrame([{'id': -1, 'categoria': 'Despesas Essenciais', 'descricao': '💳 Cartão de Crédito', 'valor': sum_cred, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10), 'forma_pagamento': 'Crédito'}])
            df_saidas = pd.concat([df_saidas, dummy], ignore_index=True)

        df_saidas['is_provisao'] = df_saidas['descricao'].str.contains(r'\(Provisão\)', case=False, na=False)
        saidas_fila = df_saidas.to_dict('records')
        for s in saidas_fila: s['valor_restante'] = float(s['valor'])

        is_hr_var = df_entradas['subgrupo'].isin(['Humana', 'Radioclim']) & (df_entradas['categoria'] == 'Valores Variáveis')
        df_hr = df_entradas[is_hr_var].copy()
        df_outras = df_entradas[~is_hr_var].copy()

        st.subheader("🗓️ Fluxo Geral Consolidado Diário")
        df_outras['valor'] = df_outras['valor'].astype(float)
        if not df_outras.empty:
            entradas_agrup = df_outras.groupby('data_vencimento').agg({
                'valor':'sum', 
                'descricao': lambda x: ' + '.join(pd.Series(x).unique().astype(str))
            }).reset_index().sort_values('data_vencimento')
            
            for _, e in entradas_agrup.iterrows():
                saldo_inicial = float(e['valor'])
                saldo = saldo_inicial
                nome_entrada = e['descricao']
                data_rec = pd.to_datetime(e['data_vencimento']).strftime('%d/%m/%Y')
                
                st.markdown(f"### 📥 Dia {data_rec} | {nome_entrada} | Saldo: R$ {format_brl(saldo_inicial)}")
                elegiveis = [s for s in saidas_fila if s['valor_restante'] > 0 and not s['is_provisao']]
                elegiveis.sort(key=lambda x: (0 if x['categoria'] == 'Despesas Essenciais' or x['forma_pagamento'] == 'Crédito' else 1, x['data_vencimento']))
                
                aloc = []
                soma_abatida = 0.0
                for s in elegiveis:
                    if saldo > 0:
                        pg = min(saldo, s['valor_restante'])
                        s['valor_restante'] -= pg
                        saldo -= pg
                        soma_abatida += pg
                        aloc.append({"Conta": s['descricao'], "Venc.": pd.to_datetime(s['data_vencimento']).strftime('%d/%m/%Y'), "Abatido": f"R$ {format_brl(pg)}"})
                
                if aloc: 
                    st.dataframe(pd.DataFrame(aloc), use_container_width=True, hide_index=True)
                    c1, c2 = st.columns(2)
                    c1.metric("Total Distribuído no Dia", f"R$ {format_brl(soma_abatida)}")
                    c2.metric("Saldo Restante (Sobrante)", f"R$ {format_brl(saldo)}")
                else:
                    st.warning("Nenhuma despesa pendente pôde ser alocada a este saldo diário.")
                    st.metric("Saldo Intacto", f"R$ {format_brl(saldo)}")
                st.divider()

        st.subheader("🎯 Tabela de Provisões (Humana/Radioclim)")
        for _, e in df_hr.iterrows():
            saldo_inicial = float(e['valor'])
            saldo = saldo_inicial
            nome_entrada = e['descricao']
            data_rec = pd.to_datetime(e['data_vencimento']).strftime('%d/%m/%Y')
            
            st.markdown(f"### 📥 {nome_entrada} | Recebimento: {data_rec} | Saldo: R$ {format_brl(saldo_inicial)}")
            aloc = []
            soma_abatida = 0.0
            for s in saidas_fila:
                if s['is_provisao'] and s['valor_restante'] > 0 and saldo > 0:
                    pg = min(saldo, s['valor_restante'])
                    s['valor_restante'] -= pg
                    saldo -= pg
                    soma_abatida += pg
                    aloc.append({"Provisão": s['descricao'], "Abatido": f"R$ {format_brl(pg)}"})
            
            if aloc: 
                st.dataframe(pd.DataFrame(aloc), use_container_width=True, hide_index=True)
                c1, c2 = st.columns(2)
                c1.metric("Total Distribuído", f"R$ {format_brl(soma_abatida)}")
                c2.metric("Saldo Restante (Sobrante)", f"R$ {format_brl(saldo)}")
            else: 
                st.info("Sem provisões pendentes para este saldo.")
                st.metric("Saldo Intacto", f"R$ {format_brl(saldo)}")
            st.divider()

# =================================================================
# 10. MÓDULO 5: ESCALA VISUAL DE PLANTÕES
# =================================================================

elif menu == "🏥 Escala de Plantões":
    st.header("🏥 Escala Visual de Plantões")
    
    # SELEÇÃO DO MÊS DO CALENDÁRIO
    c_mes, c_ano = st.columns(2)
    with c_mes:
        cal_mes = st.selectbox("Mês do Calendário", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1)
    with c_ano:
        cal_ano = st.selectbox("Ano do Calendário", range(hoje.year-1, hoje.year+2), index=1)

    st.divider()

    # BUSCAR PLANTÕES DO BANCO
    df_todas = fetch_dataframe("SELECT * FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'")
    
    if not df_todas.empty:
        df_todas['data_plantao_str'] = df_todas['descricao'].str.extract(r'\((.*?)\)')
        df_todas['data_plantao'] = pd.to_datetime(df_todas['data_plantao_str'], format='%d/%m/%Y', errors='coerce').dt.date
        df_mes_cal = df_todas[
            (pd.to_datetime(df_todas['data_plantao']).dt.month == cal_mes) &
            (pd.to_datetime(df_todas['data_plantao']).dt.year == cal_ano)
        ].copy()
    else:
        df_mes_cal = pd.DataFrame()

    # DESENHANDO O CALENDÁRIO
    dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    cols = st.columns(7)
    for i, col in enumerate(cols):
        col.markdown(f"<div style='text-align: center; font-weight: bold; padding: 5px; border-bottom: 2px solid #4CAF50;'>{dias_semana[i]}</div>", unsafe_allow_html=True)

    cal = calendar.monthcalendar(cal_ano, cal_mes)

    for week in cal:
        w_cols = st.columns(7)
        for i, day in enumerate(week):
            with w_cols[i]:
                if day == 0:
                    st.markdown("<div style='min-height: 90px;'></div>", unsafe_allow_html=True)
                else:
                    current_date = datetime.date(cal_ano, cal_mes, day)
                    is_today = current_date == hoje
                    
                    bg_style = "background-color: rgba(76, 175, 80, 0.1);" if is_today else ""
                    border_style = "border: 2px solid #4CAF50;" if is_today else "border: 1px solid rgba(150, 150, 150, 0.3);"
                    color_day = "#4CAF50" if is_today else "inherit"
                    
                    html_content = f"<div style='{bg_style} {border_style} border-radius: 5px; padding: 5px; min-height: 90px; margin-top: 5px;'>"
                    html_content += f"<div style='text-align:right; font-size:14px; font-weight:bold; color:{color_day};'>{day}</div>"
                    
                    if not df_mes_cal.empty:
                        day_shifts = df_mes_cal[df_mes_cal['data_plantao'] == current_date]
                        for _, s in day_shifts.iterrows():
                            html_content += f"<div style='background-color:#4CAF50; color:white; font-size:11px; padding:2px 4px; border-radius:4px; margin-top:4px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-weight: bold;'>🏥 {s['subgrupo']}</div>"
                    
                    html_content += "</div>"
                    st.markdown(html_content, unsafe_allow_html=True)

    st.divider()
    
    # --- ABA DE GESTÃO DA ESCALA ---
    st.subheader("📋 Gerenciar Escala Deste Mês")
    if not df_mes_cal.empty:
        st.markdown("🔍 **Filtros da Escala**")
        c_filt_esc1, c_filt_esc2 = st.columns(2)
        locais_disp = df_mes_cal['subgrupo'].unique().tolist()
        
        with c_filt_esc1:
            sel_locais = st.multiselect("Filtrar por Hospital", locais_disp, placeholder="Todos os Hospitais")
            
        locais_filtro = sel_locais if sel_locais else locais_disp
        df_gerenciar = df_mes_cal[df_mes_cal['subgrupo'].isin(locais_filtro)].copy()
        
        df_gerenciar = df_gerenciar.sort_values('data_plantao').reset_index(drop=True)
        df_gerenciar.insert(0, '🗑️ Apagar', False)
        df_gerenciar['Data do Plantão'] = pd.to_datetime(df_gerenciar['data_plantao']).dt.strftime('%d/%m/%Y')
        df_gerenciar['Recebimento'] = pd.to_datetime(df_gerenciar['data_vencimento']).dt.strftime('%d/%m/%Y')
        
        edit_escala = st.data_editor(
            df_gerenciar[['🗑️ Apagar', 'Data do Plantão', 'subgrupo', 'valor', 'Recebimento']],
            use_container_width=True,
            hide_index=True,
            column_config={
                "valor": st.column_config.NumberColumn("Valor (R$)", format="%.2f"),
                "subgrupo": "Hospital"
            },
            disabled=['Data do Plantão', 'subgrupo', 'valor', 'Recebimento']
        )
        
        c_btn1, c_btn2 = st.columns(2)
        with c_btn1:
            if st.button("💾 Salvar Exclusões Selecionadas", type="primary"):
                for i, row in edit_escala.iterrows():
                    if row['🗑️ Apagar']:
                        id_del = int(df_gerenciar.loc[i, 'id'])
                        execute_query("DELETE FROM lancamentos WHERE id = %s", (id_del,))
                st.success("Escala e previsões atualizadas!")
                st.rerun()
                
        with c_btn2:
            if st.button("🚨 Apagar TUDO o que está listado acima"):
                ids_to_delete = tuple(df_gerenciar['id'].tolist())
                if ids_to_delete:
                    if len(ids_to_delete) == 1:
                        execute_query("DELETE FROM lancamentos WHERE id = %s", (ids_to_delete[0],))
                    else:
                        execute_query(f"DELETE FROM lancamentos WHERE id IN {ids_to_delete}")
                    st.success("Plantões apagados com sucesso do calendário e do caixa!")
                    st.rerun()
    else:
        st.info("Você ainda não registrou nenhum plantão para este mês.")

    st.divider()

    # FORMULÁRIO DE REGISTRO
    st.subheader("➕ Adicionar à Escala")
    modo_registro = st.radio("Modo de Inserção", ["Dia Específico", "Plantões Fixos na Semana (Recorrente)"], horizontal=True)
    
    with st.container(border=True):
        c_pl1, c_pl2 = st.columns(2)
        with c_pl1:
            loc_p = st.selectbox("🏥 Local do Plantão", list(VALORES_PLANTAO.keys()))
            
            if modo_registro == "Dia Específico":
                data_padrao = datetime.date(cal_ano, cal_mes, min(hoje.day, calendar.monthrange(cal_ano, cal_mes)[1])) if cal_mes == hoje.month else datetime.date(cal_ano, cal_mes, 1)
                data_p = st.date_input("🗓️ Data do Plantão", value=data_padrao, format="DD/MM/YYYY")
            else:
                opcoes_dias = {0: "Segunda", 1: "Terça", 2: "Quarta", 3: "Quinta", 4: "Sexta", 5: "Sábado", 6: "Domingo"}
                dias_selecionados = st.multiselect("🗓️ Dias da Semana", options=list(opcoes_dias.keys()), format_func=lambda x: opcoes_dias[x])
                
        with c_pl2:
            qtd_p = st.number_input("⏰ Quantidade de Turnos", min_value=0.5, value=1.0, step=0.5)
            if modo_registro != "Dia Específico":
                meses_recorrencia = st.number_input("🔄 Repetir por quantos meses?", min_value=1, max_value=60, value=6, help="Ex: 6 meses (incluindo o atual)")
            
        regra = REGRAS_PAGAMENTO.get(loc_p, {"meses": 1, "dia": 10}) 
        v_total = VALORES_PLANTAO[loc_p] * qtd_p
        
        if modo_registro == "Dia Específico":
            mes_futuro = data_p.month + regra["meses"]
            ano_futuro = data_p.year + (mes_futuro - 1) // 12
            mes_futuro = (mes_futuro - 1) % 12 + 1
            data_vencimento_futura = datetime.date(ano_futuro, mes_futuro, regra["dia"])
            
            st.info(f"""
            **Resumo Automático:**
            * 💰 **Valor:** R$ {format_brl(v_total)}
            * 📅 **Dinheiro na Conta em:** {data_vencimento_futura.strftime('%d/%m/%Y')}
            """)
        else:
            mes_futuro_ex = cal_mes + regra["meses"]
            ano_futuro_ex = cal_ano + (mes_futuro_ex - 1) // 12
            mes_futuro_ex = (mes_futuro_ex - 1) % 12 + 1
            data_vencimento_futura_ex = datetime.date(ano_futuro_ex, mes_futuro_ex, regra["dia"])
            
            st.info(f"""
            **Resumo Automático:**
            * 💰 **Valor (por plantão):** R$ {format_brl(v_total)}
            * ⏱️ **Regra do {loc_p}:** Paga {regra['meses']} mês(es) depois. (O primeiro pagamento cairá em {data_vencimento_futura_ex.strftime('%d/%m/%Y')})
            """)
        
        if st.button("🚀 Registrar Plantão" if modo_registro == "Dia Específico" else "🚀 Registrar Escala Recorrente", type="primary"):
            comp_id = str(uuid.uuid4())
            cat_escolhida = "Valores Fixos" if loc_p in ESTRUTURA["Entrada"].get("Valores Fixos", []) else "Valores Variáveis"
            if loc_p not in ESTRUTURA["Entrada"].get(cat_escolhida, []): 
                cat_escolhida = "Valores Variáveis"

            registros = []
            if modo_registro == "Dia Específico":
                desc_plantao = f"Plantão {loc_p} ({data_p.strftime('%d/%m/%Y')})"
                registros.append(("Entrada", cat_escolhida, loc_p, desc_plantao, v_total, data_vencimento_futura, 1, 1, 0, comp_id, "Outros"))
            else:
                if not dias_selecionados:
                    st.error("Por favor, selecione pelo menos um dia da semana para repetir.")
                    st.stop()
                
                for offset in range(meses_recorrencia):
                    m_alvo = cal_mes + offset
                    a_alvo = cal_ano + (m_alvo - 1) // 12
                    m_alvo = (m_alvo - 1) % 12 + 1
                    
                    mes_pagto = m_alvo + regra["meses"]
                    ano_pagto = a_alvo + (mes_pagto - 1) // 12
                    mes_pagto = (mes_pagto - 1) % 12 + 1
                    dt_pagto = datetime.date(ano_pagto, mes_pagto, regra["dia"])
                    
                    for day in range(1, calendar.monthrange(a_alvo, m_alvo)[1] + 1):
                        current_date = datetime.date(a_alvo, m_alvo, day)
                        if current_date.weekday() in dias_selecionados:
                            desc_plantao = f"Plantão {loc_p} ({current_date.strftime('%d/%m/%Y')})"
                            registros.append(("Entrada", cat_escolhida, loc_p, desc_plantao, v_total, dt_pagto, 1, 1, 0, comp_id, "Outros"))

            if registros:
                execute_values_query('''
                    INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento)
                    VALUES %s
                ''', registros)
                
                st.success(f"✅ Sucesso! {len(registros)} plantões gerados no calendário e provisionados no fluxo de caixa.")
                st.rerun()
