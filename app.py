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
        CREATE TABLE IF NOT EXISTS categorias_personalizadas (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT,
            valor_padrao NUMERIC,
            atraso_meses INTEGER,
            dia_pagamento INTEGER
        );
    ''')
    # Garantir que colunas novas existam caso a tabela já tenha sido criada anteriormente
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS valor_padrao NUMERIC;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS atraso_meses INTEGER;")
    execute_query("ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS dia_pagamento INTEGER;")
    
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
            prioridade TEXT DEFAULT 'Baixa 🟢'
        );
    ''')
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS forma_pagamento TEXT DEFAULT 'Outros';")
    execute_query("ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS prioridade TEXT DEFAULT 'Baixa 🟢';")

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
# 3. CONFIGURAÇÃO DA PÁGINA
# =================================================================

st.set_page_config(page_title="Gestão Financeira", layout="wide")
if not check_password(): st.stop()
init_db()

# =================================================================
# 4. ESTRUTURAS DINÂMICAS E CONSTANTES
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
# 5. SIDEBAR E FILTROS
# =================================================================

st.sidebar.title("Navegação")
menu = st.sidebar.radio("Módulo:", ["📝 Lançamentos", "📊 Fluxo e Prioridades", "📑 Demonstrativo", "🔀 Otimização de Pagamentos", "🏥 Escala de Plantões"])
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
        execute_query("TRUNCATE TABLE lancamentos RESTART IDENTITY")
        registros = [(r['tipo'], r['categoria'], r['subgrupo'], r['descricao'], r['valor'], r['data_vencimento'], r['parcela_atual'], r['total_parcelas'], r['pago'], r['compra_id'], r['forma_pagamento'], r['prioridade']) for _, r in df_imp.iterrows()]
        execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade) VALUES %s''', registros)
        return True
    except Exception as e:
        st.error(f"Erro Crítico de Restauração: {e}")
        return False

c_data = exportar_csv()
if c_data: st.sidebar.download_button("📥 Baixar CSV", data=c_data, file_name=f"backup_{hoje.strftime('%d_%m_%Y')}.csv", mime="text/csv")
a_up = st.sidebar.file_uploader("Restaurar CSV", type="csv")
if a_up and st.sidebar.button("🚀 Confirmar Restauração"): 
    if importar_csv(a_up): st.rerun()

st.markdown("### 📅 Filtro de Período")
col_top1, col_top2 = st.columns(2)
with col_top1: mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], index=hoje.month-1)
with col_top2: ano_selecionado = st.selectbox("Ano", range(hoje.year-2, hoje.year+5), index=2)

# =================================================================
# 6. MÓDULO 1: LANÇAMENTOS E GESTÃO DE ESTRUTURA
# =================================================================

if menu == "📝 Lançamentos":
    st.header("📝 Novo Lançamento")
    
    with st.expander("⚙️ Gerenciar Categorias e Subgrupos Personalizados", expanded=not ESTRUTURA["Despesa"] and not ESTRUTURA["Entrada"]):
        tab_add, tab_edit, tab_del = st.tabs(["➕ Adicionar", "✏️ Editar", "🗑️ Excluir"])
        
        with tab_add:
            c_add1, c_add2 = st.columns(2)
            with c_add1:
                ntipo = st.radio("Para qual tipo?", ["Despesa", "Entrada"], horizontal=True, key="add_tipo")
                ncat = st.text_input("Nome da Categoria (Nova ou Existente)", placeholder="Ex: Valores Fixos")
            with c_add2:
                nsub = st.text_input("Nome do Subgrupo (Opcional)", placeholder="Ex: Hospital Trauma")
            
            # Campos Opcionais de Plantão para Entrada
            if ntipo == "Entrada":
                st.markdown("---")
                st.markdown("##### 🏥 Dados Padrão de Plantão (Opcional)")
                c_opt1, c_opt2, c_opt3 = st.columns(3)
                v_opt = c_opt1.number_input("Valor Padrão", min_value=0.0, step=50.0, value=0.0)
                a_opt = c_opt2.number_input("Atraso (Meses)", min_value=0, max_value=6, value=1)
                d_opt = c_opt3.number_input("Dia Pagamento", min_value=1, max_value=31, value=10)

            if st.button("Salvar Nova Categoria/Subgrupo", type="primary"):
                if not ncat.strip(): st.error("O nome da Categoria é obrigatório.")
                else:
                    if ntipo == "Entrada":
                        execute_query("INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento) VALUES (%s, %s, %s, %s, %s, %s)", 
                                      (ntipo, ncat.strip(), nsub.strip(), v_opt if v_opt > 0 else None, a_opt, d_opt))
                    else:
                        execute_query("INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo) VALUES (%s, %s, %s)", (ntipo, ncat.strip(), nsub.strip()))
                    st.success("Adicionado com sucesso!")
                    st.rerun()
                    
        with tab_edit:
            df_custom = fetch_dataframe("SELECT * FROM categorias_personalizadas")
            if not df_custom.empty:
                opcoes_edit = {r['id']: f"{r['tipo']} ➔ {r['categoria']} ➔ {r['subgrupo']}" for _, r in df_custom.iterrows()}
                sel_edit = st.selectbox("Selecione o item para editar:", options=[None] + list(opcoes_edit.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_edit[x])
                if sel_edit:
                    nó = df_custom[df_custom['id'] == sel_edit].iloc[0]
                    c_ed_n1, c_ed_n2 = st.columns(2)
                    with c_ed_n1:
                        new_cat = st.text_input("Nova Categoria", value=nó['categoria'])
                    with c_ed_n2:
                        new_sub = st.text_input("Novo Subgrupo", value=nó['subgrupo'] if nó['subgrupo'] else "")
                    
                    # Edição de campos de plantão
                    if nó['tipo'] == "Entrada":
                        st.markdown("---")
                        st.markdown("##### 🏥 Dados Padrão de Plantão")
                        c_opt_e1, c_opt_e2, c_opt_e3 = st.columns(3)
                        v_edit = c_opt_e1.number_input("Valor Padrão", value=float(nó['valor_padrao']) if nó['valor_padrao'] else 0.0)
                        a_edit = c_opt_e2.number_input("Atraso (Meses)", value=int(nó['atraso_meses']) if nó['atraso_meses'] else 1)
                        d_edit = c_opt_e3.number_input("Dia Pagamento", value=int(nó['dia_pagamento']) if nó['dia_pagamento'] else 10)

                    if st.button("💾 Confirmar Edição", type="primary"):
                        if nó['tipo'] == "Entrada":
                            execute_query("UPDATE categorias_personalizadas SET categoria=%s, subgrupo=%s, valor_padrao=%s, atraso_meses=%s, dia_pagamento=%s WHERE id=%s", 
                                          (new_cat, new_sub, v_edit if v_edit > 0 else None, a_edit, d_edit, sel_edit))
                        else:
                            execute_query("UPDATE categorias_personalizadas SET categoria=%s, subgrupo=%s WHERE id=%s", (new_cat, new_sub, sel_edit))
                        
                        execute_query("UPDATE lancamentos SET categoria=%s, subgrupo=%s WHERE tipo=%s AND categoria=%s AND subgrupo=%s", (new_cat, new_sub, nó['tipo'], nó['categoria'], nó['subgrupo']))
                        st.success("Atualizado com sucesso.")
                        st.rerun()
            else: st.info("Nenhuma categoria encontrada.")

        with tab_del:
            if not df_custom.empty:
                sel_del = st.selectbox("Selecione o item para excluir:", options=[None] + list(opcoes_edit.keys()), format_func=lambda x: "Selecione..." if x is None else opcoes_edit[x])
                if sel_del and st.button("🗑️ Excluir Selecionado", type="primary"):
                    execute_query("DELETE FROM categorias_personalizadas WHERE id = %s", (sel_del,))
                    st.success("Excluído com sucesso!")
                    st.rerun()

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        tipo = st.radio("Tipo", ["Despesa", "Entrada"], horizontal=True, key="lanc_tipo")
        forma_pgto = st.selectbox("Forma de Pagamento", ["À vista", "Crédito", "Outros"], index=0 if tipo == "Entrada" else 1)
        descricao = st.text_input("Descrição")
        valor_input = st.text_input("Valor (R$)", value="0,00")
        prioridade = st.radio("Prioridade", ["Baixa 🟢", "Média 🟡", "Alta 🔴"], index=0, horizontal=True)
    with col2:
        if not ESTRUTURA[tipo]:
            st.error("Não há categorias ativas. Crie uma no menu acima.")
            categoria, subgrupo = None, None
        else:
            categoria = st.selectbox("Categoria", list(ESTRUTURA[tipo].keys()))
            subgrupos_disp = ESTRUTURA[tipo][categoria] if categoria in ESTRUTURA[tipo] else []
            subgrupo = st.selectbox("Subgrupo", subgrupos_disp)
        
        gasto_continuo = st.checkbox("🗓️ Provisão (Mês todo)")
        data_venc_base = st.date_input("Data Referência", value=hoje, format="DD/MM/YYYY")
        
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
            for i in range(parcelas):
                m_f = data_venc_base.month - 1 + i
                a_f = data_venc_base.year + m_f // 12
                m_f = m_f % 12 + 1
                d_p = datetime.date(a_f, m_f, calendar.monthrange(a_f, m_f)[1]) if gasto_continuo else datetime.date(a_f, m_f, min(data_venc_base.day, calendar.monthrange(a_f, m_f)[1]))
                registros.append((tipo, categoria, subgrupo, desc_final, val_f, d_p, i+1, tot_p, 0, comp_id, forma_pgto, prioridade))
            execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade) VALUES %s''', registros)
            st.success("Salvo!"); st.rerun()

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
        
        mask_cred = df_view['forma_pagamento'] == 'Crédito'
        if mask_cred.any():
            sum_cred = df_view[mask_cred]['valor'].sum()
            all_paid = (df_view[mask_cred]['pago'] == 1).all()
            dummy_credito = pd.DataFrame([{'id': '-1', 'tipo': 'Despesa', 'categoria': 'N/A', 'subgrupo': '', 'descricao': '💳 Cartão de Crédito (Fatura Consolidada)', 'valor': sum_cred, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10), 'pago': 1 if all_paid else 0, 'compra_id': 'cartao_dummy', 'forma_pagamento': 'Crédito', 'prioridade': 'Alta 🔴'}])
            df_view = df_view[~mask_cred].copy()
            df_view = pd.concat([df_view, dummy_credito], ignore_index=True)
            
        mask_plantoes = (df_view['tipo'] == 'Entrada') & df_view['descricao'].str.contains('Plantão', na=False)
        if mask_plantoes.any():
            df_plantoes = df_view[mask_plantoes].copy()
            df_view = df_view[~mask_plantoes].copy()
            for nome_grupo, grupo in df_plantoes.groupby(['subgrupo', 'data_vencimento']):
                subg_nome, dt_venc = nome_grupo
                dummy_plantao = pd.DataFrame([{'id': f'plantao_{subg_nome}', 'tipo': 'Entrada', 'categoria': grupo.iloc[0]['categoria'], 'subgrupo': subg_nome, 'descricao': f'🏥 Plantões {subg_nome} (Consolidado do Mês)', 'valor': grupo['valor'].sum(), 'data_vencimento': dt_venc, 'pago': 1 if (grupo['pago'] == 1).all() else 0, 'compra_id': 'plantao_dummy', 'forma_pagamento': 'Outros', 'prioridade': 'Baixa 🟢'}])
                df_view = pd.concat([df_view, dummy_plantao], ignore_index=True)
        
        df_view['id'] = df_view['id'].astype(str)
        df_view['ordem_pri'] = df_view['prioridade'].map(prioridades_map).fillna(2)
        df_view = df_view.sort_values(['data_vencimento', 'ordem_pri']).reset_index(drop=True)
        df_view['Pago'] = df_view['pago'].astype(bool)
        df_view['Data'] = pd.to_datetime(df_view['data_vencimento']).dt.date
        
        df_view.insert(0, '🗑️ Este', False)
        df_view.insert(1, '🗑️ Futuros', False)

        st.markdown("*(Nota: Edições nos valores 'Consolidados' gerarão um novo registro de Ajuste para manter a integridade dos dados originais).*")
        edit_df = st.data_editor(
            df_view[['🗑️ Este', '🗑️ Futuros', 'Data', 'prioridade', 'descricao', 'valor', 'Pago']], 
            use_container_width=True, 
            hide_index=True, 
            column_config={
                "Data": st.column_config.DateColumn("Data", format="DD/MM/YYYY"), 
                "valor": st.column_config.NumberColumn("Valor", format="%.2f"),
                "prioridade": st.column_config.SelectboxColumn("Prioridade", options=["Alta 🔴", "Média 🟡", "Baixa 🟢"])
            }
        )
        
        if st.button("Salvar Alterações Rápidas", type="primary"):
            for i, row in edit_df.iterrows():
                id_s = str(df_view.loc[i, 'id'])
                novo_pago = 1 if row['Pago'] else 0
                velho_valor = float(df_view.loc[i, 'valor'])
                novo_valor = float(row['valor'])
                delta = novo_valor - velho_valor
                
                if row['🗑️ Este'] or row['🗑️ Futuros']:
                    if id_s == '-1': st.warning("Cartões consolidados não podem ser apagados aqui. Vá em Demonstrativo > Detalhamento de Faturas (Crédito).")
                    elif id_s.startswith('plantao_'): execute_query("DELETE FROM lancamentos WHERE tipo='Entrada' AND subgrupo=%s AND data_vencimento=%s AND descricao LIKE 'Plantão %%'", (id_s.replace('plantao_', ''), row['Data']))
                    else: execute_query("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s" if row['🗑️ Futuros'] else "DELETE FROM lancamentos WHERE id = %s", (df_view.loc[i, 'compra_id'], df_view.loc[i, 'data_vencimento']) if row['🗑️ Futuros'] else (int(id_s),))
                else:
                    if id_s == '-1':
                        execute_query("UPDATE lancamentos SET pago=%s WHERE forma_pagamento='Crédito' AND EXTRACT(MONTH FROM data_vencimento)=%s AND EXTRACT(YEAR FROM data_vencimento)=%s", (novo_pago, mes_selecionado, ano_selecionado))
                        if delta != 0: execute_query("INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, pago, forma_pagamento, prioridade) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('Despesa', 'Ajuste', '', '💳 Ajuste de Fatura Consolidada', delta, row['Data'], novo_pago, 'Outros', row['prioridade']))
                    elif id_s.startswith('plantao_'):
                        subg = id_s.replace('plantao_', '')
                        execute_query("UPDATE lancamentos SET pago=%s WHERE tipo='Entrada' AND subgrupo=%s AND data_vencimento=%s AND descricao LIKE 'Plantão %%'", (novo_pago, subg, row['Data']))
                        if delta != 0: execute_query("INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, pago, forma_pagamento, prioridade) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('Entrada', 'Ajuste', subg, f'🏥 Ajuste de Plantão {subg}', delta, row['Data'], novo_pago, 'Outros', row['prioridade']))
                    else:
                        execute_query("UPDATE lancamentos SET pago=%s, prioridade=%s, descricao=%s, valor=%s, data_vencimento=%s WHERE id=%s", (novo_pago, row['prioridade'], row['descricao'], novo_valor, row['Data'], int(id_s)))
            st.rerun()

        st.divider()
        st.subheader("📱 Compartilhar no WhatsApp")
        if st.button("Gerar Lista para WhatsApp"):
            df_despesas_wpp = df_view[df_view['tipo'] == 'Despesa'].sort_values(['ordem_pri', 'data_vencimento'])
            if df_despesas_wpp.empty: st.success("Nenhuma despesa registrada para este mês! 🎉")
            else:
                texto_wpp = f"*Resumo de Contas - {meses[mes_selecionado-1]}/{ano_selecionado}*\n\n"
                t_wpp = 0.0
                for _, r in df_despesas_wpp.iterrows():
                    d_s = pd.to_datetime(r['data_vencimento']).strftime('%d/%m')
                    if r['Pago']: texto_wpp += f"✅ ~{d_s} - {r['descricao']}: R$ {format_brl(r['valor'])}~\n"
                    else:
                        texto_wpp += f"⏳ {d_s} - {r['descricao']}: R$ {format_brl(r['valor'])}\n"
                        t_wpp += float(r['valor'])
                texto_wpp += f"\n*Total Restante a Pagar:* R$ {format_brl(t_wpp)}"
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
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s WHERE id=%s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                    else:
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s WHERE id=%s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, int(sel_id)))
                        execute_query("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, forma_pagamento=%s WHERE compra_id=%s AND data_vencimento > %s AND id != %s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_forma, r_sel['compra_id'], r_sel['data_vencimento'], int(sel_id)))
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
        df_e, df_d = df[df['tipo'] == 'Entrada'], df[df['tipo'] == 'Despesa']
        
        c_m1, c_m2, c_m3 = st.columns(3)
        c_m1.metric("Receita Total", f"R$ {format_brl(df_e['valor'].sum())}")
        c_m2.metric("Despesa Total", f"R$ {format_brl(df_d['valor'].sum())}")
        c_m3.metric("Orçamento Base-Zero (ZBB)", f"R$ {format_brl(df_e['valor'].sum() - df_d['valor'].sum())}")
        
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("🟢 Entradas Detalhadas")
            for cat in df_e['categoria'].unique():
                df_c = df_e[df_e['categoria'] == cat]
                with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                    for sub in df_c['subgrupo'].unique():
                        df_s = df_c[df_c['subgrupo'] == sub]
                        st.markdown(f"**{sub}** (R$ {format_brl(df_s['valor'].sum())})")
                        st.dataframe(df_s[['Data BR', 'descricao', 'valor', 'prioridade']], hide_index=True)
        with c2:
            st.subheader("🔴 Despesas Detalhadas")
            for cat in df_d['categoria'].unique():
                df_c = df_d[df_d['categoria'] == cat]
                with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                    for sub in df_c['subgrupo'].unique():
                        df_s = df_c[df_c['subgrupo'] == sub]
                        st.markdown(f"**{sub}** (R$ {format_brl(df_s['valor'].sum())})")
                        st.dataframe(df_s[['Data BR', 'descricao', 'valor', 'prioridade']], hide_index=True)

        st.divider()
        st.subheader("💳 Detalhamento e Edição de Faturas (Crédito)")
        df_credito = df[df['forma_pagamento'] == 'Crédito'].copy()
        if not df_credito.empty:
            df_credito = df_credito.sort_values('data_vencimento').reset_index(drop=True)
            df_credito['Data'] = pd.to_datetime(df_credito['data_vencimento']).dt.date
            df_credito.insert(0, '🗑️ Este', False)
            df_credito.insert(1, '🗑️ Futuros', False)
            edit_c = st.data_editor(df_credito[['🗑️ Este', '🗑️ Futuros', 'Data', 'categoria', 'subgrupo', 'descricao', 'valor']], use_container_width=True, hide_index=True)
            
            c_b1, c_b2 = st.columns(2)
            with c_b1:
                if st.button("💾 Salvar Alterações do Cartão", type="primary"):
                    for i, r in edit_c.iterrows():
                        if r['🗑️ Este'] or r['🗑️ Futuros']:
                            if r['🗑️ Futuros']:
                                execute_query("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s", (df_credito.loc[i, 'compra_id'], df_credito.loc[i, 'data_vencimento']))
                            else:
                                execute_query("DELETE FROM lancamentos WHERE id = %s", (int(df_credito.loc[i, 'id']),))
                    st.rerun()
            with c_b2:
                if st.button("🚨 Apagar TODOS os Lançamentos de Crédito Listados"):
                    ids_cred = tuple(df_credito['id'].tolist())
                    if ids_cred:
                        if len(ids_cred) == 1:
                            execute_query("DELETE FROM lancamentos WHERE id = %s", (ids_cred[0],))
                        else:
                            execute_query(f"DELETE FROM lancamentos WHERE id IN {ids_cred}")
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
        df_e, df_d = df_mes[df_mes['tipo'] == 'Entrada'].copy(), df_mes[df_mes['tipo'] == 'Despesa'].copy()
        
        mask_plantoes = df_e['descricao'].str.contains('Plantão', na=False)
        if mask_plantoes.any():
            df_plantoes = df_e[mask_plantoes].copy()
            df_e = df_e[~mask_plantoes].copy()
            for nome_grupo, grupo in df_plantoes.groupby(['subgrupo', 'data_vencimento']):
                subg_nome, dt_venc = nome_grupo
                dummy_plantao = pd.DataFrame([{'id': f'plantao_{subg_nome}', 'tipo': 'Entrada', 'categoria': grupo.iloc[0]['categoria'], 'subgrupo': subg_nome, 'descricao': f'🏥 Plantões {subg_nome}', 'valor': grupo['valor'].sum(), 'data_vencimento': dt_venc, 'pago': 0, 'compra_id': 'plantao_dummy', 'forma_pagamento': 'Outros', 'prioridade': 'Baixa 🟢'}])
                df_e = pd.concat([df_e, dummy_plantao], ignore_index=True)

        df_e['is_hr'] = df_e['descricao'].str.contains('Radioclim|Humana', case=False, na=False)
        df_hr, df_outras = df_e[df_e['is_hr']].copy(), df_e[~df_e['is_hr']].copy()
        
        mask_c = df_d['forma_pagamento'] == 'Crédito'
        if mask_c.any():
            sum_c = df_d[mask_c]['valor'].sum()
            df_d = pd.concat([df_d[~mask_c], pd.DataFrame([{'id': -1, 'descricao': '💳 Cartão de Crédito', 'valor': sum_c, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10), 'prioridade': 'Alta 🔴'}])], ignore_index=True)
            
        df_d['is_prov'] = df_d['descricao'].str.contains(r'\(Provisão\)', case=False, na=False)
        f_saidas = df_d.to_dict('records')
        for s in f_saidas: s['v_rest'] = float(s['valor'])

        st.subheader("🗓️ Fluxo Geral Consolidado Diário")
        if not df_outras.empty:
            ag_e = df_outras.groupby('data_vencimento').agg({'valor':'sum', 'descricao': lambda x: ' + '.join(pd.Series(x).unique().astype(str))}).reset_index().sort_values('data_vencimento')
            for _, e in ag_e.iterrows():
                saldo = float(e['valor'])
                st.markdown(f"### 📥 {pd.to_datetime(e['data_vencimento']).strftime('%d/%m')} | {e['descricao']} | R$ {format_brl(saldo)}")
                
                elg = sorted([s for s in f_saidas if s['v_rest'] > 0 and not s['is_prov']], key=lambda x: (prioridades_map.get(x.get('prioridade', 'Baixa 🟢'), 2), x['data_vencimento']))
                aloc = []
                s_abt = 0.0
                for s in elg:
                    if saldo > 0:
                        pg = min(saldo, s['v_rest'])
                        s['v_rest'] -= pg; saldo -= pg; s_abt += pg
                        aloc.append({"Conta": s['descricao'], "Venc.": pd.to_datetime(s['data_vencimento']).strftime('%d/%m'), "Abatido": f"R$ {format_brl(pg)}"})
                if aloc: st.dataframe(pd.DataFrame(aloc), use_container_width=True, hide_index=True)
                st.divider()

# =================================================================
# 10. MÓDULO 5: ESCALA VISUAL DE PLANTÕES
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
        locais_disp = df_m_cal['subgrupo'].unique().tolist()
        sel_locais = st.multiselect("Filtrar por Hospital", locais_disp, placeholder="Todos os Hospitais")
        df_geren = df_m_cal[df_m_cal['subgrupo'].isin(sel_locais)] if sel_locais else df_m_cal
        df_geren = df_geren.sort_values('d_p').reset_index(drop=True)
        df_geren.insert(0, '🗑️ Apagar', False)
        df_geren['Data do Plantão'] = pd.to_datetime(df_geren['d_p']).dt.strftime('%d/%m/%Y')
        edit_esc = st.data_editor(df_geren[['🗑️ Apagar', 'Data do Plantão', 'subgrupo', 'valor']], use_container_width=True, hide_index=True)
        c_b1, c_b2 = st.columns(2)
        with c_b1:
            if st.button("💾 Salvar Exclusões Selecionadas"):
                for i, r in edit_esc.iterrows():
                    if r['🗑️ Apagar']: execute_query("DELETE FROM lancamentos WHERE id = %s", (int(df_geren.loc[i, 'id']),))
                st.rerun()
        with c_b2:
            if st.button("🚨 Apagar TUDO o que está listado acima"):
                ids = tuple(df_geren['id'].tolist())
                if ids:
                    if len(ids) == 1: execute_query("DELETE FROM lancamentos WHERE id = %s", (ids[0],))
                    else: execute_query(f"DELETE FROM lancamentos WHERE id IN {ids}")
                    st.rerun()
    else: st.info("Você ainda não registrou nenhum plantão para este mês.")

    st.divider()
    
    st.subheader("🗑️ Purgar Todo o Histórico")
    if st.button("🚨 Apagar TODO o Histórico Global de Plantões (Banco de Dados)", type="primary"):
        execute_query("DELETE FROM lancamentos WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'")
        st.success("Histórico global de plantões purgado.")
        st.rerun()
        
    st.divider()

    st.subheader("➕ Adicionar à Escala")
    modo = st.radio("Modo de Inserção", ["Dia Específico", "Plantões Fixos na Semana (Recorrente)"], horizontal=True)
    
    locais_dyn = list(set([item for sublist in ESTRUTURA["Entrada"].values() for item in sublist]))
    
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            loc_p = st.selectbox("🏥 Local do Plantão", locais_dyn if locais_dyn else ["Vazio (Crie na aba Lançamentos)"])
            
            # Buscar valores padrão se o hospital for selecionado
            default_vals = {"v": 1000.0, "m": 1, "d": 10}
            if loc_p != "Vazio (Crie na aba Lançamentos)":
                res = fetch_dataframe("SELECT valor_padrao, atraso_meses, dia_pagamento FROM categorias_personalizadas WHERE subgrupo = %s AND tipo = 'Entrada' LIMIT 1", (loc_p,))
                if not res.empty:
                    if res.iloc[0]['valor_padrao']: default_vals["v"] = float(res.iloc[0]['valor_padrao'])
                    if res.iloc[0]['atraso_meses'] is not None: default_vals["m"] = int(res.iloc[0]['atraso_meses'])
                    if res.iloc[0]['dia_pagamento'] is not None: default_vals["d"] = int(res.iloc[0]['dia_pagamento'])

            if modo == "Dia Específico": d_p = st.date_input("🗓️ Data do Plantão", value=hoje, format="DD/MM/YYYY")
            else: dias_s = st.multiselect("🗓️ Dias da Semana", options=[0,1,2,3,4,5,6], format_func=lambda x: ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"][x])
        with c2:
            v_t = st.number_input("Valor do Plantão (R$)", value=default_vals["v"], step=100.0)
            reg_m = st.number_input("Atraso (Meses)", min_value=0, max_value=6, value=default_vals["m"])
            reg_d = st.number_input("Dia do Pagamento", min_value=1, max_value=31, value=default_vals["d"])
            if modo != "Dia Específico": m_rec = st.number_input("Repetir por quantos meses?", min_value=1, max_value=60, value=6)

        if st.button("🚀 Registrar Plantão", type="primary") and loc_p != "Vazio (Crie na aba Lançamentos)":
            cat_escolhida = next((c for c, subs in ESTRUTURA.get("Entrada", {}).items() if loc_p in subs), "N/A")
            regs = []
            if modo == "Dia Específico":
                m_f = (d_p.month + reg_m - 1) % 12 + 1
                a_f = d_p.year + (d_p.month + reg_m - 1) // 12
                regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({d_p.strftime('%d/%m/%Y')})", v_t, datetime.date(a_f, m_f, reg_d), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢"))
            elif dias_s:
                for off in range(m_rec):
                    m_a, a_a = (cal_mes + off - 1) % 12 + 1, cal_ano + (cal_mes + off - 1) // 12
                    m_p, a_p = (m_a + reg_m - 1) % 12 + 1, a_a + (m_a + reg_m - 1) // 12
                    for d in range(1, calendar.monthrange(a_a, m_a)[1] + 1):
                        curr = datetime.date(a_a, m_a, d)
                        if curr.weekday() in dias_s: regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({curr.strftime('%d/%m/%Y')})", v_t, datetime.date(a_p, m_p, reg_d), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢"))
            
            if regs:
                execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade) VALUES %s''', regs)
                st.success("Salvo com sucesso!")
                st.rerun()
