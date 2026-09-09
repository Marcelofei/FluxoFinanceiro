import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
import plotly.express as px
import datetime
import calendar
import uuid
import os
import io
import re
import json
import zipfile
from contextlib import contextmanager

# =================================================================
# 1. INFRAESTRUTURA, POOL DE CONEXÕES E TRANSAÇÕES
# =================================================================

@st.cache_resource
def get_pool():
    '''Pool compartilhado (thread-safe) em vez de uma conexão global mutável.'''
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL não configurada na variável de ambiente.")
        st.stop()

    db_url = db_url.replace(":6543/", ":5432/")
    if "sslmode=require" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"

    try:
        return ThreadedConnectionPool(
            minconn=1,
            maxconn=int(os.environ.get("DB_POOL_MAX", "8")),
            dsn=db_url,
            options="-c client_encoding=utf8",
            connect_timeout=10,
        )
    except Exception as e:
        st.error(f"Falha Crítica de Conexão com o PostgreSQL: {e}")
        st.stop()

@contextmanager
def db_connection(autocommit=True):
    pool = get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = autocommit
        yield conn
    finally:
        # Garante que uma transação interrompida não contamine o próximo uso.
        try:
            if not conn.closed and not autocommit:
                conn.rollback()
        except Exception:
            pass
        try:
            if not conn.closed:
                conn.autocommit = True
                pool.putconn(conn)
            else:
                pool.putconn(conn, close=True)
        except Exception:
            pass

@contextmanager
def transaction():
    '''Unidade atômica de trabalho: tudo confirma ou tudo volta.'''
    with db_connection(autocommit=False) as conn:
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def execute_query(query, params=None, fetch=False, silent=False):
    '''Executa SQL curto em conexão própria. Erros são propagados por padrão.'''
    try:
        with db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall() if fetch else None
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_pool.clear()
        try:
            with db_connection(autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    return cur.fetchall() if fetch else None
        except Exception as e:
            if not silent:
                st.error(f"Erro de Banco de Dados: {e}")
            raise
    except Exception as e:
        if not silent:
            st.error(f"Erro de Banco de Dados: {e}")
        raise


def execute_values_query(query, params_list):
    if not params_list:
        return
    try:
        with transaction() as cur:
            execute_values(cur, query, params_list)
    except Exception as e:
        st.error(f"Erro de Inserção Múltipla: {e}")
        raise


def fetch_dataframe(query, params=None):
    '''
    Leituras de lançamentos usam automaticamente a VIEW financeira derivada.
    Use o comentário /* RAW */ quando precisar dos valores físicos da tabela
    (backup/migração). Isso mantém os envelopes imutáveis no banco e calcula
    o saldo disponível em tempo de leitura.
    '''
    query_exec = query
    if "/* RAW */" not in query_exec:
        query_exec = re.sub(
            r"\bFROM\s+lancamentos\b",
            "FROM vw_lancamentos_financeiros",
            query_exec,
            flags=re.IGNORECASE,
        )
    try:
        with db_connection(autocommit=True) as conn:
            return pd.read_sql_query(query_exec, conn, params=params)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        get_pool.clear()
        try:
            with db_connection(autocommit=True) as conn:
                return pd.read_sql_query(query_exec, conn, params=params)
        except Exception as e:
            st.error(f"Erro de Leitura de Dados: {e}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro de Leitura de Dados: {e}")
        return pd.DataFrame()


def limites_mes(mes, ano):
    inicio = datetime.date(ano, mes, 1)
    if mes == 12:
        fim = datetime.date(ano + 1, 1, 1)
    else:
        fim = datetime.date(ano, mes + 1, 1)
    return inicio, fim


def limites_ano(ano):
    return datetime.date(ano, 1, 1), datetime.date(ano + 1, 1, 1)


@st.cache_resource
def init_db():
    '''Migrações compatíveis com a base existente, sem exigir reset manual.'''
    # Schema principal
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
    for ddl in [
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS valor_padrao NUMERIC;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS atraso_meses INTEGER;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS dia_pagamento INTEGER;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_recorrente INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS data_inicio DATE;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_envelope INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_producao_variavel INTEGER DEFAULT 0;",
    ]:
        execute_query(ddl)

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
    for ddl in [
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS forma_pagamento TEXT DEFAULT 'Outros';",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS prioridade TEXT DEFAULT 'Baixa 🟢';",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS valor_pago NUMERIC DEFAULT 0.0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS eh_estimativa INTEGER DEFAULT 0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS data_competencia DATE;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS data_pagamento DATE;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS eh_orcamento INTEGER DEFAULT 0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS valor_orcamento NUMERIC;",
    ]:
        execute_query(ddl)

    # Datas legadas: competência = vencimento; para pagamentos antigos, a melhor
    # inferência disponível é o vencimento. Novos registros passam a gravar datas reais.
    execute_query("UPDATE lancamentos SET data_competencia = data_vencimento WHERE data_competencia IS NULL")
    # Recupera a data real de plantões legados que antes existia apenas na descrição.
    execute_query(r"""
        UPDATE lancamentos
        SET data_competencia = TO_DATE(SUBSTRING(descricao FROM '\(([0-9]{2}/[0-9]{2}/[0-9]{4})\)'), 'DD/MM/YYYY')
        WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'
          AND descricao ~ '\([0-9]{2}/[0-9]{2}/[0-9]{4}\)'
    """)
    execute_query("UPDATE lancamentos SET data_pagamento = data_vencimento WHERE pago = 1 AND data_pagamento IS NULL")

    execute_query('''
        CREATE TABLE IF NOT EXISTS info_dividas (
            compra_id TEXT PRIMARY KEY,
            credor TEXT,
            taxa_juros_mensal NUMERIC
        );
    ''')
    execute_query('''
        CREATE TABLE IF NOT EXISTS reserva_emergencia (
            id INTEGER PRIMARY KEY DEFAULT 1,
            valor NUMERIC DEFAULT 0,
            atualizado_em DATE
        );
    ''')
    execute_query("INSERT INTO reserva_emergencia (id, valor, atualizado_em) VALUES (1, 0, CURRENT_DATE) ON CONFLICT (id) DO NOTHING;")

    # Entidade de pagamentos. O lançamento mantém valor_pago/pago por compatibilidade
    # com a UI atual, enquanto esta tabela cria histórico e caminho para pagamentos
    # parciais/múltiplos no futuro.
    execute_query('''
        CREATE TABLE IF NOT EXISTS pagamentos (
            id BIGSERIAL PRIMARY KEY,
            lancamento_id INTEGER NOT NULL REFERENCES lancamentos(id) ON DELETE CASCADE,
            valor NUMERIC NOT NULL,
            data_pagamento DATE NOT NULL,
            origem TEXT NOT NULL DEFAULT 'sincronizado',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (lancamento_id, origem)
        );
    ''')

    # Trigger centraliza a consistência de qualquer caminho de baixa/estorno,
    # inclusive botões consolidados que ainda fazem UPDATE direto em lancamentos.
    execute_query('''
        CREATE OR REPLACE FUNCTION fn_normalizar_pagamento_lancamento()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.pago = 1 THEN
                IF COALESCE(NEW.valor_pago, 0) = 0 THEN
                    NEW.valor_pago := NEW.valor;
                END IF;
                IF NEW.data_pagamento IS NULL THEN
                    NEW.data_pagamento := CURRENT_DATE;
                END IF;
            ELSE
                NEW.valor_pago := 0;
                NEW.data_pagamento := NULL;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ''')
    execute_query("DROP TRIGGER IF EXISTS trg_normalizar_pagamento_lancamento ON lancamentos;")
    execute_query('''
        CREATE TRIGGER trg_normalizar_pagamento_lancamento
        BEFORE INSERT OR UPDATE OF pago, valor_pago, data_pagamento
        ON lancamentos
        FOR EACH ROW EXECUTE FUNCTION fn_normalizar_pagamento_lancamento();
    ''')

    execute_query('''
        CREATE OR REPLACE FUNCTION fn_sincronizar_pagamento_lancamento()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.pago = 1 AND COALESCE(NEW.valor_pago, 0) > 0 THEN
                INSERT INTO pagamentos (lancamento_id, valor, data_pagamento, origem)
                VALUES (NEW.id, NEW.valor_pago, COALESCE(NEW.data_pagamento, CURRENT_DATE), 'sincronizado')
                ON CONFLICT (lancamento_id, origem)
                DO UPDATE SET valor = EXCLUDED.valor, data_pagamento = EXCLUDED.data_pagamento;
            ELSE
                DELETE FROM pagamentos WHERE lancamento_id = NEW.id AND origem = 'sincronizado';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ''')
    execute_query("DROP TRIGGER IF EXISTS trg_sincronizar_pagamento_lancamento ON lancamentos;")
    execute_query('''
        CREATE TRIGGER trg_sincronizar_pagamento_lancamento
        AFTER INSERT OR UPDATE OF pago, valor_pago, data_pagamento
        ON lancamentos
        FOR EACH ROW EXECUTE FUNCTION fn_sincronizar_pagamento_lancamento();
    ''')
    execute_query('''
        INSERT INTO pagamentos (lancamento_id, valor, data_pagamento, origem)
        SELECT id, valor_pago, COALESCE(data_pagamento, data_vencimento), 'sincronizado'
        FROM lancamentos
        WHERE pago = 1 AND COALESCE(valor_pago, 0) > 0
        ON CONFLICT (lancamento_id, origem)
        DO UPDATE SET valor = EXCLUDED.valor, data_pagamento = EXCLUDED.data_pagamento;
    ''')

    # Idempotência forte de recorrências: a sessão continua sendo otimização, mas
    # o banco passa a decidir atomicamente se aquela competência já foi gerada.
    execute_query('''
        CREATE TABLE IF NOT EXISTS recorrencias_geradas (
            categoria_id INTEGER NOT NULL REFERENCES categorias_personalizadas(id) ON DELETE CASCADE,
            competencia DATE NOT NULL,
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (categoria_id, competencia)
        );
    ''')
    execute_query('''
        INSERT INTO recorrencias_geradas (categoria_id, competencia)
        SELECT CAST(SUBSTRING(l.compra_id FROM 5) AS INTEGER), DATE_TRUNC('month', l.data_vencimento)::date
        FROM lancamentos l
        JOIN categorias_personalizadas c
          ON l.compra_id = ('rec_' || c.id::text)
        WHERE l.compra_id ~ '^rec_[0-9]+$'
        ON CONFLICT DO NOTHING;
    ''')

    # Identifica linhas de orçamento antigas e guarda um snapshot do teto. O valor
    # físico legado pode já ter sido abatido; a VIEW abaixo ignora essa mutação.
    execute_query('''
        UPDATE lancamentos l
        SET eh_orcamento = 1,
            valor_orcamento = COALESCE(l.valor_orcamento, c.valor_padrao, l.valor),
            valor = COALESCE(l.valor_orcamento, c.valor_padrao, l.valor),
            pago = 0, valor_pago = 0, data_pagamento = NULL
        FROM categorias_personalizadas c
        WHERE c.is_envelope = 1
          AND l.compra_id = ('rec_' || c.id::text);
    ''')

    # VIEW financeira: saldo de envelope = orçamento snapshot - realizado.
    # A subconsulta só é avaliada para linhas de orçamento, evitando agregar a
    # tabela inteira a cada leitura mensal.
    execute_query('''
        CREATE OR REPLACE VIEW vw_lancamentos_financeiros AS
        SELECT
            l.id, l.tipo, l.categoria, l.subgrupo, l.descricao,
            CASE WHEN COALESCE(l.eh_orcamento,0) = 1
                 THEN COALESCE(l.valor_orcamento, l.valor, 0) - COALESCE((
                     SELECT SUM(COALESCE(x.valor_pago,0))
                     FROM lancamentos x
                     WHERE x.tipo = 'Despesa'
                       AND x.pago = 1
                       AND COALESCE(x.eh_orcamento,0) = 0
                       AND x.categoria = l.categoria
                       AND COALESCE(x.subgrupo,'') = COALESCE(l.subgrupo,'')
                       AND x.data_vencimento >= DATE_TRUNC('month', l.data_vencimento)::date
                       AND x.data_vencimento < (DATE_TRUNC('month', l.data_vencimento) + INTERVAL '1 month')::date
                 ), 0)
                 ELSE l.valor END AS valor,
            l.data_vencimento, l.parcela_atual, l.total_parcelas, l.pago,
            l.compra_id, l.forma_pagamento, l.prioridade, l.valor_pago,
            l.eh_estimativa, l.data_competencia, l.data_pagamento,
            l.eh_orcamento, l.valor_orcamento
        FROM lancamentos l;
    ''')

    # Constraints aplicadas a dados novos sem bloquear bases legadas que eventualmente
    # contenham alguma inconsistência histórica. Podem ser VALIDATE posteriormente.
    constraints = {
        'ck_lanc_tipo': "tipo IN ('Entrada','Despesa')",
        'ck_lanc_pago': "pago IN (0,1)",
        'ck_lanc_valor': "valor >= 0",
        'ck_lanc_valor_pago': "valor_pago >= 0",
        'ck_lanc_parcela_atual': "parcela_atual IS NULL OR parcela_atual >= 1",
        'ck_lanc_total_parcelas': "total_parcelas IS NULL OR total_parcelas >= 1",
        'ck_lanc_parcelas_ordem': "parcela_atual IS NULL OR total_parcelas IS NULL OR total_parcelas = 999 OR parcela_atual <= total_parcelas",
    }
    for nome, regra in constraints.items():
        execute_query(f'''DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{nome}') THEN
                ALTER TABLE lancamentos ADD CONSTRAINT {nome} CHECK ({regra}) NOT VALID;
            END IF;
        END $$;''')

    execute_query('''DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_cat_tipo') THEN
            ALTER TABLE categorias_personalizadas
            ADD CONSTRAINT ck_cat_tipo CHECK (tipo IN ('Entrada','Despesa')) NOT VALID;
        END IF;
    END $$;''')

    # Índices para as consultas de período, status e agrupamento por compra.
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_vencimento ON lancamentos(data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_pagamento ON lancamentos(data_pagamento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_competencia ON lancamentos(data_competencia);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_compra_id ON lancamentos(compra_id);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_pago_data ON lancamentos(pago, data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_tipo_data ON lancamentos(tipo, data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_envelope_realizado ON lancamentos(categoria, subgrupo, data_vencimento, pago);",
        "CREATE INDEX IF NOT EXISTS idx_cat_tipo_categoria_subgrupo ON categorias_personalizadas(tipo, categoria, subgrupo);",
    ]:
        execute_query(ddl)

    # Unicidade lógica de categoria é criada somente se a base atual não possui
    # duplicatas, evitando quebrar uma instalação existente durante a migração.
    execute_query('''DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM categorias_personalizadas
            GROUP BY tipo, categoria, COALESCE(subgrupo,'') HAVING COUNT(*) > 1
        ) AND NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE indexname = 'uq_cat_logica'
        ) THEN
            CREATE UNIQUE INDEX uq_cat_logica
            ON categorias_personalizadas(tipo, categoria, (COALESCE(subgrupo,'')));
        END IF;
    END $$;''')


# =================================================================
# 2. MOTOR DE GERAÇÃO LAZY / RECORRÊNCIAS CONTRATUAIS
# =================================================================

def processar_recorrencias_lazy(mes, ano):
    '''
    A session_state evita trabalho repetido na UI; a tabela recorrencias_geradas
    fornece a idempotência real e segura contra duas sessões concorrentes.
    '''
    guarda = f"rec_processado_{mes}_{ano}"
    if st.session_state.get(guarda):
        return

    df_contratos = fetch_dataframe("SELECT * FROM categorias_personalizadas WHERE is_recorrente = 1")
    if df_contratos.empty:
        st.session_state[guarda] = True
        return

    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]
    competencia = datetime.date(ano, mes, 1)

    try:
        with transaction() as cur:
            for _, contrato in df_contratos.iterrows():
                dt_inicio = pd.to_datetime(contrato['data_inicio']).date() if pd.notna(contrato['data_inicio']) else competencia
                eh_envelope = int(contrato.get('is_envelope') or 0) == 1
                dia_alvo = ultimo_dia_mes if eh_envelope else min(int(contrato['dia_pagamento'] or 1), ultimo_dia_mes)
                dt_limite_alvo = datetime.date(ano, mes, dia_alvo)
                if dt_limite_alvo < dt_inicio:
                    continue

                cur.execute(
                    "INSERT INTO recorrencias_geradas (categoria_id, competencia) VALUES (%s,%s) "
                    "ON CONFLICT DO NOTHING RETURNING categoria_id",
                    (int(contrato['id']), competencia),
                )
                if cur.fetchone() is None:
                    continue

                compra_id_contrato = f"rec_{int(contrato['id'])}"
                val_p = float(contrato['valor_padrao'] or 0.0)
                sufixo = "(Envelope do Mês)" if eh_envelope else "(Recorrente)"
                desc_c = f"{contrato['categoria']} - {contrato['subgrupo'] or ''} {sufixo}"
                cur.execute('''
                    INSERT INTO lancamentos
                    (tipo, categoria, subgrupo, descricao, valor, data_vencimento,
                     parcela_atual, total_parcelas, pago, compra_id, forma_pagamento,
                     prioridade, valor_pago, data_competencia, eh_orcamento, valor_orcamento)
                    VALUES (%s,%s,%s,%s,%s,%s,1,1,0,%s,'Outros','Média 🟡',0,%s,%s,%s)
                ''', (
                    contrato['tipo'], contrato['categoria'], contrato['subgrupo'], desc_c,
                    val_p, dt_limite_alvo, compra_id_contrato, competencia,
                    1 if eh_envelope else 0, val_p if eh_envelope else None,
                ))
    except Exception as e:
        st.error(f"Erro ao gerar recorrências: {e}")
        return

    st.session_state[guarda] = True


# =================================================================
# 3. ENVELOPES DERIVADOS (SEM MUTAÇÃO DO ORÇAMENTO)
# =================================================================

def executar_abatimento_envelope(categoria, subgrupo, valor_gasto, mes, ano):
    '''Compatibilidade com chamadas antigas: o saldo agora é derivado pela VIEW.'''
    return None

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

# -----------------------------------------------------------------
# FEATURE 6 -- MESES DE SOBREVIVÊNCIA
# -----------------------------------------------------------------

def obter_reserva_emergencia():
    df = fetch_dataframe("SELECT valor, atualizado_em FROM reserva_emergencia WHERE id = 1")
    if df.empty: return 0.0, None
    valor = float(df.iloc[0]['valor']) if pd.notna(df.iloc[0]['valor']) else 0.0
    return valor, df.iloc[0]['atualizado_em']

def atualizar_reserva_emergencia(novo_valor):
    execute_query("UPDATE reserva_emergencia SET valor = %s, atualizado_em = CURRENT_DATE WHERE id = 1", (novo_valor,))

def calcular_media_despesa_mensal(hoje_ref, n_meses=3):
    """
    Média das despesas PAGAS dos últimos N meses FECHADOS (não conta o mês
    corrente, que ainda está em andamento e sub-representaria o gasto real).
    'Ajuste' fica de fora -- é lançamento de apoio interno, não gasto real.
    Retorna (média, quantidade de meses com dados encontrados).
    """
    primeiro_mes, primeiro_ano = hoje_ref.month - n_meses, hoje_ref.year
    while primeiro_mes <= 0:
        primeiro_mes += 12
        primeiro_ano -= 1
    data_inicio_janela = datetime.date(primeiro_ano, primeiro_mes, 1)
    data_fim_janela = datetime.date(hoje_ref.year, hoje_ref.month, 1) - datetime.timedelta(days=1)
    if data_fim_janela < data_inicio_janela:
        return 0.0, 0

    df = fetch_dataframe(
        "SELECT EXTRACT(MONTH FROM COALESCE(data_pagamento,data_vencimento)) as mes, EXTRACT(YEAR FROM COALESCE(data_pagamento,data_vencimento)) as ano, SUM(valor_pago) as total "
        "FROM lancamentos WHERE tipo = 'Despesa' AND pago = 1 AND categoria != 'Ajuste' "
        "AND COALESCE(data_pagamento,data_vencimento) BETWEEN %s AND %s GROUP BY ano, mes",
        (data_inicio_janela, data_fim_janela)
    )
    if df.empty: return 0.0, 0
    return float(df['total'].astype(float).mean()), len(df)

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
init_db()

# =================================================================
# 5B. IDENTIDADE VISUAL
# =================================================================
def aplicar_estilo_visual():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* =============================================================
       PALETA -- extraída do layout feito no Claude Design.
       Fundo azul-petróleo bem escuro, acento ciano (não mais teal),
       alerta laranja, sucesso verde -- tudo em OKLCH, igual ao mockup.
       ============================================================= */
    :root, .stApp {
        --bg-page: oklch(15% 0.008 250);
        --bg-sidebar: oklch(13% 0.008 250);
        --bg-card: oklch(19% 0.01 250);
        --border: oklch(30% 0.01 250 / 0.55);
        --border-strong: oklch(30% 0.01 250 / 0.7);
        --text-primary: oklch(93% 0.004 250);
        --text-heading: oklch(96% 0.003 250);
        --text-muted: oklch(60% 0.01 250);
        --text-faint: oklch(45% 0.01 250);
        --accent: oklch(72% 0.1 210);
        --accent-strong: oklch(80% 0.09 210);
        --accent-tint: oklch(72% 0.1 210 / 0.14);
        --danger: oklch(68% 0.13 25);
        --danger-text: oklch(74% 0.11 25);
        --danger-tint: oklch(68% 0.13 25 / 0.08);
        --danger-border: oklch(68% 0.13 25 / 0.3);
        --success: oklch(72% 0.11 155);
        --success-tint: oklch(72% 0.11 155 / 0.14);
        --success-border: oklch(72% 0.11 155 / 0.4);

        --background-color: var(--bg-page) !important;
        --secondary-background-color: var(--bg-card) !important;
        --text-color: var(--text-primary) !important;
        --primary-color: var(--accent) !important;
    }
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"],
    [data-testid="stHeader"],
    .main {
        background-color: var(--bg-page) !important;
    }
    [data-testid="stHeader"] {
        background-color: rgba(0,0,0,0) !important;
    }
    .stApp {
        color: var(--text-primary);
    }

    html, body, [class*="css"] {
        font-family: 'Inter', system-ui, sans-serif;
    }
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Inter', system-ui, sans-serif !important;
        font-weight: 600 !important;
        color: var(--text-heading) !important;
        letter-spacing: -0.015em;
    }
    /* Números tabulares (mesma largura por dígito) em vez de fonte mono --
       é a mesma técnica usada no mockup (.num{font-variant-numeric:tabular-nums}) */
    div[data-testid="stMetricValue"], .num-tabular {
        font-variant-numeric: tabular-nums;
    }

    div[data-testid="stMetric"], div[data-testid="metric-container"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 1.1rem 1.2rem;
        box-shadow: none;
    }
    div[data-testid="stMetricValue"] {
        font-family: 'Inter', system-ui, sans-serif !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.01em;
    }
    div[data-testid="stMetricLabel"] {
        font-weight: 500 !important;
        color: var(--text-muted) !important;
        font-size: 0.8rem !important;
        text-transform: none;
        letter-spacing: 0;
    }
    div[data-testid="stMetricLabel"] p {
        color: var(--text-muted) !important;
    }

    .stApp label, .stApp .stMarkdown, .stApp .stMarkdown p,
    .stApp [data-testid="stWidgetLabel"] p,
    .stApp [data-testid="stWidgetLabel"] {
        color: var(--text-primary) !important;
    }
    .stApp [data-testid="stCaptionContainer"] {
        color: var(--text-muted) !important;
    }

    section[data-testid="stSidebar"] {
        background: var(--bg-sidebar) !important;
        border-right: 1px solid var(--border);
    }
    section[data-testid="stSidebar"] * {
        color: var(--text-primary) !important;
    }

    /* Botões secundários (a maioria) -- estilo discreto com borda fina,
       igual ao mockup (nada de preenchimento sólido chamativo). */
    .stButton button,
    .stButton button[kind="secondary"],
    .stButton button:not([kind="primary"]) {
        background-color: var(--bg-card) !important;
        border: 1px solid var(--border-strong) !important;
        color: var(--text-primary) !important;
        border-radius: 9px !important;
        font-weight: 500 !important;
    }
    .stButton button *,
    .stButton button[kind="secondary"] *,
    .stButton button:not([kind="primary"]) * {
        color: var(--text-primary) !important;
    }
    .stButton button:hover,
    .stButton button:not([kind="primary"]):hover {
        background-color: oklch(22% 0.012 250) !important;
        border-color: var(--accent) !important;
        color: var(--text-primary) !important;
    }

    /* Botões primários (ação principal / item de menu ativo) -- usa o
       acento do mockup só que em preenchimento sólido, pra manter clareza
       de qual é a ação/página principal (o mockup usa esse acento como
       destaque translúcido; adaptei pra preenchimento sólido porque
       Streamlit precisa de contraste forte pra sinalizar 'isso é o principal'). */
    .stButton button[kind="primary"] {
        background-color: var(--accent) !important;
        border: 1px solid var(--accent) !important;
        color: oklch(15% 0.008 250) !important;
        border-radius: 9px !important;
        font-weight: 500 !important;
    }
    .stButton button[kind="primary"] * {
        color: oklch(15% 0.008 250) !important;
    }
    .stButton button[kind="primary"]:hover {
        background-color: var(--accent-strong) !important;
        border-color: var(--accent-strong) !important;
        color: oklch(15% 0.008 250) !important;
    }
    .stButton button[kind="primary"]:hover * {
        color: oklch(15% 0.008 250) !important;
    }

    section[data-testid="stSidebar"] .stButton button {
        width: 100%;
        text-align: left;
        justify-content: flex-start;
        border-radius: 8px !important;
        font-weight: 500;
        padding: 0.5rem 0.8rem;
    }
    /* Item de navegação ativo na sidebar: tinta translúcida do acento,
       igual ao navItem() do mockup -- não preenchimento sólido, porque
       aqui o botão é só rótulo de página, não uma ação a confirmar. */
    section[data-testid="stSidebar"] .stButton button[kind="primary"] {
        background-color: var(--accent-tint) !important;
        border: 1px solid transparent !important;
        color: var(--text-heading) !important;
    }
    section[data-testid="stSidebar"] .stButton button[kind="primary"] * {
        color: var(--text-heading) !important;
    }

    .nav-eyebrow {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--text-faint) !important;
        margin: 1.1rem 0 0.4rem 0.3rem;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 500;
        color: var(--text-muted) !important;
    }
    .stTabs [data-baseweb="tab"] p {
        color: var(--text-muted) !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent-strong) !important;
    }
    .stTabs [aria-selected="true"] p {
        color: var(--accent-strong) !important;
    }

    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
    }

    div[data-testid="stExpander"], div[data-testid="stForm"] {
        border: 1px solid var(--border) !important;
        border-radius: 14px !important;
        background: var(--bg-card) !important;
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
    # Plotly não entende oklch() nativamente -- estes hex são a conversão
    # matemática exata (OKLab -> sRGB) das mesmas cores usadas no CSS,
    # não uma aproximação visual.
    fig.update_layout(
        paper_bgcolor="#101418",
        plot_bgcolor="#101418",
        font=dict(family="Inter, sans-serif", color="#E6E8EA"),
        legend=dict(font=dict(color="#E6E8EA")),
        xaxis=dict(gridcolor="#2A2E33", linecolor="#2A2E33", color="#7C8186"),
        yaxis=dict(gridcolor="#2A2E33", linecolor="#2A2E33", color="#7C8186"),
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
    "<div style='font-weight:700; font-size:1.05rem; color:oklch(96% 0.003 250); margin-bottom:0.2rem;'>💰 Gestão Financeira</div>"
    "<div style='font-size:0.8rem; color:oklch(60% 0.01 250); margin-bottom:0.4rem;'>Painel de controle financeiro</div>",
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

# Botão manual do Assistente de Configuração -- some com o que já foi
# preenchido em sessões anteriores (fica só na sessão, não persiste), só
# reseta o PASSO pra 1 quando acionado manualmente.
if st.sidebar.button("🧙 Assistente de Configuração", key="btn_abrir_wizard", use_container_width=True):
    st.session_state['wizard_ativo'] = True
    st.session_state['wizard_passo'] = 1
    st.rerun()
st.sidebar.divider()

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
# CORREÇÃO: antes, o "index=" era passado toda vez que a página recarregava --
# em alguns casos (como clicar num botão de navegação, que força um rerun) isso
# reafirmava o mês/ano padrão por cima da sua escolha, mesmo a caixa de seleção
# mostrando visualmente o valor certo. Agora o valor padrão só é definido UMA
# vez (se a chave ainda não existir); depois disso, o Streamlit nunca mais
# tenta sobrescrever, só confia no que está guardado.
if "sb_mes" not in st.session_state: st.session_state["sb_mes"] = hoje.month
if "sb_ano" not in st.session_state: st.session_state["sb_ano"] = hoje.year
col_sb1, col_sb2 = st.sidebar.columns(2)
with col_sb1: mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], key="sb_mes")
with col_sb2: ano_selecionado = st.selectbox("Ano", range(hoje.year-2, hoje.year+5), key="sb_ano")

st.sidebar.divider()
st.sidebar.markdown("<div class='nav-eyebrow'>Backup</div>", unsafe_allow_html=True)

def _df_raw(tabela):
    return fetch_dataframe(f"/* RAW */ SELECT * FROM {tabela}")


def exportar_backup_completo():
    """Exporta todo o estado funcional do app em um único ZIP versionado."""
    tabelas = {
        'lancamentos.csv': _df_raw('lancamentos'),
        'categorias_personalizadas.csv': fetch_dataframe('SELECT * FROM categorias_personalizadas'),
        'info_dividas.csv': fetch_dataframe('SELECT * FROM info_dividas'),
        'reserva_emergencia.csv': fetch_dataframe('SELECT * FROM reserva_emergencia'),
        'pagamentos.csv': fetch_dataframe('SELECT * FROM pagamentos'),
        'recorrencias_geradas.csv': fetch_dataframe('SELECT * FROM recorrencias_geradas'),
    }
    metadata = {
        'schema_version': 2,
        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'format': 'gestao_financeira_full_backup',
        'tables': list(tabelas.keys()),
    }
    buff = io.BytesIO()
    with zipfile.ZipFile(buff, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('metadata.json', json.dumps(metadata, ensure_ascii=False, indent=2))
        for nome, df_bkp in tabelas.items():
            zf.writestr(nome, df_bkp.to_csv(index=False))
    return buff.getvalue()


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
            lambda r: r['data_vencimento'] if int(r['pago']) == 1 else None, axis=1
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
        'id','tipo','categoria','subgrupo','descricao','valor','data_vencimento',
        'parcela_atual','total_parcelas','pago','compra_id','forma_pagamento',
        'prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento',
        'eh_orcamento','valor_orcamento'
    ]
    with transaction() as cur:
        cur.execute("TRUNCATE TABLE pagamentos, recorrencias_geradas, lancamentos RESTART IDENTITY CASCADE")
        _insert_dataframe(cur, 'lancamentos', df_v, cols)
        cur.execute("""INSERT INTO recorrencias_geradas (categoria_id, competencia)
            SELECT CAST(SUBSTRING(l.compra_id FROM 5) AS INTEGER), DATE_TRUNC('month', l.data_vencimento)::date
            FROM lancamentos l JOIN categorias_personalizadas c ON l.compra_id=('rec_' || c.id::text)
            WHERE l.compra_id ~ '^rec_[0-9]+$' ON CONFLICT DO NOTHING""")
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

        arquivo.seek(0)
        with zipfile.ZipFile(arquivo) as zf:
            nomes = set(zf.namelist())
            obrigatorios = {'lancamentos.csv', 'categorias_personalizadas.csv', 'info_dividas.csv', 'reserva_emergencia.csv'}
            faltantes = obrigatorios - nomes
            if faltantes:
                raise ValueError(f"Backup ZIP incompleto. Faltam: {', '.join(sorted(faltantes))}")

            dfs = {}
            for nome_csv in obrigatorios | {'pagamentos.csv', 'recorrencias_geradas.csv'}:
                if nome_csv in nomes:
                    with zf.open(nome_csv) as f:
                        dfs[nome_csv] = pd.read_csv(f)
                else:
                    dfs[nome_csv] = pd.DataFrame()

        problemas, df_lanc = validar_csv_lancamentos(dfs['lancamentos.csv'])
        if problemas:
            raise ValueError('; '.join(problemas[:10]))

        cols_cat = ['id','tipo','categoria','subgrupo','valor_padrao','atraso_meses','dia_pagamento','is_recorrente','data_inicio','is_envelope','is_producao_variavel']
        cols_lanc = ['id','tipo','categoria','subgrupo','descricao','valor','data_vencimento','parcela_atual','total_parcelas','pago','compra_id','forma_pagamento','prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento','eh_orcamento','valor_orcamento']
        cols_info = ['compra_id','credor','taxa_juros_mensal']
        cols_reserva = ['id','valor','atualizado_em']
        cols_pag = ['lancamento_id','valor','data_pagamento','origem','criado_em']
        cols_rec = ['categoria_id','competencia','criado_em']

        with transaction() as cur:
            cur.execute("TRUNCATE TABLE pagamentos, recorrencias_geradas, info_dividas, reserva_emergencia, lancamentos, categorias_personalizadas RESTART IDENTITY CASCADE")
            _insert_dataframe(cur, 'categorias_personalizadas', dfs['categorias_personalizadas.csv'], cols_cat)
            _insert_dataframe(cur, 'lancamentos', df_lanc, cols_lanc)
            _insert_dataframe(cur, 'info_dividas', dfs['info_dividas.csv'], cols_info, 'ON CONFLICT (compra_id) DO UPDATE SET credor=EXCLUDED.credor, taxa_juros_mensal=EXCLUDED.taxa_juros_mensal')
            _insert_dataframe(cur, 'reserva_emergencia', dfs['reserva_emergencia.csv'], cols_reserva, 'ON CONFLICT (id) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=EXCLUDED.atualizado_em')
            _insert_dataframe(cur, 'pagamentos', dfs['pagamentos.csv'], cols_pag, 'ON CONFLICT (lancamento_id, origem) DO UPDATE SET valor=EXCLUDED.valor, data_pagamento=EXCLUDED.data_pagamento')
            _insert_dataframe(cur, 'recorrencias_geradas', dfs['recorrencias_geradas.csv'], cols_rec, 'ON CONFLICT (categoria_id, competencia) DO NOTHING')
            cur.execute("INSERT INTO reserva_emergencia (id,valor,atualizado_em) VALUES (1,0,CURRENT_DATE) ON CONFLICT DO NOTHING")
            cur.execute("SELECT setval(pg_get_serial_sequence('categorias_personalizadas','id'), COALESCE((SELECT MAX(id) FROM categorias_personalizadas),1), (SELECT COUNT(*)>0 FROM categorias_personalizadas))")
            cur.execute("SELECT setval(pg_get_serial_sequence('lancamentos','id'), COALESCE((SELECT MAX(id) FROM lancamentos),1), (SELECT COUNT(*)>0 FROM lancamentos))")
            cur.execute("SELECT setval(pg_get_serial_sequence('pagamentos','id'), COALESCE((SELECT MAX(id) FROM pagamentos),1), (SELECT COUNT(*)>0 FROM pagamentos))")
        return True, "Backup completo restaurado de forma atômica."
    except Exception as e:
        return False, str(e)


# Backup completo sob demanda; o banco inteiro não é lido a cada rerun.
if st.sidebar.button("📦 Preparar backup completo (ZIP)", key="btn_prep_backup"):
    try:
        st.session_state['_backup_blob'] = exportar_backup_completo()
        st.session_state['_backup_nome'] = f"backup_completo_{hoje.strftime('%d_%m_%Y')}.zip"
    except Exception as e:
        st.sidebar.error(f"Falha ao preparar backup: {e}")

if st.session_state.get('_backup_blob') is not None:
    st.sidebar.download_button(
        "⬇️ Baixar backup completo",
        data=st.session_state['_backup_blob'],
        file_name=st.session_state.get('_backup_nome', 'backup_completo.zip'),
        mime="application/zip",
    )

a_up = st.sidebar.file_uploader("Restaurar backup (ZIP ou CSV antigo)", type=["zip", "csv"])
if a_up and st.sidebar.button("🚀 Confirmar Restauração"):
    try:
        st.session_state['_pre_restore_blob'] = exportar_backup_completo()
        st.session_state['_pre_restore_nome'] = f"antes_da_restauracao_{hoje.strftime('%d_%m_%Y_%H%M%S')}.zip"
    except Exception as e:
        st.sidebar.error(f"Não foi possível criar o backup de segurança pré-restauração: {e}")
    else:
        ok_restore, msg_restore = importar_backup(a_up)
        if ok_restore:
            invalidar_caches_estruturais()
            flash("success", f"📥 {msg_restore}")
            st.rerun()
        else:
            st.sidebar.error(f"Restauração cancelada; banco atual preservado. Motivo: {msg_restore}")

if st.session_state.get('_pre_restore_blob') is not None:
    st.sidebar.download_button(
        "🛟 Baixar estado anterior à última restauração",
        data=st.session_state['_pre_restore_blob'],
        file_name=st.session_state.get('_pre_restore_nome', 'antes_da_restauracao.zip'),
        mime="application/zip",
        key="download_pre_restore",
    )

processar_recorrencias_lazy(mes_selecionado, ano_selecionado)
dia_maximo_alvo = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
data_contexto_ativo = datetime.date(ano_selecionado, mes_selecionado, min(hoje.day, dia_maximo_alvo))
inicio_periodo, fim_periodo = limites_mes(mes_selecionado, ano_selecionado)

exibir_flash()

# =================================================================
# 7B. ASSISTENTE DE CONFIGURAÇÃO (FEATURE 14 -- ONBOARDING GUIADO)
# =================================================================
# Detecção automática de "primeira vez": se não existe NENHUMA categoria
# cadastrada ainda, liga o assistente sozinho -- sem isso, a primeira tela
# que a pessoa veria seria "⚙️ Gerenciar Categorias" com CRUD cru, exigindo
# entender categoria/subgrupo/envelope/recorrência antes de conseguir usar
# qualquer parte do app. Só roda essa checagem 1x por sessão (session_state).
if 'wizard_ativo' not in st.session_state:
    df_check_categorias = fetch_dataframe("SELECT COUNT(*) as n FROM categorias_personalizadas")
    n_categorias_existentes = int(df_check_categorias.iloc[0]['n']) if not df_check_categorias.empty else 0
    st.session_state['wizard_ativo'] = (n_categorias_existentes == 0)
    st.session_state['wizard_passo'] = 1

for _chave in ['wizard_hospitais', 'wizard_fixas', 'wizard_envelopes', 'wizard_dividas']:
    if _chave not in st.session_state:
        st.session_state[_chave] = []

MAPA_ATRASO_AMIGAVEL = {"Paga no mesmo mês": 0, "Paga 1 mês depois": 1, "Paga 2 meses depois": 2, "Paga 3 meses depois": 3}

def _wizard_cabecalho(passo_atual, total_passos, titulo):
    st.header("🧙 Assistente de Configuração")
    st.progress(passo_atual / total_passos)
    st.caption(f"Passo {passo_atual} de {total_passos}")
    if st.button("✖️ Pular e ir direto pro app", key="wizard_sair"):
        st.session_state['wizard_ativo'] = False
        st.rerun()
    st.divider()
    st.subheader(titulo)

def _wizard_lista_com_remover(lista, chave_sessao, formatar_linha):
    if not lista:
        st.caption("Nada adicionado ainda.")
        return
    for i, item in enumerate(lista):
        c_txt, c_del = st.columns([5, 1])
        c_txt.write(formatar_linha(item))
        if c_del.button("🗑️", key=f"{chave_sessao}_del_{i}"):
            lista.pop(i)
            st.rerun()

def _wizard_navegacao(passo_atual, pode_avancar=True, texto_avancar="Próximo ➡️"):
    st.divider()
    c_voltar, c_avancar = st.columns(2)
    if passo_atual > 1:
        if c_voltar.button("⬅️ Voltar", key=f"wizard_voltar_{passo_atual}", use_container_width=True):
            st.session_state['wizard_passo'] = passo_atual - 1
            st.rerun()
    if c_avancar.button(texto_avancar, type="primary", key=f"wizard_avancar_{passo_atual}", use_container_width=True, disabled=not pode_avancar):
        st.session_state['wizard_passo'] = passo_atual + 1
        st.rerun()

def _wizard_passo1_hospitais():
    _wizard_cabecalho(1, 5, "🏥 Em quais hospitais/locais você faz plantão?")
    st.caption("Pra cada local, o app já sabe automaticamente quando o pagamento cai, sem você ter que lembrar toda vez.")

    with st.form("wizard_form_hospital", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1.3, 1])
        nome = c1.text_input("Nome do hospital/local")
        atraso_label = c2.selectbox("Quando paga?", list(MAPA_ATRASO_AMIGAVEL.keys()), index=1)
        dia_pgto = c3.number_input("Dia do pagamento", min_value=1, max_value=31, value=10)
        if st.form_submit_button("➕ Adicionar Local"):
            if nome.strip():
                st.session_state['wizard_hospitais'].append({
                    "nome": nome.strip(), "atraso_meses": MAPA_ATRASO_AMIGAVEL[atraso_label],
                    "dia_pagamento": int(dia_pgto), "atraso_label": atraso_label
                })
                st.rerun()

    st.markdown("**Locais adicionados:**")
    _wizard_lista_com_remover(
        st.session_state['wizard_hospitais'], 'wizard_hospitais',
        lambda h: f"🏥 {h['nome']} -- {h['atraso_label']}, todo dia {h['dia_pagamento']}"
    )
    _wizard_navegacao(1)

def _wizard_passo2_fixas():
    _wizard_cabecalho(2, 5, "🏠 Quais são suas despesas fixas todo mês?")
    st.caption("Aluguel, internet, plano de saúde... o app lança isso sozinho todo mês, sem você precisar lembrar.")

    with st.form("wizard_form_fixa", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1.3, 1])
        nome = c1.text_input("Nome da despesa", placeholder="Ex: Aluguel")
        valor_txt = c2.text_input("Valor (R$)", value="0,00")
        dia_venc = c3.number_input("Dia do vencimento", min_value=1, max_value=31, value=5)
        if st.form_submit_button("➕ Adicionar Despesa Fixa"):
            valor_f = parse_valor(valor_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_fixas'].append({"nome": nome.strip(), "valor": valor_f, "dia_vencimento": int(dia_venc)})
                st.rerun()

    st.markdown("**Despesas fixas adicionadas:**")
    _wizard_lista_com_remover(
        st.session_state['wizard_fixas'], 'wizard_fixas',
        lambda f: f"🏠 {f['nome']} -- R$ {format_brl(f['valor'])}, todo dia {f['dia_vencimento']}"
    )
    _wizard_navegacao(2)

def _wizard_passo3_envelopes():
    _wizard_cabecalho(3, 5, "🛒 Quais gastos variáveis você quer controlar com teto mensal?")
    st.caption("Mercado, farmácia, transporte, lazer... você define um limite mensal, e o app avisa quando estourar.")

    with st.form("wizard_form_envelope", clear_on_submit=True):
        c1, c2 = st.columns([2, 1.3])
        nome = c1.text_input("Nome do gasto", placeholder="Ex: Mercado")
        valor_txt = c2.text_input("Teto mensal (R$)", value="0,00")
        if st.form_submit_button("➕ Adicionar Teto"):
            valor_f = parse_valor(valor_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_envelopes'].append({"nome": nome.strip(), "valor": valor_f})
                st.rerun()

    st.markdown("**Tetos adicionados:**")
    _wizard_lista_com_remover(
        st.session_state['wizard_envelopes'], 'wizard_envelopes',
        lambda e: f"🛒 {e['nome']} -- até R$ {format_brl(e['valor'])}/mês"
    )
    _wizard_navegacao(3)

def _wizard_passo4_dividas():
    _wizard_cabecalho(4, 5, "💳 Você tem alguma dívida parcelada em andamento?")
    st.caption("Só o que FALTA pagar -- não precisa saber quantas parcelas já pagou antes de usar o app.")

    with st.form("wizard_form_divida", clear_on_submit=True):
        c1, c2 = st.columns([2, 1.3])
        nome = c1.text_input("Nome da dívida", placeholder="Ex: Notebook, Cartão do carro")
        valor_parcela_txt = c2.text_input("Valor da parcela (R$)", value="0,00")
        c3, c4, c5 = st.columns([1, 1, 1.4])
        parcelas_faltam = c3.number_input("Quantas parcelas faltam?", min_value=1, max_value=120, value=1)
        dia_venc = c4.number_input("Dia do vencimento", min_value=1, max_value=31, value=10)
        eh_cartao = c5.checkbox("É no cartão de crédito?")
        if st.form_submit_button("➕ Adicionar Dívida"):
            valor_f = parse_valor(valor_parcela_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_dividas'].append({
                    "nome": nome.strip(), "valor_parcela": valor_f, "parcelas_faltam": int(parcelas_faltam),
                    "dia_vencimento": int(dia_venc), "eh_cartao": eh_cartao
                })
                st.rerun()

    st.markdown("**Dívidas adicionadas:**")
    _wizard_lista_com_remover(
        st.session_state['wizard_dividas'], 'wizard_dividas',
        lambda d: f"💳 {d['nome']} -- {d['parcelas_faltam']}x de R$ {format_brl(d['valor_parcela'])}" + (" (cartão)" if d['eh_cartao'] else "")
    )
    _wizard_navegacao(4, texto_avancar="Ver Resumo ➡️")

def _wizard_passo5_revisao():
    _wizard_cabecalho(5, 5, "📋 Revisão -- confere se está tudo certo")

    hospitais = st.session_state['wizard_hospitais']
    fixas = st.session_state['wizard_fixas']
    envelopes = st.session_state['wizard_envelopes']
    dividas = st.session_state['wizard_dividas']

    if not any([hospitais, fixas, envelopes, dividas]):
        st.info("Nada foi adicionado em nenhum passo. Pode voltar e preencher, ou sair do assistente -- "
               "você ainda pode configurar tudo manualmente em '⚙️ Gerenciar Categorias' depois.")
    else:
        if hospitais:
            st.markdown("**🏥 Locais de plantão:**")
            for h in hospitais: st.write(f"  • {h['nome']} -- {h['atraso_label']}, todo dia {h['dia_pagamento']}")
        if fixas:
            st.markdown("**🏠 Despesas fixas:**")
            for f in fixas: st.write(f"  • {f['nome']} -- R$ {format_brl(f['valor'])}, todo dia {f['dia_vencimento']}")
        if envelopes:
            st.markdown("**🛒 Tetos mensais (envelopes):**")
            for e in envelopes: st.write(f"  • {e['nome']} -- até R$ {format_brl(e['valor'])}/mês")
        if dividas:
            st.markdown("**💳 Dívidas em andamento:**")
            for d in dividas: st.write(f"  • {d['nome']} -- {d['parcelas_faltam']}x de R$ {format_brl(d['valor_parcela'])}")

    st.divider()
    c_voltar, c_confirmar = st.columns(2)
    if c_voltar.button("⬅️ Voltar", key="wizard_voltar_5", use_container_width=True):
        st.session_state['wizard_passo'] = 4
        st.rerun()

    if c_confirmar.button("✅ Finalizar e Salvar Tudo", type="primary", key="wizard_finalizar", use_container_width=True):
        hoje_wizard = datetime.date.today()
        try:
            with transaction() as cur:
                for h in hospitais:
                    cur.execute(
                        "INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, atraso_meses, dia_pagamento, is_recorrente, is_envelope, data_inicio) "
                        "VALUES ('Entrada', 'Plantões', %s, %s, %s, 0, 0, %s) ON CONFLICT DO NOTHING",
                        (h['nome'], h['atraso_meses'], h['dia_pagamento'], hoje_wizard)
                    )

                for f in fixas:
                    cur.execute(
                        "INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento, is_recorrente, is_envelope, data_inicio) "
                        "VALUES ('Despesa', 'Despesas Essenciais', %s, %s, 0, %s, 1, 0, %s) ON CONFLICT DO NOTHING",
                        (f['nome'], f['valor'], f['dia_vencimento'], hoje_wizard)
                    )

                for e in envelopes:
                    cur.execute(
                        "INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, valor_padrao, atraso_meses, dia_pagamento, is_recorrente, is_envelope, data_inicio) "
                        "VALUES ('Despesa', 'Despesas Essenciais', %s, %s, 0, 10, 1, 1, %s) ON CONFLICT DO NOTHING",
                        (e['nome'], e['valor'], hoje_wizard)
                    )

                for d in dividas:
                    cur.execute(
                        "INSERT INTO categorias_personalizadas (tipo, categoria, subgrupo, is_recorrente, is_envelope) "
                        "VALUES ('Despesa', 'Dívidas', %s, 0, 0) ON CONFLICT DO NOTHING",
                        (d['nome'],)
                    )
                    comp_id = str(uuid.uuid4())
                    dia_venc = int(d['dia_vencimento'])
                    if dia_venc >= hoje_wizard.day:
                        primeira = datetime.date(
                            hoje_wizard.year, hoje_wizard.month,
                            min(dia_venc, calendar.monthrange(hoje_wizard.year, hoje_wizard.month)[1])
                        )
                    else:
                        m_f = hoje_wizard.month % 12 + 1
                        a_f = hoje_wizard.year + (hoje_wizard.month // 12)
                        primeira = datetime.date(a_f, m_f, min(dia_venc, calendar.monthrange(a_f, m_f)[1]))

                    registros_divida = []
                    for i in range(d['parcelas_faltam']):
                        m_i = primeira.month - 1 + i
                        a_i = primeira.year + m_i // 12
                        m_i = m_i % 12 + 1
                        data_i = datetime.date(a_i, m_i, min(primeira.day, calendar.monthrange(a_i, m_i)[1]))
                        registros_divida.append((
                            'Despesa', 'Dívidas', d['nome'], d['nome'], d['valor_parcela'],
                            data_i, i + 1, d['parcelas_faltam'], 0, comp_id,
                            'Crédito' if d['eh_cartao'] else 'Outros', 'Média 🟡', 0.0, data_i
                        ))
                    if registros_divida:
                        execute_values(cur,
                            "INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago, data_competencia) VALUES %s",
                            registros_divida
                        )
        except Exception as e:
            st.error(f"Não foi possível concluir a configuração. Nada foi salvo parcialmente: {e}")
        else:
            invalidar_caches_estruturais()
            for _chave in ['wizard_hospitais', 'wizard_fixas', 'wizard_envelopes', 'wizard_dividas']:
                st.session_state[_chave] = []
            st.session_state['wizard_ativo'] = False
            flash("success", "🎉 Configuração inicial salva de forma atômica! Seu app já está pronto pra usar.")
            st.rerun()

def renderizar_wizard_configuracao():
    passo = st.session_state.get('wizard_passo', 1)
    if passo == 1: _wizard_passo1_hospitais()
    elif passo == 2: _wizard_passo2_fixas()
    elif passo == 3: _wizard_passo3_envelopes()
    elif passo == 4: _wizard_passo4_dividas()
    else: _wizard_passo5_revisao()

# =================================================================
# 8. MÓDULO: TELA INICIAL
# =================================================================

if st.session_state.get('wizard_ativo'):
    renderizar_wizard_configuracao()

elif menu == "🏠 Início":
    st.header("🏠 Painel Executivo Imediato")

    # CORREÇÃO de consistência: as métricas agora seguem o Período Ativo da
    # sidebar (antes usavam sempre o mês corrente, ignorando sua seleção).
    st.caption(f"Métricas de {meses[mes_selecionado-1]}/{ano_selecionado} · agenda sempre dos próximos 7 dias")
    dt_limite = hoje + datetime.timedelta(days=7)
    cols_lanc = "id, data_vencimento, tipo, categoria, subgrupo, descricao, valor, pago, forma_pagamento, eh_orcamento"
    df_atraso = fetch_dataframe(
        f"SELECT {cols_lanc} FROM lancamentos WHERE pago = 0 AND COALESCE(eh_orcamento,0)=0 AND data_vencimento < %s "
        f"AND data_vencimento >= %s AND data_vencimento < %s "
        f"ORDER BY data_vencimento ASC, tipo",
        (hoje, inicio_periodo, fim_periodo)
    )
    df_7d = fetch_dataframe(f"SELECT {cols_lanc} FROM lancamentos WHERE COALESCE(eh_orcamento,0)=0 AND data_vencimento >= %s AND data_vencimento <= %s ORDER BY data_vencimento ASC, tipo", (hoje, dt_limite))
    df_mes_atual = fetch_dataframe("SELECT tipo, valor, valor_pago, pago, data_pagamento, eh_orcamento FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s", (inicio_periodo, fim_periodo))

    c_inc1, c_inc2, c_inc3 = st.columns(3)
    if not df_mes_atual.empty:
        df_mes_atual['valor'] = df_mes_atual['valor'].astype(float)
        df_mes_atual['valor_pago'] = df_mes_atual['valor_pago'].astype(float)

        # FÓRMULA PADRONIZADA (mesma em Início e Demonstrativo, veja calcular_confirmado_pendente()):
        # Confirmado = soma de valor_pago onde pago=1 (o que já é fato)
        # Pendente   = soma de valor onde pago=0 (o que ainda não aconteceu)
        # Antes, a despesa "confirmada" usava o valor PLANEJADO mesmo quando já paga --
        # inconsistente com a entrada, que já usava valor_pago corretamente.
        ent_confirmadas = df_mes_atual[(df_mes_atual['tipo'] == 'Entrada') & (df_mes_atual['pago'] == 1)]['valor_pago'].sum()
        ent_projetadas = df_mes_atual[(df_mes_atual['tipo'] == 'Entrada') & (df_mes_atual['pago'] == 0)]['valor'].sum()
        desp_confirmadas = df_mes_atual[(df_mes_atual['tipo'] == 'Despesa') & (df_mes_atual['pago'] == 1)]['valor_pago'].sum()
        _desp_pend = df_mes_atual[(df_mes_atual['tipo'] == 'Despesa') & (df_mes_atual['pago'] == 0)].copy()
        desp_pendentes = _desp_pend.apply(lambda r: max(float(r['valor']), 0.0) if int(r.get('eh_orcamento') or 0) == 1 else float(r['valor']), axis=1).sum()

        c_inc1.metric("📥 Entradas Confirmadas (Mês)", f"R$ {format_brl(ent_confirmadas)}")
        c_inc2.metric("⏳ Entradas Pendentes (Mês)", f"R$ {format_brl(ent_projetadas)}")
        c_inc3.metric("⚖️ Sobra Projetada (Mês)", f"R$ {format_brl((ent_confirmadas + ent_projetadas) - (desp_confirmadas + desp_pendentes))}")
    else:
        c_inc1.metric("📥 Entradas Confirmadas (Mês)", "R$ 0,00")
        c_inc2.metric("⏳ Entradas Pendentes (Mês)", "R$ 0,00")
        c_inc3.metric("⚖️ Sobra Projetada (Mês)", "R$ 0,00")

    # -----------------------------------------------------------------------
    # FEATURE 6 -- MESES DE SOBREVIVÊNCIA.
    # "Se você parar de trabalhar hoje, seu padrão de vida dura quanto tempo?"
    # É a métrica que a pesquisa do setor aponta que a profissão inteira
    # ignora -- por isso fica logo no topo, não enterrada numa aba de análise.
    # -----------------------------------------------------------------------
    st.divider()
    reserva_atual, reserva_atualizada_em = obter_reserva_emergencia()
    media_despesa_mensal, n_meses_com_dados = calcular_media_despesa_mensal(hoje)

    if media_despesa_mensal > 0:
        meses_sobrevivencia = reserva_atual / media_despesa_mensal
        if meses_sobrevivencia < 3: cor_sobrevivencia = "oklch(74% 0.11 25)"
        elif meses_sobrevivencia < 6: cor_sobrevivencia = "oklch(78% 0.12 85)"
        else: cor_sobrevivencia = "oklch(72% 0.11 155)"

        st.markdown(f"""
        <div style='background:oklch(19% 0.01 250); border:1px solid oklch(30% 0.01 250 / 0.55); border-left:4px solid {cor_sobrevivencia};
                    border-radius:14px; padding:1rem 1.3rem; margin-bottom:0.6rem;'>
            <div style='font-size:0.78rem; color:oklch(60% 0.01 250); text-transform:uppercase; letter-spacing:0.04em; font-weight:600;'>
                🛟 Meses de Sobrevivência <span style='opacity:0.7; font-weight:400; text-transform:none;'>(sempre sobre hoje, {hoje.strftime('%d/%m/%Y')} -- não muda com o mês selecionado)</span>
            </div>
            <div style='font-family: "Inter", sans-serif; font-variant-numeric: tabular-nums; font-size:2rem; font-weight:600; color:{cor_sobrevivencia}; line-height:1.3;'>
                {meses_sobrevivencia:.1f} meses
            </div>
            <div style='font-size:0.8rem; color:oklch(60% 0.01 250);'>
                Se você parasse de trabalhar hoje, sua reserva atual (R$ {format_brl(reserva_atual)}) cobriria seu padrão de
                vida por esse tempo -- baseado na média de despesas pagas dos últimos {n_meses_com_dados} mês(es) fechado(s).
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("🛟 **Meses de Sobrevivência**: cadastre sua reserva de emergência abaixo e registre ao menos 1 mês fechado de despesas pagas pra essa métrica aparecer.")

    with st.expander("✏️ Atualizar reserva de emergência"):
        st.caption(f"Última atualização: {reserva_atualizada_em.strftime('%d/%m/%Y') if reserva_atualizada_em else 'nunca'}")
        novo_valor_reserva = st.text_input("Valor atual da sua reserva de emergência (R$)", value=format_brl(reserva_atual), key="input_reserva_emergencia")
        if st.button("💾 Salvar Reserva", type="primary", key="btn_salvar_reserva"):
            atualizar_reserva_emergencia(parse_valor(novo_valor_reserva))
            flash("success", "🛟 Reserva de emergência atualizada!")
            st.rerun()

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
                'ids': grp['id'].astype(int).tolist(), 'categoria': None, 'subgrupo': None, 'eh_orcamento': 0,
            })

        mask_plant = (df['tipo'] == 'Entrada') & df['descricao'].str.contains('plant', case=False, na=False)
        if mask_plant.any():
            # Mesma lógica do Fluxo e Prioridades: agrupa por categoria (hospital),
            # não por subgrupo (turno) -- Semana/FDS/USG existem só pra calcular o
            # valor certo de cada plantão, mas o pagamento cai junto por hospital.
            for nome_grupo, grp in df[mask_plant].groupby(['categoria', 'data_vencimento']):
                cat_nome, dt_venc = nome_grupo
                linhas.append({
                    'descricao': f"🏥 Plantões {cat_nome} ({len(grp)})",
                    'tipo': 'Entrada', 'valor': grp['valor'].sum(),
                    'pago': 1 if (grp['pago'] == 1).all() else 0,
                    'data_vencimento': dt_venc,
                    'ids': grp['id'].astype(int).tolist(), 'categoria': None, 'subgrupo': None, 'eh_orcamento': 0,
                })

        for _, r in df[~mask_cred & ~mask_plant].iterrows():
            linhas.append({
                'descricao': r['descricao'], 'tipo': r['tipo'], 'valor': r['valor'], 'pago': int(r['pago']),
                'data_vencimento': r['data_vencimento'], 'ids': [int(r['id'])],
                'categoria': r['categoria'], 'subgrupo': r['subgrupo'], 'eh_orcamento': int(r.get('eh_orcamento') or 0),
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
            cor_data = "oklch(74% 0.11 25)" if atrasado else "inherit"

            c_lin1, c_lin2, c_lin3 = st.columns([5.2, 1.6, 1.4])
            with c_lin1:
                st.markdown(f"{icone} <span style='color:{cor_data}; font-weight:600;'>{dt_str}</span> · {r['descricao']}", unsafe_allow_html=True)
            with c_lin2:
                st.markdown(f"<div style='text-align:right; font-family: Inter, sans-serif; font-variant-numeric: tabular-nums;'>R$ {format_brl(r['valor'])}</div>", unsafe_allow_html=True)
            with c_lin3:
                if int(r.get('eh_orcamento') or 0) == 1:
                    st.markdown("🧮 <span style='color:oklch(60% 0.01 250);'>Orçamento</span>", unsafe_allow_html=True)
                elif eh_pago:
                    st.markdown("✅ <span style='color:oklch(60% 0.01 250);'>Pago</span>" if eh_despesa else "✅ <span style='color:oklch(60% 0.01 250);'>Recebido</span>", unsafe_allow_html=True)
                else:
                    rotulo_acao = "✓ Pagar" if eh_despesa else "✓ Receber"
                    if st.button(rotulo_acao, key=f"{prefixo_key}_{idx}_{'_'.join(map(str, r['ids']))}", use_container_width=True):
                        ids_tupla = tuple(r['ids'])
                        if len(ids_tupla) == 1:
                            execute_query("UPDATE lancamentos SET pago=1, valor_pago=valor WHERE id=%s", (ids_tupla[0],))
                        else:
                            execute_query("UPDATE lancamentos SET pago=1, valor_pago=valor WHERE id IN %s", (ids_tupla,))
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
    if mes_selecionado != hoje.month or ano_selecionado != hoje.year:
        # Essa seção é sempre sobre os 7 dias reais a partir de hoje -- não existe
        # "próximos 7 dias" de um mês diferente do atual. Antes ela aparecia mesmo
        # assim, mostrando dados de hoje enquanto o resto da tela mostrava o mês
        # selecionado -- o que parecia "o mês voltou sozinho". Agora fica claro
        # que ela só faz sentido quando você está no mês corrente.
        st.caption(f"🗓️ Essa agenda é sempre sobre os 7 dias reais a partir de hoje ({hoje.strftime('%d/%m/%Y')}) -- "
                  f"por isso fica oculta enquanto {meses[mes_selecionado-1]}/{ano_selecionado} estiver selecionado. "
                  f"Volte pro mês atual na sidebar pra vê-la.")
    elif df_7d.empty:
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

                st.caption("ℹ️ A edição altera a configuração para novos lançamentos/recorrências. O histórico já registrado mantém a classificação original.")
                if st.button("💾 Confirmar Edição", type="primary", key="edit_save_btn"):
                    try:
                        with transaction() as cur:
                            cur.execute("UPDATE categorias_personalizadas SET categoria=%s, subgrupo=%s, valor_padrao=%s, atraso_meses=%s, dia_pagamento=%s, is_recorrente=%s, is_envelope=%s WHERE id=%s",
                                        (new_cat, new_sub, v_edit if v_edit > 0 else None, a_edit, d_edit, 1 if e_rec_efetivo else 0, 1 if e_env else 0, sel_edit))
                    except Exception as e:
                        st.error(f"Falha ao atualizar categoria; nenhuma alteração parcial foi aplicada: {e}")
                    else:
                        invalidar_caches_estruturais(); flash("success", "Categoria atualizada. Histórico anterior preservado."); st.rerun()
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

    with st.expander("🧹 Corrigir Descrição Duplicada de Parcelas (bug do Assistente de Configuração)"):
        st.caption("Se você criou dívidas pelo '🧙 Assistente de Configuração' antes desta correção, a descrição "
                  "pode ter ficado duplicada (ex: 'Empréstimo Dimas 2 (1/3) (1/3)'). Esta ferramenta identifica e "
                  "remove só o texto repetido -- não mexe em valor, data ou número de parcelas.")
        df_desc_duplicada = fetch_dataframe(
            r"SELECT id, descricao FROM lancamentos WHERE descricao ~ '\(\d+/\d+\) \(\d+/\d+\)$' ORDER BY id"
        )
        if df_desc_duplicada.empty:
            st.success("Nenhuma descrição duplicada encontrada.")
        else:
            st.warning(f"Encontrado(s) {len(df_desc_duplicada)} lançamento(s) com descrição duplicada.")
            df_preview = df_desc_duplicada.copy()
            df_preview['descrição corrigida (prévia)'] = df_preview['descricao'].apply(lambda d: re.sub(r'(\s*\(\d+/\d+\))+$', '', d).strip())
            st.dataframe(df_preview.rename(columns={'descricao': 'descrição atual'}), use_container_width=True, hide_index=True)
            if st.button("🔧 Corrigir Todas Automaticamente", type="primary", key="btn_corrigir_desc_dup"):
                try:
                    with transaction() as cur:
                        for _, row in df_desc_duplicada.iterrows():
                            nova_desc = re.sub(r'(\s*\(\d+/\d+\))+$', '', row['descricao']).strip()
                            cur.execute("UPDATE lancamentos SET descricao = %s WHERE id = %s", (nova_desc, row['id']))
                except Exception as e:
                    st.error(f"Correção cancelada e revertida: {e}")
                else:
                    flash("success", f"🧹 {len(df_desc_duplicada)} descrição(ões) corrigida(s)."); st.rerun()

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

                registros.append((tipo, categoria, subgrupo, descricao, val_f, d_p, i+1, tot_p, pago_atual, comp_id, forma_pgto, prioridade, v_pago_atual, d_p, hoje if pago_atual else None))

            execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago, data_competencia, data_pagamento) VALUES %s''', registros)

            # Envelopes não são mais mutados na gravação: o saldo é derivado pela VIEW.

            flash("success", "✅ Lançamento registrado com sucesso!"); st.rerun()

# =================================================================
# 11. MÓDULO 2: FLUXO E PRIORIDADES
# =================================================================

elif menu == "📊 Fluxo e Prioridades":
    st.header("📊 Fluxo e Prioridades")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento ASC", (inicio_periodo, fim_periodo))

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
            datas_pg_cred = pd.to_datetime(df_base[mask_cred_full]['data_pagamento'], errors='coerce').dropna()
            data_pg_cred = datas_pg_cred.max().date() if all_paid and not datas_pg_cred.empty else None

            dummy_credito = pd.DataFrame([{
                'id': '-1', 'tipo': 'Despesa', 'categoria': 'N/A', 'subgrupo': '',
                'descricao': '💳 Cartão de Crédito (Fatura Consolidada)', 'valor': sum_cred,
                'valor_pago': sum_pago_cred, 'data_vencimento': datetime.date(ano_selecionado, mes_selecionado, 10),
                'pago': 1 if all_paid else 0, 'compra_id': 'cartao_dummy',
                'forma_pagamento': 'Crédito', 'prioridade': 'Alta 🔴', 'ids_alvo': ids_lote_credito,
                'data_pagamento': data_pg_cred
            }])
        df_base_sem_cred = df_base[~mask_cred_full].copy()

        mask_plantoes_full = (df_base_sem_cred['tipo'] == 'Entrada') & df_base_sem_cred['descricao'].str.contains('plant', case=False, na=False)
        dummies_plantao = []
        if mask_plantoes_full.any():
            df_plantoes_full = df_base_sem_cred[mask_plantoes_full].copy()
            # CONSOLIDAÇÃO POR CATEGORIA (hospital), não por subgrupo. Hospitais que
            # pagam turnos diferentes (Semana/FDS/USG) com valores diferentes usam
            # subgrupos distintos só pra efeito de cálculo do valor -- mas o pagamento
            # cai como 1 valor único do hospital inteiro. Cada hospital já é sua
            # própria categoria (ex: "Trauma", "Unimed"), então agrupar por categoria
            # em vez de subgrupo junta Semana+FDS+USG automaticamente, sem precisar de
            # nenhuma configuração nova -- e continua separando hospitais diferentes.
            for nome_grupo, grupo in df_plantoes_full.groupby(['categoria', 'data_vencimento']):
                cat_nome, dt_venc = nome_grupo
                sum_pago_plantao = grupo['valor_pago'].sum()
                status_lote = 1 if (grupo['pago'] == 1).all() else 0
                ids_lote_plantao = ','.join(grupo['id'].astype(str))
                datas_pg_plant = pd.to_datetime(grupo['data_pagamento'], errors='coerce').dropna()
                data_pg_plant = datas_pg_plant.max().date() if status_lote == 1 and not datas_pg_plant.empty else None

                dummies_plantao.append({
                    'id': f'plantao_{cat_nome}_{dt_venc}', 'tipo': 'Entrada', 'categoria': cat_nome,
                    'subgrupo': '', 'descricao': f'🏥 Plantões {cat_nome} (Consolidado do Mês)',
                    'valor': grupo['valor'].sum(), 'valor_pago': sum_pago_plantao,
                    'data_vencimento': dt_venc, 'pago': status_lote, 'compra_id': 'plantao_dummy',
                    'forma_pagamento': 'Outros', 'prioridade': 'Baixa 🟢', 'ids_alvo': ids_lote_plantao,
                    'data_pagamento': data_pg_plant
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
        df_view['Data Pagamento'] = pd.to_datetime(df_view['data_pagamento'], errors='coerce').dt.date

        def calcular_alerta_atraso(row):
            if int(row.get('eh_orcamento') or 0) == 1:
                return "🧮 Orçamento derivado"
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
            df_view[['🗑️ Excluir', 'Data', 'Data Pagamento', 'Alerta', 'prioridade', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago']],
            use_container_width=True, hide_index=True,
            column_config={
                "🗑️ Excluir": st.column_config.SelectboxColumn("Excluir", options=["", "Este", "Este e Futuros"], width="small"),
                "Data": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY"),
                "Data Pagamento": st.column_config.DateColumn("Pago em", format="DD/MM/YYYY"),
                "Alerta": st.column_config.TextColumn("Status", disabled=True),
                "valor": st.column_config.NumberColumn("Valor Previsto", format="%.2f"),
                "valor_pago": st.column_config.NumberColumn("Valor Pago/Real", format="%.2f"),
                "prioridade": st.column_config.SelectboxColumn("Prioridade", options=["Alta 🔴", "Média 🟡", "Baixa 🟢"]),
                "Desc. Exibição": st.column_config.TextColumn("Descrição", disabled=False)
            }
        )

        edit_df['tipo'] = df_view['tipo'].values
        edit_df['ordem_pri'] = df_view['ordem_pri'].values
        edit_df['eh_orcamento'] = df_view['eh_orcamento'].fillna(0).astype(int).values

        if st.button("Salvar Alterações Rápidas", type="primary"):
            try:
                with transaction() as cur:
                    for i, row in edit_df.iterrows():
                        orig_row = df_view.loc[i]
                        id_s = str(orig_row['id'])
                        novo_pago = 1 if row['Pago'] else 0
                        novo_valor = float(row['valor'])
                        novo_valor_pago = float(row['valor_pago']) if pd.notna(row['valor_pago']) else 0.0
                        orig_valor = float(orig_row['valor'])
                        orig_valor_pago = float(orig_row['valor_pago'])

                        if novo_pago == 1 and novo_valor_pago == 0.0:
                            novo_valor_pago = novo_valor
                        elif novo_pago == 0:
                            novo_valor_pago = 0.0

                        orig_data_pgto = None
                        if pd.notna(orig_row.get('data_pagamento')):
                            orig_data_pgto = pd.to_datetime(orig_row['data_pagamento']).date()
                        nova_data_pgto = row['Data Pagamento'] if pd.notna(row['Data Pagamento']) else None
                        if novo_pago == 1 and nova_data_pgto is None:
                            nova_data_pgto = orig_data_pgto or hoje
                        if novo_pago == 0:
                            nova_data_pgto = None

                        desc_editada = str(row['Desc. Exibição']) != str(orig_row['Desc. Exibição'])
                        nova_desc = row['Desc. Exibição'].split(' (')[0] if desc_editada else orig_row['descricao']
                        tupla_ids_reais = tuple(map(int, orig_row['ids_alvo'].split(',')))
                        excluir_futuros = row['🗑️ Excluir'] == "Este e Futuros"
                        excluir_algo = row['🗑️ Excluir'] in ("Este", "Este e Futuros")
                        data_pgto_mudou = nova_data_pgto != orig_data_pgto
                        mudou = (
                            excluir_algo
                            or novo_pago != int(orig_row['pago'])
                            or abs(novo_valor - orig_valor) > 0.004
                            or abs(novo_valor_pago - orig_valor_pago) > 0.004
                            or str(row['prioridade']) != str(orig_row['prioridade'])
                            or desc_editada
                            or row['Data'] != orig_row['Data']
                            or data_pgto_mudou
                        )
                        if not mudou:
                            continue

                        if excluir_algo:
                            if id_s == '-1':
                                st.warning("Cartões consolidados não podem ser apagados aqui.")
                            elif id_s.startswith('plantao_'):
                                cur.execute("DELETE FROM lancamentos WHERE id IN %s", (tupla_ids_reais,))
                            elif excluir_futuros:
                                cur.execute("DELETE FROM lancamentos WHERE compra_id = %s AND data_vencimento >= %s", (orig_row['compra_id'], orig_row['data_vencimento']))
                            else:
                                cur.execute("DELETE FROM lancamentos WHERE id = %s", (tupla_ids_reais[0],))
                            continue

                        if id_s == '-1' or id_s.startswith('plantao_'):
                            # Pagamento consolidado é aplicado às linhas reais; o trigger
                            # sincroniza data_pagamento e a tabela pagamentos para cada uma.
                            cur.execute(
                                "UPDATE lancamentos SET pago=%s, data_pagamento=%s WHERE id IN %s",
                                (novo_pago, nova_data_pgto, tupla_ids_reais)
                            )
                            if novo_pago == 1:
                                cur.execute("UPDATE lancamentos SET valor_pago=valor WHERE id IN %s AND COALESCE(valor_pago,0)=0", (tupla_ids_reais,))
                                cur.execute("SELECT id, valor_pago FROM lancamentos WHERE id IN %s ORDER BY id", (tupla_ids_reais,))
                                dados_grupo = cur.fetchall()
                                soma_atual_grupo = sum(float(v or 0) for _, v in dados_grupo)
                                ajuste_pago_necessario = novo_valor_pago - soma_atual_grupo
                                if abs(ajuste_pago_necessario) > 0.004 and dados_grupo:
                                    id_alvo_ajuste = int(dados_grupo[-1][0])
                                    cur.execute("UPDATE lancamentos SET valor_pago = valor_pago + %s WHERE id = %s", (ajuste_pago_necessario, id_alvo_ajuste))
                            if abs(novo_valor - orig_valor) > 0.004:
                                id_alvo_planejado = int(tupla_ids_reais[-1])
                                cur.execute("UPDATE lancamentos SET valor = valor + %s WHERE id = %s", (novo_valor - orig_valor, id_alvo_planejado))
                            continue

                        eh_orcamento = int(orig_row.get('eh_orcamento') or 0) == 1
                        if eh_orcamento:
                            # Na VIEW o valor mostrado é o saldo restante. Editar esse saldo
                            # ajusta o snapshot do orçamento pela mesma diferença.
                            delta = novo_valor - orig_valor
                            orc_atual = float(orig_row['valor_orcamento']) if pd.notna(orig_row.get('valor_orcamento')) else max(orig_valor, 0.0)
                            novo_orc = orc_atual + delta
                            if novo_orc < 0:
                                raise ValueError("O orçamento do envelope não pode ficar negativo.")
                            cur.execute(
                                "UPDATE lancamentos SET prioridade=%s, descricao=%s, valor_orcamento=%s, valor=%s, data_vencimento=%s WHERE id=%s",
                                (row['prioridade'], nova_desc, novo_orc, novo_orc, row['Data'], tupla_ids_reais[0])
                            )
                        else:
                            cur.execute(
                                "UPDATE lancamentos SET pago=%s, prioridade=%s, descricao=%s, valor=%s, valor_pago=%s, data_vencimento=%s, data_pagamento=%s WHERE id=%s",
                                (novo_pago, row['prioridade'], nova_desc, novo_valor, novo_valor_pago, row['Data'], nova_data_pgto, tupla_ids_reais[0])
                            )
            except Exception as e:
                st.error(f"Alterações canceladas; nenhuma edição parcial foi aplicada: {e}")
            else:
                flash("success", "✅ Alterações salvas em uma única transação!")
                st.rerun()

        st.divider()

        with st.expander("📱 Despesas Pendentes para WhatsApp (Copiar)", expanded=False):
            df_despesas_pendentes = edit_df[(edit_df['tipo'] == 'Despesa') & (~edit_df['Pago']) & (edit_df['eh_orcamento'] == 0)].sort_values(['ordem_pri', 'Data'])

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
                    if v_final <= 0:
                        st.error("O valor deve ser maior que zero.")
                    else:
                        try:
                            with transaction() as cur:
                                cur.execute("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, data_vencimento=%s, forma_pagamento=%s, data_competencia=COALESCE(data_competencia,%s) WHERE id=%s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_data, e_forma, e_data, int(sel_id)))
                                if e_escopo != "Apenas neste lançamento":
                                    cur.execute("UPDATE lancamentos SET tipo=%s, categoria=%s, subgrupo=%s, descricao=%s, valor=%s, forma_pagamento=%s WHERE compra_id=%s AND data_vencimento > %s AND id != %s", (e_tipo, e_cat, e_sub, e_desc, v_final, e_forma, r_sel['compra_id'], r_sel['data_vencimento'], int(sel_id)))
                        except Exception as e:
                            st.error(f"Mudança estrutural cancelada; nenhuma alteração parcial foi aplicada: {e}")
                        else:
                            flash("success", "Lançamento atualizado de forma atômica!"); st.rerun()

# =================================================================
# 12. MÓDULO 3: DEMONSTRATIVO (COM ANALÍTICO DE PROVISÕES)
# =================================================================

elif menu == "📑 Demonstrativo":
    st.header("📑 Demonstrativo Financeiro")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s", (inicio_periodo, fim_periodo))

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

            # FÓRMULA PADRONIZADA -- mesma definição de "Pendente" usada no Início:
            # soma direta de 'valor' onde pago=0. Antes, aqui era calculado como
            # "Total - Pago" usando SEMPRE a coluna 'valor' (nunca valor_pago) --
            # se você tivesse ajustado o valor realmente pago/recebido pra um número
            # diferente do planejado, esse número divergia do que o Início mostrava
            # pro mesmo mês. Agora as duas telas calculam exatamente igual.
            falta_receber = df_e[df_e['pago'] == 0]['valor'].sum()
            _df_falta_pagar = df_d[df_d['pago'] == 0].copy()
            falta_pagar = _df_falta_pagar.apply(lambda r: max(float(r['valor']), 0.0) if int(r.get('eh_orcamento') or 0) == 1 else float(r['valor']), axis=1).sum()

            c_res1, c_res2 = st.columns(2)
            c_res1.metric("⏳ Entradas Pendentes (Mês)", f"R$ {format_brl(falta_receber)}")
            c_res2.metric("🚨 Despesas Pendentes (Mês)", f"R$ {format_brl(falta_pagar)}")

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

            def exibir_demonstrativo(dataframe, chave):
                if dataframe.empty: return
                dataframe = dataframe.sort_values('data_vencimento').copy()
                dataframe['Desc. Exibição'] = dataframe.apply(lambda r: f"{r['descricao']} ({int(r['parcela_atual'])}/{int(r['total_parcelas'])})" if pd.notna(r.get('total_parcelas')) and r['total_parcelas'] > 1 and r['total_parcelas'] != 999 else r['descricao'], axis=1)
                dataframe['Status'] = dataframe.apply(lambda r: '🧮 Orçamento' if int(r.get('eh_orcamento') or 0) == 1 else ('✅ Pago' if r['pago'] == 1 else '⏳ Pendente'), axis=1)
                dataframe['Pago em'] = pd.to_datetime(dataframe['data_pagamento'], errors='coerce').dt.strftime('%d/%m/%Y').fillna('—')

                tabela = dataframe[['Data BR', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago em', 'prioridade', 'Status']].rename(
                    columns={'Data BR': 'Vencimento', 'Desc. Exibição': 'Descrição', 'valor': 'Planejado', 'valor_pago': 'Pago/Real', 'prioridade': 'Prioridade'}
                )

                def _cor_linha_demonstrativo(row):
                    if row['Status'] == '✅ Pago':
                        return ['background-color: oklch(72% 0.11 155 / 0.12); color: oklch(93% 0.004 250)'] * len(row)
                    return ['background-color: oklch(78% 0.12 85 / 0.12); color: oklch(93% 0.004 250)'] * len(row)

                estilo = tabela.style.apply(_cor_linha_demonstrativo, axis=1).format({
                    'Planejado': lambda v: f"R$ {format_brl(v)}",
                    'Pago/Real': lambda v: f"R$ {format_brl(v)}"
                })
                # CORREÇÃO: sem uma key= única, o Streamlit pode reciclar o componente
                # visual de uma tabela anterior nesse mesmo loop (categorias/subgrupos
                # com número de linhas diferente), deixando "linhas fantasma" com só a
                # cor/ícone da tabela anterior aparecendo. A key garante que cada tabela
                # seja tratada como um componente genuinamente novo.
                st.dataframe(estilo, hide_index=True, use_container_width=True, key=f"demo_tabela_{chave}")

            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🟢 Entradas Detalhadas")
                # CONSOLIDADO POR CATEGORIA (não por subgrupo): subgrupos como
                # "Trauma Semana"/"Trauma FDS"/"Trauma USG" existem só pra calcular o
                # valor certo de cada plantão -- na visualização, tudo do mesmo hospital
                # aparece junto numa tabela só, sem quebrar por turno. A descrição de
                # cada lançamento individual continua indicando de qual turno ele é.
                for cat in sorted(df_e_visivel['categoria'].unique(), key=lambda x: str(x).lower()):
                    df_c = df_e_visivel[df_e_visivel['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        exibir_demonstrativo(df_c, chave=f"e_{cat}")
            with c2:
                st.subheader("🔴 Despesas Detalhadas")
                for cat in ordenar_categorias_com_prioridade(df_d_visivel['categoria'].unique()):
                    df_c = df_d_visivel[df_d_visivel['categoria'] == cat]
                    with st.expander(f"{cat} - R$ {format_brl(df_c['valor'].sum())}"):
                        for sub in df_c['subgrupo'].unique():
                            df_s = df_c[df_c['subgrupo'] == sub].copy()
                            if df_s.empty: continue
                            st.markdown(f"**🔹 {sub if sub else 'Geral'}**")
                            exibir_demonstrativo(df_s, chave=f"d_{cat}_{sub}")
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
                        return ['background-color: oklch(68% 0.13 25 / 0.12); color: oklch(93% 0.004 250)'] * len(row)
                    if row['Métrica de Saúde'].startswith('🟡'):
                        return ['background-color: oklch(78% 0.12 85 / 0.12); color: oklch(93% 0.004 250)'] * len(row)
                    return ['background-color: oklch(72% 0.11 155 / 0.12); color: oklch(93% 0.004 250)'] * len(row)

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
                    WHERE tipo = 'Despesa' AND pago = 1 AND COALESCE(eh_orcamento,0) = 0
                      AND data_vencimento >= %s AND data_vencimento < %s
                    GROUP BY categoria, subgrupo
                ),
                teto_mes AS (
                    SELECT categoria, subgrupo, SUM(valor) as saldo_atual, COUNT(*) as qtd_linhas_teto
                    FROM lancamentos
                    WHERE tipo = 'Despesa' AND pago = 0 AND COALESCE(eh_orcamento,0) = 1
                      AND data_vencimento >= %s AND data_vencimento < %s
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
            ''', (inicio_periodo, fim_periodo, inicio_periodo, fim_periodo))

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
    st.caption("Realizado é alocado pela data efetiva de pagamento/recebimento; itens ainda pendentes permanecem no mês de vencimento.")
    anos_disp = fetch_dataframe("""SELECT DISTINCT ano FROM (
        SELECT EXTRACT(YEAR FROM data_vencimento)::int AS ano FROM lancamentos WHERE data_vencimento IS NOT NULL
        UNION
        SELECT EXTRACT(YEAR FROM data_pagamento)::int AS ano FROM lancamentos WHERE data_pagamento IS NOT NULL
    ) x ORDER BY ano DESC""")
    if anos_disp.empty:
        st.info("Sem dados suficientes para gerar balanço anual.")
    else:
        ano_balanco = st.selectbox("Ano de Referência", anos_disp['ano'].astype(int).tolist(), index=0)
        for m in range(1, 13): processar_recorrencias_lazy(m, ano_balanco)

        inicio_ano, fim_ano = limites_ano(ano_balanco)
        df_ano = fetch_dataframe("SELECT * FROM lancamentos WHERE (pago = 1 AND data_pagamento >= %s AND data_pagamento < %s) OR (pago = 0 AND data_vencimento >= %s AND data_vencimento < %s)", (inicio_ano, fim_ano, inicio_ano, fim_ano))
        if df_ano.empty:
            st.warning("Sem dados.")
        else:
            df_ano['valor'] = df_ano['valor'].astype(float)
            df_ano['valor_pago'] = df_ano['valor_pago'].fillna(0.0).astype(float)
            df_ano['data_hibrida'] = df_ano.apply(lambda r: r['data_pagamento'] if r['pago'] == 1 and pd.notna(r.get('data_pagamento')) else r['data_vencimento'], axis=1)
            df_ano['mes_num'] = pd.to_datetime(df_ano['data_hibrida']).dt.month

            df_ano['hibrido_fpa'] = df_ano.apply(lambda r: float(r['valor_pago']) if r['pago'] == 1 else (max(float(r['valor']), 0.0) if int(r.get('eh_orcamento') or 0) == 1 else float(r['valor'])), axis=1)
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
                                  color_discrete_map={'Entrada': '#68B986', 'Despesa': '#DD766F'},
                                  labels={'value': 'Valor (R$)', 'variable': 'Fluxo'})
                fig_evol.update_layout(legend_title_text='Fluxo')
                st.plotly_chart(aplicar_tema_grafico(fig_evol), use_container_width=True)

                fig_acum = px.area(mensal, x='Mes', y='Acumulado', title="Fluxo de Caixa Acumulado (Híbrido)",
                                   color_discrete_sequence=['#4AB6C7'], markers=True)
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
                                     color='hibrido_fpa', color_continuous_scale=['#2A2E33', '#DBB155', '#DD766F'])
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

        # FEATURE 5 -- TRADUTOR DE DÍVIDA EM PLANTÃO. Valor médio de 1 plantão
        # calculado a partir do SEU histórico real dos últimos 6 meses (não é
        # número fixo) -- usado só como referência de tradução, não afeta
        # nenhum cálculo de saldo devedor ou parcela.
        valor_medio_plantao, n_plantoes_hist = calcular_valor_medio_plantao(hoje)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💰 Saldo Devedor Total", f"R$ {format_brl(total_divida_geral)}")
        c2.metric("📋 Dívidas Ativas", str(n_dividas_ativas))
        c3.metric("📅 Parcelas Restantes (todas)", str(n_parcelas_restantes))
        if valor_medio_plantao and valor_medio_plantao > 0:
            c4.metric("🏥 Equivale a", f"{total_divida_geral / valor_medio_plantao:.0f} plantões")
        else:
            c4.metric("🏥 Equivale a", "—")

        if valor_medio_plantao and valor_medio_plantao > 0:
            st.caption(f"💡 Tradução baseada no valor médio dos seus últimos {n_plantoes_hist} plantão(ões) "
                      f"lançados (R$ {format_brl(valor_medio_plantao)}/plantão, últimos 6 meses).")
        else:
            st.caption("💡 Lance ao menos 1 plantão em '🏥 Escala de Plantões' pra ver suas dívidas traduzidas em plantões.")

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
                        st.markdown(f"<div style='text-align:right; font-family: Inter, sans-serif; font-variant-numeric: tabular-nums; font-weight:600; font-size:1.1rem;'>R$ {format_brl(d['saldo_devedor'])}</div>", unsafe_allow_html=True)
                        st.caption("Saldo devedor")

                st.progress(progresso)
                st.caption(f"{parc_pagas}/{total_parc} parcelas pagas")

                if d['saldo_devedor'] > 0.01 and valor_medio_plantao and valor_medio_plantao > 0:
                    parcela_mensal = d['valor_total'] / total_parc if total_parc else 0.0
                    plantoes_equiv = parcela_mensal / valor_medio_plantao
                    st.markdown(
                        f"<div style='background:oklch(19% 0.01 250); border-left:3px solid oklch(78% 0.12 85); border-radius:6px; "
                        f"padding:0.4rem 0.7rem; margin:0.3rem 0; font-size:0.85rem;'>"
                        f"🏥 Essa parcela (R$ {format_brl(parcela_mensal)}/mês) equivale a "
                        f"<b>{plantoes_equiv:.1f} plantão(ões)/mês</b> pelo seu valor médio recente.</div>",
                        unsafe_allow_html=True
                    )

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
                            valor_final, data_vencto, 1, 1, 0, str(uuid.uuid4()), 'Outros', 'Baixa 🟢', 0.0, data_plantao
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
                            INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago, data_competencia)
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
        df_t['d_p'] = pd.to_datetime(df_t['data_competencia'].fillna(df_t['data_vencimento']), errors='coerce').dt.date
        df_m_cal = df_t[(pd.to_datetime(df_t['d_p']).dt.month == cal_mes) & (pd.to_datetime(df_t['d_p']).dt.year == cal_ano)].copy()

    cols = st.columns(7)
    for i, dia in enumerate(["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]): cols[i].markdown(f"<div style='text-align: center; font-weight: 600; padding: 6px; font-family: Inter, sans-serif; color:oklch(60% 0.01 250); font-size:0.78rem; text-transform:uppercase; letter-spacing:0.04em; border-bottom: 2px solid oklch(72% 0.1 210);'>{dia}</div>", unsafe_allow_html=True)
    for week in calendar.monthcalendar(cal_ano, cal_mes):
        w_cols = st.columns(7)
        for i, day in enumerate(week):
            with w_cols[i]:
                if day != 0:
                    cd = datetime.date(cal_ano, cal_mes, day)
                    bg = "background-color: oklch(72% 0.1 210 / 0.14);" if cd == hoje else "background-color: oklch(19% 0.01 250);"
                    brdr = "border: 2px solid oklch(72% 0.1 210);" if cd == hoje else "border: 1px solid oklch(30% 0.01 250 / 0.55);"
                    html = f"<div style='{bg} {brdr} border-radius: 10px; padding: 6px; min-height: 90px; margin-top: 6px;'><div style='text-align:right; font-weight:600; font-family: Inter, sans-serif; color:oklch(93% 0.004 250); font-size:0.85rem;'>{day}</div>"
                    if not df_m_cal.empty:
                        for _, s in df_m_cal[df_m_cal['d_p'] == cd].iterrows(): html += f"<div style='background-color:oklch(72% 0.1 210); color:oklch(15% 0.008 250); font-size:10px; font-weight:600; padding:2px 4px; border-radius:4px; margin-top:2px; white-space:nowrap; overflow:hidden;'>🏥 {s['subgrupo']}</div>"
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
                ids_apagar = [int(df_geren.loc[i, 'id']) for i, r in edit_esc.iterrows() if r['🗑️ Apagar']]
                try:
                    if ids_apagar:
                        with transaction() as cur:
                            cur.execute("DELETE FROM lancamentos WHERE id = ANY(%s)", (ids_apagar,))
                except Exception as e:
                    st.error(f"Exclusão cancelada; nenhuma linha foi removida: {e}")
                else:
                    flash("success", f"🗑️ {len(ids_apagar)} plantão(ões) apagado(s) com sucesso!")
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
                dia_pgto_seguro = min(int(reg_d), calendar.monthrange(a_f, m_f)[1])
                regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({d_p.strftime('%d/%m/%Y')})", v_t, datetime.date(a_f, m_f, dia_pgto_seguro), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢", 0.0, d_p))
            elif dias_s:
                for off in range(m_rec):
                    m_a, a_a = (mes_selecionado + off - 1) % 12 + 1, ano_selecionado + (mes_selecionado + off - 1) // 12
                    m_p, a_p = (m_a + reg_m - 1) % 12 + 1, a_a + (m_a + reg_m - 1) // 12
                    for d in range(1, calendar.monthrange(a_a, m_a)[1] + 1):
                        curr = datetime.date(a_a, m_a, d)
                        if curr.weekday() in dias_s:
                            dia_pgto_seguro = min(int(reg_d), calendar.monthrange(a_p, m_p)[1])
                            regs.append(("Entrada", cat_escolhida, loc_p, f"Plantão {loc_p} ({curr.strftime('%d/%m/%Y')})", v_t, datetime.date(a_p, m_p, dia_pgto_seguro), 1, 1, 0, str(uuid.uuid4()), "Outros", "Baixa 🟢", 0.0, curr))
            if regs:
                execute_values_query('''INSERT INTO lancamentos (tipo, categoria, subgrupo, descricao, valor, data_vencimento, parcela_atual, total_parcelas, pago, compra_id, forma_pagamento, prioridade, valor_pago, data_competencia) VALUES %s''', regs)
                flash("success", f"✅ {len(regs)} plantão(ões) registrado(s) com sucesso!")
                st.rerun()
