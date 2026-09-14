import streamlit as st
APP_BUILD = "fluxo-inline-v2"
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
                eh_envelope = int_seguro(contrato.get('is_envelope')) == 1
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

def int_seguro(valor, padrao=0):
    """Converte números vindos de pandas/SQL sem quebrar com NaN/None."""
    try:
        if valor is None or pd.isna(valor):
            return int(padrao)
        return int(valor)
    except (TypeError, ValueError, OverflowError):
        return int(padrao)

def float_seguro(valor, padrao=0.0):
    """Converte números financeiros sem propagar NaN para cálculos/UI."""
    try:
        if valor is None or pd.isna(valor):
            return float(padrao)
        return float(valor)
    except (TypeError, ValueError, OverflowError):
        return float(padrao)

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
    return planejado if abs(real) <= 0.004 else real

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
# 7. NAVEGAÇÃO, PERÍODO E HIERARQUIA DE USO
# =================================================================

# CSS complementar de UX: cards, hierarquia de seção e linha operacional.
st.markdown("""
<style>
.ux-page-title { margin-bottom:.1rem; }
.ux-subtitle { color:var(--text-muted); font-size:.9rem; margin-bottom:.8rem; }
.ux-section-title { font-size:.76rem; font-weight:650; letter-spacing:.055em; text-transform:uppercase; color:var(--text-faint); margin:1.2rem 0 .5rem 0; }
.ux-card { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.1rem; margin:.35rem 0; }
.ux-card-strong { background:var(--bg-card); border:1px solid var(--accent); border-radius:14px; padding:1rem 1.1rem; margin:.35rem 0; }
.ux-muted { color:var(--text-muted); font-size:.82rem; }
.ux-value { font-variant-numeric:tabular-nums; font-size:1.35rem; font-weight:650; letter-spacing:-.015em; }
.ux-row { background:var(--bg-card); border:1px solid var(--border); border-radius:11px; padding:.55rem .7rem; margin:.25rem 0; }
.ux-badge { display:inline-block; border:1px solid var(--border-strong); border-radius:999px; padding:.12rem .48rem; font-size:.72rem; color:var(--text-muted); }
.ux-badge-accent { display:inline-block; background:var(--accent-tint); border-radius:999px; padding:.12rem .48rem; font-size:.72rem; color:var(--accent-strong); }
.ux-danger { color:var(--danger-text); }
.ux-success { color:var(--success); }
[data-testid="stSidebar"] .stExpander { background:transparent !important; border:0 !important; }
@media (max-width:640px) {
  .ux-value { font-size:1.1rem; }
  .ux-card, .ux-card-strong { padding:.8rem .85rem; }
}
</style>
""", unsafe_allow_html=True)


def _nav_btn(rotulo, key, destino=None, container=None):
    alvo = container if container is not None else st.sidebar
    destino = destino or rotulo
    ativo = st.session_state.menu_atual == destino
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


st.sidebar.markdown(
    "<div style='font-weight:700; font-size:1.08rem; color:oklch(96% 0.003 250);'>💰 Gestão Financeira</div>"
    "<div style='font-size:.78rem; color:oklch(60% 0.01 250); margin:.15rem 0 .55rem;'>Seu dinheiro, sem ruído.</div>",
    unsafe_allow_html=True,
)
st.sidebar.caption("Build fluxo-inline-v2")
st.sidebar.divider()

if "menu_atual" not in st.session_state:
    st.session_state.menu_atual = "🏠 Início"

st.sidebar.markdown("<div class='nav-eyebrow'>Dia a Dia</div>", unsafe_allow_html=True)
_nav_btn("🏠 Início", "nav_inicio", "🏠 Início")
_nav_btn("➕ Novo Lançamento", "nav_lancamentos", "📝 Lançamentos")
_nav_btn("📋 Fluxo do Mês", "nav_fluxo", "📊 Fluxo e Prioridades")

st.sidebar.markdown("<div class='nav-eyebrow'>Análise</div>", unsafe_allow_html=True)
_nav_btn("📊 Demonstrativo", "nav_demonstrativo", "📑 Demonstrativo")
_nav_btn("💳 Dívidas", "nav_dividas", "💳 Dívidas")
_nav_btn("📈 Balanço Anual", "nav_balanco", "📈 Balanço Anual")
_nav_btn("🏥 Plantões", "nav_escala", "🏥 Escala de Plantões")

_config_destinos = ("⚙️ Gerenciar Categorias", "💾 Backup e Restauração", "🧰 Manutenção e Diagnóstico")
with st.sidebar.expander("⚙️ Configurações", expanded=st.session_state.menu_atual in _config_destinos):
    _nav_btn("⚙️ Categorias e Automações", "nav_categorias", "⚙️ Gerenciar Categorias", container=st)
    _nav_btn("💾 Backup e Restauração", "nav_backup", "💾 Backup e Restauração", container=st)
    _nav_btn("🧰 Manutenção e diagnóstico", "nav_manutencao", "🧰 Manutenção e Diagnóstico", container=st)
    if st.button("🧙 Reconfigurar App", key="btn_abrir_wizard", use_container_width=True):
        st.session_state['wizard_ativo'] = True
        st.session_state['wizard_passo'] = 0
        for _wk in ['wizard_hospitais','wizard_fixas','wizard_envelopes','wizard_dividas']:
            st.session_state[_wk] = []
        st.rerun()

menu = st.session_state.menu_atual
st.sidebar.divider()
st.sidebar.markdown("<div class='nav-eyebrow'>Período</div>", unsafe_allow_html=True)

if "sb_mes" not in st.session_state: st.session_state["sb_mes"] = hoje.month
if "sb_ano" not in st.session_state: st.session_state["sb_ano"] = hoje.year

p1, p2, p3 = st.sidebar.columns([1, 3, 1])
p1.button("‹", key="sb_prev", on_click=_mover_periodo, args=(-1,), use_container_width=True)
p2.markdown(
    f"<div style='text-align:center; padding:.42rem .2rem; font-weight:600;'>{meses[int(st.session_state['sb_mes'])-1][:3]} {st.session_state['sb_ano']}</div>",
    unsafe_allow_html=True,
)
p3.button("›", key="sb_next", on_click=_mover_periodo, args=(1,), use_container_width=True)
st.sidebar.button("Ir para o mês atual", key="sb_today", on_click=_periodo_hoje, use_container_width=True)

with st.sidebar.expander("Escolher outro período"):
    col_sb1, col_sb2 = st.columns(2)
    with col_sb1:
        mes_selecionado = st.selectbox("Mês", range(1, 13), format_func=lambda x: meses[x-1], key="sb_mes")
    with col_sb2:
        ano_selecionado = st.selectbox("Ano", range(hoje.year-3, hoje.year+6), key="sb_ano")

# Fora do expander, os valores seguem o session_state mesmo quando o widget está recolhido.
mes_selecionado = int(st.session_state['sb_mes'])
ano_selecionado = int(st.session_state['sb_ano'])

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


# A interface de backup foi movida para a página dedicada em Configurações.

processar_recorrencias_lazy(mes_selecionado, ano_selecionado)
dia_maximo_alvo = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
data_contexto_ativo = datetime.date(ano_selecionado, mes_selecionado, min(hoje.day, dia_maximo_alvo))
inicio_periodo, fim_periodo = limites_mes(mes_selecionado, ano_selecionado)

exibir_flash()

# =================================================================
# 7B. ASSISTENTE DE CONFIGURAÇÃO — ONBOARDING GUIADO
# =================================================================
if 'wizard_ativo' not in st.session_state:
    df_check_categorias = fetch_dataframe("SELECT COUNT(*) as n FROM categorias_personalizadas")
    n_categorias_existentes = int(df_check_categorias.iloc[0]['n']) if not df_check_categorias.empty else 0
    st.session_state['wizard_ativo'] = (n_categorias_existentes == 0)
    st.session_state['wizard_passo'] = 0

for _chave in ['wizard_hospitais', 'wizard_fixas', 'wizard_envelopes', 'wizard_dividas']:
    if _chave not in st.session_state:
        st.session_state[_chave] = []

MAPA_ATRASO_AMIGAVEL = {"Paga no mesmo mês": 0, "Paga 1 mês depois": 1, "Paga 2 meses depois": 2, "Paga 3 meses depois": 3}


def _wizard_intro():
    st.header("🧙 Vamos preparar seu controle financeiro")
    st.markdown("Em cinco etapas rápidas vamos cadastrar o essencial para o app trabalhar por você.")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
        <div class='ux-card'>
          <b>1. 🏥 Receitas e hospitais</b><br><span class='ux-muted'>Onde você trabalha e quando recebe.</span><br><br>
          <b>2. 🏠 Despesas fixas</b><br><span class='ux-muted'>Contas que se repetem todo mês.</span>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""
        <div class='ux-card'>
          <b>3. 💳 Dívidas</b><br><span class='ux-muted'>Parcelas que ainda faltam pagar.</span><br><br>
          <b>4. 🎯 Limites mensais</b><br><span class='ux-muted'>Mercado, lazer, transporte e outros tetos.</span>
        </div>""", unsafe_allow_html=True)
    st.markdown("<div class='ux-muted'>5. Revisamos tudo antes de salvar.</div>", unsafe_allow_html=True)
    c_skip, c_go = st.columns([1, 2])
    if c_skip.button("Pular por enquanto", key="wizard_intro_skip", use_container_width=True):
        st.session_state['wizard_ativo'] = False
        st.rerun()
    if c_go.button("Começar configuração →", type="primary", key="wizard_intro_go", use_container_width=True):
        st.session_state['wizard_passo'] = 1
        st.rerun()


def _wizard_cabecalho(passo_atual, titulo):
    st.header("🧙 Configuração inicial")
    st.progress(passo_atual / 5)
    st.caption(f"Passo {passo_atual} de 5")
    if st.button("✖️ Sair do assistente", key=f"wizard_sair_{passo_atual}"):
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


def _wizard_navegacao(passo_atual, texto_avancar="Próximo →"):
    st.divider()
    c_voltar, c_avancar = st.columns(2)
    if c_voltar.button("← Voltar", key=f"wizard_voltar_{passo_atual}", use_container_width=True):
        st.session_state['wizard_passo'] = passo_atual - 1
        st.rerun()
    if c_avancar.button(texto_avancar, type="primary", key=f"wizard_avancar_{passo_atual}", use_container_width=True):
        st.session_state['wizard_passo'] = passo_atual + 1
        st.rerun()


def _wizard_passo1_hospitais():
    _wizard_cabecalho(1, "🏥 Onde você faz plantão?")
    st.caption("Para cada local, informe quando o pagamento costuma cair.")
    with st.form("wizard_form_hospital", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1.4, 1])
        nome = c1.text_input("Hospital/local")
        atraso_label = c2.selectbox("Quando paga?", list(MAPA_ATRASO_AMIGAVEL.keys()), index=1)
        dia_pgto = c3.number_input("Dia", min_value=1, max_value=31, value=10)
        if st.form_submit_button("＋ Adicionar local") and nome.strip():
            st.session_state['wizard_hospitais'].append({
                "nome": nome.strip(), "atraso_meses": MAPA_ATRASO_AMIGAVEL[atraso_label],
                "dia_pagamento": int(dia_pgto), "atraso_label": atraso_label
            })
            st.rerun()
    _wizard_lista_com_remover(st.session_state['wizard_hospitais'], 'wizard_hospitais',
        lambda h: f"🏥 {h['nome']} · {h['atraso_label']} · dia {h['dia_pagamento']}")
    _wizard_navegacao(1)


def _wizard_passo2_fixas():
    _wizard_cabecalho(2, "🏠 Quais contas se repetem todo mês?")
    st.caption("Ex.: aluguel, internet, plano de saúde, escola.")
    with st.form("wizard_form_fixa", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 1.3, 1])
        nome = c1.text_input("Despesa", placeholder="Ex: Aluguel")
        valor_txt = c2.text_input("Valor (R$)", value="0,00")
        dia_venc = c3.number_input("Vence dia", min_value=1, max_value=31, value=5)
        if st.form_submit_button("＋ Adicionar despesa fixa"):
            valor_f = parse_valor(valor_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_fixas'].append({"nome": nome.strip(), "valor": valor_f, "dia_vencimento": int(dia_venc)})
                st.rerun()
    _wizard_lista_com_remover(st.session_state['wizard_fixas'], 'wizard_fixas',
        lambda f: f"🏠 {f['nome']} · R$ {format_brl(f['valor'])} · dia {f['dia_vencimento']}")
    _wizard_navegacao(2)


def _wizard_passo3_dividas():
    _wizard_cabecalho(3, "💳 Você tem alguma dívida parcelada em andamento?")
    st.caption("Cadastre apenas o que ainda falta pagar.")
    with st.form("wizard_form_divida", clear_on_submit=True):
        c1, c2 = st.columns([2, 1.3])
        nome = c1.text_input("Dívida", placeholder="Ex: Financiamento, notebook")
        valor_parcela_txt = c2.text_input("Valor da parcela (R$)", value="0,00")
        c3, c4, c5 = st.columns([1, 1, 1.4])
        parcelas_faltam = c3.number_input("Parcelas restantes", min_value=1, max_value=120, value=1)
        dia_venc = c4.number_input("Vence dia", min_value=1, max_value=31, value=10)
        eh_cartao = c5.checkbox("É no cartão de crédito?")
        if st.form_submit_button("＋ Adicionar dívida"):
            valor_f = parse_valor(valor_parcela_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_dividas'].append({
                    "nome": nome.strip(), "valor_parcela": valor_f, "parcelas_faltam": int(parcelas_faltam),
                    "dia_vencimento": int(dia_venc), "eh_cartao": eh_cartao
                })
                st.rerun()
    _wizard_lista_com_remover(st.session_state['wizard_dividas'], 'wizard_dividas',
        lambda d: f"💳 {d['nome']} · {d['parcelas_faltam']}x de R$ {format_brl(d['valor_parcela'])}")
    _wizard_navegacao(3)


def _wizard_passo4_envelopes():
    _wizard_cabecalho(4, "🎯 Quais gastos variáveis precisam de limite mensal?")
    st.caption("Ex.: mercado, lazer, farmácia, transporte. O saldo é calculado automaticamente.")
    with st.form("wizard_form_envelope", clear_on_submit=True):
        c1, c2 = st.columns([2, 1.3])
        nome = c1.text_input("Gasto", placeholder="Ex: Mercado")
        valor_txt = c2.text_input("Limite mensal (R$)", value="0,00")
        if st.form_submit_button("＋ Adicionar limite"):
            valor_f = parse_valor(valor_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_envelopes'].append({"nome": nome.strip(), "valor": valor_f})
                st.rerun()
    _wizard_lista_com_remover(st.session_state['wizard_envelopes'], 'wizard_envelopes',
        lambda e: f"🎯 {e['nome']} · até R$ {format_brl(e['valor'])}/mês")
    _wizard_navegacao(4, texto_avancar="Revisar →")


def _wizard_passo5_revisao():
    _wizard_cabecalho(5, "📋 Revise antes de salvar")
    hospitais = st.session_state['wizard_hospitais']
    fixas = st.session_state['wizard_fixas']
    envelopes = st.session_state['wizard_envelopes']
    dividas = st.session_state['wizard_dividas']
    with st.container(border=True):
        if hospitais:
            st.markdown("**🏥 Receitas / locais**")
            for h in hospitais: st.write(f"• {h['nome']} · {h['atraso_label']} · dia {h['dia_pagamento']}")
        if fixas:
            st.markdown("**🏠 Despesas fixas**")
            for f in fixas: st.write(f"• {f['nome']} · R$ {format_brl(f['valor'])} · dia {f['dia_vencimento']}")
        if dividas:
            st.markdown("**💳 Dívidas**")
            for d in dividas: st.write(f"• {d['nome']} · {d['parcelas_faltam']}x R$ {format_brl(d['valor_parcela'])}")
        if envelopes:
            st.markdown("**🎯 Limites mensais**")
            for e in envelopes: st.write(f"• {e['nome']} · R$ {format_brl(e['valor'])}/mês")
        if not any([hospitais, fixas, envelopes, dividas]):
            st.info("Nenhum item foi adicionado. Você pode voltar ou sair do assistente.")

    c_voltar, c_confirmar = st.columns(2)
    if c_voltar.button("← Voltar", key="wizard_voltar_5", use_container_width=True):
        st.session_state['wizard_passo'] = 4
        st.rerun()
    if c_confirmar.button("✅ Salvar configuração", type="primary", key="wizard_finalizar", use_container_width=True):
        hoje_wizard = datetime.date.today()
        try:
            with transaction() as cur:
                for h in hospitais:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,atraso_meses,dia_pagamento,is_recorrente,is_envelope,data_inicio) VALUES ('Entrada','Plantões',%s,%s,%s,0,0,%s) ON CONFLICT DO NOTHING",
                                (h['nome'], h['atraso_meses'], h['dia_pagamento'], hoje_wizard))
                for f in fixas:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,is_envelope,data_inicio) VALUES ('Despesa','Despesas Essenciais',%s,%s,0,%s,1,0,%s) ON CONFLICT DO NOTHING",
                                (f['nome'], f['valor'], f['dia_vencimento'], hoje_wizard))
                for e in envelopes:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,is_envelope,data_inicio) VALUES ('Despesa','Despesas Essenciais',%s,%s,0,10,1,1,%s) ON CONFLICT DO NOTHING",
                                (e['nome'], e['valor'], hoje_wizard))
                for d in dividas:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,is_recorrente,is_envelope) VALUES ('Despesa','Dívidas',%s,0,0) ON CONFLICT DO NOTHING", (d['nome'],))
                    comp_id = str(uuid.uuid4())
                    dia_venc = int(d['dia_vencimento'])
                    if dia_venc >= hoje_wizard.day:
                        primeira = datetime.date(hoje_wizard.year, hoje_wizard.month, min(dia_venc, calendar.monthrange(hoje_wizard.year, hoje_wizard.month)[1]))
                    else:
                        m_f = hoje_wizard.month % 12 + 1
                        a_f = hoje_wizard.year + (hoje_wizard.month // 12)
                        primeira = datetime.date(a_f, m_f, min(dia_venc, calendar.monthrange(a_f, m_f)[1]))
                    regs = []
                    for i in range(d['parcelas_faltam']):
                        m_i = primeira.month - 1 + i
                        a_i = primeira.year + m_i // 12
                        m_i = m_i % 12 + 1
                        data_i = datetime.date(a_i, m_i, min(primeira.day, calendar.monthrange(a_i, m_i)[1]))
                        regs.append(('Despesa','Dívidas',d['nome'],d['nome'],d['valor_parcela'],data_i,i+1,d['parcelas_faltam'],0,comp_id,'Crédito' if d['eh_cartao'] else 'Outros','Média 🟡',0.0,data_i))
                    if regs:
                        execute_values(cur, "INSERT INTO lancamentos (tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia) VALUES %s", regs)
        except Exception as e:
            st.error(f"Não foi possível concluir a configuração. Nada foi salvo parcialmente: {e}")
        else:
            invalidar_caches_estruturais()
            for k in ['wizard_hospitais','wizard_fixas','wizard_envelopes','wizard_dividas']:
                st.session_state[k] = []
            st.session_state['wizard_ativo'] = False
            flash("success", "🎉 Configuração salva. Seu painel já está pronto.")
            st.rerun()


def renderizar_wizard_configuracao():
    passo = int(st.session_state.get('wizard_passo', 0))
    if passo <= 0: _wizard_intro()
    elif passo == 1: _wizard_passo1_hospitais()
    elif passo == 2: _wizard_passo2_fixas()
    elif passo == 3: _wizard_passo3_dividas()
    elif passo == 4: _wizard_passo4_envelopes()
    else: _wizard_passo5_revisao()

# =================================================================
# 8+. INTERFACE UX — USO DIÁRIO, ANÁLISE E CONFIGURAÇÕES
# =================================================================


def _valor_previsto_linha(r):
    v = float_seguro(r.get('valor'))
    if int_seguro(r.get('eh_orcamento')) == 1:
        return max(v, 0.0)
    return v


def _sub_norm(v):
    return '' if pd.isna(v) else str(v).strip()


def _total_despesa_projetada(df):
    """Realizado + compromissos conhecidos + saldo ainda livre dos limites, sem dupla contagem."""
    if df.empty:
        return 0.0
    d = df[df['tipo'] == 'Despesa'].copy()
    if d.empty:
        return 0.0
    d['valor'] = pd.to_numeric(d['valor'], errors='coerce').fillna(0.0)
    d['valor_pago'] = pd.to_numeric(d['valor_pago'], errors='coerce').fillna(0.0)
    eh_orc = d['eh_orcamento'].fillna(0).astype(int) == 1
    normal = d[~eh_orc]
    total = float(normal.apply(lambda r: float(r['valor_pago']) if int_seguro(r.get('pago')) == 1 else float(r['valor']), axis=1).sum()) if not normal.empty else 0.0
    pendentes = normal[normal['pago'] == 0]
    for _, o in d[eh_orc].iterrows():
        key_cat, key_sub = o['categoria'], _sub_norm(o.get('subgrupo'))
        pend_key = pendentes[(pendentes['categoria'] == key_cat) & (pendentes['subgrupo'].apply(_sub_norm) == key_sub)]
        compromisso_pendente = float(pend_key['valor'].sum()) if not pend_key.empty else 0.0
        # A VIEW já retirou os pagamentos realizados do saldo. Retiramos também
        # compromissos pendentes conhecidos antes de somar o saldo livre ao forecast.
        total += max(float(o['valor']) - compromisso_pendente, 0.0)
    return total


def _total_despesa_planejada(df):
    """Orçamento original: limites substituem as compras que pertencem a eles."""
    if df.empty:
        return 0.0
    d = df[df['tipo'] == 'Despesa'].copy()
    if d.empty:
        return 0.0
    eh_orc = d['eh_orcamento'].fillna(0).astype(int) == 1
    orcs = d[eh_orc]
    keys = {(r['categoria'], _sub_norm(r.get('subgrupo'))) for _, r in orcs.iterrows()}
    total = 0.0
    for _, o in orcs.iterrows():
        total += float(o['valor_orcamento']) if pd.notna(o.get('valor_orcamento')) else max(float(o['valor']), 0.0)
    for _, r in d[~eh_orc].iterrows():
        if (r['categoria'], _sub_norm(r.get('subgrupo'))) in keys:
            continue
        total += float(r['valor'] or 0.0)
    return total


def _descricao_exibicao(r):
    if pd.notna(r.get('total_parcelas')) and float_seguro(r.get('total_parcelas')) > 1 and int_seguro(r.get('total_parcelas')) != 999:
        return f"{r['descricao']} ({int_seguro(r.get('parcela_atual'), 1)}/{int_seguro(r.get('total_parcelas'), 1)})"
    return str(r.get('descricao') or '')


def _marcar_ids(ids, pago=True, data_pagamento=None):
    ids = [int(x) for x in ids]
    if not ids: return
    with transaction() as cur:
        if pago:
            cur.execute("UPDATE lancamentos SET pago=1, valor_pago=CASE WHEN COALESCE(valor_pago,0)=0 THEN valor ELSE valor_pago END, data_pagamento=%s WHERE id = ANY(%s)",
                        (data_pagamento or hoje, ids))
        else:
            cur.execute("UPDATE lancamentos SET pago=0, valor_pago=0, data_pagamento=NULL WHERE id = ANY(%s)", (ids,))

def _registrar_pagamento_ids(ids, valor_real_total=None, data_pagamento=None):
    """
    Marca um lançamento/lote como pago ou recebido sem alterar o planejado.
    Se valor_real_total vier vazio/zero, usa a soma dos valores planejados.
    Em lotes consolidados, distribui o realizado proporcionalmente entre as
    linhas reais para que os relatórios somem exatamente o total informado.
    """
    ids = [int(x) for x in ids]
    if not ids:
        return 0.0
    data_ref = data_pagamento or hoje
    with transaction() as cur:
        cur.execute("SELECT id, COALESCE(valor,0) FROM lancamentos WHERE id = ANY(%s) ORDER BY id", (ids,))
        linhas = cur.fetchall()
        if not linhas:
            raise ValueError("Nenhum lançamento real encontrado para este pagamento.")

        planejados = [max(float_seguro(v), 0.0) for _, v in linhas]
        total_planejado = round(sum(planejados), 2)
        total_real = float_seguro(valor_real_total, 0.0)
        if abs(total_real) <= 0.004:
            total_real = total_planejado
        if total_real < 0:
            raise ValueError("O valor pago/recebido não pode ser negativo.")

        soma_pesos = sum(planejados)
        if soma_pesos <= 0:
            planejados = [1.0] * len(linhas)
            soma_pesos = float(len(linhas))

        acumulado = 0.0
        for pos, ((lanc_id, _), peso) in enumerate(zip(linhas, planejados)):
            if pos == len(linhas) - 1:
                valor_linha = round(total_real - acumulado, 2)
            else:
                valor_linha = round(total_real * peso / soma_pesos, 2)
                acumulado = round(acumulado + valor_linha, 2)
            cur.execute(
                "UPDATE lancamentos SET pago=1, valor_pago=%s, data_pagamento=%s WHERE id=%s",
                (valor_linha, data_ref, int(lanc_id))
            )
    return total_real

def _consolidar_operacional(df):
    cols_saida = ['id_ui','tipo','categoria','descricao','valor','valor_pago','pago','data_vencimento','data_pagamento','prioridade','ids','consolidado','ordem_pri','atrasado','ordem_atraso']
    if df.empty: return pd.DataFrame(columns=cols_saida)
    base = df.copy()
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)
    linhas = []
    # Orçamentos/limites não são contas a pagar e ficam fora do fluxo operacional.
    base = base[base['eh_orcamento'].fillna(0).astype(int) == 0].copy()
    mask_cred = (base['tipo'] == 'Despesa') & (base['forma_pagamento'] == 'Crédito')
    if mask_cred.any():
        grp = base[mask_cred]
        all_paid = bool((grp['pago'] == 1).all())
        datas_pg = pd.to_datetime(grp['data_pagamento'], errors='coerce').dropna() if 'data_pagamento' in grp.columns else pd.Series(dtype='datetime64[ns]')
        data_pg = datas_pg.max().date() if all_paid and not datas_pg.empty else None
        linhas.append({
            'id_ui':'cartao', 'tipo':'Despesa', 'categoria':'Cartão de Crédito', 'descricao':f"💳 Fatura do cartão · {len(grp)} compra(s)",
            'valor':float(grp['valor'].sum()), 'valor_pago':float(grp['valor_pago'].sum()), 'pago':1 if all_paid else 0,
            'data_vencimento':pd.to_datetime(grp['data_vencimento']).min().date(), 'data_pagamento':data_pg, 'prioridade':'Alta 🔴',
            'ids':grp['id'].astype(int).tolist(), 'consolidado':True,
        })
    restante = base[~mask_cred].copy()
    mask_plant = (restante['tipo'] == 'Entrada') & restante['descricao'].str.contains('plant', case=False, na=False)
    plant = restante[mask_plant].copy()
    if not plant.empty:
        def _grupo_hospital(r):
            cat = str(r.get('categoria') or '').strip()
            sub = str(r.get('subgrupo') or '').strip()
            cat_norm = cat.lower().replace('õ','o').replace('ã','a')
            return sub if cat_norm in ('plantoes','plantao') and sub else cat
        plant['_grupo_hospital'] = plant.apply(_grupo_hospital, axis=1)
        for (hospital, dt), grp in plant.groupby(['_grupo_hospital','data_vencimento']):
            all_paid = bool((grp['pago'] == 1).all())
            datas_pg = pd.to_datetime(grp['data_pagamento'], errors='coerce').dropna() if 'data_pagamento' in grp.columns else pd.Series(dtype='datetime64[ns]')
            data_pg = datas_pg.max().date() if all_paid and not datas_pg.empty else None
            linhas.append({
                'id_ui':f"plant_{hospital}_{dt}", 'tipo':'Entrada', 'categoria':hospital, 'descricao':f"🏥 {hospital} · {len(grp)} plantão(ões)",
                'valor':float(grp['valor'].sum()), 'valor_pago':float(grp['valor_pago'].sum()), 'pago':1 if all_paid else 0,
                'data_vencimento':pd.to_datetime(dt).date(), 'data_pagamento':data_pg, 'prioridade':'Baixa 🟢',
                'ids':grp['id'].astype(int).tolist(), 'consolidado':True,
            })
    for _, r in restante[~mask_plant].iterrows():
        data_pg = pd.to_datetime(r.get('data_pagamento'), errors='coerce')
        linhas.append({
            'id_ui':str(r['id']), 'tipo':r['tipo'], 'categoria':r['categoria'], 'descricao':_descricao_exibicao(r),
            'valor':float(r['valor']), 'valor_pago':float_seguro(r.get('valor_pago')), 'pago':int_seguro(r.get('pago')),
            'data_vencimento':pd.to_datetime(r['data_vencimento']).date(), 'data_pagamento':data_pg.date() if pd.notna(data_pg) else None, 'prioridade':r['prioridade'],
            'ids':[int(r['id'])], 'consolidado':False,
        })
    out = pd.DataFrame(linhas) if linhas else pd.DataFrame(columns=cols_saida)
    if not out.empty:
        out['ordem_pri'] = out['prioridade'].map(prioridades_map).fillna(2)
        out['atrasado'] = (out['pago'] == 0) & (out['data_vencimento'] < hoje)
        out['ordem_atraso'] = (~out['atrasado']).astype(int)
        out = out.sort_values(['ordem_atraso','data_vencimento','ordem_pri']).reset_index(drop=True)
    return out


def _render_linhas_operacionais(df_ops, prefixo, max_linhas=None, permitir_editar=False):
    if df_ops.empty:
        st.info("Nada para mostrar neste filtro.")
        return

    dados = df_ops.head(max_linhas) if max_linhas else df_ops
    for i, r in dados.iterrows():
        atrasado = bool(r['atrasado'])
        pago = int_seguro(r.get('pago')) == 1
        status = "🔴" if atrasado else ("✅" if pago else "🟡")
        data_txt = pd.to_datetime(r['data_vencimento']).strftime('%d/%m')
        planejado = float_seguro(r.get('valor'))
        realizado = float_seguro(r.get('valor_pago'))
        valor_mostrar = realizado if pago and realizado > 0 else planejado

        # A lista principal fica enxuta; detalhes financeiros aparecem só quando necessários.
        if permitir_editar:
            c1, c2, c3, c4 = st.columns([4.8, 1.45, 1.25, .85])
        else:
            c1, c2, c3 = st.columns([5.25, 1.55, 1.25])
            c4 = None

        categoria_txt = '' if pd.isna(r.get('categoria')) else str(r.get('categoria') or '')
        valor_label = "recebido" if r['tipo'] == 'Entrada' and pago else ("pago" if pago else "planejado")
        c1.markdown(
            f"<div class='ux-row'>{status} <b>{data_txt}</b> · {r['descricao']}"
            f"<br><span class='ux-muted'>{categoria_txt}</span></div>",
            unsafe_allow_html=True
        )
        c2.markdown(
            f"<div style='text-align:right;padding:.56rem .1rem 0;font-variant-numeric:tabular-nums;'>"
            f"<b>R$ {format_brl(valor_mostrar)}</b><br><span class='ux-muted'>{valor_label}</span></div>",
            unsafe_allow_html=True
        )

        chave_acao = f"{prefixo}:{r['id_ui']}"
        if pago:
            if c3.button("↩ Estornar", key=f"{prefixo}_est_{i}_{r['id_ui']}", use_container_width=True):
                _marcar_ids(r['ids'], pago=False)
                if st.session_state.get('_pagamento_aberto') == chave_acao:
                    st.session_state.pop('_pagamento_aberto', None)
                flash('success', 'Pagamento/recebimento estornado. O valor planejado foi preservado.')
                st.rerun()
        else:
            rotulo = "✓ Pagar" if r['tipo'] == 'Despesa' else "✓ Receber"
            if c3.button(rotulo, key=f"{prefixo}_pay_{i}_{r['id_ui']}", type="primary" if atrasado else "secondary", use_container_width=True):
                st.session_state['_pagamento_aberto'] = chave_acao
                st.rerun()

        if c4 is not None:
            if bool(r.get('consolidado')):
                c4.caption("lote")
            elif c4.button("Editar", key=f"{prefixo}_edit_{i}_{r['id_ui']}", use_container_width=True):
                st.session_state['fluxo_editar_id'] = int(r['ids'][0])
                st.session_state['fluxo_editor_aberto'] = True
                st.rerun()

        # Caixa de pagamento aparece somente para o item cujo botão foi clicado.
        if (not pago) and st.session_state.get('_pagamento_aberto') == chave_acao:
            acao_nome = "pagamento" if r['tipo'] == 'Despesa' else "recebimento"
            with st.container(border=True):
                st.markdown(f"**Confirmar {acao_nome} · {r['descricao']}**")
                st.caption(
                    f"Planejado: R$ {format_brl(planejado)} · "
                    "deixe o valor abaixo em branco para usar automaticamente o planejado."
                )
                with st.form(f"form_pagamento_{prefixo}_{i}_{r['id_ui']}"):
                    f1, f2 = st.columns([1.4, 1])
                    valor_txt = f1.text_input(
                        "Valor efetivamente pago" if r['tipo'] == 'Despesa' else "Valor efetivamente recebido",
                        value="",
                        placeholder=f"Em branco = R$ {format_brl(planejado)}",
                        key=f"valor_pag_{prefixo}_{i}_{r['id_ui']}"
                    )
                    data_real = f2.date_input(
                        "Data", value=hoje, format="DD/MM/YYYY",
                        key=f"data_pag_{prefixo}_{i}_{r['id_ui']}"
                    )
                    b1, b2 = st.columns(2)
                    confirmar = b1.form_submit_button(
                        "Confirmar pagamento" if r['tipo'] == 'Despesa' else "Confirmar recebimento",
                        type="primary", use_container_width=True
                    )
                    cancelar = b2.form_submit_button("Cancelar", use_container_width=True)

                if cancelar:
                    st.session_state.pop('_pagamento_aberto', None)
                    st.rerun()

                if confirmar:
                    valor_informado = parse_valor(valor_txt) if str(valor_txt).strip() else 0.0
                    try:
                        total_real = _registrar_pagamento_ids(
                            r['ids'], valor_real_total=valor_informado, data_pagamento=data_real
                        )
                    except Exception as e:
                        st.error(f"Não foi possível registrar o {acao_nome}: {e}")
                    else:
                        st.session_state.pop('_pagamento_aberto', None)
                        diferenca = total_real - planejado
                        if abs(diferenca) > 0.004:
                            sinal = "+" if diferenca > 0 else "-"
                            msg = (
                                f"{acao_nome.capitalize()} registrado: R$ {format_brl(total_real)} "
                                f"({sinal} R$ {format_brl(abs(diferenca))} vs. planejado)."
                            )
                        else:
                            msg = f"{acao_nome.capitalize()} registrado por R$ {format_brl(total_real)}."
                        flash('success', msg)
                        st.rerun()


def _dados_mes():
    df_mes_local = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento", (inicio_periodo, fim_periodo))
    if df_mes_local.empty and len(df_mes_local.columns) == 0:
        return pd.DataFrame(columns=['id','tipo','categoria','subgrupo','descricao','valor','data_vencimento','parcela_atual','total_parcelas','pago','compra_id','forma_pagamento','prioridade','valor_pago','eh_estimativa','data_competencia','data_pagamento','eh_orcamento','valor_orcamento'])
    return df_mes_local


if st.session_state.get('wizard_ativo'):
    renderizar_wizard_configuracao()

# -----------------------------------------------------------------
# INÍCIO
# -----------------------------------------------------------------
elif menu == "🏠 Início":
    cabecalho_pagina(f"🏠 Visão de {meses[mes_selecionado-1]}", "O que já aconteceu, o que ainda falta e o que exige sua atenção.", "inicio")
    df_mes = _dados_mes()
    if df_mes.empty:
        st.info("Ainda não há lançamentos neste mês.")
        c1, c2 = st.columns(2)
        if c1.button("＋ Criar primeiro lançamento", type="primary", use_container_width=True):
            st.session_state.menu_atual = "📝 Lançamentos"; st.rerun()
        if c2.button("🏥 Registrar plantão", use_container_width=True):
            st.session_state.menu_atual = "🏥 Escala de Plantões"; st.rerun()
    else:
        df_mes['valor'] = pd.to_numeric(df_mes['valor'], errors='coerce').fillna(0.0)
        df_mes['valor_pago'] = pd.to_numeric(df_mes['valor_pago'], errors='coerce').fillna(0.0)
        ent = df_mes[df_mes['tipo']=='Entrada']
        desp = df_mes[df_mes['tipo']=='Despesa']
        recebido = float(ent[ent['pago']==1]['valor_pago'].sum())
        a_receber = float(ent[ent['pago']==0]['valor'].sum())
        pago = float(desp[(desp['pago']==1) & (desp['eh_orcamento'].fillna(0).astype(int)==0)]['valor_pago'].sum())
        despesa_projetada = _total_despesa_projetada(df_mes)
        saldo_proj = recebido + a_receber - despesa_projetada

        m1,m2,m3,m4 = st.columns(4)
        m1.metric("✅ Já recebi", f"R$ {format_brl(recebido)}")
        m2.metric("💸 Já paguei", f"R$ {format_brl(pago)}")
        m3.metric("⏳ Ainda vou receber", f"R$ {format_brl(a_receber)}")
        m4.metric("🎯 Saldo projetado", f"R$ {format_brl(saldo_proj)}")

        st.markdown("<div class='ux-section-title'>Ações rápidas</div>", unsafe_allow_html=True)
        a1,a2,a3 = st.columns(3)
        if a1.button("＋ Novo lançamento", type="primary", use_container_width=True):
            st.session_state.menu_atual="📝 Lançamentos"; st.rerun()
        if a2.button("🏥 Registrar plantão", use_container_width=True):
            st.session_state.menu_atual="🏥 Escala de Plantões"; st.rerun()
        if a3.button("📋 Abrir fluxo do mês", use_container_width=True):
            st.session_state.menu_atual="📊 Fluxo e Prioridades"; st.rerun()

        st.markdown("<div class='ux-section-title'>Hoje</div>", unsafe_allow_html=True)
        ops = _consolidar_operacional(df_mes)
        atrasados = ops[(ops['pago']==0) & (ops['data_vencimento'] < hoje)] if not ops.empty else pd.DataFrame()
        proximos = ops[(ops['pago']==0) & (ops['data_vencimento'] >= hoje) & (ops['data_vencimento'] <= hoje + datetime.timedelta(days=7))] if not ops.empty else pd.DataFrame()
        c1,c2,c3 = st.columns(3)
        c1.metric("🔴 Atrasados", len(atrasados), f"R$ {format_brl(atrasados['valor'].sum())}" if not atrasados.empty else "R$ 0,00")
        c2.metric("🟡 Próximos 7 dias", len(proximos), f"R$ {format_brl(proximos[proximos['tipo']=='Despesa']['valor'].sum())}" if not proximos.empty else "R$ 0,00")
        entradas7 = proximos[proximos['tipo']=='Entrada'] if not proximos.empty else pd.DataFrame()
        c3.metric("🟢 Entradas em 7 dias", len(entradas7), f"R$ {format_brl(entradas7['valor'].sum())}" if not entradas7.empty else "R$ 0,00")

        if not atrasados.empty:
            st.subheader("🔴 Precisa de atenção")
            _render_linhas_operacionais(atrasados, 'home_atraso', max_linhas=6)
        if not proximos.empty:
            st.subheader("Próximos 7 dias")
            _render_linhas_operacionais(proximos, 'home_7d', max_linhas=7)

        st.markdown("<div class='ux-section-title'>Reserva de emergência</div>", unsafe_allow_html=True)
        reserva_atual, reserva_atualizada_em = obter_reserva_emergencia()
        media_despesa_mensal, n_meses_com_dados = calcular_media_despesa_mensal(hoje)
        meses_sobrevivencia = reserva_atual / media_despesa_mensal if media_despesa_mensal > 0 else 0
        r1,r2 = st.columns([2,1])
        with r1:
            cobertura_txt = f"{meses_sobrevivencia:.1f} meses de despesas · média de {n_meses_com_dados} mês(es) fechado(s)" if media_despesa_mensal > 0 else "Registre ao menos um mês fechado de despesas para calcular a cobertura"
            st.markdown(
                f"<div class='ux-card'><span class='ux-muted'>Reserva atual</span>"
                f"<div class='ux-value'>R$ {format_brl(reserva_atual)}</div>"
                f"<span class='ux-muted'>{cobertura_txt}</span></div>",
                unsafe_allow_html=True,
            )
        with r2:
            with st.expander("Atualizar reserva"):
                novo = st.text_input("Valor (R$)", value=format_brl(reserva_atual), key="reserva_home")
                if st.button("Salvar", type="primary", key="salvar_reserva_home", use_container_width=True):
                    atualizar_reserva_emergencia(parse_valor(novo)); flash('success','Reserva atualizada.'); st.rerun()

# -----------------------------------------------------------------
# NOVO LANÇAMENTO
# -----------------------------------------------------------------
elif menu == "📝 Lançamentos":
    cabecalho_pagina("➕ Novo Lançamento", "Registre o essencial primeiro; detalhes avançados ficam opcionais.", "novo")
    tipo = st.radio("O que aconteceu?", ["Despesa","Entrada"], horizontal=True, key="novo_tipo")
    if not ESTRUTURA.get(tipo):
        st.warning("Você ainda não tem categorias para este tipo. Crie uma em Categorias e Automações.")
        if st.button("Abrir Categorias", use_container_width=True): st.session_state.menu_atual="⚙️ Gerenciar Categorias"; st.rerun()
    else:
        c1,c2 = st.columns([2,1.2])
        descricao = c1.text_input("Descrição", placeholder="Ex: Aluguel, supermercado, plantão extra")
        valor_input = c2.text_input("Valor (R$)", value="0,00")
        c3,c4,c5 = st.columns([1.5,1.5,1])
        categoria = c3.selectbox("Categoria", list(ESTRUTURA[tipo].keys()))
        subs = ESTRUTURA[tipo].get(categoria, [])
        subgrupo = c4.selectbox("Subgrupo", subs if subs else [""])
        data_ref = c5.date_input("Data", value=data_contexto_ativo, format="DD/MM/YYYY")

        with st.expander("Mais opções"):
            a1,a2 = st.columns(2)
            forma_pgto = a1.selectbox("Forma de pagamento", ["À vista","Crédito","Outros"], index=0 if tipo=='Entrada' else 1)
            prioridade = a2.radio("Prioridade", ["Baixa 🟢","Média 🟡","Alta 🔴"], horizontal=True)
            rec_label = st.radio("Repetição", ["Uma vez","Parcelada","Repete todo mês"], horizontal=True)
            parcelas = 1
            if rec_label == "Parcelada":
                parcelas = st.number_input("Número de parcelas", min_value=2, max_value=240, value=2)
            elif rec_label == "Repete todo mês":
                st.caption("A interface mostra uma recorrência mensal; internamente o app mantém uma janela futura de 60 meses, como na versão anterior.")
                parcelas = 60
            pago_imediato = st.checkbox("Já foi pago/recebido")
            data_pgto = st.date_input("Data efetiva do pagamento/recebimento", value=hoje, format="DD/MM/YYYY", disabled=not pago_imediato)

        if st.button("Registrar lançamento", type="primary", use_container_width=True):
            val = parse_valor(valor_input)
            if not descricao.strip(): st.error("Informe uma descrição.")
            elif val <= 0: st.error("O valor deve ser maior que zero.")
            else:
                comp_id = str(uuid.uuid4())
                total_p = 999 if rec_label == "Repete todo mês" else int(parcelas)
                regs=[]
                for i in range(int(parcelas)):
                    m = data_ref.month - 1 + i; a = data_ref.year + m//12; m = m%12+1
                    d = datetime.date(a,m,min(data_ref.day, calendar.monthrange(a,m)[1]))
                    p = 1 if pago_imediato and i==0 else 0
                    vp = val if p else 0.0
                    dp = data_pgto if p else None
                    regs.append((tipo,categoria,subgrupo or None,descricao.strip(),val,d,i+1,total_p,p,comp_id,forma_pgto,prioridade,vp,d,dp))
                try:
                    execute_values_query("INSERT INTO lancamentos (tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia,data_pagamento) VALUES %s", regs)
                except Exception:
                    pass
                else:
                    flash('success','✅ Lançamento registrado.'); st.rerun()

# 11. MÓDULO 2: FLUXO E PRIORIDADES
# =================================================================

elif menu == "📊 Fluxo e Prioridades":
    cabecalho_pagina("📋 Fluxo do Mês", "Ações rápidas primeiro; a edição completa continua disponível abaixo.", "fluxo")
    st.caption("Pagamento inline ativo: clique em Pagar/Receber para informar o valor real; vazio = planejado.")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento ASC", (inicio_periodo, fim_periodo))

    if df.empty: st.warning("Sem dados.")
    else:
        df['valor'] = df['valor'].astype(float)
        df['valor_pago'] = df['valor_pago'].fillna(0.0).astype(float)

        # Camada operacional simples: mantém o editor completo abaixo, mas o uso diário
        # não exige abrir uma planilha com todas as colunas.
        ops_rapido = _consolidar_operacional(df)
        filtro_rapido = st.radio("Mostrar", ["Todos","A pagar","A receber","Pagos","Atrasados"], horizontal=True, label_visibility="collapsed", key="fluxo_rapido_status")
        fr1, fr2 = st.columns(2)
        tipos_rapidos = fr1.multiselect("Tipo", ["Despesa","Entrada"], placeholder="Entradas e despesas", key="fluxo_rapido_tipos")
        cats_rapidas = sorted([x for x in ops_rapido['categoria'].dropna().unique().tolist() if x]) if not ops_rapido.empty else []
        cats_sel_rapidas = fr2.multiselect("Categoria", cats_rapidas, placeholder="Todas as categorias", key="fluxo_rapido_cats")
        vis_rapida = ops_rapido.copy()
        if tipos_rapidos: vis_rapida = vis_rapida[vis_rapida['tipo'].isin(tipos_rapidos)]
        if filtro_rapido == "A pagar": vis_rapida = vis_rapida[(vis_rapida['tipo']=='Despesa') & (vis_rapida['pago']==0)]
        elif filtro_rapido == "A receber": vis_rapida = vis_rapida[(vis_rapida['tipo']=='Entrada') & (vis_rapida['pago']==0)]
        elif filtro_rapido == "Pagos": vis_rapida = vis_rapida[vis_rapida['pago']==1]
        elif filtro_rapido == "Atrasados": vis_rapida = vis_rapida[vis_rapida['atrasado']]
        if cats_sel_rapidas: vis_rapida = vis_rapida[vis_rapida['categoria'].isin(cats_sel_rapidas)]
        _render_linhas_operacionais(vis_rapida, 'fluxo_rapido')

        st.caption("Use a lista acima para pagar/receber. Abra as ferramentas avançadas apenas para edições estruturais, séries ou exclusões em lote.")

        with st.expander("⚙️ Edição avançada e ferramentas", expanded=False):
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

            mask_cred_full = (df_base['tipo'] == 'Despesa') & (df_base['forma_pagamento'] == 'Crédito')
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
                    'data_pagamento': data_pg_cred, 'eh_orcamento': 0, 'valor_orcamento': None,
                    'parcela_atual': 1, 'total_parcelas': 1
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
                def _grupo_hospital_fluxo(r):
                    cat = str(r.get('categoria') or '').strip()
                    sub = str(r.get('subgrupo') or '').strip()
                    cat_norm = cat.lower().replace('õ','o').replace('ã','a')
                    return sub if cat_norm in ('plantoes','plantao') and sub else cat
                df_plantoes_full['_grupo_hospital'] = df_plantoes_full.apply(_grupo_hospital_fluxo, axis=1)
                for nome_grupo, grupo in df_plantoes_full.groupby(['_grupo_hospital', 'data_vencimento']):
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
                        'data_pagamento': data_pg_plant, 'eh_orcamento': 0, 'valor_orcamento': None,
                        'parcela_atual': 1, 'total_parcelas': 1
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
            if 'eh_orcamento' not in df_view.columns:
                df_view['eh_orcamento'] = 0
            df_view['eh_orcamento'] = pd.to_numeric(df_view['eh_orcamento'], errors='coerce').fillna(0).astype(int)
            df_view['ordem_pri'] = df_view['prioridade'].map(prioridades_map).fillna(2)
            df_view = df_view.sort_values(['data_vencimento', 'ordem_pri']).reset_index(drop=True)
            df_view['Pago'] = df_view['pago'].astype(bool)
            df_view['Data'] = pd.to_datetime(df_view['data_vencimento']).dt.date
            df_view['Data Pagamento'] = pd.to_datetime(df_view['data_pagamento'], errors='coerce').dt.date

            def calcular_alerta_atraso(row):
                if int_seguro(row.get('eh_orcamento')) == 1:
                    return "🧮 Orçamento derivado"
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
                "Ao marcar **Pago**, se o valor real estiver 0/vazio, o app usa automaticamente o Planejado. "
                "Se o real for diferente, informe o valor recebido/pago e o Planejado será preservado.*"
            )
            edit_df = st.data_editor(
                df_view[['🗑️ Excluir', 'Data', 'Data Pagamento', 'Alerta', 'prioridade', 'Desc. Exibição', 'valor', 'valor_pago', 'Pago']],
                use_container_width=True, hide_index=True,
                column_config={
                    "🗑️ Excluir": st.column_config.SelectboxColumn("Excluir", options=["", "Este", "Este e Futuros"], width="small"),
                    "Data": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY"),
                    "Data Pagamento": st.column_config.DateColumn("Pago em", format="DD/MM/YYYY"),
                    "Alerta": st.column_config.TextColumn("Status", disabled=True),
                    "valor": st.column_config.NumberColumn("Planejado", format="%.2f"),
                    "valor_pago": st.column_config.NumberColumn("Pago/Recebido", format="%.2f"),
                    "prioridade": st.column_config.SelectboxColumn("Prioridade", options=["Alta 🔴", "Média 🟡", "Baixa 🟢"]),
                    "Desc. Exibição": st.column_config.TextColumn("Descrição", disabled=False)
                }
            )

            edit_df['tipo'] = df_view['tipo'].values
            edit_df['ordem_pri'] = df_view['ordem_pri'].values
            edit_df['eh_orcamento'] = pd.to_numeric(df_view['eh_orcamento'], errors='coerce').fillna(0).astype(int).values

            if st.button("Salvar Alterações Rápidas", type="primary"):
                try:
                    with transaction() as cur:
                        for i, row in edit_df.iterrows():
                            orig_row = df_view.loc[i]
                            id_s = str(orig_row['id'])
                            novo_pago = 1 if bool(row['Pago']) else 0
                            novo_valor = float_seguro(row.get('valor'))
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
                            nova_desc = row['Desc. Exibição'].split(' (')[0] if desc_editada else orig_row['descricao']
                            tupla_ids_reais = tuple(map(int, orig_row['ids_alvo'].split(',')))
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
                                    # Se o usuário deixou o real em 0/vazio, resolver_valor_real() já
                                    # trouxe o total planejado. Se digitou outro valor, distribuímos
                                    # esse realizado entre as linhas reais sem tocar no planejamento.
                                    cur.execute("SELECT id, COALESCE(valor,0) FROM lancamentos WHERE id IN %s ORDER BY id", (tupla_ids_reais,))
                                    linhas_grupo = cur.fetchall()
                                    pesos = [max(float_seguro(v), 0.0) for _, v in linhas_grupo]
                                    soma_pesos = sum(pesos)
                                    if soma_pesos <= 0 and linhas_grupo:
                                        pesos = [1.0] * len(linhas_grupo)
                                        soma_pesos = float(len(linhas_grupo))
                                    acumulado_real = 0.0
                                    for pos_g, ((id_g, _), peso_g) in enumerate(zip(linhas_grupo, pesos)):
                                        if pos_g == len(linhas_grupo) - 1:
                                            valor_real_g = round(novo_valor_pago - acumulado_real, 2)
                                        else:
                                            valor_real_g = round(novo_valor_pago * peso_g / soma_pesos, 2)
                                            acumulado_real = round(acumulado_real + valor_real_g, 2)
                                        cur.execute("UPDATE lancamentos SET valor_pago=%s, data_pagamento=%s WHERE id=%s", (valor_real_g, nova_data_pgto, int(id_g)))
                                if abs(novo_valor - orig_valor) > 0.004:
                                    id_alvo_planejado = int(tupla_ids_reais[-1])
                                    cur.execute("UPDATE lancamentos SET valor = valor + %s WHERE id = %s", (novo_valor - orig_valor, id_alvo_planejado))
                                continue

                            eh_orcamento = int_seguro(orig_row.get('eh_orcamento')) == 1
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
    cabecalho_pagina("📊 Demonstrativo", "Planejado, realizado, detalhamento e limites no mesmo lugar.", "demo")
    df = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s", (inicio_periodo, fim_periodo))

    tab_res, tab_dem, tab_env = st.tabs(["📊 Resumo", "📋 Detalhamento", "🎯 Limites mensais"])


    with tab_res:
        if df.empty:
            st.info("Sem dados neste período.")
        else:
            df_res = df.copy()
            df_res['valor'] = pd.to_numeric(df_res['valor'], errors='coerce').fillna(0.0)
            df_res['valor_pago'] = pd.to_numeric(df_res['valor_pago'], errors='coerce').fillna(0.0)
            ent_res = df_res[df_res['tipo']=='Entrada']
            desp_res = df_res[df_res['tipo']=='Despesa']
            rec_plan = float(ent_res['valor'].sum())
            rec_real = float(ent_res[ent_res['pago']==1]['valor_pago'].sum())
            desp_plan = _total_despesa_planejada(df_res)
            desp_real = float(desp_res[(desp_res['pago']==1) & (desp_res['eh_orcamento'].fillna(0).astype(int)==0)]['valor_pago'].sum())
            r1,r2,r3 = st.columns(3)
            r1.metric("Receitas planejadas", f"R$ {format_brl(rec_plan)}")
            r2.metric("Despesas planejadas", f"R$ {format_brl(desp_plan)}")
            r3.metric("Saldo planejado", f"R$ {format_brl(rec_plan-desp_plan)}")
            q1,q2,q3 = st.columns(3)
            q1.metric("Recebido", f"R$ {format_brl(rec_real)}")
            q2.metric("Pago", f"R$ {format_brl(desp_real)}")
            q3.metric("Resultado realizado", f"R$ {format_brl(rec_real-desp_real)}")
            gasto_res = desp_res[(desp_res['eh_orcamento'].fillna(0).astype(int)==0)].copy()
            if not gasto_res.empty:
                gasto_res['base_gasto'] = gasto_res.apply(lambda r: float(r['valor_pago']) if int_seguro(r.get('pago'))==1 else float(r['valor']), axis=1)
                grp_res = gasto_res.groupby('categoria')['base_gasto'].sum().sort_values().reset_index()
                st.subheader("Onde o dinheiro está saindo")
                fig_res = px.bar(grp_res, x='base_gasto', y='categoria', orientation='h', labels={'base_gasto':'R$','categoria':''})
                st.plotly_chart(aplicar_tema_grafico(fig_res), use_container_width=True)

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
            falta_pagar = _df_falta_pagar.apply(lambda r: max(float(r['valor']), 0.0) if int_seguro(r.get('eh_orcamento')) == 1 else float(r['valor']), axis=1).sum()

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
                dataframe['Desc. Exibição'] = dataframe.apply(lambda r: f"{r['descricao']} ({int_seguro(r.get('parcela_atual'), 1)}/{int_seguro(r.get('total_parcelas'), 1)})" if pd.notna(r.get('total_parcelas')) and r['total_parcelas'] > 1 and r['total_parcelas'] != 999 else r['descricao'], axis=1)
                dataframe['Status'] = dataframe.apply(lambda r: '🧮 Orçamento' if int_seguro(r.get('eh_orcamento')) == 1 else ('✅ Pago' if r['pago'] == 1 else '⏳ Pendente'), axis=1)
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
        st.subheader("🎯 Limites mensais")
        st.markdown("Comparação em tempo real entre o limite mensal e o que já foi gasto.")

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
# -----------------------------------------------------------------
# BALANÇO ANUAL
# -----------------------------------------------------------------
elif menu == "📈 Balanço Anual":
    cabecalho_pagina("📈 Balanço Anual", "Veja evolução mensal e compare com o ano anterior.")
    anos=fetch_dataframe("SELECT DISTINCT EXTRACT(YEAR FROM COALESCE(data_pagamento,data_vencimento))::int ano FROM lancamentos ORDER BY ano DESC")
    if anos.empty: st.info('Sem dados suficientes.')
    else:
        lista=anos['ano'].astype(int).tolist(); ano_balanco=st.selectbox('Ano',lista,index=0)
        for m in range(1,13): processar_recorrencias_lazy(m,ano_balanco)
        ia,fa=limites_ano(ano_balanco)
        dfy=fetch_dataframe("SELECT * FROM lancamentos WHERE (pago=1 AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ia,fa,ia,fa))
        if dfy.empty: st.info('Sem dados no ano.')
        else:
            dfy['valor']=pd.to_numeric(dfy['valor'],errors='coerce').fillna(0); dfy['valor_pago']=pd.to_numeric(dfy['valor_pago'],errors='coerce').fillna(0)
            dfy['data_h']=dfy.apply(lambda r:r['data_pagamento'] if int_seguro(r.get('pago'))==1 and pd.notna(r['data_pagamento']) else r['data_vencimento'],axis=1)
            dfy['mes_num']=pd.to_datetime(dfy['data_h']).dt.month
            def _valor_hibrido_ano(r):
                if int_seguro(r.get('eh_orcamento')) != 1:
                    return float(r['valor_pago']) if int_seguro(r.get('pago')) == 1 else float(r['valor'])
                dt = pd.to_datetime(r['data_vencimento'])
                pend = dfy[(dfy['tipo']=='Despesa') & (dfy['pago']==0) & (dfy['eh_orcamento'].fillna(0).astype(int)==0) &
                           (dfy['categoria']==r['categoria']) & (dfy['subgrupo'].apply(_sub_norm)==_sub_norm(r.get('subgrupo'))) &
                           (pd.to_datetime(dfy['data_vencimento']).dt.year==dt.year) & (pd.to_datetime(dfy['data_vencimento']).dt.month==dt.month)]
                return max(float(r['valor']) - float(pd.to_numeric(pend['valor'],errors='coerce').fillna(0).sum()), 0.0)
            dfy['h']=dfy.apply(_valor_hibrido_ano,axis=1)
            mens=dfy.groupby(['mes_num','tipo'])['h'].sum().unstack(fill_value=0).reindex(range(1,13),fill_value=0).reset_index()
            for c in ['Entrada','Despesa']:
                if c not in mens: mens[c]=0.0
            mens['Resultado']=mens['Entrada']-mens['Despesa']; mens['Mês']=mens['mes_num'].apply(lambda m:meses[m-1][:3])
            te=float(mens['Entrada'].sum()); td=float(mens['Despesa'].sum()); res=te-td; margem=res/te*100 if te else 0
            # comparação ano anterior
            ip,fp=limites_ano(ano_balanco-1); prev=fetch_dataframe("SELECT tipo,valor,valor_pago,pago,eh_orcamento FROM lancamentos WHERE (pago=1 AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ip,fp,ip,fp))
            pe=pdv=pr=0.0
            if not prev.empty:
                prev['valor']=pd.to_numeric(prev['valor'],errors='coerce').fillna(0); prev['valor_pago']=pd.to_numeric(prev['valor_pago'],errors='coerce').fillna(0)
                # Comparativo usa a mesma lógica de projeção, evitando somar limite + compras do limite duas vezes.
                prev_ent=prev[prev['tipo']=='Entrada']; pe=float(prev_ent.apply(lambda r:float(r['valor_pago']) if int_seguro(r.get('pago'))==1 else float(r['valor']),axis=1).sum()) if not prev_ent.empty else 0.0
                pdv=_total_despesa_projetada(prev); pr=pe-pdv
            def delta(cur,ant): return f"{((cur/ant)-1)*100:+.1f}% vs {ano_balanco-1}" if ant else None
            q1,q2,q3,q4=st.columns(4); q1.metric('Receita',f"R$ {format_brl(te)}",delta(te,pe)); q2.metric('Despesa',f"R$ {format_brl(td)}",delta(td,pdv),delta_color='inverse'); q3.metric('Resultado',f"R$ {format_brl(res)}",delta(res,pr)); q4.metric('Margem',f"{margem:.1f}%")
            st.subheader('Resultado mês a mês'); fig=px.bar(mens,x='Mês',y='Resultado',labels={'Resultado':'R$'}); st.plotly_chart(aplicar_tema_grafico(fig),use_container_width=True)
            st.subheader('Receitas x despesas'); fig2=px.bar(mens,x='Mês',y=['Entrada','Despesa'],barmode='group'); st.plotly_chart(aplicar_tema_grafico(fig2),use_container_width=True)
            mens['Acumulado'] = mens['Resultado'].cumsum()
            with st.expander('📈 Análises anuais adicionais'):
                st.subheader('Fluxo acumulado')
                fig_acum=px.area(mens,x='Mês',y='Acumulado',markers=True,labels={'Acumulado':'R$'})
                st.plotly_chart(aplicar_tema_grafico(fig_acum),use_container_width=True)
                ca1,ca2=st.columns(2)
                gasto_cat=dfy[dfy['tipo']=='Despesa'].groupby('categoria')['h'].sum().sort_values().reset_index()
                with ca1:
                    st.subheader('Distribuição por categoria')
                    if not gasto_cat.empty:
                        fig_cat=px.bar(gasto_cat,x='h',y='categoria',orientation='h',labels={'h':'R$','categoria':''})
                        st.plotly_chart(aplicar_tema_grafico(fig_cat),use_container_width=True)
                with ca2:
                    st.subheader('Maiores centros de custo (subgrupos)')
                    gasto_sub=dfy[dfy['tipo']=='Despesa'].groupby('subgrupo',dropna=False)['h'].sum().sort_values().tail(12).reset_index()
                    gasto_sub['subgrupo']=gasto_sub['subgrupo'].fillna('Geral')
                    if not gasto_sub.empty:
                        fig_sub=px.bar(gasto_sub,x='h',y='subgrupo',orientation='h',labels={'h':'R$','subgrupo':''})
                        st.plotly_chart(aplicar_tema_grafico(fig_sub),use_container_width=True)
            gasto=dfy[dfy['tipo']=='Despesa'].groupby('categoria')['h'].sum().sort_values().tail(12).reset_index();
            if not gasto.empty:
                st.subheader('Maiores centros de custo'); fig3=px.bar(gasto,x='h',y='categoria',orientation='h',labels={'h':'R$','categoria':''}); st.plotly_chart(aplicar_tema_grafico(fig3),use_container_width=True)

# -----------------------------------------------------------------
# DÍVIDAS
# -----------------------------------------------------------------
elif menu == "💳 Dívidas":
    cabecalho_pagina("💳 Dívidas", "Quanto falta, quanto pesa neste mês e quantos plantões isso representa.", "dividas")
    dd=fetch_dataframe("""SELECT compra_id,categoria,subgrupo,MIN(descricao) descricao,SUM(valor) valor_total,SUM(CASE WHEN pago=1 THEN valor_pago ELSE 0 END) valor_pago_total,MAX(total_parcelas) total_parcelas,SUM(CASE WHEN pago=1 THEN 1 ELSE 0 END) parcelas_pagas,MIN(data_vencimento) data_inicio,MAX(data_vencimento) data_fim,MIN(CASE WHEN pago=0 THEN data_vencimento END) proxima_parcela FROM lancamentos WHERE tipo='Despesa' AND total_parcelas>1 AND total_parcelas!=999 AND compra_id IS NOT NULL GROUP BY compra_id,categoria,subgrupo ORDER BY data_fim""")
    if dd.empty: st.info('Nenhuma dívida parcelada encontrada.')
    else:
        info=fetch_dataframe('SELECT * FROM info_dividas'); dd=dd.merge(info,on='compra_id',how='left'); dd['valor_total']=dd['valor_total'].astype(float); dd['valor_pago_total']=dd['valor_pago_total'].astype(float); dd['saldo']=dd['valor_total']-dd['valor_pago_total']
        total=float(dd['saldo'].clip(lower=0).sum()); ativos=int((dd['saldo']>.01).sum()); rest=int((dd['total_parcelas']-dd['parcelas_pagas']).clip(lower=0).sum()); vm,np=calcular_valor_medio_plantao(hoje)
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
# PLANTÕES
# -----------------------------------------------------------------
elif menu == "🏥 Escala de Plantões":
    cabecalho_pagina("🏥 Plantões", "Escala para consultar; cadastro, produção e manutenção em espaços separados.", "plantoes")
    tab_cal,tab_add,tab_prod,tab_ger=st.tabs(["📅 Escala","➕ Adicionar","📊 Produção","⚙️ Gerenciar"])
    df_t=fetch_dataframe("SELECT * FROM lancamentos WHERE tipo='Entrada' AND descricao LIKE 'Plantão %'")
    if not df_t.empty: df_t['d_p']=pd.to_datetime(df_t['data_competencia'].fillna(df_t['data_vencimento']),errors='coerce').dt.date
    with tab_cal:
        c1,c2=st.columns(2); cal_mes=c1.selectbox('Mês',range(1,13),format_func=lambda x:meses[x-1],index=mes_selecionado-1,key='plant_cal_mes'); anos_cal=list(range(hoje.year-2,hoje.year+4)); cal_ano=c2.selectbox('Ano',anos_cal,index=anos_cal.index(ano_selecionado) if ano_selecionado in anos_cal else 2,key='plant_cal_ano')
        dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==cal_mes)&(pd.to_datetime(df_t['d_p']).dt.year==cal_ano)].copy() if not df_t.empty else pd.DataFrame()
        heads=st.columns(7)
        for i,dn in enumerate(['Seg','Ter','Qua','Qui','Sex','Sáb','Dom']): heads[i].markdown(f"<div style='text-align:center;color:var(--text-muted);font-size:.75rem;font-weight:600'>{dn}</div>",unsafe_allow_html=True)
        for week in calendar.monthcalendar(cal_ano,cal_mes):
            cols=st.columns(7)
            for i,day in enumerate(week):
                if day:
                    cd=datetime.date(cal_ano,cal_mes,day); shifts=dm[dm['d_p']==cd] if not dm.empty else pd.DataFrame(); txt='' if shifts.empty else '<br>'.join([f"🏥 {x}" for x in shifts['subgrupo'].fillna('Local').astype(str).tolist()[:3]])
                    cols[i].markdown(f"<div class='ux-card' style='min-height:88px;padding:.45rem'><b>{day}</b><br><span style='font-size:.7rem'>{txt}</span></div>",unsafe_allow_html=True)
        if not dm.empty:
            dias=sorted(dm['d_p'].unique()); dia_det=st.selectbox('Ver detalhes do dia',dias,format_func=lambda d:pd.to_datetime(d).strftime('%d/%m/%Y'))
            for _,r in dm[dm['d_p']==dia_det].iterrows():
                st.markdown(f"<div class='ux-row'>🏥 <b>{r['subgrupo']}</b> · R$ {format_brl(r['valor'])} · recebe {pd.to_datetime(r['data_vencimento']).strftime('%d/%m')}</div>",unsafe_allow_html=True)
    with tab_add:
        modo=st.radio('Modo',['Dia específico','Plantões fixos na semana'],horizontal=True)
        locais=sorted(set([x for subs in ESTRUTURA.get('Entrada',{}).values() for x in subs]))
        if not locais: st.warning('Cadastre primeiro um local de plantão em Categorias e Automações.')
        else:
            loc=st.selectbox('Local',locais); defaults={'v':1000.0,'m':1,'d':10}; res=fetch_dataframe("SELECT valor_padrao,atraso_meses,dia_pagamento FROM categorias_personalizadas WHERE subgrupo=%s AND tipo='Entrada' LIMIT 1",(loc,))
            if not res.empty:
                if pd.notna(res.iloc[0]['valor_padrao']): defaults['v']=float(res.iloc[0]['valor_padrao'])
                if pd.notna(res.iloc[0]['atraso_meses']): defaults['m']=int(res.iloc[0]['atraso_meses'])
                if pd.notna(res.iloc[0]['dia_pagamento']): defaults['d']=int(res.iloc[0]['dia_pagamento'])
            a,b=st.columns(2); valor=a.number_input('Valor (R$)',value=defaults['v']); atraso=b.number_input('Recebe quantos meses depois?',0,6,defaults['m']); dia_pg=b.number_input('Dia do pagamento',1,31,defaults['d'])
            if modo=='Dia específico': data_p=a.date_input('Data do plantão',value=data_contexto_ativo); dias_sem=None; repetir=1
            else: dias_sem=a.multiselect('Dias da semana',range(7),format_func=lambda x:['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'][x]); repetir=a.number_input('Repetir por meses',1,24,6); data_p=None
            if st.button('Registrar plantão',type='primary',use_container_width=True):
                cat=next((c for c,subs in ESTRUTURA.get('Entrada',{}).items() if loc in subs),'Plantões'); regs=[]
                datas=[]
                if modo=='Dia específico': datas=[data_p]
                else:
                    for off in range(int(repetir)):
                        ma=(mes_selecionado+off-1)%12+1; aa=ano_selecionado+(mes_selecionado+off-1)//12
                        for d in range(1,calendar.monthrange(aa,ma)[1]+1):
                            dt=datetime.date(aa,ma,d)
                            if dt.weekday() in dias_sem: datas.append(dt)
                for dt in datas:
                    mf=(dt.month+int(atraso)-1)%12+1; af=dt.year+(dt.month+int(atraso)-1)//12; ds=min(int(dia_pg),calendar.monthrange(af,mf)[1]); venc=datetime.date(af,mf,ds)
                    regs.append(('Entrada',cat,loc,f"Plantão {loc} ({dt.strftime('%d/%m/%Y')})",valor,venc,1,1,0,str(uuid.uuid4()),'Outros','Baixa 🟢',0.0,dt))
                if regs: execute_values_query("INSERT INTO lancamentos (tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia) VALUES %s",regs); flash('success',f'{len(regs)} plantão(ões) registrado(s).'); st.rerun()
    with tab_prod:
        dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==mes_selecionado)&(pd.to_datetime(df_t['d_p']).dt.year==ano_selecionado)].copy() if not df_t.empty else pd.DataFrame()
        if dm.empty: st.info('Sem plantões no período ativo.')
        else:
            dm['valor']=pd.to_numeric(dm['valor'],errors='coerce').fillna(0); x1,x2,x3=st.columns(3); x1.metric('Plantões',len(dm)); x2.metric('Produção prevista',f"R$ {format_brl(dm['valor'].sum())}"); x3.metric('Média por plantão',f"R$ {format_brl(dm['valor'].mean())}")
            by=dm.groupby('subgrupo')['valor'].agg(['count','sum']).sort_values('sum').reset_index(); fig=px.bar(by,x='sum',y='subgrupo',orientation='h',labels={'sum':'R$','subgrupo':''}); st.plotly_chart(aplicar_tema_grafico(fig),use_container_width=True)
    with tab_ger:
        with st.expander('📥 Importar CSV'):
            st.caption("Colunas: data (DD/MM/AAAA), local e valor opcional. Importar novamente não duplica a mesma descrição de plantão.")
            arq=st.file_uploader('CSV de plantões',type='csv',key='plant_csv')
            if arq is not None:
                try:
                    imp=pd.read_csv(arq); imp.columns=[c.strip().lower() for c in imp.columns]; cd=next((c for c in imp if c in ('data','data_plantao','date')),None); cl=next((c for c in imp if c in ('local','hospital','subgrupo')),None); cv=next((c for c in imp if c in ('valor','value')),None)
                    if not cd or not cl: st.error("O CSV precisa de 'data' e 'local'.")
                    else:
                        defs=fetch_dataframe("SELECT categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento FROM categorias_personalizadas WHERE tipo='Entrada'"); exist=set(df_t['descricao'].tolist()) if not df_t.empty else set(); novos=[]; problemas=[]
                        for _,r in imp.iterrows():
                            try: dt=pd.to_datetime(str(r[cd]).strip(),format='%d/%m/%Y').date()
                            except: problemas.append(str(r[cd])); continue
                            loc=str(r[cl]).strip(); inf=defs[defs['subgrupo'].fillna('').str.strip().str.lower()==loc.lower()]
                            if inf.empty: problemas.append(f'{loc} · local não cadastrado'); continue
                            inf=inf.iloc[0]; desc=f"Plantão {inf['subgrupo']} ({dt.strftime('%d/%m/%Y')})"
                            if desc in exist: continue
                            val=parse_valor(r[cv]) if cv and pd.notna(r.get(cv)) else float_seguro(inf.get('valor_padrao'))
                            if val<=0: problemas.append(f'{desc} · sem valor'); continue
                            am=int(inf['atraso_meses'] or 1); dp=int(inf['dia_pagamento'] or 10); mf=(dt.month+am-1)%12+1; af=dt.year+(dt.month+am-1)//12; venc=datetime.date(af,mf,min(dp,calendar.monthrange(af,mf)[1])); novos.append(('Entrada',inf['categoria'],inf['subgrupo'],desc,val,venc,1,1,0,str(uuid.uuid4()),'Outros','Baixa 🟢',0.0,dt)); exist.add(desc)
                        st.write(f"Novos: **{len(novos)}** · Problemas: **{len(problemas)}**")
                        if problemas: st.caption('Problemas: '+', '.join(problemas[:10]))
                        if novos and st.button('Confirmar importação',type='primary'): execute_values_query("INSERT INTO lancamentos (tipo,categoria,subgrupo,descricao,valor,data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia) VALUES %s",novos); flash('success',f'{len(novos)} plantões importados.'); st.rerun()
                except Exception as e: st.error(f'Erro no CSV: {e}')
        if df_t.empty: st.info('Sem plantões para gerenciar.')
        else:
            dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==mes_selecionado)&(pd.to_datetime(df_t['d_p']).dt.year==ano_selecionado)].copy()
            if dm.empty: st.info('Sem plantões no período ativo.')
            else:
                locais_ger=sorted(dm['subgrupo'].dropna().astype(str).unique().tolist())
                filtro_locais=st.multiselect('Filtrar por hospital/local',locais_ger,placeholder='Todos os locais',key='plant_ger_filtro')
                if filtro_locais: dm=dm[dm['subgrupo'].astype(str).isin(filtro_locais)].copy()
                dm=dm.sort_values('d_p').reset_index(drop=True); dm['Apagar']=False; dm['Data']=pd.to_datetime(dm['d_p']).dt.strftime('%d/%m/%Y'); ed=st.data_editor(dm[['id','Apagar','Data','subgrupo','valor']],hide_index=True,use_container_width=True,column_config={'id':st.column_config.NumberColumn(disabled=True),'valor':st.column_config.NumberColumn('Valor',format='R$ %.2f')})
                conf=st.checkbox('Confirmo a exclusão dos itens marcados',key='conf_del_plant')
                pb1,pb2=st.columns(2)
                if pb1.button('Excluir selecionados',disabled=not conf,use_container_width=True):
                    ids=[int(r['id']) for _,r in ed.iterrows() if r['Apagar']]
                    if ids:
                        with transaction() as cur: cur.execute('DELETE FROM lancamentos WHERE id = ANY(%s)',(ids,))
                        flash('success',f'{len(ids)} plantão(ões) excluído(s).'); st.rerun()
                conf_todos=pb2.checkbox('Confirmar apagar tudo listado',key='plant_del_all_conf')
                if pb2.button('🚨 Apagar TUDO listado',disabled=not conf_todos,use_container_width=True):
                    ids_todos=dm['id'].astype(int).tolist()
                    if ids_todos:
                        with transaction() as cur: cur.execute('DELETE FROM lancamentos WHERE id = ANY(%s)',(ids_todos,))
                        flash('success',f'{len(ids_todos)} plantão(ões) listado(s) apagado(s).'); st.rerun()

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
                        if int_seguro(r.get('is_envelope'))==1: tags.append(f"🎯 Limite R$ {format_brl(float_seguro(r.get('valor_padrao')))}/mês")
                        elif int_seguro(r.get('is_recorrente'))==1: tags.append(f"🔄 Repete todo mês · R$ {format_brl(float_seguro(r.get('valor_padrao')))}")
                        elif tipo=='Entrada' and pd.notna(r.get('dia_pagamento')): tags.append(f"recebe dia {int(r['dia_pagamento'])}")
                        st.markdown(f"• **{r['subgrupo'] if pd.notna(r['subgrupo']) and str(r['subgrupo']).strip() else 'Geral'}** <span class='ux-muted'>· {' · '.join(tags) if tags else 'manual'}</span>",unsafe_allow_html=True)
    tab_new,tab_edit=st.tabs(['＋ Nova categoria','✏️ Editar / excluir'])
    with tab_new:
        ntipo=st.radio('Tipo',['Despesa','Entrada'],horizontal=True,key='cat_new_tipo'); c1,c2=st.columns(2); ncat=c1.text_input('Categoria',key='cat_new_cat'); nsub=c2.text_input('Subgrupo (opcional)',key='cat_new_sub')
        n_env=st.checkbox('🎯 Usar como limite mensal',key='cat_new_lim') if ntipo=='Despesa' else False; n_rec=st.checkbox('🔄 Repete todo mês',key='cat_new_rec',disabled=n_env,value=n_env); rec=n_env or n_rec
        val=0.0; atraso=0; dia=10; inicio=data_contexto_ativo
        if ntipo=='Entrada' or rec:
            x1,x2,x3=st.columns(3); val=x1.number_input('Valor padrão',min_value=0.0,step=50.0,key='cat_new_val'); atraso=x2.number_input('Atraso em meses',0,6,1 if ntipo=='Entrada' else 0,key='cat_new_atraso'); dia=x3.number_input('Dia pagamento/vencimento',1,31,10,key='cat_new_dia')
            if rec: inicio=st.date_input('Começar em',value=data_contexto_ativo,key='cat_new_inicio')
        if st.button('Salvar categoria',type='primary',key='cat_new_save'):
            if not ncat.strip(): st.error('Categoria é obrigatória.')
            else:
                execute_query("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio,is_envelope) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(ntipo,ncat.strip(),nsub.strip() or None,val if val>0 else None,atraso,dia,1 if rec else 0,inicio if rec else None,1 if n_env else 0)); invalidar_caches_estruturais(); flash('success','Categoria criada.'); st.rerun()
    with tab_edit:
        if cfg.empty: st.info('Nada para editar.')
        else:
            op={int(r['id']):f"{r['tipo']} · {r['categoria']} · {r['subgrupo'] if pd.notna(r['subgrupo']) and str(r['subgrupo']).strip() else 'Geral'}" for _,r in cfg.iterrows()}; sel=st.selectbox('Escolha o item',[None]+list(op),format_func=lambda x:'Selecione...' if x is None else op[x],key='cat_edit_sel')
            if sel:
                r=cfg[cfg['id']==sel].iloc[0]; e1,e2=st.columns(2); cat=e1.text_input('Categoria',value=r['categoria'],key='cat_edit_cat'); sub=e2.text_input('Subgrupo',value=r['subgrupo'] if pd.notna(r['subgrupo']) else '',key='cat_edit_sub'); env=st.checkbox('🎯 Limite mensal',value=bool(r['is_envelope']==1),disabled=r['tipo']!='Despesa',key='cat_edit_env'); rec=st.checkbox('🔄 Repete todo mês',value=bool(r['is_recorrente']==1) or env,disabled=env,key='cat_edit_rec'); efet=env or rec
                val=float(r['valor_padrao']) if pd.notna(r['valor_padrao']) else 0.0; atraso=int(r['atraso_meses']) if pd.notna(r['atraso_meses']) else 0; dia=int(r['dia_pagamento']) if pd.notna(r['dia_pagamento']) else 10
                if r['tipo']=='Entrada' or efet:
                    z1,z2,z3=st.columns(3); val=z1.number_input('Valor padrão',value=val,key='cat_edit_val'); atraso=z2.number_input('Atraso em meses',0,6,atraso,key='cat_edit_atraso'); dia=z3.number_input('Dia pagamento/vencimento',1,31,dia,key='cat_edit_dia')
                st.caption('Mudanças valem para novos lançamentos e recorrências; o histórico anterior é preservado.')
                b1,b2=st.columns(2)
                if b1.button('Salvar alterações',type='primary',use_container_width=True):
                    execute_query("UPDATE categorias_personalizadas SET categoria=%s,subgrupo=%s,valor_padrao=%s,atraso_meses=%s,dia_pagamento=%s,is_recorrente=%s,is_envelope=%s WHERE id=%s",(cat.strip(),sub.strip() or None,val if val>0 else None,atraso,dia,1 if efet else 0,1 if env else 0,int(sel))); invalidar_caches_estruturais(); flash('success','Categoria atualizada.'); st.rerun()
                confirmar=b2.checkbox('Confirmar exclusão',key='cat_del_confirm')
                if b2.button('Excluir',disabled=not confirmar,use_container_width=True): execute_query('DELETE FROM categorias_personalizadas WHERE id=%s',(int(sel),)); invalidar_caches_estruturais(); flash('success','Categoria excluída.'); st.rerun()

# -----------------------------------------------------------------
# BACKUP
# -----------------------------------------------------------------
elif menu == "💾 Backup e Restauração":
    cabecalho_pagina("💾 Backup e Restauração", "Ferramentas administrativas ficam fora do uso diário.")
    st.subheader('Criar backup completo')
    st.caption('Inclui lançamentos, categorias, dívidas, reserva, pagamentos e controle de recorrências.')
    if st.button('📦 Preparar backup ZIP',type='primary'):
        try: st.session_state['_backup_blob']=exportar_backup_completo(); st.session_state['_backup_nome']=f"backup_completo_{hoje.strftime('%d_%m_%Y')}.zip"
        except Exception as e: st.error(f'Falha ao preparar backup: {e}')
    if st.session_state.get('_backup_blob') is not None:
        st.download_button('⬇️ Baixar backup',data=st.session_state['_backup_blob'],file_name=st.session_state.get('_backup_nome','backup_completo.zip'),mime='application/zip')
    st.divider(); st.subheader('Restaurar backup'); st.warning('A restauração substitui o estado do banco. Antes dela, o app cria automaticamente um ZIP de segurança do estado atual.')
    up=st.file_uploader('Backup ZIP ou CSV legado',type=['zip','csv'],key='backup_restore_file'); conf=st.checkbox('Confirmo que quero restaurar este arquivo',key='backup_restore_confirm')
    if up is not None and st.button('Restaurar',type='primary',disabled=not conf):
        try: st.session_state['_pre_restore_blob']=exportar_backup_completo(); st.session_state['_pre_restore_nome']=f"antes_da_restauracao_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        except Exception as e: st.error(f'Não foi possível criar o backup de segurança: {e}')
        else:
            ok,msg=importar_backup(up)
            if ok: invalidar_caches_estruturais(); flash('success',msg); st.rerun()
            else: st.error(f'Restauração cancelada; dados atuais preservados. {msg}')
    if st.session_state.get('_pre_restore_blob') is not None: st.download_button('🛟 Baixar estado anterior à última restauração',data=st.session_state['_pre_restore_blob'],file_name=st.session_state.get('_pre_restore_nome','antes_da_restauracao.zip'),mime='application/zip')

# -----------------------------------------------------------------
# MANUTENÇÃO E DIAGNÓSTICO
# -----------------------------------------------------------------
elif menu == "🧰 Manutenção e Diagnóstico":
    cabecalho_pagina("🧰 Manutenção e diagnóstico", "Ferramentas técnicas e correções históricas — fora do fluxo normal.")
    with st.expander('🔍 Verificar integridade dos limites mensais',expanded=True):
        conc=fetch_dataframe("""WITH e AS (SELECT categoria,subgrupo,COALESCE(valor_padrao,0) valor_padrao FROM categorias_personalizadas WHERE is_envelope=1 AND tipo='Despesa'), r AS (SELECT categoria,subgrupo,SUM(valor_pago) realizado FROM lancamentos WHERE tipo='Despesa' AND pago=1 AND COALESCE(eh_orcamento,0)=0 AND data_vencimento >= %s AND data_vencimento < %s GROUP BY categoria,subgrupo), t AS (SELECT categoria,subgrupo,MAX(valor_orcamento) teto,COUNT(*) qtd FROM lancamentos WHERE COALESCE(eh_orcamento,0)=1 AND data_vencimento >= %s AND data_vencimento < %s GROUP BY categoria,subgrupo) SELECT e.categoria,e.subgrupo,e.valor_padrao,COALESCE(r.realizado,0) realizado,COALESCE(t.teto,e.valor_padrao) teto,COALESCE(t.qtd,0) qtd FROM e LEFT JOIN r ON r.categoria=e.categoria AND COALESCE(r.subgrupo,'')=COALESCE(e.subgrupo,'') LEFT JOIN t ON t.categoria=e.categoria AND COALESCE(t.subgrupo,'')=COALESCE(e.subgrupo,'')""",(inicio_periodo,fim_periodo,inicio_periodo,fim_periodo))
        if conc.empty: st.info('Nenhum limite configurado.')
        elif not {'valor_padrao','teto','qtd'}.issubset(conc.columns):
            st.warning('Não foi possível concluir o diagnóstico dos limites nesta execução.')
        else:
            conc['dif']=pd.to_numeric(conc['valor_padrao'])-pd.to_numeric(conc['teto']); prob=conc[(conc['qtd']!=1)|(conc['dif'].abs()>.01)]
            if prob.empty: st.success('Tudo consistente neste período.')
            else: st.warning(f'{len(prob)} item(ns) requerem revisão.'); st.dataframe(prob,hide_index=True,use_container_width=True)
    with st.expander("🧹 Limpar lançamentos antigos com tag 'Provisão'"):
        prov=fetch_dataframe("SELECT id,tipo,categoria,subgrupo,descricao,valor,data_vencimento,pago FROM lancamentos WHERE descricao ILIKE %s ORDER BY data_vencimento",('%(Provisão)%',))
        if prov.empty: st.success('Nenhum lançamento antigo encontrado.')
        else:
            st.dataframe(prov,hide_index=True,use_container_width=True); c=st.checkbox('Confirmo a exclusão permanente destes itens',key='maint_prov_conf')
            if st.button('Apagar itens listados',disabled=not c,key='maint_prov_del'):
                ids=prov['id'].astype(int).tolist();
                with transaction() as cur: cur.execute('DELETE FROM lancamentos WHERE id = ANY(%s)',(ids,))
                flash('success',f'{len(ids)} item(ns) apagado(s).'); st.rerun()
    with st.expander('🧹 Corrigir descrições antigas duplicadas'):
        dup=fetch_dataframe(r"SELECT id,descricao FROM lancamentos WHERE descricao ~ '\(\d+/\d+\) \(\d+/\d+\)$' ORDER BY id")
        if dup.empty: st.success('Nenhuma descrição duplicada encontrada.')
        else:
            prev=dup.copy(); prev['corrigida']=prev['descricao'].apply(lambda d:re.sub(r'(\s*\(\d+/\d+\))+$','',d).strip()); st.dataframe(prev,hide_index=True,use_container_width=True)
            if st.button('Corrigir todas',type='primary'):
                with transaction() as cur:
                    for _,r in dup.iterrows(): cur.execute('UPDATE lancamentos SET descricao=%s WHERE id=%s',(re.sub(r'(\s*\(\d+/\d+\))+$','',r['descricao']).strip(),int(r['id'])))
                flash('success','Descrições corrigidas.'); st.rerun()
    with st.expander('🚨 Apagar histórico global de plantões'):
        st.error('Ação irreversível. Use apenas se realmente quiser remover todos os plantões do banco.')
        conf=st.checkbox('Confirmo que quero apagar TODO o histórico de plantões',key='maint_purge_plant')
        if st.button('Purgar histórico de plantões',disabled=not conf,key='maint_purge_btn'):
            execute_query("DELETE FROM lancamentos WHERE tipo='Entrada' AND descricao LIKE 'Plantão %'"); flash('success','Histórico de plantões apagado.'); st.rerun()
