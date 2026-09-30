import streamlit as st
APP_BUILD = "ui-refino-planejamento-v16"
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

@st.cache_resource
def get_pool():
    '''Pool compartilhado com validação na retirada de cada conexão.'''
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
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
        )
    except Exception as e:
        st.error(f"Falha Crítica de Conexão com o PostgreSQL: {e}")
        st.stop()


def _fechar_pool_atual():
    '''Descarta todo o pool após reinício/queda do servidor PostgreSQL.'''
    try:
        pool = get_pool()
        pool.closeall()
    except Exception:
        pass
    try:
        get_pool.clear()
    except Exception:
        pass


def _eh_erro_de_conexao(exc):
    '''Reconhece erro de conexão mesmo quando pandas o encapsula em DatabaseError.'''
    atual = exc
    vistos = set()
    marcadores = (
        "server closed the connection unexpectedly",
        "connection already closed",
        "connection not open",
        "ssl connection has been closed unexpectedly",
        "terminating connection",
        "could not connect to server",
        "connection refused",
        "connection timed out",
        "closed the connection",
    )
    while atual is not None and id(atual) not in vistos:
        vistos.add(id(atual))
        if isinstance(atual, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            return True
        msg = str(atual).lower()
        if any(m in msg for m in marcadores):
            return True
        atual = getattr(atual, "__cause__", None) or getattr(atual, "__context__", None)
    return False


@contextmanager
def db_connection(autocommit=True):
    '''Retira uma conexão do pool, valida com SELECT 1 e descarta sockets mortos.'''
    pool = get_pool()
    conn = None
    ultimo_erro = None

    # Uma conexão pode continuar presente no pool mesmo depois de o provedor do
    # PostgreSQL encerrar o socket por ociosidade/restart. Validamos antes do uso.
    for _ in range(2):
        candidata = pool.getconn()
        try:
            if candidata.closed:
                raise psycopg2.InterfaceError("Conexão do pool já estava fechada.")
            try:
                candidata.rollback()
            except Exception:
                pass
            candidata.autocommit = True
            with candidata.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            candidata.autocommit = autocommit
            conn = candidata
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            ultimo_erro = e
            try:
                pool.putconn(candidata, close=True)
            except Exception:
                try:
                    candidata.close()
                except Exception:
                    pass

    if conn is None:
        raise ultimo_erro or psycopg2.OperationalError("Não foi possível obter uma conexão válida.")

    try:
        yield conn
    finally:
        conexao_quebrada = bool(conn.closed)
        try:
            if not conn.closed and not autocommit:
                conn.rollback()
        except Exception:
            conexao_quebrada = True
        try:
            if conexao_quebrada or conn.closed:
                pool.putconn(conn, close=True)
            else:
                conn.autocommit = True
                pool.putconn(conn)
        except Exception:
            try:
                conn.close()
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
        _fechar_pool_atual()
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


def fetch_dataframe(query, params=None, silent=False, raise_on_error=False):
    '''
    Leituras de lançamentos usam automaticamente a VIEW financeira derivada.
    Se uma conexão ociosa tiver sido encerrada pelo servidor, recria o pool e
    repete a leitura uma única vez. O pandas pode encapsular OperationalError,
    por isso a detecção não depende apenas do tipo da exceção externa.
    '''
    query_exec = query
    if "/* RAW */" not in query_exec:
        query_exec = re.sub(
            r"\bFROM\s+lancamentos\b",
            "FROM vw_lancamentos_financeiros",
            query_exec,
            flags=re.IGNORECASE,
        )

    ultimo_erro = None
    for tentativa in range(2):
        try:
            with db_connection(autocommit=True) as conn:
                return pd.read_sql_query(query_exec, conn, params=params)
        except Exception as e:
            ultimo_erro = e
            if tentativa == 0 and _eh_erro_de_conexao(e):
                _fechar_pool_atual()
                continue
            break

    if raise_on_error and ultimo_erro is not None:
        raise ultimo_erro
    if not silent and ultimo_erro is not None:
        st.error(f"Erro de Leitura de Dados: {ultimo_erro}")
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


def _migrar_envelopes_legados():
    """Converte envelopes antigos em orçamento mensal por categoria.

    A versão 2.0 não usa orçamento como lançamento. As colunas legadas continuam
    no schema apenas para restaurar backups antigos, mas ficam inativas depois
    desta migração.
    """
    with transaction() as cur:
        cur.execute("""
            INSERT INTO orcamentos_categorias
                (competencia, categoria, subgrupo, valor_planejado, origem)
            SELECT DATE_TRUNC('month', l.data_vencimento)::date,
                   l.categoria, NULLIF(BTRIM(COALESCE(l.subgrupo,'')), ''),
                   MAX(COALESCE(l.valor_orcamento, l.valor, 0)), 'migrado_envelope'
            FROM lancamentos l
            WHERE COALESCE(l.eh_orcamento,0)=1
            GROUP BY DATE_TRUNC('month', l.data_vencimento)::date,
                     l.categoria, NULLIF(BTRIM(COALESCE(l.subgrupo,'')), '')
            ON CONFLICT DO NOTHING
        """)
        cur.execute("""
            INSERT INTO orcamentos_categorias
                (competencia, categoria, subgrupo, valor_planejado, origem)
            SELECT DATE_TRUNC('month', CURRENT_DATE)::date, c.categoria,
                   NULLIF(BTRIM(COALESCE(c.subgrupo,'')), ''),
                   COALESCE(c.valor_padrao,0), 'migrado_categoria'
            FROM categorias_personalizadas c
            WHERE COALESCE(c.is_envelope,0)=1 AND COALESCE(c.valor_padrao,0) > 0
            ON CONFLICT DO NOTHING
        """)
        cur.execute("""
            DELETE FROM recorrencias_geradas rg
            USING categorias_personalizadas c
            WHERE rg.categoria_id=c.id AND COALESCE(c.is_envelope,0)=1
        """)
        cur.execute("DELETE FROM lancamentos WHERE COALESCE(eh_orcamento,0)=1")
        cur.execute("""
            UPDATE categorias_personalizadas
            SET is_envelope=0, is_recorrente=0
            WHERE COALESCE(is_envelope,0)=1
        """)


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
        # is_envelope permanece somente para importar backups antigos; a v2 não o usa.
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_envelope INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_producao_variavel INTEGER DEFAULT 0;",
    ]:
        execute_query(ddl)

    execute_query('''
        CREATE TABLE IF NOT EXISTS orcamentos_categorias (
            id BIGSERIAL PRIMARY KEY,
            competencia DATE NOT NULL,
            categoria TEXT NOT NULL,
            subgrupo TEXT,
            valor_planejado NUMERIC NOT NULL DEFAULT 0 CHECK (valor_planejado >= 0),
            origem TEXT NOT NULL DEFAULT 'manual',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    ''')
    execute_query('''
        CREATE UNIQUE INDEX IF NOT EXISTS ux_orcamento_categoria_mes
        ON orcamentos_categorias (competencia, categoria, COALESCE(subgrupo,''));
    ''')

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

    # Migração: orçamento deixa de ser lançamento e passa a ser dado mensal da categoria.
    _migrar_envelopes_legados()

    # VIEW mantida por compatibilidade com consultas existentes; sem saldo derivado.
    execute_query('''
        CREATE OR REPLACE VIEW vw_lancamentos_financeiros AS
        SELECT
            l.id, l.tipo, l.categoria, l.subgrupo, l.descricao, l.valor,
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
        "CREATE INDEX IF NOT EXISTS idx_lanc_categoria_subgrupo_data ON lancamentos(categoria, subgrupo, data_vencimento, pago);",
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
                dia_alvo = min(int(contrato['dia_pagamento'] or 1), ultimo_dia_mes)
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
                desc_c = f"{contrato['categoria']} - {contrato['subgrupo'] or ''} (Recorrente)"
                cur.execute('''
                    INSERT INTO lancamentos
                    (tipo, categoria, subgrupo, descricao, valor, data_vencimento,
                     parcela_atual, total_parcelas, pago, compra_id, forma_pagamento,
                     prioridade, valor_pago, data_competencia)
                    VALUES (%s,%s,%s,%s,%s,%s,1,1,0,%s,'Outros','Média 🟡',0,%s)
                ''', (
                    contrato['tipo'], contrato['categoria'], contrato['subgrupo'], desc_c,
                    val_p, dt_limite_alvo, compra_id_contrato, competencia,
                ))
    except Exception as e:
        st.error(f"Erro ao gerar recorrências: {e}")
        return

    st.session_state[guarda] = True


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

/* Tabelas de leitura do Demonstrativo: HTML próprio para evitar que o grid
   interno do st.dataframe force células brancas no tema escuro. */
.ux-table-wrap { width:100%; overflow-x:auto; border:1px solid #2a3036; border-radius:10px; background:#101418; margin:.35rem 0 .75rem 0; }
.ux-dark-table { width:100%; border-collapse:separate; border-spacing:0; min-width:720px; background:#101418; color:#e6e8ea; font-size:.82rem; }
.ux-dark-table th { background:#23282f; color:#c2c8ce; font-weight:600; text-align:left; padding:.58rem .65rem; border-bottom:1px solid #343a40; border-right:1px solid #343a40; white-space:nowrap; }
.ux-dark-table th:last-child, .ux-dark-table td:last-child { border-right:0; }
.ux-dark-table td { background:#101418; color:#e6e8ea; padding:.55rem .65rem; border-bottom:1px solid #272d33; border-right:1px solid #272d33; vertical-align:middle; white-space:nowrap; }
.ux-dark-table tbody tr:last-child td { border-bottom:0; }
.ux-dark-table tbody tr:hover td { background:#182027; }
.ux-dark-table tr.ux-row-paid td { background:#111c18; }
.ux-dark-table tr.ux-row-pending td { background:#1b1811; }
.ux-dark-table tr.ux-row-budget td { background:#121a20; }
.ux-dark-table tr.ux-row-danger td { background:#211315; }
.ux-dark-table tr.ux-row-warning td { background:#211d13; }
.ux-dark-table td.ux-num, .ux-dark-table th.ux-num { text-align:right; font-variant-numeric:tabular-nums; }
.ux-dark-table .ux-real-strong { font-weight:650; color:#f1f3f5; }
.ux-dark-table .ux-plan-muted { color:#a6adb5; }
[data-testid="stSidebar"] .stExpander { background:transparent !important; border:0 !important; }
.ux-kpi { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.05rem; min-height:108px; }
.ux-kpi-label { color:var(--text-muted); font-size:.78rem; font-weight:550; }
.ux-kpi-value { margin-top:.3rem; font-size:1.42rem; font-weight:680; font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
.ux-kpi-note { margin-top:.2rem; color:var(--text-muted); font-size:.75rem; }
.ux-positive { color:var(--success) !important; }
.ux-negative { color:var(--danger-text) !important; }
.ux-accent { color:var(--accent-strong) !important; }
.ux-flow-desc { font-weight:620; line-height:1.2; }
.ux-flow-date { font-variant-numeric:tabular-nums; font-weight:620; margin-right:.35rem; }
.ux-flow-category { color:var(--text-muted); font-size:.76rem; margin-top:.18rem; }
.ux-flow-paid { opacity:.63; }
.ux-flow-overdue { color:var(--danger-text); }
.ux-flow-pending { color:var(--text-primary); }
.ux-flow-value-main { font-variant-numeric:tabular-nums; font-weight:680; font-size:1rem; white-space:nowrap; }
.ux-flow-value-sub { color:var(--text-muted); font-size:.72rem; white-space:nowrap; }
.ux-group-icon { color:var(--text-muted); font-size:.8rem; margin-right:.3rem; }
.ux-empty { text-align:center; border:1px dashed var(--border-strong); border-radius:14px; padding:1.35rem 1rem; color:var(--text-muted); background:var(--bg-card); }
.ux-empty b { color:var(--text-primary); }
.ux-empty-icon { font-size:1.35rem; color:var(--success); margin-bottom:.25rem; }
.ux-payment-box { border-left:3px solid var(--accent); padding-left:.85rem; margin:.35rem 0 .65rem; }
.ux-cover-summary { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.05rem; min-height:106px; }
.ux-cover-label { color:var(--text-muted); font-size:.76rem; font-weight:600; }
.ux-cover-value { margin-top:.28rem; font-size:1.32rem; font-weight:700; font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
.ux-cover-note { margin-top:.18rem; color:var(--text-muted); font-size:.74rem; }
.ux-cover-alert { border:1px solid var(--danger-border); background:var(--danger-tint); border-radius:11px; padding:.65rem .8rem; margin:.65rem 0 1rem; color:var(--text-primary); }
.ux-cover-ok { border:1px solid var(--success-border); background:var(--success-tint); border-radius:11px; padding:.65rem .8rem; margin:.65rem 0 1rem; color:var(--text-primary); }
.ux-income-head { display:flex; justify-content:space-between; gap:1rem; align-items:flex-start; margin-bottom:.2rem; }
.ux-income-name { font-weight:680; font-size:1rem; line-height:1.25; }
.ux-income-meta { color:var(--text-muted); font-size:.76rem; margin-top:.14rem; }
.ux-income-amount { font-size:1.08rem; font-weight:700; font-variant-numeric:tabular-nums; white-space:nowrap; }
.ux-income-stats { display:flex; flex-wrap:wrap; gap:.45rem .9rem; margin:.6rem 0 .45rem; color:var(--text-muted); font-size:.76rem; }
.ux-income-stats b { color:var(--text-primary); font-weight:620; }
.ux-cover-bar { height:7px; border-radius:999px; overflow:hidden; background:#20262c; margin:.45rem 0 .65rem; }
.ux-cover-fill-ok { height:100%; background:var(--success); }
.ux-cover-fill-warn { height:100%; background:#d2a13a; }
.ux-cover-fill-danger { height:100%; background:var(--danger); }
.ux-match-line { display:grid; grid-template-columns:58px minmax(0,1fr) auto; gap:.55rem; align-items:center; padding:.48rem 0; border-top:1px solid var(--border); }
.ux-match-date { color:var(--text-muted); font-size:.76rem; font-variant-numeric:tabular-nums; }
.ux-match-desc { font-size:.84rem; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.ux-match-value { font-size:.82rem; font-weight:620; font-variant-numeric:tabular-nums; white-space:nowrap; }
.ux-match-warning { color:var(--danger-text); font-size:.72rem; margin-top:.15rem; }
.ux-source-badge { display:inline-block; border-radius:999px; padding:.1rem .45rem; font-size:.68rem; font-weight:650; margin-left:.35rem; vertical-align:1px; }
.ux-source-received { background:var(--success-tint); color:var(--success); }
.ux-source-planned { background:var(--accent-tint); color:var(--accent-strong); }
.ux-source-risk { background:var(--danger-tint); color:var(--danger-text); }
.ux-source-attn { background:rgba(210,161,58,.13); color:#d9ae55; }
.ux-source-ok { background:var(--success-tint); color:var(--success); }
.ux-secondary-note { color:var(--text-muted); font-size:.78rem; margin:.4rem 0 .8rem; }


/* Fluxo 2.0 — agenda financeira com casamento renda → conta */
.flow2-head { margin:.15rem 0 .7rem; }
.flow2-title { font-size:1.7rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); }
.flow2-sub { margin-top:.2rem; color:var(--text-muted); font-size:.84rem; }
.flow2-bridge { border:1px solid var(--success-border); background:linear-gradient(100deg, rgba(25,165,116,.10), var(--bg-card) 58%); border-radius:15px; padding:.9rem 1rem; margin:.55rem 0 1rem; }
.flow2-bridge.warn { border-color:rgba(210,161,58,.38); background:linear-gradient(100deg, rgba(210,161,58,.10), var(--bg-card) 58%); }
.flow2-bridge.danger { border-color:var(--danger-border); background:linear-gradient(100deg, var(--danger-tint), var(--bg-card) 58%); }
.flow2-bridge-grid { display:grid; grid-template-columns:1.25fr .85fr auto; gap:1rem; align-items:center; }
.flow2-bridge-label { color:var(--text-muted); font-size:.72rem; font-weight:650; }
.flow2-bridge-name { margin-top:.17rem; font-size:1rem; font-weight:700; color:var(--text-heading); }
.flow2-bridge-meta { margin-top:.12rem; color:var(--text-muted); font-size:.76rem; }
.flow2-bridge-value { margin-top:.12rem; font-size:1.25rem; font-weight:720; font-variant-numeric:tabular-nums; }
.flow2-pill { display:inline-block; border-radius:999px; padding:.28rem .64rem; font-size:.74rem; font-weight:700; white-space:nowrap; }
.flow2-pill.ok { background:var(--success-tint); color:var(--success); }
.flow2-pill.warn { background:rgba(210,161,58,.13); color:#d9ae55; }
.flow2-pill.danger { background:var(--danger-tint); color:var(--danger-text); }
.flow2-day { margin:1rem 0 .38rem; display:flex; align-items:center; gap:.45rem; color:var(--text-muted); font-size:.78rem; font-weight:700; letter-spacing:.015em; }
.flow2-day.today { color:var(--accent-strong); }
.flow2-dot { width:8px; height:8px; border-radius:50%; display:inline-block; background:var(--border-strong); }
.flow2-dot.today { background:var(--accent); box-shadow:0 0 0 4px var(--accent-tint); }
.flow2-row-anchor { display:none; }
.flow2-name { color:var(--text-heading); font-size:.9rem; font-weight:680; line-height:1.15; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.flow2-meta { color:var(--text-muted); font-size:.72rem; margin-top:.18rem; line-height:1.25; }
.flow2-meta.danger { color:var(--danger-text); }
.flow2-meta.warn { color:#d9ae55; }
.flow2-amount { font-size:1rem; font-weight:730; font-variant-numeric:tabular-nums; text-align:right; white-space:nowrap; }
.flow2-match { margin-top:.18rem; text-align:right; color:var(--text-muted); font-size:.7rem; line-height:1.22; }
.flow2-match.ok { color:var(--success); }
.flow2-match.warn { color:#d9ae55; }
.flow2-match.danger { color:var(--danger-text); }
.flow2-paid { opacity:.58; }
.flow2-income-details { margin:.35rem 0 .7rem 2.5rem; border-left:2px solid var(--accent); padding:.15rem 0 .15rem .8rem; }
.flow2-income-line { display:grid; grid-template-columns:58px minmax(0,1fr) auto; gap:.55rem; align-items:center; padding:.38rem 0; border-bottom:1px solid var(--border); font-size:.76rem; }
.flow2-income-line:last-child { border-bottom:0; }
.flow2-selection { border:1px solid rgba(47,124,246,.34); background:linear-gradient(100deg, rgba(47,124,246,.12), var(--bg-card)); border-radius:13px; padding:.72rem .85rem; margin:.55rem 0 .9rem; }
.flow2-selection strong { font-variant-numeric:tabular-nums; }
.flow2-help { color:var(--text-muted); font-size:.73rem; margin:.35rem 0 .8rem; }
.flow2-batch-note { border-left:3px solid var(--accent); padding-left:.75rem; margin:.35rem 0 .65rem; }

/* Faz o container Streamlit das linhas se aproximar do card horizontal do mockup. */
[data-testid="stVerticalBlockBorderWrapper"]:has(.flow2-row-anchor) {
    border-color:var(--border) !important; border-radius:13px !important; background:rgba(255,255,255,.012) !important;
}
[data-testid="stVerticalBlockBorderWrapper"]:has(.flow2-row-anchor):hover { border-color:var(--border-strong) !important; }

/* Home 2.0 — orientação antes de análise */
.home2-head { margin:.2rem 0 1rem; }
.home2-hello { font-size:1.65rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); line-height:1.08; }
.home2-sub { margin-top:.32rem; color:var(--text-muted); font-size:.9rem; }
.home2-period { display:inline-flex; align-items:center; gap:.35rem; border:1px solid var(--border-strong); border-radius:9px; padding:.4rem .65rem; color:var(--text-muted); font-size:.78rem; }
.home2-hero { border:1px solid var(--success-border); background:linear-gradient(100deg, var(--success-tint), var(--bg-card) 55%); border-radius:16px; padding:1.15rem 1.25rem; margin:.35rem 0 1rem; }
.home2-hero-grid { display:grid; grid-template-columns:1.05fr 1fr; gap:1.2rem; align-items:center; }
.home2-hero-side { border-left:1px solid var(--border); padding-left:1.2rem; }
.home2-hero.warn { border-color:rgba(210,161,58,.38); background:linear-gradient(100deg, rgba(210,161,58,.10), var(--bg-card) 55%); }
.home2-hero.danger { border-color:var(--danger-border); background:linear-gradient(100deg, var(--danger-tint), var(--bg-card) 55%); }
.home2-eyebrow { color:var(--text-muted); font-size:.74rem; font-weight:650; letter-spacing:.02em; }
.home2-income-name { margin-top:.28rem; color:var(--text-heading); font-size:1.08rem; font-weight:700; }
.home2-income-value { margin-top:.18rem; font-size:1.8rem; font-weight:760; font-variant-numeric:tabular-nums; color:var(--success); letter-spacing:-.035em; }
.home2-income-date { color:var(--text-muted); font-size:.78rem; }
.home2-bridge-value { margin-top:.2rem; font-size:1.4rem; font-weight:720; font-variant-numeric:tabular-nums; color:var(--text-heading); }
.home2-status { margin-top:.55rem; border-radius:10px; padding:.55rem .7rem; font-size:.79rem; line-height:1.35; }
.home2-status.ok { background:var(--success-tint); color:var(--text-primary); }
.home2-status.warn { background:rgba(210,161,58,.13); color:var(--text-primary); }
.home2-status.danger { background:var(--danger-tint); color:var(--text-primary); }
.home2-panel { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; margin:.55rem 0; }
.home2-panel-title { font-size:1rem; font-weight:700; color:var(--text-heading); margin-bottom:.55rem; }
.home2-alert-row { border-radius:11px; padding:.68rem .75rem; margin:.36rem 0; border:1px solid var(--border); background:rgba(255,255,255,.012); }
.home2-alert-row.danger { background:var(--danger-tint); border-color:var(--danger-border); }
.home2-alert-row.warn { background:rgba(210,161,58,.08); border-color:rgba(210,161,58,.25); }
.home2-alert-row.info { background:var(--accent-tint); border-color:rgba(80,180,220,.22); }
.home2-alert-name { font-weight:670; color:var(--text-heading); font-size:.87rem; }
.home2-alert-meta { color:var(--text-muted); font-size:.75rem; margin-top:.1rem; }
.home2-month-card { border:1px solid var(--border); border-radius:13px; background:var(--bg-card); padding:.9rem .95rem; min-height:104px; }
.home2-month-card.green { background:linear-gradient(140deg, var(--success-tint), var(--bg-card)); }
.home2-month-card.red { background:linear-gradient(140deg, var(--danger-tint), var(--bg-card)); }
.home2-month-card.blue { background:linear-gradient(140deg, var(--accent-tint), var(--bg-card)); }
.home2-month-value { font-size:1.32rem; font-weight:730; font-variant-numeric:tabular-nums; letter-spacing:-.025em; }
.home2-month-label { color:var(--text-muted); font-size:.76rem; margin-top:.16rem; }
.home2-forecast { border-left:1px solid var(--border); padding-left:.9rem; min-height:104px; }
.home2-forecast-title { color:var(--text-muted); font-size:.72rem; }
.home2-forecast-line { font-size:.8rem; font-weight:620; margin-top:.38rem; font-variant-numeric:tabular-nums; }
.home2-timeline { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.55rem; align-items:stretch; }
.home2-event { border:1px solid var(--border); border-radius:11px; padding:.65rem .7rem; background:rgba(255,255,255,.012); min-height:94px; }
.home2-event.today { border-color:rgba(80,180,220,.25); background:var(--accent-tint); }
.home2-event.in { border-color:var(--success-border); background:var(--success-tint); }
.home2-event.out { border-color:rgba(210,161,58,.26); background:rgba(210,161,58,.08); }
.home2-event-date { font-size:.72rem; font-weight:700; color:var(--text-heading); }
.home2-event-name { margin-top:.2rem; color:var(--text-muted); font-size:.72rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.home2-event-value { margin-top:.28rem; font-size:.8rem; font-weight:700; font-variant-numeric:tabular-nums; }
.home2-quick-note { color:var(--text-muted); font-size:.75rem; margin-top:.1rem; }
.home2-tip { border:1px solid rgba(80,180,220,.2); background:var(--accent-tint); border-radius:10px; padding:.55rem .75rem; color:var(--text-muted); font-size:.76rem; margin-top:.8rem; }
@media (max-width:640px) {
  .ux-value { font-size:1.1rem; }
  .ux-card, .ux-card-strong { padding:.8rem .85rem; }
  .ux-kpi { min-height:88px; padding:.8rem .85rem; }
  .ux-kpi-value { font-size:1.16rem; }
  .ux-flow-category { display:none; }
  .ux-flow-value-main { font-size:.95rem; }
  .home2-hello { font-size:1.35rem; }
  .home2-income-value { font-size:1.5rem; }
  .home2-hero-grid { grid-template-columns:1fr; }
  .home2-hero-side { border-left:0; border-top:1px solid var(--border); padding-left:0; padding-top:.8rem; }
  .home2-timeline { grid-template-columns:1fr 1fr; }
  .home2-forecast { border-left:0; border-top:1px solid var(--border); padding-left:0; padding-top:.7rem; min-height:0; }
  .flow2-title { font-size:1.4rem; }
  .flow2-bridge-grid { grid-template-columns:1fr; gap:.55rem; }
  .flow2-bridge-value { font-size:1.08rem; }
  .flow2-income-details { margin-left:.35rem; }
  .flow2-income-line { grid-template-columns:48px minmax(0,1fr) auto; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) { flex-wrap:wrap !important; gap:.18rem !important; align-items:center !important; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(1) { flex:0 0 32px !important; min-width:32px !important; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(2) { flex:1 1 calc(100% - 42px) !important; min-width:180px !important; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(3) { flex:1 1 58% !important; min-width:150px !important; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(4) { flex:0 0 112px !important; min-width:112px !important; }
  [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(5) { flex:0 0 42px !important; min-width:42px !important; }

  .ux-dark-table { min-width:620px; font-size:.76rem; }
  .ux-dark-table th, .ux-dark-table td { padding:.48rem .52rem; }
  /* No Fluxo, mantém descrição e valor lado a lado; o botão desce inteiro. */
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) {
      flex-direction:row !important; flex-wrap:wrap !important; align-items:center !important; gap:.2rem !important;
  }
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div { min-width:0 !important; }
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(1) { flex:0 0 34px !important; width:34px !important; }
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(2) { flex:1 1 calc(100% - 154px) !important; width:auto !important; }
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(3) { flex:0 0 112px !important; width:112px !important; }
  [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(4) { flex:1 1 100% !important; width:100% !important; }
}
</style>
""", unsafe_allow_html=True)


# Planejamento 2.0 — leitura rápida de plano x realizado, sem tabelas na visão principal.
st.markdown("""
<style>
.plan2-head { margin:.12rem 0 .8rem; }
.plan2-title { font-size:1.7rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); }
.plan2-sub { margin-top:.22rem; color:var(--text-muted); font-size:.84rem; }
.plan2-summary { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; min-height:154px; }
.plan2-summary-top { display:flex; align-items:center; gap:.65rem; margin-bottom:.8rem; }
.plan2-icon { width:38px; height:38px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:1.05rem; font-weight:800; }
.plan2-icon.in { background:var(--success-tint); color:var(--success); }
.plan2-icon.out { background:var(--danger-tint); color:var(--danger-text); }
.plan2-icon.result { background:var(--accent-tint); color:var(--accent-strong); }
.plan2-summary-name { font-weight:700; color:var(--text-heading); font-size:.94rem; }
.plan2-pair { display:grid; grid-template-columns:1fr 1fr; gap:.7rem; }
.plan2-small-label { color:var(--text-muted); font-size:.7rem; }
.plan2-big { margin-top:.12rem; font-size:1.08rem; font-weight:720; font-variant-numeric:tabular-nums; }
.plan2-delta { margin-top:.72rem; border-radius:9px; padding:.42rem .55rem; font-size:.76rem; font-weight:680; font-variant-numeric:tabular-nums; }
.plan2-delta.good { background:var(--success-tint); color:var(--success); }
.plan2-delta.bad { background:var(--danger-tint); color:var(--danger-text); }
.plan2-delta.neutral { background:var(--accent-tint); color:var(--accent-strong); }
.plan2-panel { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; margin:.65rem 0; }
.plan2-panel-head { display:flex; justify-content:space-between; align-items:center; gap:1rem; margin-bottom:.55rem; }
.plan2-panel-title { font-size:1rem; font-weight:720; color:var(--text-heading); }
.plan2-panel-note { color:var(--text-muted); font-size:.72rem; }
.plan2-row { display:grid; grid-template-columns:minmax(145px,.9fr) minmax(160px,1.55fr) minmax(145px,.7fr) minmax(120px,.58fr); gap:.8rem; align-items:center; padding:.62rem 0; border-top:1px solid var(--border); }
.plan2-row:first-child { border-top:0; }
.plan2-name { font-weight:650; color:var(--text-heading); font-size:.82rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.plan2-name-sub { margin-top:.1rem; color:var(--text-muted); font-size:.68rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.plan2-bar { height:7px; background:#20262c; border-radius:999px; overflow:hidden; }
.plan2-fill { height:100%; border-radius:999px; }
.plan2-fill.good { background:var(--success); }
.plan2-fill.warn { background:#d2a13a; }
.plan2-fill.bad { background:var(--danger); }
.plan2-values { text-align:right; color:var(--text-muted); font-size:.73rem; font-variant-numeric:tabular-nums; white-space:nowrap; }
.plan2-values b { color:var(--text-primary); font-weight:680; }
.plan2-status { justify-self:end; border-radius:9px; padding:.28rem .48rem; font-size:.7rem; font-weight:680; white-space:nowrap; }
.plan2-status.good { background:var(--success-tint); color:var(--success); }
.plan2-status.warn { background:rgba(210,161,58,.13); color:#d9ae55; }
.plan2-status.bad { background:var(--danger-tint); color:var(--danger-text); }
.plan2-limit-row { display:grid; grid-template-columns:minmax(125px,.8fr) minmax(130px,1.1fr) 48px minmax(130px,.9fr); gap:.65rem; align-items:center; padding:.52rem 0; border-top:1px solid var(--border); }
.plan2-limit-row:first-child { border-top:0; }
.plan2-percent { font-size:.74rem; font-weight:720; font-variant-numeric:tabular-nums; text-align:right; }
.plan2-debt-row { padding:.65rem 0; border-top:1px solid var(--border); }
.plan2-debt-row:first-child { border-top:0; }
.plan2-debt-head { display:flex; justify-content:space-between; align-items:flex-start; gap:.7rem; }
.plan2-debt-name { font-weight:680; color:var(--text-heading); font-size:.84rem; }
.plan2-debt-balance { font-weight:700; font-variant-numeric:tabular-nums; white-space:nowrap; }
.plan2-debt-meta { display:flex; flex-wrap:wrap; gap:.35rem .8rem; color:var(--text-muted); font-size:.7rem; margin:.3rem 0 .35rem; }
.plan2-debt-progress { height:6px; background:#20262c; border-radius:999px; overflow:hidden; }
.plan2-debt-progress > span { display:block; height:100%; background:var(--accent); border-radius:999px; }
@media (max-width:640px) {
  .plan2-title { font-size:1.4rem; }
  .plan2-summary { min-height:0; }
  .plan2-row { grid-template-columns:1fr auto; gap:.38rem .7rem; }
  .plan2-row .plan2-bar { grid-column:1 / -1; grid-row:2; }
  .plan2-row .plan2-values { grid-column:1; grid-row:3; text-align:left; }
  .plan2-row .plan2-status { grid-column:2; grid-row:3; }
  .plan2-limit-row { grid-template-columns:1fr auto; }
  .plan2-limit-row .plan2-bar { grid-column:1 / -1; }
  .plan2-limit-row .plan2-percent { text-align:left; }
}
</style>
""", unsafe_allow_html=True)

# Refinamento visual 2.0 — aproxima o shell e o Planejamento do mockup aprovado.
st.markdown("""
<style>
:root, .stApp {
  --bg-page:#071018;
  --bg-sidebar:#08121b;
  --bg-card:#0d1822;
  --bg-card-2:#101e29;
  --border:rgba(130,157,176,.16);
  --border-strong:rgba(130,157,176,.25);
  --text-primary:#eef5f7;
  --text-heading:#f7fbfc;
  --text-muted:#8da2b2;
  --text-faint:#607585;
  --accent:#2dd4bf;
  --accent-strong:#5eead4;
  --accent-tint:rgba(45,212,191,.14);
  --success:#39d98a;
  --success-tint:rgba(57,217,138,.12);
  --danger:#ff646f;
  --danger-text:#ff7b84;
  --danger-tint:rgba(255,100,111,.11);
}

/* Shell */
.stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
  background:
    radial-gradient(900px 520px at 78% -12%, rgba(36,113,156,.12), transparent 62%),
    linear-gradient(180deg,#071018 0%,#081119 46%,#070e15 100%) !important;
}
.block-container {
  max-width:1500px !important;
  padding-top:1.65rem !important;
  padding-left:2rem !important;
  padding-right:2rem !important;
  padding-bottom:3rem !important;
}
section[data-testid="stSidebar"] {
  width:252px !important; min-width:252px !important;
  background:linear-gradient(180deg,#091520 0%,#08121b 66%,#071019 100%) !important;
  border-right:1px solid rgba(120,150,170,.13) !important;
  box-shadow:18px 0 45px rgba(0,0,0,.08);
}
section[data-testid="stSidebar"] > div { width:252px !important; }
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding:1.25rem .78rem 1.1rem !important; }

.brand2 { display:flex; align-items:center; gap:.72rem; padding:.22rem .3rem .55rem; }
.brand2-mark {
  width:34px;height:34px;border-radius:11px;display:flex;align-items:center;justify-content:center;
  background:linear-gradient(145deg,rgba(45,212,191,.22),rgba(45,212,191,.06));
  border:1px solid rgba(45,212,191,.22); color:var(--accent-strong)!important;
  font-size:1.55rem;font-weight:800;line-height:1;transform:rotate(-12deg);
  box-shadow:0 8px 25px rgba(45,212,191,.08);
}
.brand2-name { font-size:1.02rem;font-weight:760;letter-spacing:-.02em;color:var(--text-heading)!important;line-height:1.12; }
.brand2-sub { margin-top:.18rem;font-size:.68rem;color:var(--text-muted)!important; }
.brand2-version { margin:.1rem .34rem .65rem; font-size:.65rem;color:var(--text-faint)!important; }
.sidebar-period { text-align:center;padding:.47rem .15rem;font-weight:650;color:var(--text-heading)!important;font-size:.78rem; }

.nav-eyebrow { margin:1.05rem .45rem .42rem !important;font-size:.62rem!important;letter-spacing:.11em!important;color:#617687!important; }
section[data-testid="stSidebar"] hr { border-color:rgba(125,151,169,.13)!important;margin:1rem .25rem!important; }
section[data-testid="stSidebar"] .stButton { margin:.14rem 0; }
section[data-testid="stSidebar"] .stButton button {
  min-height:42px!important;padding:.55rem .72rem!important;border-radius:10px!important;
  background:transparent!important;border:1px solid transparent!important;color:#b7c6d1!important;
  font-size:.82rem!important;transition:background .14s ease,border-color .14s ease,transform .14s ease!important;
}
section[data-testid="stSidebar"] .stButton button:hover { background:rgba(255,255,255,.035)!important;border-color:rgba(126,154,174,.13)!important;transform:translateX(1px); }
section[data-testid="stSidebar"] .stButton button[kind="primary"] {
  background:linear-gradient(90deg,rgba(45,212,191,.18),rgba(45,212,191,.105))!important;
  border:1px solid rgba(45,212,191,.18)!important;
  box-shadow:inset 3px 0 0 var(--accent),0 8px 24px rgba(0,0,0,.08)!important;
  color:#e9fffb!important;
}
section[data-testid="stSidebar"] .stButton button[kind="primary"] * { color:#e9fffb!important; }

/* Controles */
.stButton button { border-radius:10px!important; min-height:40px; }
[data-baseweb="select"] > div, .stTextInput input, .stNumberInput input {
  background:#0c1720!important;border-color:rgba(127,155,175,.2)!important;border-radius:11px!important;
}

/* Cabeçalho do Planejamento */
.plan2-shell-head { margin:.05rem 0 1.05rem; }
.plan2-head { margin:.05rem 0 0!important; }
.plan2-title { font-size:2rem!important;font-weight:760!important;letter-spacing:-.045em!important;line-height:1.02;color:#f8fbfc!important; }
.plan2-sub { margin-top:.34rem!important;font-size:.86rem!important;color:#8ba0af!important; }
[data-testid="stHorizontalBlock"]:has(.plan2-period-anchor) { align-items:flex-start!important; }
[data-testid="stVerticalBlock"]:has(.plan2-period-anchor) [data-baseweb="select"] > div {
  background:linear-gradient(180deg,#10202c,#0e1b26)!important;border:1px solid rgba(116,151,174,.24)!important;
  min-height:44px!important;border-radius:13px!important;box-shadow:0 8px 28px rgba(0,0,0,.13);
}
[data-testid="stVerticalBlock"]:has(.plan2-period-anchor) [data-baseweb="select"] span { color:#eaf3f6!important;font-weight:600!important;font-size:.8rem!important; }

/* Tabs em pills */
.stTabs [data-baseweb="tab-list"] {
  width:max-content!important;max-width:100%;gap:3px!important;padding:4px!important;margin:.2rem 0 .9rem!important;
  background:#0c1923!important;border:1px solid rgba(119,149,169,.12)!important;border-radius:999px!important;
  box-shadow:inset 0 1px 0 rgba(255,255,255,.015);
}
.stTabs [data-baseweb="tab"] {
  height:38px!important;padding:0 1.05rem!important;border-radius:999px!important;border:1px solid transparent!important;
  background:transparent!important;color:#91a4b2!important;font-size:.79rem!important;
}
.stTabs [data-baseweb="tab"] p { color:inherit!important; }
.stTabs [aria-selected="true"] {
  background:linear-gradient(180deg,rgba(45,212,191,.18),rgba(45,212,191,.10))!important;
  border-color:rgba(45,212,191,.55)!important;color:#effffc!important;
  box-shadow:0 0 0 1px rgba(45,212,191,.08),0 0 22px rgba(45,212,191,.09)!important;
}
.stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] { display:none!important; }

/* Cards de resumo */
.plan2-summary {
  position:relative;overflow:hidden;border:1px solid rgba(125,151,170,.16)!important;
  background:linear-gradient(155deg,rgba(18,34,46,.98),rgba(12,24,34,.98))!important;
  border-radius:17px!important;padding:1.05rem 1.1rem!important;min-height:171px!important;
  box-shadow:0 12px 30px rgba(0,0,0,.13),inset 0 1px 0 rgba(255,255,255,.018);
}
.plan2-summary::after { content:"";position:absolute;width:150px;height:150px;border-radius:50%;right:-65px;top:-78px;opacity:.24;filter:blur(1px); }
.plan2-summary.income::after { background:radial-gradient(circle,rgba(45,212,191,.34),transparent 67%); }
.plan2-summary.outcome::after { background:radial-gradient(circle,rgba(255,100,111,.29),transparent 67%); }
.plan2-summary.result-card::after { background:radial-gradient(circle,rgba(56,189,248,.28),transparent 67%); }
.plan2-summary-top { margin-bottom:.9rem!important;gap:.72rem!important;position:relative;z-index:1; }
.plan2-icon { width:46px!important;height:46px!important;font-size:1.18rem!important;border:1px solid rgba(255,255,255,.035);box-shadow:inset 0 1px 0 rgba(255,255,255,.03); }
.plan2-icon.in { background:rgba(45,212,191,.15)!important;color:#55e6d1!important; }
.plan2-icon.out { background:rgba(255,100,111,.14)!important;color:#ff7d86!important; }
.plan2-icon.result { background:rgba(56,189,248,.14)!important;color:#67cdf8!important; }
.plan2-summary-name { font-size:.96rem!important;font-weight:720!important; }
.plan2-pair { gap:0!important;position:relative;z-index:1; }
.plan2-pair > div { padding-right:.85rem; }
.plan2-pair > div + div { border-left:1px solid rgba(124,150,168,.15);padding-left:.95rem; }
.plan2-small-label { font-size:.66rem!important;color:#8297a6!important; }
.plan2-big { font-size:1.15rem!important;font-weight:750!important;color:#f2f7f9!important;margin-top:.16rem!important;letter-spacing:-.025em; }
.plan2-delta {
  position:relative;z-index:1;margin-top:.85rem!important;border-radius:10px!important;padding:.48rem .65rem!important;
  font-size:.76rem!important;font-weight:720!important;
}
.plan2-delta.good { background:rgba(45,212,191,.12)!important;color:#53e4cf!important; }
.plan2-delta.bad { background:rgba(255,100,111,.105)!important;color:#ff7e87!important; }
.plan2-delta.neutral { background:rgba(89,132,161,.11)!important;color:#a9bfcd!important; }

/* Painéis */
div[data-testid="stVerticalBlockBorderWrapper"]:has(.plan2-panel-anchor) {
  border:1px solid rgba(126,153,172,.15)!important;border-radius:17px!important;
  background:linear-gradient(155deg,rgba(14,28,39,.97),rgba(10,21,30,.98))!important;
  box-shadow:0 13px 34px rgba(0,0,0,.12),inset 0 1px 0 rgba(255,255,255,.016)!important;
  overflow:hidden!important;
}
div[data-testid="stVerticalBlockBorderWrapper"]:has(.plan2-panel-anchor) > div { padding:1rem 1.08rem!important; }
.plan2-panel-anchor { display:block;width:0;height:0;overflow:hidden; }
.plan2-panel-head { margin-bottom:.58rem!important;padding-bottom:.56rem;border-bottom:1px solid rgba(125,151,170,.12); }
.plan2-panel-title { font-size:1.02rem!important;font-weight:730!important;letter-spacing:-.02em; }
.plan2-panel-note { color:#768b9a!important;font-size:.68rem!important; }
.plan2-empty-inline { padding:.75rem .05rem .25rem;color:#7f94a3;font-size:.76rem; }
.plan2-row { grid-template-columns:minmax(150px,.92fr) minmax(210px,1.45fr) minmax(155px,.72fr) minmax(118px,.55fr)!important;gap:.9rem!important;padding:.68rem .12rem!important;border-color:rgba(126,153,172,.10)!important; }
.plan2-name { font-size:.79rem!important;font-weight:680!important; }
.plan2-name-cell { display:flex;align-items:center;gap:.62rem;min-width:0; }
.plan2-cat-icon { width:30px;height:30px;flex:0 0 30px;border-radius:9px;display:flex;align-items:center;justify-content:center;background:#12232f;border:1px solid rgba(118,150,171,.13);color:#9fb6c5;font-size:.68rem;font-weight:760;box-shadow:inset 0 1px 0 rgba(255,255,255,.02); }
.plan2-name-sub { font-size:.63rem!important;color:#718695!important; }
.plan2-bar,.plan2-debt-progress { background:#1b2a35!important;box-shadow:inset 0 1px 2px rgba(0,0,0,.22); }
.plan2-bar { height:7px!important; }
.plan2-fill.good { background:linear-gradient(90deg,#34d8c1,#50dfca)!important; }
.plan2-fill.warn { background:linear-gradient(90deg,#f3b74f,#ffc866)!important; }
.plan2-fill.bad { background:linear-gradient(90deg,#ff6671,#ff7f87)!important; }
.plan2-values { font-size:.69rem!important;color:#718695!important; }
.plan2-values b { color:#eaf1f4!important;font-weight:700!important; }
.plan2-status { border-radius:9px!important;padding:.34rem .55rem!important;font-size:.66rem!important;font-weight:700!important; }
.plan2-status.good { background:rgba(45,212,191,.105)!important;color:#4ee0cb!important; }
.plan2-status.warn { background:rgba(243,183,79,.12)!important;color:#f5bf5c!important; }
.plan2-status.bad { background:rgba(255,100,111,.11)!important;color:#ff7e87!important; }

.plan2-limit-row { grid-template-columns:minmax(120px,.8fr) minmax(120px,1.05fr) 46px minmax(120px,.9fr)!important;padding:.58rem .05rem!important;border-color:rgba(126,153,172,.10)!important; }
.plan2-percent { font-size:.7rem!important;color:#dfe9ed; }
.plan2-debt-row { padding:.72rem .05rem!important;border-color:rgba(126,153,172,.10)!important; }
.plan2-debt-head { align-items:center!important; }
.plan2-debt-name-wrap { display:flex;align-items:center;gap:.62rem;min-width:0; }
.plan2-debt-icon { width:34px;height:34px;flex:0 0 34px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:rgba(45,212,191,.09);border:1px solid rgba(45,212,191,.13);color:#52dfcb;font-size:.9rem; }
.plan2-debt-name { font-size:.8rem!important;font-weight:700!important; }
.plan2-debt-balance { font-size:.87rem!important;color:#f1f6f8!important; }
.plan2-debt-meta { font-size:.64rem!important;color:#78909f!important;gap:.4rem .9rem!important;margin:.35rem 0 .45rem!important; }
.plan2-debt-progress { height:6px!important; }
.plan2-debt-progress > span { background:linear-gradient(90deg,#2dd4bf,#55e5d1)!important; }

/* Filtros e expansores das abas secundárias */
div[data-testid="stExpander"] { background:rgba(12,25,35,.75)!important;border-color:rgba(126,153,172,.15)!important; }

@media (max-width:900px) {
  .block-container { padding-left:1.2rem!important;padding-right:1.2rem!important; }
  .plan2-title { font-size:1.7rem!important; }
  .plan2-row { grid-template-columns:1fr 1.4fr!important; }
  .plan2-row .plan2-values { text-align:left!important; }
  .plan2-row .plan2-status { justify-self:start!important; }
}
@media (max-width:640px) {
  section[data-testid="stSidebar"] { width:242px!important;min-width:242px!important; }
  section[data-testid="stSidebar"] > div { width:242px!important; }
  .block-container { padding-top:1rem!important;padding-left:.85rem!important;padding-right:.85rem!important; }
  .plan2-summary { min-height:0!important; }
  .plan2-row { grid-template-columns:1fr auto!important; }
  .stTabs [data-baseweb="tab"] { padding:0 .75rem!important; }
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


def _altura_tabela(qtd_linhas, max_altura=430):
    """Evita áreas vazias grandes/brancas no st.dataframe em tabelas pequenas."""
    qtd = max(int(qtd_linhas or 0), 1)
    return min(max_altura, 42 + (qtd * 36))


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


def _totais_planejado_real(dataframe):
    """Retorna planejado, realizado e diferença sem misturar os conceitos."""
    if dataframe is None or dataframe.empty:
        return 0.0, 0.0, 0.0
    planejado = float(pd.to_numeric(dataframe['valor'], errors='coerce').fillna(0).sum())
    pagos = dataframe[dataframe['pago'].fillna(0).astype(int) == 1]
    realizado = float(pd.to_numeric(pagos['valor_pago'], errors='coerce').fillna(0).sum())
    return planejado, realizado, realizado - planejado


def _rotulo_comparativo(dataframe, tipo):
    planejado, realizado, diferenca = _totais_planejado_real(dataframe)
    nome_real = 'Recebido' if tipo == 'Entrada' else 'Pago'
    sinal = '+' if diferenca > 0 else ''
    return (f"Planejado R$ {format_brl(planejado)} · {nome_real} R$ {format_brl(realizado)} "
            f"· Dif. {sinal}R$ {format_brl(diferenca)}")


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
    st.session_state.menu_atual = "🏠 Início"

_nav_btn("🏠 Início", "nav_inicio", "🏠 Início")
_nav_btn("📋 Fluxo", "nav_fluxo", "📊 Fluxo e Prioridades")
_nav_btn("💡 Planejamento", "nav_planejamento", "📑 Demonstrativo")
_nav_btn("💰 Rendas", "nav_rendas", "🏥 Escala de Plantões")
_nav_btn("⚙️ Mais", "nav_mais", "⚙️ Mais")

menu = st.session_state.menu_atual

if "sb_mes" not in st.session_state: st.session_state["sb_mes"] = hoje.month
if "sb_ano" not in st.session_state: st.session_state["sb_ano"] = hoje.year

# No Planejamento 2.0 o período fica no cabeçalho, como no layout de produto.
# Nas demais telas o controle lateral permanece para manter navegação rápida.
if menu != "📑 Demonstrativo":
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
    """Exporta todo o estado funcional do app em um único ZIP versionado."""
    tabelas = {
        'lancamentos.csv': _df_raw('lancamentos'),
        'categorias_personalizadas.csv': fetch_dataframe('SELECT * FROM categorias_personalizadas'),
        'info_dividas.csv': fetch_dataframe('SELECT * FROM info_dividas'),
        'reserva_emergencia.csv': fetch_dataframe('SELECT * FROM reserva_emergencia'),
        'pagamentos.csv': fetch_dataframe('SELECT * FROM pagamentos'),
        'recorrencias_geradas.csv': fetch_dataframe('SELECT * FROM recorrencias_geradas'),
        'orcamentos_categorias.csv': fetch_dataframe('SELECT * FROM orcamentos_categorias'),
    }
    metadata = {
        'schema_version': 3,
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
            _migrar_envelopes_legados()
            return True, "Backup CSV legado restaurado. Categorias/configurações existentes foram preservadas."

        arquivo.seek(0)
        with zipfile.ZipFile(arquivo) as zf:
            nomes = set(zf.namelist())
            obrigatorios = {'lancamentos.csv', 'categorias_personalizadas.csv', 'info_dividas.csv', 'reserva_emergencia.csv'}
            faltantes = obrigatorios - nomes
            if faltantes:
                raise ValueError(f"Backup ZIP incompleto. Faltam: {', '.join(sorted(faltantes))}")

            dfs = {}
            for nome_csv in obrigatorios | {'pagamentos.csv', 'recorrencias_geradas.csv', 'orcamentos_categorias.csv'}:
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
        cols_orc = ['id','competencia','categoria','subgrupo','valor_planejado','origem','criado_em','atualizado_em']

        with transaction() as cur:
            cur.execute("TRUNCATE TABLE pagamentos, recorrencias_geradas, orcamentos_categorias, info_dividas, reserva_emergencia, lancamentos, categorias_personalizadas RESTART IDENTITY CASCADE")
            _insert_dataframe(cur, 'categorias_personalizadas', dfs['categorias_personalizadas.csv'], cols_cat)
            _insert_dataframe(cur, 'lancamentos', df_lanc, cols_lanc)
            _insert_dataframe(cur, 'info_dividas', dfs['info_dividas.csv'], cols_info, 'ON CONFLICT (compra_id) DO UPDATE SET credor=EXCLUDED.credor, taxa_juros_mensal=EXCLUDED.taxa_juros_mensal')
            _insert_dataframe(cur, 'reserva_emergencia', dfs['reserva_emergencia.csv'], cols_reserva, 'ON CONFLICT (id) DO UPDATE SET valor=EXCLUDED.valor, atualizado_em=EXCLUDED.atualizado_em')
            _insert_dataframe(cur, 'pagamentos', dfs['pagamentos.csv'], cols_pag, 'ON CONFLICT (lancamento_id, origem) DO UPDATE SET valor=EXCLUDED.valor, data_pagamento=EXCLUDED.data_pagamento')
            _insert_dataframe(cur, 'recorrencias_geradas', dfs['recorrencias_geradas.csv'], cols_rec, 'ON CONFLICT (categoria_id, competencia) DO NOTHING')
            _insert_dataframe(cur, 'orcamentos_categorias', dfs.get('orcamentos_categorias.csv', pd.DataFrame()), cols_orc, 'ON CONFLICT DO NOTHING')
            cur.execute("INSERT INTO reserva_emergencia (id,valor,atualizado_em) VALUES (1,0,CURRENT_DATE) ON CONFLICT DO NOTHING")
            cur.execute("SELECT setval(pg_get_serial_sequence('categorias_personalizadas','id'), COALESCE((SELECT MAX(id) FROM categorias_personalizadas),1), (SELECT COUNT(*)>0 FROM categorias_personalizadas))")
            cur.execute("SELECT setval(pg_get_serial_sequence('lancamentos','id'), COALESCE((SELECT MAX(id) FROM lancamentos),1), (SELECT COUNT(*)>0 FROM lancamentos))")
            cur.execute("SELECT setval(pg_get_serial_sequence('pagamentos','id'), COALESCE((SELECT MAX(id) FROM pagamentos),1), (SELECT COUNT(*)>0 FROM pagamentos))")
            cur.execute("SELECT setval(pg_get_serial_sequence('orcamentos_categorias','id'), COALESCE((SELECT MAX(id) FROM orcamentos_categorias),1), (SELECT COUNT(*)>0 FROM orcamentos_categorias))")
        _migrar_envelopes_legados()
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
        st.error("Não foi possível verificar sua configuração porque o banco ficou indisponível. O assistente não será aberto automaticamente.")
        if st.button("🔄 Reconectar ao banco", key="retry_onboarding_db"):
            _fechar_pool_atual()
            st.rerun()
        st.stop()
    st.session_state['wizard_ativo'] = (n_categorias_existentes == 0)
    st.session_state['wizard_passo'] = 0

if 'wizard_orcamentos' not in st.session_state and 'wizard_envelopes' in st.session_state:
    st.session_state['wizard_orcamentos'] = list(st.session_state.get('wizard_envelopes') or [])
for _chave in ['wizard_hospitais', 'wizard_fixas', 'wizard_orcamentos', 'wizard_dividas']:
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
          <b>4. 🎯 Orçamentos por categoria</b><br><span class='ux-muted'>Mercado, lazer, transporte e outros planos mensais.</span>
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


def _wizard_passo4_orcamentos():
    _wizard_cabecalho(4, "🎯 Quanto você pretende gastar nas categorias variáveis?")
    st.caption("Ex.: mercado, lazer, farmácia, transporte. Isso será o orçamento do mês, não uma conta a pagar.")
    with st.form("wizard_form_orcamento", clear_on_submit=True):
        c1, c2 = st.columns([2, 1.3])
        nome = c1.text_input("Gasto", placeholder="Ex: Mercado")
        valor_txt = c2.text_input("Orçamento do mês (R$)", value="0,00")
        if st.form_submit_button("＋ Adicionar orçamento"):
            valor_f = parse_valor(valor_txt)
            if nome.strip() and valor_f > 0:
                st.session_state['wizard_orcamentos'].append({"nome": nome.strip(), "valor": valor_f})
                st.rerun()
    _wizard_lista_com_remover(st.session_state['wizard_orcamentos'], 'wizard_orcamentos',
        lambda e: f"🎯 {e['nome']} · R$ {format_brl(e['valor'])} planejados")
    _wizard_navegacao(4, texto_avancar="Revisar →")


def _wizard_passo5_revisao():
    _wizard_cabecalho(5, "📋 Revise antes de salvar")
    hospitais = st.session_state['wizard_hospitais']
    fixas = st.session_state['wizard_fixas']
    orcamentos = st.session_state['wizard_orcamentos']
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
        if orcamentos:
            st.markdown("**🎯 Orçamentos do mês**")
            for e in orcamentos: st.write(f"• {e['nome']} · R$ {format_brl(e['valor'])}/mês")
        if not any([hospitais, fixas, orcamentos, dividas]):
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
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,atraso_meses,dia_pagamento,is_recorrente,data_inicio) VALUES ('Entrada','Plantões',%s,%s,%s,0,%s) ON CONFLICT DO NOTHING",
                                (h['nome'], h['atraso_meses'], h['dia_pagamento'], hoje_wizard))
                for f in fixas:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento,is_recorrente,data_inicio) VALUES ('Despesa','Despesas Essenciais',%s,%s,0,%s,1,%s) ON CONFLICT DO NOTHING",
                                (f['nome'], f['valor'], f['dia_vencimento'], hoje_wizard))
                for e in orcamentos:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,is_recorrente,data_inicio) VALUES ('Despesa','Despesas Essenciais',%s,0,%s) ON CONFLICT DO NOTHING",
                                (e['nome'], hoje_wizard))
                    cur.execute('''
                        INSERT INTO orcamentos_categorias (competencia,categoria,subgrupo,valor_planejado,origem)
                        VALUES (DATE_TRUNC('month', %s::date)::date,'Despesas Essenciais',%s,%s,'onboarding')
                        ON CONFLICT DO NOTHING
                    ''', (hoje_wizard, e['nome'], e['valor']))
                for d in dividas:
                    cur.execute("INSERT INTO categorias_personalizadas (tipo,categoria,subgrupo,is_recorrente) VALUES ('Despesa','Dívidas',%s,0) ON CONFLICT DO NOTHING", (d['nome'],))
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
            for k in ['wizard_hospitais','wizard_fixas','wizard_orcamentos','wizard_dividas']:
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
    elif passo == 4: _wizard_passo4_orcamentos()
    else: _wizard_passo5_revisao()

# =================================================================
# 8+. INTERFACE UX — USO DIÁRIO, ANÁLISE E CONFIGURAÇÕES
# =================================================================


def _valor_previsto_linha(r):
    return float_seguro(r.get('valor'))


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


def _total_despesa_planejada(df):
    """Compatibilidade: soma lançamentos; o Planejamento 2.0 usa orçamento por categoria."""
    if df is None or df.empty:
        return 0.0
    d = df[df['tipo'] == 'Despesa'].copy()
    if d.empty:
        return 0.0
    return float(pd.to_numeric(d['valor'], errors='coerce').fillna(0).sum())


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

def _consolidar_operacional(df, consolidar_cartao=False):
    """Prepara os lançamentos exibidos no uso diário.

    Na UX 2.0 a Home e o Fluxo não devem inventar uma entidade financeira que
    o usuário nunca cadastrou. Por isso, despesas com forma_pagamento='Crédito'
    permanecem como os próprios lançamentos por padrão. A consolidação legada
    de cartão continua disponível apenas quando chamada explicitamente com
    consolidar_cartao=True (ex.: ferramentas avançadas/migração futura).
    """
    cols_saida = ['id_ui','tipo','categoria','descricao','valor','valor_pago','pago','data_vencimento','data_pagamento','prioridade','ids','consolidado','ordem_pri','atrasado','ordem_atraso']
    if df.empty: return pd.DataFrame(columns=cols_saida)
    base = df.copy()
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)
    linhas = []
    if consolidar_cartao and 'forma_pagamento' in base.columns:
        mask_cred = (base['tipo'] == 'Despesa') & (base['forma_pagamento'] == 'Crédito')
    else:
        # UX diária: preserve cada despesa real exatamente como foi cadastrada.
        mask_cred = pd.Series(False, index=base.index)

    if mask_cred.any():
        credito = base[mask_cred].copy()
        credito['_mes_fatura'] = pd.to_datetime(credito['data_vencimento']).dt.to_period('M').astype(str)
        for mes_fatura, grp in credito.groupby('_mes_fatura'):
            all_paid = bool((grp['pago'] == 1).all())
            datas_pg = pd.to_datetime(grp['data_pagamento'], errors='coerce').dropna() if 'data_pagamento' in grp.columns else pd.Series(dtype='datetime64[ns]')
            data_pg = datas_pg.max().date() if all_paid and not datas_pg.empty else None
            linhas.append({
                'id_ui':f'cartao_{mes_fatura}', 'tipo':'Despesa', 'categoria':'Cartão de Crédito', 'descricao':"💳 Fatura do cartão",
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
                'id_ui':f"plant_{hospital}_{dt}", 'tipo':'Entrada', 'categoria':hospital, 'descricao':f"🏥 {hospital}",
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


def _valor_operacional(r):
    planejado = max(float_seguro(r.get('valor')), 0.0)
    realizado = max(float_seguro(r.get('valor_pago')), 0.0)
    return realizado if int_seguro(r.get('pago')) == 1 and realizado > 0 else planejado


def _data_operacional(r):
    if int_seguro(r.get('pago')) == 1:
        dp = pd.to_datetime(r.get('data_pagamento'), errors='coerce')
        if pd.notna(dp):
            return dp.date()
    dv = pd.to_datetime(r.get('data_vencimento'), errors='coerce')
    return dv.date() if pd.notna(dv) else hoje


def _montar_plano_pagamentos(df_ops, ano, mes):
    """Cria uma agenda de caixa sem assumir saldo bancário externo ao app.

    As fontes recebidas são consumidas primeiro pelos pagamentos já realizados.
    Depois, o que sobra nelas e as entradas ainda previstas são alocados às contas
    pendentes em ordem de vencimento. Se a cobertura só aparece depois do vencimento,
    a conta recebe um alerta de risco.
    """
    resultado = {
        'fontes': [], 'contas': [], 'risco_contas': [], 'reserva_minima': 0.0,
        'reserva_sugerida': 0.0, 'recebido_nao_alocado': 0.0,
        'recebido_total': 0.0, 'previsto_total': 0.0, 'uso_externo_historico': 0.0,
    }
    if df_ops is None or df_ops.empty:
        return resultado

    base = df_ops.copy()
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)

    fontes = []
    for _, r in base[base['tipo'] == 'Entrada'].iterrows():
        valor = _valor_operacional(r)
        if valor <= 0:
            continue
        fonte = {
            'id': str(r.get('id_ui')),
            'descricao': str(r.get('descricao') or r.get('categoria') or 'Entrada'),
            'categoria': str(r.get('categoria') or ''),
            'data': _data_operacional(r),
            'valor': round(valor, 2),
            'restante': round(valor, 2),
            'recebido': int_seguro(r.get('pago')) == 1,
            'compromissos': [],
        }
        fontes.append(fonte)
    fontes.sort(key=lambda x: (x['data'], 0 if x['recebido'] else 1, x['descricao']))

    resultado['recebido_total'] = round(sum(f['valor'] for f in fontes if f['recebido']), 2)
    resultado['previsto_total'] = round(sum(f['valor'] for f in fontes if not f['recebido']), 2)

    despesas = []
    for _, r in base[base['tipo'] == 'Despesa'].iterrows():
        valor = _valor_operacional(r)
        if valor <= 0:
            continue
        despesas.append({
            'id': str(r.get('id_ui')),
            'descricao': str(r.get('descricao') or r.get('categoria') or 'Despesa'),
            'categoria': str(r.get('categoria') or ''),
            'data': _data_operacional(r),
            'vencimento': pd.to_datetime(r.get('data_vencimento'), errors='coerce').date(),
            'valor': round(valor, 2),
            'pago': int_seguro(r.get('pago')) == 1,
            'prioridade': str(r.get('prioridade') or ''),
            'alocacoes': [],
            'risco_valor': 0.0,
            'descoberto': 0.0,
        })

    pagos = sorted([d for d in despesas if d['pago']], key=lambda x: (x['data'], x['descricao']))
    pendentes = sorted([d for d in despesas if not d['pago']], key=lambda x: (x['vencimento'], prioridades_map.get(x['prioridade'], 2), x['descricao']))

    def alocar(conta, valor_restante, predicado, tipo_alocacao):
        for fonte in fontes:
            if valor_restante <= 0.004:
                break
            if fonte['restante'] <= 0.004 or not predicado(fonte):
                continue
            uso = round(min(fonte['restante'], valor_restante), 2)
            if uso <= 0:
                continue
            fonte['restante'] = round(fonte['restante'] - uso, 2)
            valor_restante = round(valor_restante - uso, 2)
            conta['alocacoes'].append({'fonte_id': fonte['id'], 'fonte': fonte['descricao'], 'data': fonte['data'], 'valor': uso, 'tipo': tipo_alocacao})
            fonte['compromissos'].append({'conta_id': conta['id'], 'descricao': conta['descricao'], 'vencimento': conta['vencimento'], 'valor': uso, 'pago': conta['pago']})
        return max(round(valor_restante, 2), 0.0)

    # O que já foi pago consome apenas entradas que realmente já foram recebidas
    # até aquela data. Diferenças representam recursos trazidos de fora do mês/app.
    for conta in pagos:
        faltante = conta['valor']
        faltante = alocar(conta, faltante, lambda f, dt=conta['data']: f['recebido'] and f['data'] <= dt, 'historico')
        if faltante > 0.004:
            resultado['uso_externo_historico'] += faltante
            conta['descoberto'] = faltante

    # Contas futuras usam primeiro recursos que chegam até o vencimento; só depois
    # recorrem a entradas posteriores, que indicam risco de atraso sem reserva.
    for conta in pendentes:
        faltante = conta['valor']
        faltante = alocar(conta, faltante, lambda f, dt=conta['vencimento']: f['data'] <= dt, 'no_prazo')
        risco = faltante
        if faltante > 0.004:
            faltante = alocar(conta, faltante, lambda f, dt=conta['vencimento']: f['data'] > dt, 'apos_vencimento')
        conta['risco_valor'] = round(risco, 2)
        conta['descoberto'] = round(faltante, 2)
        if conta['risco_valor'] > 0.004:
            resultado['risco_contas'].append(conta)

    # Reserva de virada: pior déficit acumulado do mês, partindo de zero.
    eventos = []
    for _, r in base.iterrows():
        valor = _valor_operacional(r)
        if valor <= 0:
            continue
        data_ev = _data_operacional(r)
        sinal = 1 if r['tipo'] == 'Entrada' else -1
        eventos.append((data_ev, 0 if sinal > 0 else 1, sinal * valor))
    eventos.sort(key=lambda x: (x[0], x[1]))
    acumulado = 0.0
    minimo = 0.0
    for _, _, valor in eventos:
        acumulado += valor
        minimo = min(minimo, acumulado)
    reserva = max(-minimo, 0.0)
    resultado['reserva_minima'] = round(reserva, 2)
    resultado['reserva_sugerida'] = round(reserva * 1.10, 2) if reserva > 0 else 0.0

    resultado['recebido_nao_alocado'] = round(sum(max(f['restante'], 0.0) for f in fontes if f['recebido']), 2)
    resultado['fontes'] = fontes
    resultado['contas'] = pagos + pendentes
    return resultado


def _render_plano_pagamentos(df_ops, ano, mes):
    """Renderiza o casamento renda → contas como informação principal do Fluxo."""
    plano = _montar_plano_pagamentos(df_ops, ano, mes)
    fontes = plano['fontes']
    contas = plano['contas']
    pendentes = [c for c in contas if not c['pago']]

    if not fontes and not pendentes:
        render_empty_state("Nada para planejar", "Cadastre entradas e contas reais neste período para montar a cobertura do mês.", "◎")
        return

    total_pendente = round(sum(c['valor'] for c in pendentes), 2)
    risco_total = round(sum(c['risco_valor'] for c in plano['risco_contas']), 2)
    coberto_prazo = max(round(total_pendente - risco_total, 2), 0.0)
    pct_coberto = (coberto_prazo / total_pendente * 100.0) if total_pendente > 0 else 100.0
    n_risco = len(plano['risco_contas'])

    st.markdown("### Cobertura do mês")
    st.caption("Veja qual recebimento sustenta cada conta. O cálculo usa apenas o que está registrado no app — não é saldo bancário.")

    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown(
            f"<div class='ux-cover-summary'><div class='ux-cover-label'>Cobertura no prazo</div>"
            f"<div class='ux-cover-value ux-positive'>R$ {format_brl(coberto_prazo)}</div>"
            f"<div class='ux-cover-note'>de R$ {format_brl(total_pendente)} · {pct_coberto:.0f}% das contas pendentes</div></div>",
            unsafe_allow_html=True,
        )
    with s2:
        tom_risco = "ux-negative" if risco_total > 0.004 else "ux-positive"
        st.markdown(
            f"<div class='ux-cover-summary'><div class='ux-cover-label'>Em risco de data</div>"
            f"<div class='ux-cover-value {tom_risco}'>R$ {format_brl(risco_total)}</div>"
            f"<div class='ux-cover-note'>{n_risco} conta(s) dependem de dinheiro que chega tarde</div></div>",
            unsafe_allow_html=True,
        )
    with s3:
        st.markdown(
            f"<div class='ux-cover-summary'><div class='ux-cover-label'>Colchão necessário</div>"
            f"<div class='ux-cover-value'>R$ {format_brl(plano['reserva_sugerida'])}</div>"
            f"<div class='ux-cover-note'>reserva de virada com 10% de margem</div></div>",
            unsafe_allow_html=True,
        )

    if n_risco:
        st.markdown(
            f"<div class='ux-cover-alert'><b>⚠️ {n_risco} conta(s) dependem de uma renda que entra depois do vencimento.</b> "
            f"Os conflitos aparecem diretamente abaixo da fonte de renda correspondente.</div>",
            unsafe_allow_html=True,
        )
    elif pendentes:
        st.markdown(
            "<div class='ux-cover-ok'><b>✓ Todas as contas pendentes têm cobertura registrada até o vencimento.</b></div>",
            unsafe_allow_html=True,
        )

    if plano['recebido_nao_alocado'] > 0.004:
        st.markdown(
            f"<div class='ux-secondary-note'>Dos recebimentos já confirmados, <b>R$ {format_brl(plano['recebido_nao_alocado'])}</b> "
            f"ainda não estão comprometidos com contas cadastradas.</div>",
            unsafe_allow_html=True,
        )

    st.markdown("### Casamento de rendas")
    visao = st.radio(
        "Organizar por",
        ["Por renda", "Por vencimento"],
        horizontal=True,
        label_visibility="collapsed",
        key=f"cobertura_visao_{ano}_{mes}",
    )

    def _status_fonte(fonte):
        futuros = [x for x in fonte['compromissos'] if not x['pago']]
        conflito = any(x['vencimento'] < fonte['data'] for x in futuros)
        uso = max(fonte['valor'] - fonte['restante'], 0.0)
        taxa = (uso / fonte['valor']) if fonte['valor'] > 0 else 0.0
        if conflito:
            return "🔴 Conflito de data", "danger", taxa
        if taxa >= 0.85:
            return "🟡 Quase toda comprometida", "warn", taxa
        return "🟢 Cobertura saudável", "ok", taxa

    def _render_fonte(fonte):
        compromissos_futuros = sorted(
            [x for x in fonte['compromissos'] if not x['pago']],
            key=lambda x: (x['vencimento'], x['descricao'])
        )
        usados = round(sum(x['valor'] for x in fonte['compromissos'] if x['pago']), 2)
        comprometido = round(sum(x['valor'] for x in compromissos_futuros), 2)
        nao_comprometido = max(round(fonte['restante'], 2), 0.0)
        status_txt, status_tom, taxa = _status_fonte(fonte)
        bar_cls = {"danger":"ux-cover-fill-danger", "warn":"ux-cover-fill-warn", "ok":"ux-cover-fill-ok"}[status_tom]
        badge_status = "ux-source-received" if fonte['recebido'] else "ux-source-planned"
        badge_txt = "RECEBIDO" if fonte['recebido'] else "PREVISTO"
        status_badge = {"danger":"ux-source-risk", "warn":"ux-source-attn", "ok":"ux-source-ok"}[status_tom]
        pct_bar = min(max(taxa * 100.0, 0.0), 100.0)

        with st.container(border=True):
            h1, h2 = st.columns([4, 1.45])
            with h1:
                st.markdown(
                    f"<div class='ux-income-name'>{'✓' if fonte['recebido'] else '◷'} {fonte['descricao']} "
                    f"<span class='ux-source-badge {badge_status}'>{badge_txt}</span> "
                    f"<span class='ux-source-badge {status_badge}'>{status_txt}</span></div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div class='ux-income-meta'>{fonte['data'].strftime('%d/%m/%Y')} · "
                    f"Recebimento de R$ {format_brl(fonte['valor'])}</div>", unsafe_allow_html=True
                )
            with h2:
                st.markdown(
                    f"<div style='text-align:right'><div class='ux-cover-label'>Não comprometido</div>"
                    f"<div class='ux-income-amount'>R$ {format_brl(nao_comprometido)}</div></div>", unsafe_allow_html=True
                )

            st.markdown(
                f"<div class='ux-income-stats'>"
                + (f"<span>Já utilizado <b>R$ {format_brl(usados)}</b></span>" if fonte['recebido'] and usados > 0.004 else "")
                + f"<span>Próximas contas <b>R$ {format_brl(comprometido)}</b></span>"
                + f"<span>Comprometimento <b>{taxa*100:.0f}%</b></span></div>"
                f"<div class='ux-cover-bar'><div class='{bar_cls}' style='width:{pct_bar:.1f}%'></div></div>",
                unsafe_allow_html=True,
            )

            if compromissos_futuros:
                for item in compromissos_futuros:
                    dias_conflito = (fonte['data'] - item['vencimento']).days
                    conflito = dias_conflito > 0
                    alerta = f"<div class='ux-match-warning'>⚠ vence {dias_conflito} dia(s) antes desta renda</div>" if conflito else ""
                    st.markdown(
                        f"<div class='ux-match-line'><div class='ux-match-date'>{item['vencimento'].strftime('%d/%m')}</div>"
                        f"<div><div class='ux-match-desc'>{item['descricao']}</div>{alerta}</div>"
                        f"<div class='ux-match-value'>R$ {format_brl(item['valor'])}</div></div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.caption("Nenhuma conta futura foi atribuída a este recebimento.")

    if visao == "Por renda":
        # Fontes já totalmente consumidas por pagamentos passados não ajudam na decisão
        # das próximas contas e ficam fora da visão principal para reduzir ruído.
        recebidas = [
            f for f in fontes if f['recebido'] and (
                f['restante'] > 0.004 or any(not x['pago'] for x in f['compromissos'])
            )
        ]
        previstas = [f for f in fontes if not f['recebido']]

        if recebidas:
            st.markdown("#### Dinheiro já recebido")
            st.caption("Fontes confirmadas que ainda têm valor não comprometido ou sustentam próximas contas.")
            for fonte in recebidas:
                _render_fonte(fonte)

        if previstas:
            st.markdown("#### Próximos recebimentos")
            st.caption("Receitas ainda previstas. Conflitos de vencimento aparecem dentro do próprio casamento.")
            for fonte in previstas:
                _render_fonte(fonte)

        if not fontes:
            st.info("Nenhuma fonte de renda foi registrada neste período.")

    else:
        st.markdown("#### Contas por vencimento")
        st.caption("A mesma cobertura vista pela ordem em que as contas precisam ser pagas.")
        if not pendentes:
            render_empty_state("Nenhuma conta pendente", "As contas reais deste período já foram baixadas.", "✓")
        else:
            for conta in sorted(pendentes, key=lambda x: (x['vencimento'], prioridades_map.get(x['prioridade'], 2), x['descricao'])):
                alocs = conta['alocacoes']
                partes = []
                tem_tardia = False
                for a in alocs:
                    tardia = a['tipo'] == 'apos_vencimento'
                    tem_tardia = tem_tardia or tardia
                    partes.append(f"{a['fonte']} · {a['data'].strftime('%d/%m')} · R$ {format_brl(a['valor'])}" + (" ⚠️" if tardia else ""))
                fonte_txt = " + ".join(partes) if partes else "Sem fonte registrada"
                with st.container(border=True):
                    c1, c2 = st.columns([4.2, 1.2])
                    c1.markdown(f"**{conta['vencimento'].strftime('%d/%m')} · {conta['descricao']}**")
                    c1.caption(f"Fonte: {fonte_txt}")
                    if tem_tardia:
                        c1.caption("⚠️ Parte da cobertura entra depois do vencimento.")
                    if conta['descoberto'] > 0.004:
                        c1.caption(f"🔴 Ainda sem cobertura no mês: R$ {format_brl(conta['descoberto'])}")
                    c2.markdown(f"<div class='ux-income-amount' style='text-align:right'>R$ {format_brl(conta['valor'])}</div>", unsafe_allow_html=True)

    sem_cobertura = [c for c in pendentes if c['descoberto'] > 0.004]
    if sem_cobertura:
        st.markdown("### Sem cobertura registrada")
        st.caption("Estas contas continuam sem uma fonte suficiente mesmo considerando os recebimentos previstos do mês.")
        for conta in sem_cobertura:
            st.markdown(
                f"<div class='ux-cover-alert'><b>{conta['vencimento'].strftime('%d/%m')} · {conta['descricao']}</b> · "
                f"R$ {format_brl(conta['descoberto'])} ainda sem cobertura.</div>", unsafe_allow_html=True
            )

    with st.expander("Entender o colchão necessário", expanded=False):
        st.write(
            "É o valor necessário para atravessar os dias em que contas vencem antes das entradas correspondentes. "
            "Ele não é reserva de emergência e não representa saldo bancário."
        )
        st.metric("Mínimo calculado", f"R$ {format_brl(plano['reserva_minima'])}")
        st.metric("Meta com 10% de margem", f"R$ {format_brl(plano['reserva_sugerida'])}")
        if plano['uso_externo_historico'] > 0.004:
            st.caption(
                f"Pagamentos já realizados indicam R$ {format_brl(plano['uso_externo_historico'])} de recursos "
                "que não vieram das entradas recebidas registradas neste mês."
            )



def _fluxo2_texto_cobertura(conta):
    """Texto curto de casamento para uma conta pendente."""
    if not conta:
        return "", ""
    if float_seguro(conta.get('descoberto')) > 0.004:
        return "danger", f"Sem renda suficiente · faltam R$ {format_brl(conta['descoberto'])}"

    tardias = [a for a in conta.get('alocacoes', []) if a.get('tipo') == 'apos_vencimento']
    if tardias:
        a = min(tardias, key=lambda x: x['data'])
        dias = max((a['data'] - conta['vencimento']).days, 1)
        plural = "dia" if dias == 1 else "dias"
        return "warn", f"{a['fonte']} entra em {a['data'].strftime('%d/%m')} · {dias} {plural} depois"

    no_prazo = [a for a in conta.get('alocacoes', []) if a.get('tipo') in ('no_prazo', 'historico')]
    if no_prazo:
        nomes = []
        for a in no_prazo:
            nome = str(a['fonte'])
            if nome not in nomes:
                nomes.append(nome)
        if len(nomes) == 1:
            data_fonte = max(a['data'] for a in no_prazo if str(a['fonte']) == nomes[0])
            return "ok", f"Coberto por {nomes[0]} · {data_fonte.strftime('%d/%m')}"
        return "ok", "Coberto por " + " + ".join(nomes[:2]) + (" + …" if len(nomes) > 2 else "")
    return "danger", "Sem fonte de renda associada"


def _fluxo2_resumo_proxima_renda(plano, ano, mes):
    fontes = plano.get('fontes', [])
    pendentes = [c for c in plano.get('contas', []) if not c.get('pago')]
    referencia = hoje if (ano == hoje.year and mes == hoje.month) else datetime.date(ano, mes, 1)
    candidatas = [f for f in fontes if (not f.get('recebido')) and f.get('data') >= referencia]
    if not candidatas:
        return None
    fonte = min(candidatas, key=lambda f: (f['data'], f['descricao']))
    contas_ate = [c for c in pendentes if referencia <= c['vencimento'] <= fonte['data']]
    total = round(sum(float_seguro(c['valor']) for c in contas_ate), 2)
    risco = round(sum(float_seguro(c.get('risco_valor')) for c in contas_ate), 2)
    return {'fonte': fonte, 'contas': contas_ate, 'total': total, 'risco': risco}


def _render_fluxo2_ponte(plano, ano, mes):
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
        status = '✓ Coberto'
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
    """Baixa em lote usando o valor planejado de cada linha real, em uma transação."""
    ids = []
    for r in linhas:
        ids.extend(int(x) for x in r['ids'])
    ids = sorted(set(ids))
    if not ids:
        return
    with transaction() as cur:
        cur.execute(
            "UPDATE lancamentos SET pago=1, valor_pago=valor, data_pagamento=%s WHERE id = ANY(%s)",
            (data_pagamento, ids),
        )


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
            valor_exibir = realizado if pago and realizado > 0 else planejado
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
                csel.checkbox("Selecionar", key=f"{prefixo}_sel_{id_ui}", label_visibility="collapsed")
                paid_cls = ' flow2-paid' if pago else ''
                cdesc.markdown(
                    f"<span class='flow2-row-anchor'></span><div class='{paid_cls.strip()}'>"
                    f"<div class='flow2-name'>{html.escape(str(r['descricao']))}</div>"
                    f"<div class='flow2-meta {meta_cls}'>{html.escape(status_meta)}</div></div>",
                    unsafe_allow_html=True,
                )

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
                    if cextra.button("↩", key=f"{prefixo}_undo_{id_ui}", help="Estornar", use_container_width=True):
                        _marcar_ids(r['ids'], pago=False)
                        flash('success', 'Baixa desfeita. O planejado foi preservado.')
                        st.rerun()
                elif r['tipo'] == 'Entrada' and fonte:
                    detalhe_key = f"{prefixo}_detail_{id_ui}"
                    if cextra.button("›", key=f"{prefixo}_detail_btn_{id_ui}", help="Ver contas ligadas a esta renda", use_container_width=True):
                        st.session_state[detalhe_key] = not bool(st.session_state.get(detalhe_key, False))
                        st.rerun()
                else:
                    cextra.write("")

            if (not pago) and st.session_state.get('_pagamento_aberto') == chave_acao:
                acao_nome = 'pagamento' if r['tipo'] == 'Despesa' else 'recebimento'
                with st.container(border=True):
                    st.markdown(f"**Confirmar {acao_nome} · {r['descricao']}**")
                    st.caption(f"Planejado: R$ {format_brl(planejado)}")
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
            f"<div class='ux-flow-desc'>{grupo_icon}<span class='ux-flow-date'>{data_txt}</span>{r['descricao']}</div>"
            f"<div class='ux-flow-category'>{status_icon} {status_text}"
            + (f" · {categoria_txt}" if categoria_txt else "") + "</div></div>",
            unsafe_allow_html=True,
        )

        if pago:
            principal = realizado if realizado > 0 else planejado
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

        chave_acao = f"{prefixo}:{r['id_ui']}"
        if pago:
            if c3.button("↩ Estornar", key=f"{prefixo}_est_{i}_{r['id_ui']}", use_container_width=True):
                _marcar_ids(r['ids'], pago=False)
                if st.session_state.get('_pagamento_aberto') == chave_acao:
                    st.session_state.pop('_pagamento_aberto', None)
                flash('success', 'Baixa desfeita. O valor planejado foi preservado.')
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
                    valor_informado = parse_valor(valor_txt) if str(valor_txt).strip() else 0.0
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


def _planejamento_unidades(df, ano=None, mes=None):
    """Planejado x realizado por categoria/subgrupo, sem lançamentos de orçamento."""
    ano = int(ano if ano is not None else ano_selecionado)
    mes = int(mes if mes is not None else mes_selecionado)
    d = df[df['tipo'] == 'Despesa'].copy() if df is not None and not df.empty else pd.DataFrame()
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
        realizado = float(pd.to_numeric(g[g['pago'] == 1]['valor_pago'], errors='coerce').fillna(0).sum()) if not g.empty else 0.0
        diferenca = realizado - planejado
        percentual = (realizado / planejado * 100.0) if planejado > 0 else (100.0 if realizado > 0 else 0.0)
        rows.append({'categoria':cat,'subgrupo':sub,'nome':sub if sub else cat,'planejado':planejado,'realizado':realizado,'diferenca':diferenca,'percentual':percentual,'tem_orcamento':tem_orcamento})
    return pd.DataFrame(rows, columns=['categoria','subgrupo','nome','planejado','realizado','diferenca','percentual','tem_orcamento'])


def _planejamento_resumo(df, ano=None, mes=None, unidades=None):
    vazio = {'receita_planejada':0.0,'receita_realizada':0.0,'despesa_planejada':0.0,'despesa_realizada':0.0,'resultado_planejado':0.0,'resultado_realizado':0.0}
    base = df.copy() if df is not None else pd.DataFrame()
    unidades = unidades if unidades is not None else _planejamento_unidades(base, ano, mes)
    desp_plan = float(unidades['planejado'].sum()) if not unidades.empty else 0.0
    if base.empty:
        vazio['despesa_planejada'] = desp_plan
        vazio['resultado_planejado'] = -desp_plan
        return vazio
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)
    base['pago'] = pd.to_numeric(base['pago'], errors='coerce').fillna(0).astype(int)
    entradas = base[base['tipo'] == 'Entrada']
    despesas = base[base['tipo'] == 'Despesa']
    rec_plan = float(entradas['valor'].sum())
    rec_real = float(entradas[entradas['pago'] == 1]['valor_pago'].sum())
    desp_real = float(despesas[despesas['pago'] == 1]['valor_pago'].sum())
    return {'receita_planejada':rec_plan,'receita_realizada':rec_real,'despesa_planejada':desp_plan,'despesa_realizada':desp_real,'resultado_planejado':rec_plan-desp_plan,'resultado_realizado':rec_real-desp_real}


def _planejamento_orcamentos(df, ano=None, mes=None):
    unidades = _planejamento_unidades(df, ano, mes)
    if unidades.empty:
        return pd.DataFrame(columns=['categoria','subgrupo','nome','orcamento','realizado','disponivel','percentual'])
    o = unidades[unidades['tem_orcamento']].copy()
    if o.empty:
        return pd.DataFrame(columns=['categoria','subgrupo','nome','orcamento','realizado','disponivel','percentual'])
    o['orcamento'] = o['planejado']
    o['disponivel'] = o['planejado'] - o['realizado']
    return o[['categoria','subgrupo','nome','orcamento','realizado','disponivel','percentual']]


def _planejamento_dividas():
    """Resumo das compras parceladas para a aba Planejamento > Dívidas."""
    dd = fetch_dataframe("""
        SELECT compra_id, categoria, subgrupo, MIN(descricao) descricao,
               SUM(valor) valor_total,
               SUM(CASE WHEN pago=1 THEN valor_pago ELSE 0 END) valor_pago_total,
               MAX(total_parcelas) total_parcelas,
               SUM(CASE WHEN pago=1 THEN 1 ELSE 0 END) parcelas_pagas,
               MIN(CASE WHEN pago=0 THEN data_vencimento END) proxima_parcela,
               MAX(data_vencimento) data_fim,
               MIN(CASE WHEN pago=0 THEN valor END) parcela_referencia
        FROM lancamentos
        WHERE tipo='Despesa' AND total_parcelas>1 AND total_parcelas!=999 AND compra_id IS NOT NULL
        GROUP BY compra_id,categoria,subgrupo
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
    dd['saldo'] = (dd['valor_total'] - dd['valor_pago_total']).clip(lower=0.0)
    dd['parcelas_restantes'] = (dd['total_parcelas'] - dd['parcelas_pagas']).clip(lower=0)
    dd['progresso'] = dd.apply(lambda r: min(max(r['parcelas_pagas'] / r['total_parcelas'], 0.0), 1.0) if r['total_parcelas'] else 0.0, axis=1)
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
        f"<div class='plan2-values'><b>R$ {format_brl(realizado)}</b> de R$ {format_brl(planejado)}</div>"
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


if st.session_state.get('wizard_ativo'):
    renderizar_wizard_configuracao()

# Build UX 2.0: ui-refino-planejamento-v16
# -----------------------------------------------------------------
# INÍCIO
# -----------------------------------------------------------------
elif menu == "🏠 Início":
    df_mes = _dados_mes()

    # Cabeçalho orientado a contexto, não a análise.
    st.markdown(
        f"<div class='home2-head'><div class='home2-hello'>Olá 👋</div>"
        f"<div class='home2-sub'>{meses[mes_selecionado-1]} de {ano_selecionado} · veja o que precisa da sua atenção agora.</div></div>",
        unsafe_allow_html=True,
    )

    if df_mes.empty:
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

        ent = df_real[df_real['tipo']=='Entrada']
        desp = df_real[df_real['tipo']=='Despesa']
        recebido = float(ent[pd.to_numeric(ent['pago'], errors='coerce').fillna(0).astype(int)==1]['valor_pago'].sum())
        a_receber = float(ent[pd.to_numeric(ent['pago'], errors='coerce').fillna(0).astype(int)==0]['valor'].sum())
        pago = float(desp[pd.to_numeric(desp['pago'], errors='coerce').fillna(0).astype(int)==1]['valor_pago'].sum())
        a_pagar = float(desp[pd.to_numeric(desp['pago'], errors='coerce').fillna(0).astype(int)==0]['valor'].sum())
        resultado_atual = recebido - pago

        # A Home olha além da borda do mês: no fim de setembro, por exemplo,
        # a próxima renda de 05/10 precisa aparecer. O resumo mensal continua
        # restrito ao mês selecionado; apenas orientação, alertas e timeline
        # usam uma janela curta para frente.
        data_ref = data_contexto_ativo
        limite_home = max(fim_periodo, data_ref + datetime.timedelta(days=45))
        df_janela = fetch_dataframe(
            "SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento",
            (inicio_periodo, limite_home),
        )
        if df_janela.empty and len(df_janela.columns) == 0:
            df_janela = df_real.copy()

        ops_home = _consolidar_operacional(df_janela) if not df_janela.empty else pd.DataFrame()
        plano_home = _montar_plano_pagamentos(ops_home, ano_selecionado, mes_selecionado)

        # ---------------------------------------------------------
        # 1. PRÓXIMA RENDA + PONTE ATÉ ELA
        # ---------------------------------------------------------
        fontes_futuras = [f for f in plano_home['fontes'] if (not f['recebido']) and f['data'] >= data_ref]
        proxima_renda = min(fontes_futuras, key=lambda f: (f['data'], f['descricao'])) if fontes_futuras else None
        pendentes = [c for c in plano_home['contas'] if not c['pago']]

        if proxima_renda:
            contas_ate = [c for c in pendentes if data_ref <= c['vencimento'] <= proxima_renda['data']]
            total_ate = round(sum(c['valor'] for c in contas_ate), 2)
            risco_ate = round(sum(c['risco_valor'] for c in contas_ate), 2)
            qtd_ate = len(contas_ate)
            dias_renda = (proxima_renda['data'] - data_ref).days
            quando = "hoje" if dias_renda == 0 else ("amanhã" if dias_renda == 1 else proxima_renda['data'].strftime('%d/%m'))
            if risco_ate <= 0.004:
                hero_cls, status_cls = "", "ok"
                status_titulo = "✓ Cobertura suficiente"
                status_texto = "Suas próximas contas estão cobertas com as rendas registradas."
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
                f"<div class='home2-hero-side'><div class='home2-eyebrow'>Até essa data vencem</div>"
                f"<div class='home2-bridge-value'>R$ {format_brl(total_ate)}</div>"
                f"<div class='home2-income-date'>{qtd_ate} conta(s)</div>"
                f"<div class='home2-status {status_cls}'><b>{status_titulo}</b><br>{status_texto}</div></div></div></div>",
                unsafe_allow_html=True,
            )
            if st.button("Ver contas até essa renda →", key="home2_ver_ponte", use_container_width=True):
                st.session_state.menu_atual = "📊 Fluxo e Prioridades"
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
            forma_pgto = a1.selectbox(
                "Forma de pagamento", ["À vista","Crédito","Outros"], index=0,
                help="Use Crédito apenas quando a despesa realmente tiver sido feita no cartão."
            )
            prioridade = a2.radio("Prioridade", ["Baixa 🟢","Média 🟡","Alta 🔴"], horizontal=True)
            rec_label = st.radio("Repetição", ["Uma vez","Parcelada","Repete todo mês"], horizontal=True)
            parcelas = 1
            if rec_label == "Parcelada":
                parcelas = st.number_input("Número de parcelas", min_value=2, max_value=240, value=2)
            elif rec_label == "Repete todo mês":
                st.caption("A interface mostra uma recorrência mensal; internamente o app mantém uma janela futura de 60 meses, como na versão anterior.")
                parcelas = 60
            if "novo_pago_imediato" not in st.session_state:
                st.session_state["novo_pago_imediato"] = False
            pago_imediato = st.checkbox("Já foi pago/recebido", key="novo_pago_imediato")
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
    cabecalho_pagina("📋 Fluxo", "O que entra, o que sai e quando — com a renda que cobre cada conta.", "fluxo")
    st.caption("Uma agenda financeira simples: vencimento, valor, status e de onde vem o dinheiro.")
    df_todos_fluxo = fetch_dataframe("SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento ASC", (inicio_periodo, fim_periodo))
    df = df_todos_fluxo.copy() if not df_todos_fluxo.empty else pd.DataFrame()

    # A linha do tempo continua 15 dias no período seguinte. Isso mantém visível a
    # próxima janela financeira (ex.: fim de setembro → renda do início de outubro)
    # sem misturar esses lançamentos nas ferramentas avançadas do mês selecionado.
    fim_contexto_fluxo = fim_periodo + datetime.timedelta(days=15)
    df_contexto_fluxo = fetch_dataframe(
        "SELECT * FROM lancamentos WHERE data_vencimento >= %s AND data_vencimento < %s ORDER BY data_vencimento ASC",
        (inicio_periodo, fim_contexto_fluxo),
    )
    tab_fluxo = st.container()

    with tab_fluxo:
        if df.empty:
            render_empty_state("Nenhuma conta ou entrada neste mês", "Registre uma conta ou renda para começar a organizar o fluxo.", "○")
        else:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0.0)
            df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce').fillna(0.0)

            df_ui = df_contexto_fluxo if not df_contexto_fluxo.empty else df
            df_ui['valor'] = pd.to_numeric(df_ui['valor'], errors='coerce').fillna(0.0)
            df_ui['valor_pago'] = pd.to_numeric(df_ui['valor_pago'], errors='coerce').fillna(0.0)
            ops_rapido = _consolidar_operacional(df_ui)
            plano_fluxo = _montar_plano_pagamentos(ops_rapido, ano_selecionado, mes_selecionado)
            _render_fluxo2_ponte(plano_fluxo, ano_selecionado, mes_selecionado)

            filtro_rapido = st.radio(
                "Mostrar", ["Todos", "A pagar", "A receber", "Pagos", "Atrasados"],
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
            if filtro_rapido == "A pagar":
                vis_rapida = vis_rapida[(vis_rapida['tipo'] == 'Despesa') & (vis_rapida['pago'] == 0)]
            elif filtro_rapido == "A receber":
                vis_rapida = vis_rapida[(vis_rapida['tipo'] == 'Entrada') & (vis_rapida['pago'] == 0)]
            elif filtro_rapido == "Pagos":
                vis_rapida = vis_rapida[vis_rapida['pago'] == 1]
            elif filtro_rapido == "Atrasados":
                vis_rapida = vis_rapida[vis_rapida['atrasado']]
            if cats_sel_rapidas:
                vis_rapida = vis_rapida[vis_rapida['categoria'].isin(cats_sel_rapidas)]

            _render_fluxo2_timeline(vis_rapida, ops_rapido, plano_fluxo, prefixo='fluxo2')

            st.caption("Edição estrutural, séries, exclusões e ferramentas técnicas ficam fora do uso diário.")

            with st.expander("••• Ferramentas avançadas", expanded=False):
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
                        'descricao': '💳 Fatura do cartão', 'valor': sum_cred,
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
                            'subgrupo': '', 'descricao': f'🏥 {cat_nome}',
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
                df_view['ordem_pri'] = df_view['prioridade'].map(prioridades_map).fillna(2)
                df_view = df_view.sort_values(['data_vencimento', 'ordem_pri']).reset_index(drop=True)
                df_view['Pago'] = df_view['pago'].astype(bool)
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

                        if st.button("💾 Salvar alteração", type="primary"):
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

    df = _dados_mes()
    unidades = _planejamento_unidades(df, ano_selecionado, mes_selecionado)
    resumo = _planejamento_resumo(df, ano_selecionado, mes_selecionado, unidades=unidades)
    orcamentos_plan = _planejamento_orcamentos(df, ano_selecionado, mes_selecionado)
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
                    valor_orc = st.number_input("Orçamento do mês", min_value=0.0, step=50.0, value=float(atual), key=f"plan2_orc_val_{escolha[0]}_{escolha[1]}")
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
                    cred = st.text_input('Credor', value=lr['credor'] if pd.notna(lr.get('credor')) else '', key='plan2_div_credor')
                    taxa = st.number_input('Taxa mensal (%)', min_value=0.0, step=.1, value=float(lr['taxa_juros_mensal']) if pd.notna(lr.get('taxa_juros_mensal')) else 0.0, key='plan2_div_taxa')
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
        for m in range(1,13): processar_recorrencias_lazy(m,ano_balanco)
        ia,fa=limites_ano(ano_balanco)
        dfy=fetch_dataframe("SELECT * FROM lancamentos WHERE (pago=1 AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ia,fa,ia,fa))
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
            ip,fp=limites_ano(ano_balanco-1); prev=fetch_dataframe("SELECT tipo,valor,valor_pago,pago FROM lancamentos WHERE (pago=1 AND data_pagamento >= %s AND data_pagamento < %s) OR (pago=0 AND data_vencimento >= %s AND data_vencimento < %s)",(ip,fp,ip,fp))
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
# MAIS — concentra recursos que não precisam competir na navegação diária
# -----------------------------------------------------------------
elif menu == "⚙️ Mais":
    cabecalho_pagina("⚙️ Mais", "Recursos avançados e configurações. Você não precisa passar por aqui no dia a dia.", "mais")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("＋ Novo lançamento", use_container_width=True):
            st.session_state["novo_pago_imediato"] = False
            st.session_state.menu_atual = "📝 Lançamentos"; st.rerun()
        if st.button("📈 Balanço anual", use_container_width=True):
            st.session_state.menu_atual = "📈 Balanço Anual"; st.rerun()
    with c2:
        if st.button("⚙️ Categorias e automações", use_container_width=True):
            st.session_state.menu_atual = "⚙️ Gerenciar Categorias"; st.rerun()
        if st.button("💾 Backup e restauração", use_container_width=True):
            st.session_state.menu_atual = "💾 Backup e Restauração"; st.rerun()
        if st.button("🧰 Manutenção e diagnóstico", use_container_width=True):
            st.session_state.menu_atual = "🧰 Manutenção e Diagnóstico"; st.rerun()
    st.divider()
    if st.button("🧙 Reconfigurar aplicativo", key="mais_reconfigurar", use_container_width=True):
        st.session_state['wizard_ativo'] = True
        st.session_state['wizard_passo'] = 0
        for _wk in ['wizard_hospitais','wizard_fixas','wizard_orcamentos','wizard_dividas']:
            st.session_state[_wk] = []
        st.rerun()

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
                r=cfg[cfg['id']==sel].iloc[0]; e1,e2=st.columns(2); cat=e1.text_input('Categoria',value=r['categoria'],key='cat_edit_cat'); sub=e2.text_input('Subgrupo',value=r['subgrupo'] if pd.notna(r['subgrupo']) else '',key='cat_edit_sub'); rec=st.checkbox('🔄 Repete todo mês',value=bool(r['is_recorrente']==1),key='cat_edit_rec'); efet=rec
                val=float(r['valor_padrao']) if pd.notna(r['valor_padrao']) else 0.0; atraso=int(r['atraso_meses']) if pd.notna(r['atraso_meses']) else 0; dia=int(r['dia_pagamento']) if pd.notna(r['dia_pagamento']) else 10
                if r['tipo']=='Entrada' or efet:
                    z1,z2,z3=st.columns(3); val=z1.number_input('Valor padrão',value=val,key='cat_edit_val'); atraso=z2.number_input('Atraso em meses',0,6,atraso,key='cat_edit_atraso'); dia=z3.number_input('Dia pagamento/vencimento',1,31,dia,key='cat_edit_dia')
                st.caption('Mudanças valem para novos lançamentos e recorrências; o histórico anterior é preservado.')
                b1,b2=st.columns(2)
                if b1.button('Salvar alterações',type='primary',use_container_width=True):
                    execute_query("UPDATE categorias_personalizadas SET categoria=%s,subgrupo=%s,valor_padrao=%s,atraso_meses=%s,dia_pagamento=%s,is_recorrente=%s WHERE id=%s",(cat.strip(),sub.strip() or None,val if val>0 else None,atraso,dia,1 if efet else 0,int(sel))); invalidar_caches_estruturais(); flash('success','Categoria atualizada.'); st.rerun()
                confirmar=b2.checkbox('Confirmar exclusão',key='cat_del_confirm')
                if b2.button('Excluir',disabled=not confirmar,use_container_width=True): execute_query('DELETE FROM categorias_personalizadas WHERE id=%s',(int(sel),)); invalidar_caches_estruturais(); flash('success','Categoria excluída.'); st.rerun()

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
