import os, re, logging
import pandas as pd
import psycopg2
import psycopg2.pool
from psycopg2 import sql
from contextlib import contextmanager
import streamlit as st
from security import validate_schema
@st.cache_resource
def get_pool():
    '''Pool compartilhado com validação na retirada de cada conexão.'''
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL não configurada na variável de ambiente.")
        st.stop()

    if "sslmode=require" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"

    try:
        return psycopg2.pool.ThreadedConnectionPool(
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
                schema = validate_schema(st.session_state.get('tenant_schema', 'public'))
                cur.execute(sql.SQL("SET search_path TO {}; SELECT set_config('app.actor', %s, false); "
                                    "SET TIME ZONE 'America/Sao_Paulo'; SELECT 1").format(sql.Identifier(schema)),
                            (st.session_state.get('actor', 'owner'),))
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


@contextmanager
def ui_transaction():
    try:
        with transaction() as cur:
            yield cur
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    except psycopg2.IntegrityError:
        st.error('A alteração conflita com um cadastro existente ou uma regra de integridade. Confira os dados e tente novamente.')
        st.stop()


def execute_query(query, params=None, fetch=False, silent=False):
    '''Executa SQL curto em conexão própria. Erros são propagados por padrão.'''
    try:
        with db_connection(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall() if fetch else None
    except Exception as e:
        logging.getLogger('fluxo').error('db_write_failed', extra={'error_type': type(e).__name__})
        if not silent:
            st.error('Não foi possível concluir a operação. Atualize os dados antes de tentar novamente.')
        raise


def fetch_dataframe(query, params=None, silent=False, raise_on_error=True):
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
    raise ultimo_erro or RuntimeError('Falha de leitura')


def init_db():
    try:
        rows = execute_query('SELECT MAX(version) FROM schema_migrations', fetch=True, silent=True)
        if not rows or rows[0][0] != 5:
            raise RuntimeError('Migrações pendentes')
    except Exception:
        st.error('Atualização do banco pendente. Execute python migrate.py antes de iniciar o app.')
        st.stop()
