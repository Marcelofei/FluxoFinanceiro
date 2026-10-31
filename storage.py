"""Persistência nova, sem chamadas aos módulos ou tabelas do app anterior."""
import os
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from finance import add_months, month_bounds
from security import validate_schema


@contextmanager
def connection(schema):
    schema = validate_schema(schema)
    url = os.environ['DATABASE_URL']
    # TLS é padrão; banco local de testes pode declarar sslmode=disable.
    conn = psycopg2.connect(url, connect_timeout=10, sslmode=os.getenv('DB_SSLMODE', 'require'))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
                cur.execute("SET TIME ZONE 'America/Fortaleza'")
            yield conn
    finally:
        conn.close()


def initialize(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS simple_entries (
                id BIGSERIAL PRIMARY KEY,
                request_id UUID NOT NULL,
                occurrence INTEGER NOT NULL DEFAULT 0,
                kind TEXT NOT NULL CHECK (kind IN ('income','expense')),
                description TEXT NOT NULL CHECK (length(trim(description)) > 0),
                expected_cents BIGINT NOT NULL CHECK (expected_cents > 0),
                due_date DATE NOT NULL,
                settled BOOLEAN NOT NULL DEFAULT FALSE,
                actual_cents BIGINT CHECK (actual_cents >= 0),
                settled_date DATE,
                version INTEGER NOT NULL DEFAULT 0,
                UNIQUE(request_id, occurrence),
                CHECK ((settled AND actual_cents IS NOT NULL AND settled_date IS NOT NULL)
                    OR (NOT settled AND actual_cents IS NULL AND settled_date IS NULL))
            );
            ALTER TABLE simple_entries ADD COLUMN IF NOT EXISTS category TEXT NOT NULL DEFAULT 'A revisar';
            CREATE INDEX IF NOT EXISTS simple_entries_due ON simple_entries(due_date);
            CREATE TABLE IF NOT EXISTS simple_balance (
                id INTEGER PRIMARY KEY CHECK (id=1),
                cents BIGINT NOT NULL DEFAULT 0 CHECK (cents >= 0),
                updated_at TIMESTAMPTZ,
                version INTEGER NOT NULL DEFAULT 0
            );
            INSERT INTO simple_balance(id) VALUES(1) ON CONFLICT DO NOTHING;
        ''')


def get_balance(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM simple_balance WHERE id=1')
        return dict(cur.fetchone())


def set_balance(conn, value, version):
    if not isinstance(value, int) or value < 0:
        raise ValueError('Disponível inválido.')
    with conn.cursor() as cur:
        cur.execute('''UPDATE simple_balance SET cents=%s,updated_at=now(),version=version+1
                       WHERE id=1 AND version=%s''', (value, version))
        if not cur.rowcount:
            raise ValueError('O saldo mudou em outra sessão. Atualize a página.')


def list_entries(conn, year, month):
    start, end = month_bounds(year, month)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT * FROM simple_entries WHERE due_date >= %s AND due_date < %s ORDER BY due_date,id', (start, end))
        return [dict(row) for row in cur.fetchall()]


def create_entries(conn, request_id, kind, description, value, due, count=1, divide=False, category="A revisar"):
    UUID(str(request_id))
    if kind not in ('income', 'expense') or not description.strip() or len(description.strip()) > 160:
        raise ValueError('Confira o tipo e a descrição (até 160 caracteres).')
    if not isinstance(value, int) or value <= 0 or not isinstance(count, int) or not 1 <= count <= 120:
        raise ValueError('Confira o valor e a quantidade de meses.')
    if divide and value < count:
        raise ValueError('O total precisa permitir ao menos um centavo por parcela.')
    for index in range(count):
        amount = value // count + (index < value % count) if divide else value
        description_row = f'{description.strip()} ({index+1}/{count})' if divide else description.strip()
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO simple_entries(request_id,occurrence,kind,description,expected_cents,due_date,category)
                           VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(request_id,occurrence) DO NOTHING''',
                        (str(request_id), index, kind, description_row, amount, add_months(due, index), category))


def edit_entry(conn, entry, description, value, due, category=None):
    if not description.strip() or len(description.strip()) > 160 or value <= 0:
        raise ValueError('Confira descrição e valor.')
    with conn.cursor() as cur:
        cur.execute('''UPDATE simple_entries SET description=%s,expected_cents=%s,due_date=%s,category=%s,version=version+1
                       WHERE id=%s AND version=%s AND NOT settled''',
                    (description.strip(), value, due, category or entry.get("category", "A revisar"), entry['id'], entry['version']))
        if not cur.rowcount:
            raise ValueError('Este lançamento mudou. Atualize a página.')


def settle_entry(conn, entry, actual, paid_on, balance_after=None, balance_version=None):
    if not isinstance(actual, int) or actual < 0 or paid_on > datetime.now(ZoneInfo('America/Fortaleza')).date():
        raise ValueError('Confira o valor real e a data efetiva.')
    with conn.cursor() as cur:
        cur.execute('''UPDATE simple_entries SET settled=TRUE,actual_cents=%s,settled_date=%s,version=version+1
                       WHERE id=%s AND version=%s AND NOT settled''',
                    (actual, paid_on, entry['id'], entry['version']))
        if not cur.rowcount:
            raise ValueError('Este lançamento já mudou. Atualize a página.')
    if balance_after is not None:
        if entry['kind'] != 'expense':
            raise ValueError('Atualize o disponível separadamente após receber.')
        set_balance(conn, balance_after, balance_version)


def reverse_entry(conn, entry):
    with conn.cursor() as cur:
        cur.execute('''UPDATE simple_entries SET settled=FALSE,actual_cents=NULL,settled_date=NULL,version=version+1
                       WHERE id=%s AND version=%s AND settled''', (entry['id'], entry['version']))
        if not cur.rowcount:
            raise ValueError('Este lançamento mudou. Atualize a página.')


def delete_entry(conn, entry):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM simple_entries WHERE id=%s AND version=%s AND NOT settled', (entry['id'], entry['version']))
        if not cur.rowcount:
            raise ValueError('Só é possível excluir uma previsão ainda pendente e atualizada.')
