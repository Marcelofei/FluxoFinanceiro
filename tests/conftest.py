import os
from contextlib import closing
from uuid import uuid4
import psycopg2
from psycopg2 import sql
import pytest
import storage


@pytest.fixture
def database(monkeypatch):
    url = os.getenv('TEST_DATABASE_URL')
    if not url:
        pytest.skip('Defina TEST_DATABASE_URL para PostgreSQL descartável.')
    schema = 'tenant_test_' + uuid4().hex[:12]
    monkeypatch.setenv('DATABASE_URL', url)
    monkeypatch.setenv('DB_SSLMODE', 'disable')
    with closing(psycopg2.connect(url)) as conn, conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    with storage.connection(schema) as conn:
        storage.initialize(conn)
    yield schema
    with closing(psycopg2.connect(url)) as conn, conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
