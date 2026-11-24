"""A volta ao app completo não toca nas tabelas da versão simplificada."""
import pytest
from psycopg2 import sql
from migrate import migrate
from tests.test_database import db


@pytest.mark.parametrize('legacy_exists', [True, False])
def test_full_migrations_preserve_simple_data(db, legacy_exists):
    schema='tenant_test' if legacy_exists else 'tenant_restore_test'
    try:
        with db,db.cursor() as cur:
            if not legacy_exists:
                cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
            cur.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            cur.execute('''
                CREATE TABLE simple_entries(id BIGINT PRIMARY KEY, description TEXT, expected_cents BIGINT, actual_cents BIGINT, settled BOOLEAN);
                INSERT INTO simple_entries VALUES (1,'Teste de preservação',12345,12000,TRUE);
                CREATE TABLE simple_balance(id INTEGER PRIMARY KEY,cents BIGINT);
                INSERT INTO simple_balance VALUES (1,43210);
                CREATE TABLE simple_reminders(source TEXT PRIMARY KEY,amount_cents BIGINT);
                INSERT INTO simple_reminders VALUES ('test-reminder',98765);
            ''')
            if legacy_exists:
                cur.execute("INSERT INTO lancamentos(tipo,descricao,valor,data_vencimento,pago,valor_pago) VALUES('Despesa','Teste anterior',100,'2026-11-01',0,0)")
                cur.execute('SELECT count(*) FROM lancamentos')
                legacy_count=cur.fetchone()[0]
        for _ in range(2): migrate(db,schema)
        with db,db.cursor() as cur:
            cur.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
            cur.execute('SELECT * FROM simple_entries')
            assert cur.fetchall()==[(1,'Teste de preservação',12345,12000,True)]
            cur.execute('SELECT * FROM simple_balance')
            assert cur.fetchall()==[(1,43210)]
            cur.execute('SELECT * FROM simple_reminders')
            assert cur.fetchall()==[('test-reminder',98765)]
            cur.execute('SELECT MAX(version) FROM schema_migrations')
            assert cur.fetchone()[0]==3
            if legacy_exists:
                cur.execute('SELECT count(*) FROM lancamentos')
                assert cur.fetchone()[0]==legacy_count
    finally:
        with db,db.cursor() as cur:
            if legacy_exists:
                cur.execute('DROP TABLE IF EXISTS simple_entries,simple_balance,simple_reminders')
            else:
                cur.execute('DROP SCHEMA IF EXISTS tenant_restore_test CASCADE')
            cur.execute('SET search_path TO tenant_test')
