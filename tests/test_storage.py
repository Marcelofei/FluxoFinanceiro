from datetime import date, timedelta
from uuid import uuid4
import pytest
import storage

D = date(2026, 11, 1)


def create(conn, kind='expense', value=10000, due=D, count=1, divide=False, token=None):
    storage.create_entries(conn, token or str(uuid4()), kind, 'Teste', value, due, count, divide)


def test_initialize_preserves_legacy_and_new_data(database):
    with storage.connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE lancamentos (descricao TEXT)')
            cur.execute("INSERT INTO lancamentos VALUES ('Legado preservado')")
        create(conn)
        storage.initialize(conn)
        assert len(storage.list_entries(conn,2026,11)) == 1
        with conn.cursor() as cur:
            cur.execute('SELECT descricao FROM lancamentos')
            assert cur.fetchone()[0] == 'Legado preservado'


def test_retries_installments_and_month_boundaries(database):
    with storage.connection(database) as conn:
        token = str(uuid4())
        create(conn, value=10000, due=date(2026,12,31), count=3, divide=True, token=token)
        create(conn, value=10000, due=date(2026,12,31), count=3, divide=True, token=token)
        all_rows = sum((storage.list_entries(conn,y,m) for y,m in [(2026,12),(2027,1),(2027,2)]),[])
        assert [r['expected_cents'] for r in all_rows] == [3334,3333,3333]
        assert [r['due_date'].day for r in all_rows] == [31,31,28]
        assert storage.list_entries(conn,2026,11) == []


def test_received_preserves_expected_and_does_not_add_balance(database):
    with storage.connection(database) as conn:
        create(conn,kind='income')
        row = storage.list_entries(conn,2026,11)[0]
        storage.set_balance(conn,5000,0)
        storage.settle_entry(conn,row,12000,date.today())
        result = storage.list_entries(conn,2026,11)[0]
        assert (result['expected_cents'],result['actual_cents']) == (10000,12000)
        assert storage.get_balance(conn)['cents'] == 5000
        storage.reverse_entry(conn,result)
        assert storage.get_balance(conn)['cents'] == 5000
        assert not storage.list_entries(conn,2026,11)[0]['settled']


def test_payment_and_confirmed_balance_saved_atomically(database):
    with storage.connection(database) as conn:
        create(conn)
        row = storage.list_entries(conn,2026,11)[0]
    with pytest.raises(ValueError):
        with storage.connection(database) as conn:
            storage.settle_entry(conn,row,9000,date.today(),2000,999)
    with storage.connection(database) as conn:
        assert not storage.list_entries(conn,2026,11)[0]['settled']
        storage.settle_entry(conn,row,9000,date.today(),2000,0)
        assert storage.get_balance(conn)['cents'] == 2000
    with pytest.raises(ValueError):
        with storage.connection(database) as conn:
            storage.settle_entry(conn,row,9000,date.today(),0,1)
    with storage.connection(database) as conn:
        assert storage.get_balance(conn)['cents'] == 2000


def test_edit_delete_concurrency_and_no_cascade(database):
    with storage.connection(database) as conn:
        create(conn,count=2)
        row = storage.list_entries(conn,2026,11)[0]
        storage.edit_entry(conn,row,'Alterada',15000,D)
    with pytest.raises(ValueError):
        with storage.connection(database) as conn:
            storage.delete_entry(conn,row)
    with storage.connection(database) as conn:
        row = storage.list_entries(conn,2026,11)[0]
        storage.delete_entry(conn,row)
        assert storage.list_entries(conn,2026,11) == []
        assert len(storage.list_entries(conn,2026,12)) == 1


def test_invalid_settlement_and_tenant_isolation(database):
    with storage.connection(database) as conn:
        create(conn)
        row = storage.list_entries(conn,2026,11)[0]
        with pytest.raises(ValueError):
            storage.settle_entry(conn,row,100,date.today()+timedelta(days=1))
        assert storage.validate_schema(database) == database
        with pytest.raises(ValueError):
            storage.validate_schema('public; DROP TABLE simple_entries')


def test_new_tables_do_not_cross_tenant_boundary(database):
    from psycopg2 import sql
    other = 'tenant_other_' + uuid4().hex[:12]
    with storage.connection(database) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(other)))
        create(conn,value=12300)
    try:
        with storage.connection(other) as conn:
            storage.initialize(conn)
            assert storage.list_entries(conn,2026,11) == []
            storage.set_balance(conn,999,0)
        with storage.connection(database) as conn:
            assert len(storage.list_entries(conn,2026,11)) == 1
            assert storage.get_balance(conn)['cents'] == 0
    finally:
        with storage.connection(database) as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(other)))
