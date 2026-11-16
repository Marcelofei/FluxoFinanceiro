import json
from datetime import date,datetime
from uuid import uuid4
from zoneinfo import ZoneInfo
import pytest
import storage
from finance import schedule
from installments import progress,read_selection


def test_progress_and_reminders_are_independent(database):
    with storage.connection(database) as conn:
        storage.create_entries(conn,str(uuid4()),'expense','Curso',60000,date(2026,11,20),3,True,'Educação')
        first=storage.list_entries(conn,2026,11)[0]
        storage.save_reminder(conn,'r1','Lembrete de teste',999000)
        storage.save_reminder(conn,'r1','Lembrete de teste',999000)
        rows=storage.list_installments(conn)
        assert progress(rows)[0]['fraction']==0
        storage.settle_entry(conn,first,20000,datetime.now(ZoneInfo('America/Fortaleza')).date())
        plan=progress(storage.list_installments(conn))[0]
        assert (plan['paid'],plan['total'],plan['remaining_cents'])==(1,3,40000)
        assert plan['fraction']==1/3
        assert len(storage.list_reminders(conn))==1
        assert len(storage.list_entries(conn,2026,11))==1
        assert schedule(storage.list_entries(conn,2026,11),0,date(2026,11,1))==[]
        storage.resolve_reminder(conn,'r1',True)
        assert storage.get_balance(conn)['cents']==0
        storage.reverse_entry(conn,storage.list_entries(conn,2026,11)[0])
        assert progress(storage.list_installments(conn))[0]['fraction']==0


def test_monthly_recurrence_is_not_installment(database):
    with storage.connection(database) as conn:
        storage.create_entries(conn,str(uuid4()),'expense','Mensalidade',10000,date(2026,11,1),12)
        assert storage.list_installments(conn)==[]


def test_selection_refuses_confirmed_or_invalid_values():
    with pytest.raises(ValueError):read_selection(b'{"format":"unknown"}')
    bad=dict(format='fluxo-selection-v1',entries=[],reminders=[dict(source='x',description='x',amount_cents=-1)])
    with pytest.raises(ValueError):read_selection(json.dumps(bad).encode())
