import datetime as dt
import uuid
from zoneinfo import ZoneInfo
import pytest
from agenda import create_event,cancel_event,insert_shifts,update_event
from tests.test_database import db

START=dt.datetime(2026,11,1,7,tzinfo=ZoneInfo('America/Fortaleza'))

def test_commitments_without_money_and_same_time_are_independent(db):
    with db,db.cursor() as c:
        for kind in ('Pessoal','Profissional','Plantão'):
            create_event(c,'Compromisso',START,START+dt.timedelta(hours=1),kind,'Hospital')
        c.execute('SELECT count(*) FROM agenda_eventos');assert c.fetchone()[0]==3
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==0

def test_import_identity_is_event_not_place_or_date(db):
    with db,db.cursor() as c:
        for key in ('diurno','noturno','consulta-1','consulta-2'):
            _,created=create_event(c,'Hospital',START,START+dt.timedelta(hours=1),external_id=key,origin='calendar')
            assert created
        _,created=create_event(c,'Alterado',START,START+dt.timedelta(hours=1),external_id='diurno',origin='calendar')
        assert not created
        c.execute('SELECT count(*) FROM agenda_eventos');assert c.fetchone()[0]==4

def shift(key):
    return ('Entrada','Rendas','Hospital','Plantão Hospital',100,dt.date(2026,12,10),1,1,0,key,'Outros','Baixa 🟢',0,START.date())

def test_two_shifts_same_day_and_replay(db):
    with db,db.cursor() as c:
        assert insert_shifts(c,[shift('day'),shift('night')])==2
        assert insert_shifts(c,[shift('day'),shift('night')])==0
        c.execute('SELECT count(*),sum(valor) FROM lancamentos');assert c.fetchone()==(2,200)

def test_receipt_survives_calendar_rename_move_and_cancel(db):
    from payments import settle
    with db,db.cursor() as c:
        insert_shifts(c,[shift('paid')])
        c.execute('SELECT id,compromisso_id FROM lancamentos');item,event=c.fetchone()
        settle(c,[item],90,dt.date(2026,11,1))
        c.execute('SELECT valor_pago,pago,data_pagamento,data_vencimento FROM lancamentos');before=c.fetchone()
        update_event(c,event,'Reunião renomeada',START+dt.timedelta(days=2),START+dt.timedelta(days=2,hours=1))
        cancel_event(c,event)
        c.execute('SELECT valor_pago,pago,data_pagamento,data_vencimento FROM lancamentos');assert c.fetchone()==before

def test_cancel_pending_keeps_commitment_identity(db):
    with db,db.cursor() as c:
        insert_shifts(c,[shift('cancel')])
        c.execute('SELECT id FROM agenda_eventos');event=c.fetchone()[0]
        cancel_event(c,event)
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==0
        c.execute('SELECT status FROM agenda_eventos');assert c.fetchone()[0]=='cancelado'
        assert insert_shifts(c,[shift('cancel')])==0
