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

def test_backup_roundtrip_includes_commitment_without_receipt(db):
    import io,pandas as pd
    from contextlib import contextmanager
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    from backup import export_snapshot
    from finance import int_seguro
    from tests.helpers import app_functions
    with db,db.cursor() as c:create_event(c,'Pessoal',START,START+dt.timedelta(hours=1),'Pessoal')
    data=export_snapshot(db);db.rollback()
    @contextmanager
    def transaction():
        with db,db.cursor() as c:yield c
    ns=app_functions(['validar_csv_lancamentos','_limpar_df_para_banco','_insert_dataframe','importar_backup'],dict(pd=pd,sql=sql,execute_values=execute_values,int_seguro=int_seguro,transaction=transaction))
    f=io.BytesIO(data);f.name='agenda.zip'
    ok,msg=ns['importar_backup'](f);assert ok,msg
    with db,db.cursor() as c:
        c.execute('SELECT count(*) FROM agenda_eventos');assert c.fetchone()[0]==1
        c.execute('SELECT count(*) FROM lancamentos');assert c.fetchone()[0]==0

def test_adoption_preserves_same_day_legacy_shifts(db):
    from agenda import adopt_legacy
    with db,db.cursor() as c:
        for title in ('Plantão manhã','Plantão noite'):
            c.execute("INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_competencia,data_vencimento) VALUES ('Entrada','Rendas','Hospital',%s,100,'2026-11-01','2026-12-10')",(title,))
        adopt_legacy(c);adopt_legacy(c)
        c.execute('SELECT count(*) FROM agenda_eventos');assert c.fetchone()[0]==2
        c.execute('SELECT count(*),sum(valor),count(DISTINCT compromisso_id) FROM lancamentos');assert c.fetchone()==(2,200,2)
