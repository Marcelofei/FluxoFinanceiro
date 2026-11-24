"""Commitments have their own identity; financial links are optional."""
import datetime as dt
import uuid
from zoneinfo import ZoneInfo

def create_event(cur,title,start,end,kind='Profissional',location='',event_id=None,origin='manual',external_id=None):
    if not str(title).strip() or kind not in ('Pessoal','Profissional','Plantão'):
        raise ValueError('Informe título e tipo válidos.')
    if start.tzinfo is None or end.tzinfo is None or end<=start:
        raise ValueError('Informe horários e fuso válidos; fim posterior ao início.')
    key=str(uuid.uuid5(uuid.NAMESPACE_URL,f'{origin}:{external_id}')) if external_id is not None else str(event_id or uuid.uuid4())
    cur.execute('''INSERT INTO agenda_eventos(id,titulo,inicio,fim,tipo,local,origem,chave_externa)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id''',
        (key,title.strip(),start,end,kind,location,origin,external_id))
    return key,bool(cur.fetchone())

def cancel_event(cur,key):
    # The lane also serializes administrative removal of linked forecasts.
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()||':agenda'))")
    cur.execute('SELECT id FROM agenda_eventos WHERE id=%s FOR UPDATE',(str(key),))
    if not cur.fetchone():raise ValueError('Compromisso removido. Atualize a tela.')
    cur.execute("UPDATE agenda_eventos SET status='cancelado' WHERE id=%s",(str(key),))
    cur.execute('DELETE FROM lancamentos WHERE compromisso_id=%s AND pago=0',(str(key),))

def adopt_legacy(cur):
    cur.execute('''INSERT INTO agenda_eventos(id,titulo,inicio,fim,tipo,local,origem,chave_externa)
        SELECT md5('legacy-shift:'||id::text)::uuid,COALESCE(descricao,'Plantão'),
            COALESCE(data_competencia,data_vencimento)::timestamp AT TIME ZONE 'America/Fortaleza',
            (COALESCE(data_competencia,data_vencimento)+1)::timestamp AT TIME ZONE 'America/Fortaleza',
            'Plantão',COALESCE(subgrupo,''),'legado',id::text
        FROM lancamentos WHERE tipo='Entrada' AND descricao LIKE 'Plantão %'
        AND compromisso_id IS NULL AND COALESCE(data_competencia,data_vencimento) IS NOT NULL
        ON CONFLICT DO NOTHING;
        UPDATE lancamentos SET compromisso_id=md5('legacy-shift:'||id::text)::uuid,origem_evento='plantao'
        WHERE compromisso_id IS NULL AND tipo='Entrada' AND descricao LIKE 'Plantão %'
        AND COALESCE(data_competencia,data_vencimento) IS NOT NULL''')

def insert_shifts(cur,records):
    inserted=0
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()||':agenda'))")
    for row in records:
        if not row[9]:raise ValueError('O plantão precisa de uma identidade própria.')
        start=row[14] if len(row)>14 else dt.datetime.combine(row[13],dt.time(),ZoneInfo('America/Fortaleza'))
        end=row[15] if len(row)>15 else start+dt.timedelta(days=1)
        key,created=create_event(cur,row[3],start,end,'Plantão',row[2] or '',origin='escala',external_id=str(row[9]))
        if not created:continue
        cur.execute('''INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,
            parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago,data_competencia,
            compromisso_id,origem_evento) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'plantao')''',tuple(row[:14])+(key,))
        inserted+=1
    return inserted

def update_event(cur,key,title,start,end,expected_start=None):
    if not str(title).strip() or start.tzinfo is None or end.tzinfo is None or end<=start:
        raise ValueError('Informe título e horários válidos.')
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema()||':agenda'))")
    cur.execute('SELECT inicio FROM agenda_eventos WHERE id=%s FOR UPDATE',(str(key),))
    row=cur.fetchone()
    if not row or (expected_start is not None and row[0]!=expected_start):
        raise ValueError('O compromisso mudou. Atualize antes de salvar.')
    cur.execute('UPDATE agenda_eventos SET titulo=%s,inicio=%s,fim=%s WHERE id=%s',(title.strip(),start,end,str(key)))
    cur.execute('''SELECT l.id,c.atraso_meses,c.dia_pagamento FROM lancamentos l
        LEFT JOIN categorias_personalizadas c ON c.id=l.fonte_id
        WHERE l.compromisso_id=%s AND l.pago=0 FOR UPDATE OF l''',(str(key),))
    from operations import income_due_date
    date=start.astimezone(ZoneInfo('America/Fortaleza')).date()
    for item,delay,day in cur.fetchall():
        if day:
            cur.execute('UPDATE lancamentos SET descricao=%s,data_competencia=%s,data_vencimento=%s WHERE id=%s',
                        (title.strip(),date,income_due_date(date,delay or 0,day),item))
        else:cur.execute('UPDATE lancamentos SET descricao=%s,data_competencia=%s WHERE id=%s',(title.strip(),date,item))
