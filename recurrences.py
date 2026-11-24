"""Materialize a receipt month independently of navigation history."""
import datetime as dt
from operations import income_due_date


def generate_month(cur, month, year):
    target=dt.date(year,month,1)
    cur.execute("SELECT id FROM categorias_personalizadas WHERE is_recorrente=1 ORDER BY id")
    for (source_id,) in cur.fetchall():
        cur.execute('SELECT row_to_json(c) FROM categorias_personalizadas c WHERE id=%s AND is_recorrente=1 FOR UPDATE',(source_id,))
        row=cur.fetchone()
        if not row:continue
        c=row[0]
        delay=int(c.get('atraso_meses') or 0)
        competence=income_due_date(target,-delay,1)
        start=dt.date.fromisoformat(c['data_inicio']) if c.get('data_inicio') else competence
        end=dt.date.fromisoformat(c['data_fim']) if c.get('data_fim') else None
        if competence < start.replace(day=1) or (end and competence>end):continue
        due=income_due_date(competence,delay,int(c.get('dia_pagamento') or 1))
        cur.execute('''INSERT INTO recorrencias_geradas(categoria_id,competencia)
            VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING categoria_id''',(source_id,competence))
        if not cur.fetchone():continue
        cur.execute('''INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,
            data_vencimento,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,
            prioridade,valor_pago,data_competencia,fonte_id)
            VALUES (%s,%s,%s,%s,%s,%s,1,1,0,%s,%s,%s,0,%s,%s)''',
            (c['tipo'],c['categoria'],c['subgrupo'],c.get('descricao_padrao') or f"{c['categoria']} - {c['subgrupo'] or ''} (Recorrente)",
             c.get('valor_padrao') or 0,due,f'rec_{source_id}',c['forma_padrao'],c['prioridade_padrao'],competence,source_id))
    cur.execute('SELECT compra_id FROM series_manuais ORDER BY compra_id')
    for (group,) in cur.fetchall():
        cur.execute('SELECT row_to_json(s) FROM series_manuais s WHERE compra_id=%s FOR UPDATE',(group,))
        c=cur.fetchone()[0]
        start=dt.date.fromisoformat(c['inicio'])
        competence=income_due_date(target,-c['atraso_meses'],1)
        due=income_due_date(target,0,c['dia'])
        if competence<start.replace(day=1) or (c.get('fim') and competence>dt.date.fromisoformat(c['fim'])):continue
        cur.execute('''INSERT INTO series_manuais_geradas(compra_id,competencia) VALUES (%s,%s)
            ON CONFLICT DO NOTHING RETURNING compra_id''',(group,competence))
        if not cur.fetchone():continue
        number=(competence.year-start.year)*12+competence.month-start.month+1
        cur.execute('''INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,
            data_vencimento,data_competencia,parcela_atual,total_parcelas,pago,compra_id,
            forma_pagamento,prioridade,valor_pago)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,999,0,%s,%s,%s,0)''',
            (c['tipo'],c['categoria'],c['subgrupo'],c['descricao'],c['valor'],due,competence,number,group,c['forma_pagamento'],c['prioridade']))


def recover_manual_rules(cur):
    """Old snapshots may contain materialized monthly entries without a rule."""
    cur.execute('''INSERT INTO series_manuais(compra_id,tipo,categoria,subgrupo,descricao,valor,inicio,fim,dia,forma_pagamento,prioridade)
        SELECT DISTINCT ON (compra_id) compra_id,tipo,COALESCE(categoria,'Sem categoria'),subgrupo,
               COALESCE(descricao,''),valor,data_vencimento,NULL,
               EXTRACT(DAY FROM data_vencimento)::int,COALESCE(forma_pagamento,'Outros'),COALESCE(prioridade,'Média 🟡')
        FROM lancamentos WHERE total_parcelas=999 AND compra_id IS NOT NULL
        ORDER BY compra_id,data_vencimento,id ON CONFLICT DO NOTHING;
        INSERT INTO series_manuais_geradas
        SELECT DISTINCT compra_id,date_trunc('month',data_vencimento)::date FROM lancamentos
        WHERE total_parcelas=999 AND compra_id IN (SELECT compra_id FROM series_manuais)
        ON CONFLICT DO NOTHING;''')
