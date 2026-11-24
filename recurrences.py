"""Materialize a receipt month independently of navigation history."""
import datetime as dt


def generate_month(cur, month, year):
    generate_months(cur, [(month, year)])


def generate_months(cur, periods):
    """Two database round trips for any number of rules or receipt months.

    Parent row locks preserve the existing ordering with edits. Reservations are
    still atomic and remain as tombstones after deleting individual occurrences.
    Already reserved months do not lock the parent rules on normal navigation.
    """
    months = sorted({dt.date(int(year), int(month), 1) for month, year in periods})
    if not months:
        return
    cur.execute('''
        WITH months AS (SELECT unnest(%s::date[]) AS target),
        sources AS MATERIALIZED (
            SELECT c.* FROM categorias_personalizadas c
            WHERE c.is_recorrente=1 AND EXISTS (
                SELECT 1 FROM months m WHERE NOT EXISTS (
                    SELECT 1 FROM recorrencias_geradas rg WHERE rg.categoria_id=c.id
                    AND rg.competencia=(m.target-make_interval(months=>COALESCE(c.atraso_meses,0)))::date))
            ORDER BY c.id FOR UPDATE OF c
        ), candidates AS MATERIALIZED (
            SELECT c.*,
                (m.target-make_interval(months=>COALESCE(c.atraso_meses,0)))::date AS competence,
                make_date(EXTRACT(YEAR FROM m.target)::int,EXTRACT(MONTH FROM m.target)::int,
                    LEAST(COALESCE(c.dia_pagamento,1),
                        EXTRACT(DAY FROM m.target+INTERVAL '1 month - 1 day')::int)) AS due
            FROM sources c CROSS JOIN months m
        ), reserved AS (
            INSERT INTO recorrencias_geradas(categoria_id,competencia)
            SELECT id,competence FROM candidates
            WHERE (data_inicio IS NULL OR (competence>=date_trunc('month',data_inicio)::date AND due>=data_inicio))
              AND (data_fim IS NULL OR competence<=data_fim)
            ORDER BY id,competence
            ON CONFLICT DO NOTHING RETURNING categoria_id,competencia
        )
        INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,
            parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,
            valor_pago,data_competencia,fonte_id)
        SELECT c.tipo,c.categoria,c.subgrupo,
            COALESCE(c.descricao_padrao,concat(c.categoria,' - ',COALESCE(c.subgrupo,''),' (Recorrente)')),
            COALESCE(c.valor_padrao,0),c.due,1,1,0,'rec_'||c.id::text,c.forma_padrao,
            c.prioridade_padrao,0,c.competence,c.id
        FROM candidates c JOIN reserved r ON r.categoria_id=c.id AND r.competencia=c.competence
        ORDER BY c.id,c.competence
    ''', (months,))
    cur.execute('''
        WITH months AS (SELECT unnest(%s::date[]) AS target),
        sources AS MATERIALIZED (
            SELECT s.* FROM series_manuais s WHERE EXISTS (
                SELECT 1 FROM months m WHERE NOT EXISTS (
                    SELECT 1 FROM series_manuais_geradas rg WHERE rg.compra_id=s.compra_id
                    AND rg.competencia=(m.target-make_interval(months=>s.atraso_meses))::date))
            ORDER BY s.compra_id FOR UPDATE OF s
        ), candidates AS MATERIALIZED (
            SELECT s.*,(m.target-make_interval(months=>s.atraso_meses))::date AS competence,
                make_date(EXTRACT(YEAR FROM m.target)::int,EXTRACT(MONTH FROM m.target)::int,
                    LEAST(s.dia,EXTRACT(DAY FROM m.target+INTERVAL '1 month - 1 day')::int)) AS due
            FROM sources s CROSS JOIN months m
        ), reserved AS (
            INSERT INTO series_manuais_geradas(compra_id,competencia)
            SELECT compra_id,competence FROM candidates
            WHERE competence>=date_trunc('month',inicio)::date AND (fim IS NULL OR competence<=fim)
            ORDER BY compra_id,competence
            ON CONFLICT DO NOTHING RETURNING compra_id,competencia
        )
        INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,data_vencimento,
            data_competencia,parcela_atual,total_parcelas,pago,compra_id,forma_pagamento,prioridade,valor_pago)
        SELECT c.tipo,c.categoria,c.subgrupo,c.descricao,c.valor,c.due,c.competence,
            (EXTRACT(YEAR FROM c.competence)::int-EXTRACT(YEAR FROM c.inicio)::int)*12
                +EXTRACT(MONTH FROM c.competence)::int-EXTRACT(MONTH FROM c.inicio)::int+1,
            999,0,c.compra_id,c.forma_pagamento,c.prioridade,0
        FROM candidates c JOIN reserved r ON r.compra_id=c.compra_id AND r.competencia=c.competence
        ORDER BY c.compra_id,c.competence
    ''', (months,))


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
