"""Previously valid signed rows must not prevent deploying schema 4."""
from decimal import Decimal
import os
from contextlib import closing
import pytest
import psycopg2
from migrate import baseline,integrity,replay_protection,migrate
from legacy_amounts import signed_amounts


@pytest.mark.parametrize('kind',['Entrada','Despesa'])
@pytest.mark.parametrize('planned,actual',[
    (-100,-90),(-100,0),(-100,90),(100,-90),(0,-90),(-100,None),
    (None,-90),(Decimal('-0.005'),Decimal('-0.003'))])
@pytest.mark.parametrize('paid',[0,1])
def test_conversion_preserves_signed_plan_and_actual(kind,planned,actual,paid):
    result=signed_amounts(kind,planned,actual,paid)
    sign=lambda t:1 if t=='Entrada' else -1
    old_plan=sign(kind)*Decimal(str(planned or 0))
    old_real=sign(kind)*Decimal(str(actual or 0)) if paid else 0
    assert sign(result['tipo'])*result['valor']==old_plan
    new_real=sign(result['tipo'])*result['valor_pago']
    if result['extra_tipo']:new_real+=sign(result['extra_tipo'])*result['extra_valor']
    assert new_real==old_real
    assert all(result[k]>=0 for k in ('valor','valor_pago','extra_valor'))


@pytest.mark.parametrize('kind',['Entrada','Despesa'])
@pytest.mark.parametrize('planned,actual,paid',[
    (-100,-90,1),(-100,90,1),(100,-90,1),(0,-90,1),(-100,0,0)])
def test_upgrade_v3_with_signed_adjustment_under_not_valid_checks(kind,planned,actual,paid):
    url=os.environ.get('TEST_DATABASE_URL')
    if not url:pytest.skip('Requires disposable PostgreSQL TEST_DATABASE_URL')
    with closing(psycopg2.connect(url)) as conn:
        with conn,conn.cursor() as c:
            c.execute('DROP SCHEMA IF EXISTS tenant_signed CASCADE; CREATE SCHEMA tenant_signed; SET search_path TO tenant_signed')
            baseline(c);integrity(c);replay_protection(c)
            c.execute('CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY); INSERT INTO schema_migrations VALUES (1),(2),(3)')
            # Model the historical row that already existed when NOT VALID checks
            # were installed. Keep these checks enabled throughout the upgrade.
            c.execute('ALTER TABLE lancamentos DROP CONSTRAINT ck_lanc_valor, DROP CONSTRAINT ck_lanc_valor_pago')
            c.execute('''INSERT INTO lancamentos(tipo,categoria,descricao,valor,valor_pago,pago,
                data_vencimento,data_pagamento,data_competencia,parcela_atual,total_parcelas)
                VALUES (%s,'Ajuste','Ajuste legado',%s,%s,%s,'2026-09-20',%s,'2026-09-20',1,1) RETURNING id''',
                (kind,planned,actual,paid,'2026-09-21' if paid else None))
            item=c.fetchone()[0]
            c.execute('''ALTER TABLE lancamentos ADD CONSTRAINT ck_lanc_valor CHECK(valor>=0) NOT VALID;
                ALTER TABLE lancamentos ADD CONSTRAINT ck_lanc_valor_pago CHECK(valor_pago>=0) NOT VALID''')
            c.execute('SAVEPOINT metadata_only')
            with pytest.raises(psycopg2.errors.CheckViolation):
                c.execute('UPDATE lancamentos SET data_competencia=data_competencia WHERE id=%s',(item,))
            c.execute('ROLLBACK TO SAVEPOINT metadata_only')
            c.execute('''SELECT SUM(CASE WHEN tipo='Entrada' THEN valor ELSE -valor END),
                SUM(CASE WHEN pago=1 THEN CASE WHEN tipo='Entrada' THEN valor_pago ELSE -valor_pago END ELSE 0 END)
                FROM lancamentos''')
            expected=c.fetchone()
        migrate(conn,'tenant_signed')
        with conn,conn.cursor() as c:
            c.execute('SET search_path TO tenant_signed')
            c.execute('''SELECT SUM(CASE WHEN tipo='Entrada' THEN valor ELSE -valor END),
                SUM(CASE WHEN pago=1 THEN CASE WHEN tipo='Entrada' THEN valor_pago ELSE -valor_pago END ELSE 0 END)
                FROM lancamentos''')
            assert c.fetchone()==expected
            c.execute('SELECT max(version) FROM schema_migrations');assert c.fetchone()[0]==4
            c.execute('SELECT count(*) FROM lancamentos WHERE valor<0 OR valor_pago<0');assert c.fetchone()[0]==0
            c.execute("SELECT anterior FROM auditoria WHERE entidade='lancamentos' AND operacao='UPDATE' AND anterior->>'id'=%s ORDER BY id LIMIT 1",(str(item),))
            original=c.fetchone()[0]
            assert original['valor']==planned and original['valor_pago']==(actual if paid else 0)
            c.execute('SELECT id,tipo,valor,valor_pago,pago,data_vencimento,data_pagamento FROM lancamentos ORDER BY id')
            before=c.fetchall()
            c.execute('SELECT count(*) FROM auditoria');audit_count=c.fetchone()[0]
        migrate(conn,'tenant_signed')
        with conn,conn.cursor() as c:
            c.execute('SET search_path TO tenant_signed')
            c.execute('SELECT id,tipo,valor,valor_pago,pago,data_vencimento,data_pagamento FROM lancamentos ORDER BY id')
            assert c.fetchall()==before
            c.execute('SELECT count(*) FROM auditoria');assert c.fetchone()[0]==audit_count
            with pytest.raises(psycopg2.errors.CheckViolation):
                c.execute("INSERT INTO lancamentos(tipo,valor) VALUES ('Entrada',-1)")
        conn.rollback()
