"""Upgrade the exact old schema with representative records, then rerun safely."""
from contextlib import contextmanager, closing
import os
import datetime as dt
from decimal import Decimal
import pytest
import psycopg2
from migrate import migrate
from tests import legacy_schema


def test_upgrade_deployed_schema_preserves_financial_records(monkeypatch):
    url=os.environ.get('TEST_DATABASE_URL')
    if not url: pytest.skip('Requires disposable TEST_DATABASE_URL')
    with closing(psycopg2.connect(url)) as conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA IF EXISTS tenant_legacy CASCADE; CREATE SCHEMA tenant_legacy; SET search_path TO tenant_legacy')
            def execute(query, params=None, **kwargs):
                cur.execute(query,params)
                return cur.fetchall() if kwargs.get('fetch') else None
            monkeypatch.setattr(legacy_schema,'execute_query',execute,raising=False)
            @contextmanager
            def transaction():
                yield cur
            monkeypatch.setattr(legacy_schema,'transaction',transaction,raising=False)
            legacy_schema.init_db()
            cur.execute("""INSERT INTO categorias_personalizadas(tipo,categoria,subgrupo,valor_padrao,is_recorrente,dia_pagamento,data_inicio)
                VALUES ('Despesa','Casa','Escola',100,1,5,'2026-08-01') RETURNING id""")
            cat_id=cur.fetchone()[0]
            cur.execute("""INSERT INTO lancamentos(tipo,categoria,descricao,valor,data_vencimento,data_competencia,pago,valor_pago,data_pagamento,compra_id,parcela_atual,total_parcelas,forma_pagamento)
                VALUES ('Despesa','Casa','Quitado com desconto',100,'2026-08-31','2026-08-01',1,90,'2026-09-05','old-paid',1,1,'À vista'),
                       ('Entrada','Renda','Recebido',2500,'2026-09-10','2026-09-01',1,2490,'2026-09-12','old-income',1,1,'Outros'),
                       ('Despesa','Cartão','Compra 1/3',33.34,'2026-10-05','2026-09-30',0,0,NULL,'old-card',1,3,'Crédito'),
                       ('Despesa','Cartão','Compra 2/3',33.33,'2026-11-05','2026-09-30',0,0,NULL,'old-card',2,3,'Crédito'),
                       ('Despesa','Casa','Atrasada',80,'2026-07-05','2026-07-01',0,0,NULL,'old-late',1,1,'À vista')""")
            cur.execute('INSERT INTO recorrencias_geradas(categoria_id,competencia) VALUES (%s,%s)',(cat_id,dt.date(2026,9,1)))
            cols='id,tipo,categoria,descricao,valor,data_vencimento,data_competencia,pago,valor_pago,data_pagamento,compra_id,parcela_atual,total_parcelas,forma_pagamento'
            cur.execute('SELECT '+cols+' FROM lancamentos ORDER BY id');before=cur.fetchall()
            cur.execute('SELECT lancamento_id,valor,data_pagamento FROM pagamentos ORDER BY lancamento_id');payments=cur.fetchall()
        conn.commit()
        # Exercise the documented deployment command, not just the helper.
        import subprocess,sys,json
        env={**os.environ,'DATABASE_URL':url,'APP_USERS_JSON':json.dumps({'test':{'schema':'tenant_legacy'}})}
        subprocess.run([sys.executable,'migrate.py'],env=env,check=True,capture_output=True,text=True)
        migrate(conn,'tenant_legacy')
        with conn.cursor() as cur:
            cur.execute('SET search_path TO tenant_legacy')
            cur.execute('SELECT '+cols+' FROM lancamentos ORDER BY id');assert cur.fetchall()==before
            cur.execute('SELECT lancamento_id,valor,data_pagamento FROM pagamentos ORDER BY lancamento_id');assert cur.fetchall()==payments
            cur.execute('SELECT categoria_id,competencia FROM recorrencias_geradas');assert cur.fetchall()==[(cat_id,dt.date(2026,9,1))]
            cur.execute('SELECT max(version) FROM schema_migrations');assert cur.fetchone()==(3,)
            cur.execute("SELECT to_regclass('auditoria'),to_regclass('faturas')");assert all(cur.fetchone())
