"""Versioned PostgreSQL migrations. Run with python migrate.py before starting UI."""
import os
import json
import psycopg2
from psycopg2 import sql
from security import validate_schema
SCHEMA_VERSION = 3

def migrate_envelopes(cur):
    """Converte envelopes antigos em orçamento mensal por categoria.

    A versão 2.0 não usa orçamento como lançamento. As colunas legadas continuam
    no schema apenas para restaurar backups antigos, mas ficam inativas depois
    desta migração.
    """
    cur.execute("""
        INSERT INTO orcamentos_categorias
            (competencia, categoria, subgrupo, valor_planejado, origem)
        SELECT DATE_TRUNC('month', l.data_vencimento)::date,
               l.categoria, NULLIF(BTRIM(COALESCE(l.subgrupo,'')), ''),
               MAX(COALESCE(l.valor_orcamento, l.valor, 0)), 'migrado_envelope'
        FROM lancamentos l
        WHERE COALESCE(l.eh_orcamento,0)=1
        GROUP BY DATE_TRUNC('month', l.data_vencimento)::date,
                 l.categoria, NULLIF(BTRIM(COALESCE(l.subgrupo,'')), '')
        ON CONFLICT DO NOTHING
    """)
    cur.execute("""
        INSERT INTO orcamentos_categorias
            (competencia, categoria, subgrupo, valor_planejado, origem)
        SELECT DATE_TRUNC('month', CURRENT_DATE)::date, c.categoria,
               NULLIF(BTRIM(COALESCE(c.subgrupo,'')), ''),
               COALESCE(c.valor_padrao,0), 'migrado_categoria'
        FROM categorias_personalizadas c
        WHERE COALESCE(c.is_envelope,0)=1 AND COALESCE(c.valor_padrao,0) > 0
        ON CONFLICT DO NOTHING
    """)
    cur.execute("""
        DELETE FROM recorrencias_geradas rg
        USING categorias_personalizadas c
        WHERE rg.categoria_id=c.id AND COALESCE(c.is_envelope,0)=1
    """)
    cur.execute("DELETE FROM lancamentos WHERE COALESCE(eh_orcamento,0)=1")
    cur.execute("""
        UPDATE categorias_personalizadas
        SET is_envelope=0, is_recorrente=0
        WHERE COALESCE(is_envelope,0)=1
    """)

def baseline(cur):
    def execute_query(query): cur.execute(query)
    def _migrar_envelopes_legados(): migrate_envelopes(cur)
    '''Migrações compatíveis com a base existente, sem exigir reset manual.'''
    # Schema principal
    execute_query('''
        CREATE TABLE IF NOT EXISTS categorias_personalizadas (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT,
            valor_padrao NUMERIC,
            atraso_meses INTEGER,
            dia_pagamento INTEGER,
            is_recorrente INTEGER DEFAULT 0,
            data_inicio DATE,
            is_envelope INTEGER DEFAULT 0
        );
    ''')
    for ddl in [
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS valor_padrao NUMERIC;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS atraso_meses INTEGER;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS dia_pagamento INTEGER;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_recorrente INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS data_inicio DATE;",
        # is_envelope permanece somente para importar backups antigos; a v2 não o usa.
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_envelope INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS is_producao_variavel INTEGER DEFAULT 0;",
        "ALTER TABLE categorias_personalizadas ADD COLUMN IF NOT EXISTS modalidade_renda TEXT;",
    ]:
        execute_query(ddl)

    # V2: o tipo da renda é independente do recurso especializado de Plantões.
    # Migra somente classificações antigas explícitas/legadas para a nova semântica.
    execute_query("""
        UPDATE categorias_personalizadas
        SET is_producao_variavel=1
        WHERE tipo='Entrada'
          AND (TRIM(COALESCE(modalidade_renda,''))='Plantões'
               OR LOWER(TRIM(COALESCE(categoria,''))) LIKE 'plant%')
    """)
    execute_query("""
        UPDATE categorias_personalizadas
        SET modalidade_renda='Variável'
        WHERE tipo='Entrada' AND TRIM(COALESCE(modalidade_renda,''))='Plantões'
    """)

    execute_query('''
        CREATE TABLE IF NOT EXISTS orcamentos_categorias (
            id BIGSERIAL PRIMARY KEY,
            competencia DATE NOT NULL,
            categoria TEXT NOT NULL,
            subgrupo TEXT,
            valor_planejado NUMERIC NOT NULL DEFAULT 0 CHECK (valor_planejado >= 0),
            origem TEXT NOT NULL DEFAULT 'manual',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    ''')
    execute_query('''
        CREATE UNIQUE INDEX IF NOT EXISTS ux_orcamento_categoria_mes
        ON orcamentos_categorias (competencia, categoria, COALESCE(subgrupo,''));
    ''')

    execute_query('''
        CREATE TABLE IF NOT EXISTS lancamentos (
            id SERIAL PRIMARY KEY,
            tipo TEXT,
            categoria TEXT,
            subgrupo TEXT,
            descricao TEXT,
            valor NUMERIC,
            data_vencimento DATE,
            parcela_atual INTEGER,
            total_parcelas INTEGER,
            pago INTEGER DEFAULT 0,
            compra_id TEXT,
            forma_pagamento TEXT DEFAULT 'Outros',
            prioridade TEXT DEFAULT 'Baixa 🟢',
            valor_pago NUMERIC DEFAULT 0.0
        );
    ''')
    for ddl in [
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS forma_pagamento TEXT DEFAULT 'Outros';",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS prioridade TEXT DEFAULT 'Baixa 🟢';",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS valor_pago NUMERIC DEFAULT 0.0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS eh_estimativa INTEGER DEFAULT 0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS data_competencia DATE;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS data_pagamento DATE;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS eh_orcamento INTEGER DEFAULT 0;",
        "ALTER TABLE lancamentos ADD COLUMN IF NOT EXISTS valor_orcamento NUMERIC;",
    ]:
        execute_query(ddl)

    # Datas legadas: competência = vencimento; para pagamentos antigos, a melhor
    # inferência disponível é o vencimento. Novos registros passam a gravar datas reais.
    execute_query("UPDATE lancamentos SET data_competencia = data_vencimento WHERE data_competencia IS NULL")
    # Recupera a data real de plantões legados que antes existia apenas na descrição.
    execute_query(r"""
        UPDATE lancamentos
        SET data_competencia = TO_DATE(SUBSTRING(descricao FROM '\(([0-9]{2}/[0-9]{2}/[0-9]{4})\)'), 'DD/MM/YYYY')
        WHERE tipo = 'Entrada' AND descricao LIKE 'Plantão %'
          AND descricao ~ '\([0-9]{2}/[0-9]{2}/[0-9]{4}\)'
    """)
    execute_query("UPDATE lancamentos SET data_pagamento = data_vencimento WHERE pago = 1 AND data_pagamento IS NULL")

    execute_query('''
        CREATE TABLE IF NOT EXISTS info_dividas (
            compra_id TEXT PRIMARY KEY,
            credor TEXT,
            taxa_juros_mensal NUMERIC
        );
    ''')
    execute_query('''
        CREATE TABLE IF NOT EXISTS reserva_emergencia (
            id INTEGER PRIMARY KEY DEFAULT 1,
            valor NUMERIC DEFAULT 0,
            atualizado_em DATE
        );
    ''')
    execute_query("INSERT INTO reserva_emergencia (id, valor, atualizado_em) VALUES (1, 0, CURRENT_DATE) ON CONFLICT (id) DO NOTHING;")

    # Entidade de pagamentos. O lançamento mantém valor_pago/pago por compatibilidade
    # com a UI atual, enquanto esta tabela cria histórico e caminho para pagamentos
    # parciais/múltiplos no futuro.
    execute_query('''
        CREATE TABLE IF NOT EXISTS pagamentos (
            id BIGSERIAL PRIMARY KEY,
            lancamento_id INTEGER NOT NULL REFERENCES lancamentos(id) ON DELETE CASCADE,
            valor NUMERIC NOT NULL,
            data_pagamento DATE NOT NULL,
            origem TEXT NOT NULL DEFAULT 'sincronizado',
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (lancamento_id, origem)
        );
    ''')

    # Trigger centraliza a consistência de qualquer caminho de baixa/estorno,
    # inclusive botões consolidados que ainda fazem UPDATE direto em lancamentos.
    execute_query('''
        CREATE OR REPLACE FUNCTION fn_normalizar_pagamento_lancamento()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.pago = 1 THEN
                IF COALESCE(NEW.valor_pago, 0) = 0 THEN
                    NEW.valor_pago := NEW.valor;
                END IF;
                IF NEW.data_pagamento IS NULL THEN
                    NEW.data_pagamento := CURRENT_DATE;
                END IF;
            ELSE
                NEW.valor_pago := 0;
                NEW.data_pagamento := NULL;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ''')
    execute_query("DROP TRIGGER IF EXISTS trg_normalizar_pagamento_lancamento ON lancamentos;")
    execute_query('''
        CREATE TRIGGER trg_normalizar_pagamento_lancamento
        BEFORE INSERT OR UPDATE OF pago, valor_pago, data_pagamento
        ON lancamentos
        FOR EACH ROW EXECUTE FUNCTION fn_normalizar_pagamento_lancamento();
    ''')

    execute_query('''
        CREATE OR REPLACE FUNCTION fn_sincronizar_pagamento_lancamento()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.pago = 1 AND COALESCE(NEW.valor_pago, 0) > 0 THEN
                INSERT INTO pagamentos (lancamento_id, valor, data_pagamento, origem)
                VALUES (NEW.id, NEW.valor_pago, COALESCE(NEW.data_pagamento, CURRENT_DATE), 'sincronizado')
                ON CONFLICT (lancamento_id, origem)
                DO UPDATE SET valor = EXCLUDED.valor, data_pagamento = EXCLUDED.data_pagamento;
            ELSE
                DELETE FROM pagamentos WHERE lancamento_id = NEW.id AND origem = 'sincronizado';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ''')
    execute_query("DROP TRIGGER IF EXISTS trg_sincronizar_pagamento_lancamento ON lancamentos;")
    execute_query('''
        CREATE TRIGGER trg_sincronizar_pagamento_lancamento
        AFTER INSERT OR UPDATE OF pago, valor_pago, data_pagamento
        ON lancamentos
        FOR EACH ROW EXECUTE FUNCTION fn_sincronizar_pagamento_lancamento();
    ''')
    execute_query('''
        INSERT INTO pagamentos (lancamento_id, valor, data_pagamento, origem)
        SELECT id, valor_pago, COALESCE(data_pagamento, data_vencimento), 'sincronizado'
        FROM lancamentos
        WHERE pago = 1 AND COALESCE(valor_pago, 0) > 0
        ON CONFLICT (lancamento_id, origem)
        DO UPDATE SET valor = EXCLUDED.valor, data_pagamento = EXCLUDED.data_pagamento;
    ''')

    # Idempotência forte de recorrências: a sessão continua sendo otimização, mas
    # o banco passa a decidir atomicamente se aquela competência já foi gerada.
    execute_query('''
        CREATE TABLE IF NOT EXISTS recorrencias_geradas (
            categoria_id INTEGER NOT NULL REFERENCES categorias_personalizadas(id) ON DELETE CASCADE,
            competencia DATE NOT NULL,
            criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (categoria_id, competencia)
        );
    ''')
    execute_query('''
        INSERT INTO recorrencias_geradas (categoria_id, competencia)
        SELECT CAST(SUBSTRING(l.compra_id FROM 5) AS INTEGER), DATE_TRUNC('month', l.data_vencimento)::date
        FROM lancamentos l
        JOIN categorias_personalizadas c
          ON l.compra_id = ('rec_' || c.id::text)
        WHERE l.compra_id ~ '^rec_[0-9]+$'
        ON CONFLICT DO NOTHING;
    ''')

    # Preferências leves da interface. Não substitui autenticação/contas; guarda
    # apenas escolhas do produto como nome de exibição e página inicial.
    execute_query('''
        CREATE TABLE IF NOT EXISTS preferencias_app (
            chave TEXT PRIMARY KEY,
            valor TEXT,
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    ''')

    # Migração: orçamento deixa de ser lançamento e passa a ser dado mensal da categoria.
    _migrar_envelopes_legados()

    # VIEW mantida por compatibilidade com consultas existentes; sem saldo derivado.
    execute_query('''
        CREATE OR REPLACE VIEW vw_lancamentos_financeiros AS
        SELECT
            l.id, l.tipo, l.categoria, l.subgrupo, l.descricao, l.valor,
            l.data_vencimento, l.parcela_atual, l.total_parcelas, l.pago,
            l.compra_id, l.forma_pagamento, l.prioridade, l.valor_pago,
            l.eh_estimativa, l.data_competencia, l.data_pagamento,
            l.eh_orcamento, l.valor_orcamento
        FROM lancamentos l;
    ''')

    # Constraints aplicadas a dados novos sem bloquear bases legadas que eventualmente
    # contenham alguma inconsistência histórica. Podem ser VALIDATE posteriormente.
    constraints = {
        'ck_lanc_tipo': "tipo IN ('Entrada','Despesa')",
        'ck_lanc_pago': "pago IN (0,1)",
        'ck_lanc_valor': "valor >= 0",
        'ck_lanc_valor_pago': "valor_pago >= 0",
        'ck_lanc_parcela_atual': "parcela_atual IS NULL OR parcela_atual >= 1",
        'ck_lanc_total_parcelas': "total_parcelas IS NULL OR total_parcelas >= 1",
        'ck_lanc_parcelas_ordem': "parcela_atual IS NULL OR total_parcelas IS NULL OR total_parcelas = 999 OR parcela_atual <= total_parcelas",
    }
    for nome, regra in constraints.items():
        execute_query(f'''DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{nome}' AND conrelid='lancamentos'::regclass) THEN
                ALTER TABLE lancamentos ADD CONSTRAINT {nome} CHECK ({regra}) NOT VALID;
            END IF;
        END $$;''')

    execute_query('''DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_cat_tipo' AND conrelid='categorias_personalizadas'::regclass) THEN
            ALTER TABLE categorias_personalizadas
            ADD CONSTRAINT ck_cat_tipo CHECK (tipo IN ('Entrada','Despesa')) NOT VALID;
        END IF;
    END $$;''')

    # Índices para as consultas de período, status e agrupamento por compra.
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_vencimento ON lancamentos(data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_pagamento ON lancamentos(data_pagamento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_data_competencia ON lancamentos(data_competencia);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_compra_id ON lancamentos(compra_id);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_pago_data ON lancamentos(pago, data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_tipo_data ON lancamentos(tipo, data_vencimento);",
        "CREATE INDEX IF NOT EXISTS idx_lanc_categoria_subgrupo_data ON lancamentos(categoria, subgrupo, data_vencimento, pago);",
        "CREATE INDEX IF NOT EXISTS idx_cat_tipo_categoria_subgrupo ON categorias_personalizadas(tipo, categoria, subgrupo);",
    ]:
        execute_query(ddl)

    # Unicidade lógica de categoria é criada somente se a base atual não possui
    # duplicatas, evitando quebrar uma instalação existente durante a migração.
    execute_query('''DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM categorias_personalizadas
            GROUP BY tipo, categoria, COALESCE(subgrupo,'') HAVING COUNT(*) > 1
        ) AND NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE indexname = 'uq_cat_logica' AND schemaname=current_schema()
        ) THEN
            CREATE UNIQUE INDEX uq_cat_logica
            ON categorias_personalizadas(tipo, categoria, (COALESCE(subgrupo,'')));
        END IF;
    END $$;''')

def integrity(cur):
    cur.execute("""
        CREATE TABLE cartoes (
            id BIGSERIAL PRIMARY KEY, nome TEXT NOT NULL UNIQUE,
            dia_fechamento INTEGER NOT NULL CHECK (dia_fechamento BETWEEN 1 AND 31),
            dia_vencimento INTEGER NOT NULL CHECK (dia_vencimento BETWEEN 1 AND 31)
        );
        CREATE TABLE faturas (
            id BIGSERIAL PRIMARY KEY, cartao_id BIGINT NOT NULL REFERENCES cartoes(id),
            vencimento DATE NOT NULL, UNIQUE(cartao_id, vencimento)
        );
        ALTER TABLE lancamentos ADD COLUMN fatura_id BIGINT REFERENCES faturas(id);
        CREATE INDEX idx_lanc_fatura ON lancamentos(fatura_id);
        ALTER TABLE lancamentos ADD COLUMN ajuste_pagamento_ids INTEGER[];
        CREATE TABLE auditoria (
            id BIGSERIAL PRIMARY KEY, entidade TEXT NOT NULL, operacao TEXT NOT NULL,
            anterior JSONB, posterior JSONB, ator TEXT, criado_em TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE OR REPLACE FUNCTION auditar_lancamento() RETURNS trigger AS $$
        BEGIN
            INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator) VALUES
                (TG_TABLE_NAME,TG_OP,CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
                 CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END,current_setting('app.actor',true));
            RETURN COALESCE(NEW,OLD);
        END; $$ LANGUAGE plpgsql;
        CREATE TRIGGER trg_auditar_lancamento AFTER INSERT OR UPDATE OR DELETE ON lancamentos
        FOR EACH ROW EXECUTE FUNCTION auditar_lancamento();
        CREATE OR REPLACE FUNCTION fn_normalizar_pagamento_lancamento() RETURNS trigger AS $$
        BEGIN
            IF NEW.pago = 1 THEN
                -- Only missing values use the plan. Zero is a valid realized value.
                IF NEW.valor_pago IS NULL THEN NEW.valor_pago := NEW.valor; END IF;
                IF NEW.data_pagamento IS NULL THEN NEW.data_pagamento := CURRENT_DATE; END IF;
            ELSE NEW.valor_pago := 0; NEW.data_pagamento := NULL; END IF;
            RETURN NEW;
        END; $$ LANGUAGE plpgsql;
        CREATE OR REPLACE VIEW vw_lancamentos_financeiros AS
            SELECT l.id, l.tipo, l.categoria, l.subgrupo, l.descricao, l.valor,
                   l.data_vencimento, l.parcela_atual, l.total_parcelas, l.pago,
                   l.compra_id, l.forma_pagamento, l.prioridade, l.valor_pago,
                   l.eh_estimativa, l.data_competencia, l.data_pagamento,
                   l.eh_orcamento, l.valor_orcamento, l.fatura_id, c.nome AS cartao_nome,
                   l.ajuste_pagamento_ids
            FROM lancamentos l LEFT JOIN faturas f ON f.id=l.fatura_id
            LEFT JOIN cartoes c ON c.id=f.cartao_id;
    """)

def replay_protection(cur):
    cur.execute("""
        ALTER TABLE lancamentos ADD COLUMN requisicao_id TEXT;
        CREATE UNIQUE INDEX ux_lanc_requisicao_parcela
        ON lancamentos(requisicao_id, parcela_atual) WHERE requisicao_id IS NOT NULL;
    """)


def migrate(conn, schema='public'):
    validate_schema(schema)
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ('fluxo-migration-'+schema,))
            cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
            cur.execute(sql.SQL('SET LOCAL search_path TO {}').format(sql.Identifier(schema)))
            cur.execute('CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())')
            cur.execute('SELECT version FROM schema_migrations')
            applied={r[0] for r in cur.fetchall()}
            for version, migration in [(1,baseline),(2,integrity),(3,replay_protection)]:
                if version not in applied:
                    migration(cur)
                    cur.execute('INSERT INTO schema_migrations(version) VALUES (%s)',(version,))

if __name__ == '__main__':
    schemas={'public'}
    accounts=json.loads(os.environ.get('APP_USERS_JSON','{}'))
    if accounts: schemas={a['schema'] for a in accounts.values()}
    with psycopg2.connect(os.environ['DATABASE_URL']) as connection:
        for schema in sorted(schemas): migrate(connection,schema)
    print('Migrations applied.')
