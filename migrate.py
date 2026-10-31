"""Cria somente as tabelas novas. Não altera nem apaga dados anteriores."""
from security import accounts
from storage import connection, initialize


def main():
    schemas = {a['schema'] for a in accounts().values()} or {'public'}
    for schema in sorted(schemas):
        with connection(schema) as conn:
            initialize(conn)
    print('Estrutura essencial pronta. Dados anteriores preservados.')


if __name__ == '__main__':
    main()
