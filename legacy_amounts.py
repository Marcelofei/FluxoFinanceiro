"""Convert historical signed amounts to positive entry/expense representation."""
from decimal import Decimal


def signed_amounts(kind, planned, actual, paid):
    if kind not in ('Entrada', 'Despesa'):
        raise ValueError('Tipo inválido no lançamento legado; revise o registro antes de migrar.')
    planned = Decimal(str(planned)) if planned is not None else Decimal(0)
    actual = Decimal(str(actual)) if actual is not None and paid else Decimal(0)
    if not planned.is_finite() or not actual.is_finite():
        raise ValueError('Valor não finito no lançamento legado; revise o registro antes de migrar.')
    opposite = 'Despesa' if kind == 'Entrada' else 'Entrada'
    flip = planned < 0 or (planned == 0 and actual < 0)
    primary = opposite if flip else kind
    # Opposite plan/actual signs need a realized-only counterpart. Otherwise
    # taking abs() would silently change the economic direction of a receipt.
    same_direction = actual == 0 or (actual < 0) == flip
    return dict(tipo=primary, valor=abs(planned),
                valor_pago=abs(actual) if same_direction else Decimal(0),
                extra_tipo=None if same_direction else (kind if flip else opposite),
                extra_valor=Decimal(0) if same_direction else abs(actual))


def normalize_legacy_amounts(cur):
    """Run before metadata UPDATEs, with constraints/triggers fully enabled.

    PostgreSQL rechecks NOT VALID constraints when updating *any* column.
    The existing audit trigger captures the complete original signed record.
    The surrounding migration transaction makes the conversion all-or-nothing.
    """
    cur.execute('''SELECT id,tipo,valor,valor_pago,pago FROM lancamentos
        WHERE valor<0 OR valor_pago<0 ORDER BY id FOR UPDATE''')
    for item, kind, planned, actual, paid in cur.fetchall():
        normalized = signed_amounts(kind, planned, actual, paid)
        cur.execute('''UPDATE lancamentos SET tipo=%s,valor=%s,valor_pago=%s,
            data_pagamento=CASE WHEN pago=1 THEN COALESCE(data_pagamento,data_vencimento,CURRENT_DATE)
                                ELSE NULL END WHERE id=%s''',
            (normalized['tipo'],normalized['valor'],normalized['valor_pago'],item))
        extra_id = None
        if normalized['extra_tipo']:
            cur.execute('''INSERT INTO lancamentos(tipo,categoria,subgrupo,descricao,valor,
                valor_pago,pago,data_vencimento,data_pagamento,data_competencia,compra_id,
                forma_pagamento,prioridade,parcela_atual,total_parcelas)
                SELECT %s,categoria,subgrupo,COALESCE(descricao,'') || ' · contrapartida legada',0,
                    %s,1,data_vencimento,data_pagamento,data_competencia,%s,
                    'Outros',prioridade,1,1 FROM lancamentos WHERE id=%s RETURNING id''',
                (normalized['extra_tipo'],normalized['extra_valor'],f'legacy-sign-{item}',item))
            extra_id=cur.fetchone()[0]
        cur.execute('''INSERT INTO auditoria(entidade,operacao,anterior,posterior,ator)
            VALUES ('migracao','NORMALIZAR_SINAL_LEGADO',
                    jsonb_build_object('lancamento_id',%s),
                    jsonb_build_object('lancamento_id',%s,'contrapartida_id',%s),
                    'python migrate.py')''',(item,item,extra_id))
