"""Atomic settlement services; caller owns transaction and tenant connection."""
import uuid
from decimal import Decimal
from finance import money, allocate_amount


def settle(cur, ids, total, date):
    ids = sorted(set(int(i) for i in ids))
    if not ids: raise ValueError('Selecione um lançamento')
    cur.execute('SELECT id,valor,pago,forma_pagamento,tipo FROM lancamentos WHERE id=ANY(%s) ORDER BY id FOR UPDATE', (ids,))
    rows = cur.fetchall()
    if len(rows) != len(ids): raise ValueError('Um lançamento foi removido. Atualize a tela.')
    if any(r[2] for r in rows): raise ValueError('Um lançamento já foi pago. Atualize a tela; pagamentos anteriores foram preservados.')
    planned = sum((money(r[1]) for r in rows), Decimal('0'))
    actual = planned if total is None else money(total)
    if actual < 0: raise ValueError('O valor realizado não pode ser negativo')
    credit = any(r[3]=='Crédito' and r[4]=='Despesa' for r in rows)
    if credit and not all(r[3]=='Crédito' and r[4]=='Despesa' for r in rows):
        raise ValueError('Pague a fatura separadamente de outras contas')
    values = [money(r[1]) for r in rows] if credit else allocate_amount(actual, [r[1] for r in rows])
    for row, value in zip(rows, values):
        cur.execute('UPDATE lancamentos SET pago=1,valor_pago=%s,data_pagamento=%s WHERE id=%s', (value,date,row[0]))
    # Fees/discounts must not rewrite the original purchase categories.
    difference = actual-planned
    if credit and difference:
        cur.execute('''INSERT INTO lancamentos(tipo,categoria,descricao,valor,valor_pago,pago,
            data_vencimento,data_pagamento,data_competencia,compra_id,forma_pagamento,prioridade,ajuste_pagamento_ids)
            VALUES (%s,'Ajustes de fatura',%s,%s,%s,1,%s,%s,%s,%s,'À vista','Baixa 🟢',%s)''',
            ('Despesa' if difference>0 else 'Entrada','Encargos da fatura' if difference>0 else 'Desconto da fatura',
             abs(difference),abs(difference),date,date,date,str(uuid.uuid4()),ids))
    return float(actual)


def reverse(cur, ids):
    ids=sorted(set(int(i) for i in ids))
    if not ids: raise ValueError('Selecione um lançamento')
    cur.execute('SELECT id FROM lancamentos WHERE id=ANY(%s) ORDER BY id FOR UPDATE',(ids,))
    if len(cur.fetchall()) != len(ids):
        raise ValueError('Um lançamento foi removido. Atualize a tela.')
    # A fee/discount belongs to the settlement: reverse the complete settlement, never a fraction.
    cur.execute('SELECT id,ajuste_pagamento_ids FROM lancamentos WHERE ajuste_pagamento_ids && %s FOR UPDATE',(ids,))
    adjustments=cur.fetchall()
    for _,linked in adjustments:
        if not set(linked).issubset(ids):
            raise ValueError('Estorne a fatura completa para preservar seu ajuste de pagamento')
    if adjustments:
        cur.execute('DELETE FROM lancamentos WHERE id=ANY(%s)',([r[0] for r in adjustments],))
    cur.execute('UPDATE lancamentos SET pago=0,valor_pago=0,data_pagamento=NULL WHERE id=ANY(%s) AND pago=1',(ids,))
