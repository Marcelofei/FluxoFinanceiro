"""Regras do planejamento. Valores monetários são centavos inteiros."""
import calendar
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def cents(value):
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise ValueError('Informe um valor positivo ou zero.')
        return int((amount * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError):
        raise ValueError('Valor inválido.') from None


def brl(value):
    return f'R$ {value / 100:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')


def month_bounds(year, month):
    return date(year, month, 1), date(year + (month == 12), month % 12 + 1, 1)


def add_months(day, offset):
    index = day.year * 12 + day.month - 1 + offset
    year, month = divmod(index, 12)
    return date(year, month + 1, min(day.day, calendar.monthrange(year, month + 1)[1]))


def schedule(entries, available, today):
    """Distribui cada centavo uma única vez, por vencimento e id estável.

    O chamador fornece apenas o mês escolhido. Recebidos e pagos ficam fora.
    Disponível vale hoje; renda vencida e não recebida não vira dinheiro garantido.
    """
    if not isinstance(available, int) or available < 0:
        raise ValueError('Disponível inválido.')
    sources = [{'id': None, 'name': 'Disponível informado', 'date': today,
                'remaining': available, 'overdue': False}]
    incomes = sorted((e for e in entries if e['kind'] == 'income' and not e['settled']),
                     key=lambda e: (e['due_date'], e['id']))
    for entry in incomes:
        sources.append({'id': entry['id'], 'name': entry['description'],
                        'date': max(entry['due_date'], today),
                        'remaining': entry['expected_cents'],
                        'overdue': entry['due_date'] < today})
    bills = sorted((e for e in entries if e['kind'] == 'expense' and not e['settled']),
                   key=lambda e: (e['due_date'], e['id']))
    result = []
    for bill in bills:
        remaining = bill['expected_cents']
        allocations = []
        # Evita depender de renda já atrasada quando há outra fonte no prazo.
        ordered = sorted(sources, key=lambda s: (
            2 if s['overdue'] else (0 if s['date'] <= max(today, bill['due_date']) else 1),
            s['date'], s['id'] or 0))
        for source in ordered:
            amount = min(remaining, source['remaining'])
            if amount:
                allocations.append({'source': source['name'], 'source_id': source['id'],
                                    'date': source['date'], 'cents': amount,
                                    'overdue_income': source['overdue']})
                source['remaining'] -= amount
                remaining -= amount
            if not remaining:
                break
        late = any(a['date'] > bill['due_date'] for a in allocations)
        uncertain = any(a['overdue_income'] for a in allocations)
        status = ('Sem cobertura completa' if remaining else
                  'Depende de renda atrasada' if uncertain else
                  'Dinheiro após o vencimento' if late else 'Coberta no prazo')
        result.append({'entry': bill, 'allocations': allocations,
                       'uncovered_cents': remaining, 'status': status})
    return result
