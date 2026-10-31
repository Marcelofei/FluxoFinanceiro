from datetime import date
from decimal import Decimal
import pytest
from finance import add_months, month_bounds, cents, schedule

D = date(2026, 11, 1)


def entry(id, kind, amount, day, settled=False):
    return dict(id=id, kind=kind, description=f'Item {id}', expected_cents=amount,
                due_date=date(2026, 11, day), settled=settled)


def test_exact_money_and_invalid_values():
    assert cents('10.005') == 1001
    assert cents(Decimal('123456.78')) == 12345678
    for value in ['NaN', 'Infinity', '-1', 'x']:
        with pytest.raises(ValueError): cents(value)


def test_month_end_keeps_original_day_and_year_boundary():
    assert add_months(date(2027, 1, 31), 1) == date(2027, 2, 28)
    assert add_months(date(2027, 1, 31), 2) == date(2027, 3, 31)
    assert add_months(date(2026, 12, 31), 1) == date(2027, 1, 31)
    assert month_bounds(2026, 12) == (date(2026, 12, 1), date(2027, 1, 1))


def test_manual_balance_plus_only_unreceived_income():
    rows = [entry(1, 'income', 100000, 5, True), entry(2, 'income', 10000, 15),
            entry(3, 'expense', 7000, 10), entry(4, 'expense', 9000, 20)]
    plan = schedule(rows, 5000, D)
    assert plan[0]['allocations'][0]['cents'] == 5000
    assert plan[0]['status'] == 'Dinheiro após o vencimento'
    assert plan[1]['uncovered_cents'] == 1000
    assert all(a['source_id'] != 1 for p in plan for a in p['allocations'])
    assert sum(a['cents'] for p in plan for a in p['allocations']) == 15000


def test_paid_bills_do_not_consume_current_balance():
    plan = schedule([entry(1, 'expense', 9000, 2, True), entry(2, 'expense', 5000, 3)], 5000, D)
    assert len(plan) == 1
    assert plan[0]['status'] == 'Coberta no prazo'


def test_overdue_income_is_uncertain_and_cannot_erase_warning():
    plan = schedule([entry(1, 'income', 5000, 1), entry(2, 'expense', 3000, 10)], 0, date(2026,11,5))
    assert plan[0]['status'] == 'Depende de renda atrasada'


def test_today_balance_does_not_retroactively_cover_due_date():
    plan = schedule([entry(1,'expense',3000,1)], 3000, date(2026,11,5))
    assert plan[0]['status'] == 'Dinheiro após o vencimento'


def test_priority_and_no_reuse_across_many_entries():
    rows = [entry(i,'income',i*101, i) for i in range(1,10)]
    rows += [entry(i+20,'expense',i*300,i) for i in range(1,20)]
    plan = schedule(list(reversed(rows)), 999, D)
    assert [p['entry']['id'] for p in plan] == list(range(21,40))
    allocated = sum(a['cents'] for p in plan for a in p['allocations'])
    assert allocated == 999 + sum(i*101 for i in range(1,10))
    for p in plan:
        assert sum(a['cents'] for a in p['allocations']) + p['uncovered_cents'] == p['entry']['expected_cents']
