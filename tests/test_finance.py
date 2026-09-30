import datetime as dt
from decimal import Decimal
import pandas as pd
import pytest
import finance as f
TODAY=dt.date(2026,9,30)


def row(i,kind,value,due,paid=0,real=0,payment=None,credit=False,invoice=None):
    return dict(id=i,tipo=kind,valor=value,valor_pago=real,pago=paid,
                data_vencimento=dt.date.fromisoformat(due),data_pagamento=dt.date.fromisoformat(payment) if payment else None,
                categoria='Teste',subgrupo='',descricao=f'Item {i}',prioridade='Baixa 🟢',
                forma_pagamento='Crédito' if credit else 'À vista',parcela_atual=1,total_parcelas=1,
                fatura_id=invoice,cartao_nome='Cartão teste')


def plan(rows):
    ops=f._consolidar_operacional(pd.DataFrame(rows),True,hoje=TODAY)
    return ops,f._montar_plano_pagamentos(ops,2026,9,hoje=TODAY)


def test_overdue_income_never_green():
    _,p=plan([row(1,'Entrada',1000,'2026-09-20'),row(2,'Despesa',700,'2026-09-30')])
    c=p['contas'][0]
    assert c['risco_valor']==700
    assert f._fluxo2_texto_cobertura(c)[0]=='danger'
    assert p['reserva_minima']==700


def test_received_income_has_priority_over_forecast():
    _,p=plan([row(1,'Entrada',1000,'2026-09-30'),row(2,'Entrada',1000,'2026-09-30',1,1000,'2026-09-30'),row(3,'Despesa',500,'2026-10-01')])
    assert p['contas'][0]['alocacoes'][0]['fonte_id']=='2'


def test_forecast_coverage_is_amber():
    _,p=plan([row(1,'Entrada',1000,'2026-10-01'),row(2,'Despesa',700,'2026-10-02')])
    assert f._fluxo2_texto_cobertura(p['contas'][0])[0]=='warn'


def test_bridge_includes_overdue_bills():
    _,p=plan([row(1,'Entrada',1000,'2026-10-05'),row(2,'Despesa',500,'2026-09-29')])
    bridge=f._fluxo2_resumo_proxima_renda(p,2026,9,hoje=TODAY)
    assert bridge['total']==bridge['risco']==500


def test_mixed_credit_preserves_paid_and_pending_amounts():
    ops,p=plan([row(1,'Despesa',100,'2026-09-10',1,90,'2026-09-10',True),row(2,'Despesa',200,'2026-09-10',credit=True)])
    assert ops.loc[ops.pago==0,'valor'].sum()==200
    assert ops.loc[ops.pago==1,'valor_pago'].sum()==90
    assert [c['valor'] for c in p['contas'] if not c['pago']]==[200]
    assert ops.loc[ops.pago==0,'ids'].iloc[0]==[2]


def test_cards_and_due_dates_never_merge():
    ops,_=plan([row(1,'Despesa',100,'2026-10-05',credit=True,invoice=1),row(2,'Despesa',200,'2026-10-05',credit=True,invoice=2),row(3,'Despesa',300,'2026-10-10',credit=True)])
    assert len(ops)==3


def test_non_credit_is_never_inferred():
    r=row(1,'Despesa',100,'2026-10-01');r['descricao']='Cartão da escola'
    ops,_=plan([r]); assert not ops.iloc[0]['consolidado']


def test_paid_zero_does_not_fall_back_to_plan():
    ops,p=plan([row(1,'Despesa',100,'2026-09-10',1,0,'2026-09-10')])
    assert f._valor_operacional(ops.iloc[0])==0
    assert p['reserva_minima']==0


def test_different_payment_dates_are_preserved():
    ops,_=plan([row(1,'Despesa',100,'2026-09-10',1,100,'2026-09-09',True,1),row(2,'Despesa',200,'2026-09-10',1,200,'2026-09-12',True,1)])
    assert len(ops)==2


def test_income_not_reused_across_bills():
    _,p=plan([row(1,'Entrada',100,'2026-09-30',1,100,'2026-09-30'),row(2,'Despesa',80,'2026-10-01'),row(3,'Despesa',80,'2026-10-02')])
    assert sum(c['descoberto'] for c in p['contas'])==60


@pytest.mark.parametrize('total,n',[(0.01,3),(100,3),(0,5),(1,99)])
def test_installments_exact(total,n):
    values=f.split_total(total,n)
    assert sum(values)==f.money(total)
    assert all(v>=0 for v in values)


@pytest.mark.parametrize('total,weights',[(0.01,[1,1,1]),(100,[30,70]),(0,[1,1]),(0.02,[1]*10)])
def test_allocation_exact_nonnegative(total,weights):
    values=f.allocate_amount(total,weights)
    assert sum(values)==f.money(total)
    assert all(v>=0 for v in values)


def test_closing_day_and_month_end():
    assert f.invoice_due(dt.date(2026,9,24),25,5)==dt.date(2026,10,5)
    assert f.invoice_due(dt.date(2026,9,25),25,5)==dt.date(2026,11,5)
    assert f.invoice_due(dt.date(2026,2,2),10,31)==dt.date(2026,2,28)
