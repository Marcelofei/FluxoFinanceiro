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


def test_reorganization_uses_other_income_preserves_paid_history():
    rows=[row(1,'Entrada',1000,'2026-09-25',1,1000,'2026-09-25'),
          row(2,'Entrada',500,'2026-10-05'),
          row(3,'Despesa',100,'2026-09-26',1,100,'2026-09-26'),
          row(4,'Despesa',400,'2026-09-30')]
    base=pd.DataFrame(rows); base['desconsiderar_cobertura']=base['id'].eq(1)
    ops=f._consolidar_operacional(base,True,hoje=TODAY)
    p=f._montar_plano_pagamentos(ops,2026,9,hoje=TODAY)
    paid=next(c for c in p['contas'] if c['id']=='3')
    pending=next(c for c in p['contas'] if c['id']=='4')
    assert paid['alocacoes'][0]['fonte_id']=='1'
    assert pending['alocacoes'][0]['fonte_id']=='2'
    assert pending['risco_valor']==400
    assert p['reserva_minima']==400
    assert p['recebido_nao_alocado']==0


def test_reorganization_does_not_discard_new_receipt_in_same_group():
    rows=[row(1,'Entrada',100,'2026-09-30',1,100,'2026-09-30'),
          row(2,'Entrada',200,'2026-09-30',1,200,'2026-09-30'),row(3,'Despesa',250,'2026-09-30')]
    rows[0]['descricao']=rows[1]['descricao']='Plantão Hospital'
    base=pd.DataFrame(rows);base['desconsiderar_cobertura']=base['id'].eq(1)
    ops=f._consolidar_operacional(base,True,hoje=TODAY)
    p=f._montar_plano_pagamentos(ops,2026,9,hoje=TODAY)
    pending=p['contas'][0]
    assert sum(a['valor'] for a in pending['alocacoes'])==200
    assert pending['descoberto']==50


def test_monthly_reorganization_never_uses_other_months():
    df=pd.DataFrame([row(1,'Entrada',1000,'2026-09-20',1,1000,'2026-09-20'),
        row(2,'Entrada',700,'2026-09-30'),row(3,'Despesa',500,'2026-09-30'),
        row(4,'Despesa',900,'2026-08-31'),row(5,'Entrada',3000,'2026-10-01')])
    scoped=f.coverage_month_scope(df,2026,9,{'ativo':True,'ids':[1]})
    assert scoped.id.tolist()==[1,2,3]
    ops=f._consolidar_operacional(scoped,True,hoje=TODAY)
    p=f._montar_plano_pagamentos(ops,2026,9,hoje=TODAY)
    assert len(p['contas'])==1
    assert p['contas'][0]['alocacoes'][0]['fonte_id']=='2'
    unchanged=f.coverage_month_scope(df,2026,10,{'ids':[]})
    assert unchanged.id.tolist()==df.id.tolist()
    assert not unchanged.desconsiderar_cobertura.any()


def test_shift_hospitals_stay_separate_in_coverage():
    a=row(101,'Entrada',1000,'2026-09-30');a.update(categoria='Rendas',subgrupo='Hospital A',descricao='Plantão Hospital A')
    b=row(102,'Entrada',2000,'2026-09-30');b.update(categoria='Rendas',subgrupo='Hospital B',descricao='Plantão Hospital B')
    ops=f._consolidar_operacional(pd.DataFrame([a,b]),True,hoje=TODAY)
    assert len(ops)==2
    assert set(ops.descricao)=={'🏥 Hospital A','🏥 Hospital B'}


def test_legacy_shift_links_only_to_unambiguous_source():
    df=pd.DataFrame([dict(tipo='Entrada',descricao='Plantão Hospital A',categoria='Plantões',subgrupo='Hospital A',valor=100)])
    definitions=pd.DataFrame([dict(categoria='Rendas',subgrupo='Hospital A')])
    linked=f.align_shift_sources(df,definitions)
    assert linked.iloc[0].categoria=='Rendas'
    assert df.iloc[0].categoria=='Plantões'
    ambiguous=pd.concat([definitions,pd.DataFrame([dict(categoria='Outra',subgrupo='Hospital A')])])
    assert f.align_shift_sources(df,ambiguous).iloc[0].categoria=='Plantões'
