"""Progresso das parcelas cadastradas e seleção revisada pelo usuário."""
import json
from datetime import date
from categories import CATEGORIES


def progress(rows):
    groups = {}
    for row in rows:
        if row.get('installment_total', 1) <= 1: continue
        key = row.get('series_key') or str(row['request_id'])
        groups.setdefault(key, []).append(row)
    result = []
    for key, items in groups.items():
        items.sort(key=lambda r: (r['due_date'], r['id']))
        paid = sum(r['settled'] for r in items)
        pending = [r for r in items if not r['settled']]
        result.append(dict(key=key,description=items[0]['description'],paid=paid,total=len(items),
                           fraction=paid/len(items),remaining_cents=sum(r['expected_cents'] for r in pending),
                           actual_cents=sum(r['actual_cents'] or 0 for r in items if r['settled']),
                           next_date=pending[0]['due_date'] if pending else None,entries=items))
    return result


def read_selection(data):
    if len(data)>10_000_000: raise ValueError('Arquivo muito grande.')
    obj=json.loads(data)
    if obj.get('format')!='fluxo-selection-v1': raise ValueError('Formato inválido.')
    rows=obj.get('entries',[]); reminders=obj.get('reminders',[])
    if not isinstance(rows,list) or not isinstance(reminders,list): raise ValueError('Listas inválidas.')
    for r in rows:
        if r.get('kind')!='expense' or r.get('settled') is not False: raise ValueError('Seleção exige previsões pendentes.')
        if not str(r.get('source','')).strip() or not 0<len(r.get('description',''))<=160: raise ValueError('Identificação inválida.')
        if r.get('category') not in CATEGORIES: raise ValueError('Categoria inválida.')
        if type(r.get('expected_cents')) is not int or r['expected_cents']<=0: raise ValueError('Valor inválido.')
        date.fromisoformat(r['due_date'])
        total=r.get('installment_total',1); number=r.get('installment_number',1)
        if type(total) is not int or type(number) is not int or not 1<=number<=total<=120: raise ValueError('Parcelas inválidas.')
    for r in reminders:
        if not 0<len(r.get('description',''))<=160 or not r.get('source'): raise ValueError('Lembrete inválido.')
        if type(r.get('amount_cents')) is not int or r['amount_cents']<=0: raise ValueError('Valor do lembrete inválido.')
    return rows,reminders
