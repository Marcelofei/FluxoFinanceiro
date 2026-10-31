"""Converte backup local em prévia. Nenhum dado pessoal pertence ao código."""
import csv
import io
import zipfile
from collections import defaultdict
from datetime import date
from decimal import Decimal
from uuid import uuid5, NAMESPACE_URL
from categories import classify
from finance import cents


def prepare_backup(data, start):
    rows, warnings = [], []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if sum(i.file_size for i in archive.infolist()) > 30_000_000:
            raise ValueError('Backup excede o limite de leitura.')
        def read(name):
            return list(csv.DictReader(io.StringIO(archive.read(name).decode('utf-8-sig'))))
        entries = read('lancamentos.csv')
        budgets = read('orcamentos_categorias.csv')
    # Previsões quinzenais são criadas somente para meses futuros completos.
    budget_keys = {(r['competencia'][:7],r['categoria'],r['subgrupo']) for r in budgets if r['competencia'] >= str(start)}
    duplicates = defaultdict(list)
    for r in entries:
        if r['data_vencimento'] < str(start): continue
        if r['tipo'] != 'Despesa': continue
        if r['categoria'] == 'Ajuste' or int(float(r.get('eh_orcamento') or 0)): continue
        value = Decimal(r['valor'])
        if value <= 0:
            warnings.append(f"ID {r['id']}: valor não positivo; revisão necessária.")
            continue
        key=(r['data_vencimento'][:7],r['categoria'],r['subgrupo'])
        # Não substituir compras individuais por orçamento: se já existem despesas,
        # o orçamento vira só o complemento até o teto mensal.
        paid = int(float(r.get('pago') or 0)) == 1
        actual = r.get('valor_pago')
        paid_date = r.get('data_pagamento')
        if paid and (not actual or not paid_date):
            warnings.append(f"ID {r['id']}: confirmação incompleta; não importado.")
            continue
        total = int(float(r.get('total_parcelas') or 1))
        current = int(float(r.get('parcela_atual') or 1))
        description = r['descricao']
        if 1 < total < 999: description += f' ({current}/{total})'
        row=dict(series_key='legacy-series:'+r['compra_id'] if r.get('compra_id') else 'legacy-entry:'+r['id'],source='legacy-entry:'+r['id'],kind='expense',description=description,
                 category=classify(r['categoria'],r['subgrupo'],r['descricao']),
                 expected_cents=cents(value),due_date=r['data_vencimento'],settled=paid,
                 actual_cents=cents(actual) if paid else None,settled_date=paid_date if paid else None)
        rows.append(row)
        duplicates[(description,str(value),r['data_vencimento'])].append(r['id'])
    for ids in duplicates.values():
        if len(ids)>1: warnings.append('Possível duplicidade de despesas: IDs '+', '.join(ids)+'. Confira antes de importar.')
    # Uma previsão por quinzena, descontando o que já está lançado no mês.
    seen=set()
    for budget in budgets:
        month=budget['competencia'][:7]
        key=(month,budget['categoria'],budget['subgrupo'])
        if key not in budget_keys or key in seen: continue
        seen.add(key)
        amount=cents(budget['valor_planejado'])
        recorded=sum(cents(r['valor']) for r in entries if r['tipo']=='Despesa'
                     and (r['data_vencimento'][:7],r['categoria'],r['subgrupo'])==key
                     and Decimal(r['valor'])>0 and not int(float(r.get('eh_orcamento') or 0)))
        remainder=max(0,amount-recorded)
        for day,value in [(1,(remainder+1)//2),(16,remainder//2)]:
            if not value: continue
            rows.append(dict(series_key=f'legacy-budget-series:{budget["categoria"]}:{budget["subgrupo"]}:{day}',source=f'legacy-budget:{month}:{budget["categoria"]}:{budget["subgrupo"]}:{day}',
                kind='expense',description=f'{budget["subgrupo"]} — {1 if day==1 else 2}ª quinzena (previsão restante)',
                category=classify(budget['categoria'],budget['subgrupo'],budget['subgrupo']),
                expected_cents=value,due_date=f'{month}-{day:02d}',settled=False,actual_cents=None,settled_date=None))
    warnings.append('Entradas não importadas: o backup contém possíveis duplicidades de renda. Cadastre as médias manuais por fonte conforme o novo modelo.')
    return rows,warnings


def import_rows(conn, rows):
    inserted=0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute('''INSERT INTO simple_entries(request_id,kind,description,category,expected_cents,due_date,
                          settled,actual_cents,settled_date,series_key) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                          ON CONFLICT(request_id,occurrence) DO UPDATE SET series_key=EXCLUDED.series_key WHERE simple_entries.series_key IS DISTINCT FROM EXCLUDED.series_key''',
                        (str(uuid5(NAMESPACE_URL,r['source'])),r['kind'],r['description'],r['category'],r['expected_cents'],
                         r['due_date'],r['settled'],r['actual_cents'],r['settled_date'],r.get('series_key',r['source'])))
            inserted+=cur.rowcount
    return inserted
