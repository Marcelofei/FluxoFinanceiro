import csv,io,zipfile
from datetime import date
from legacy_import import prepare_backup,import_rows
import storage


def sample():
    out=io.BytesIO()
    with zipfile.ZipFile(out,'w') as z:
        for name,rows in [('lancamentos.csv',[dict(id='1',tipo='Despesa',categoria='Despesas Recorrentes',subgrupo='Supermercado',descricao='Mercado',valor='100',data_vencimento='2026-11-03',pago='0',eh_orcamento='0',total_parcelas='1',parcela_atual='1')]),('orcamentos_categorias.csv',[dict(competencia='2026-11-01',categoria='Despesas Recorrentes',subgrupo='Supermercado',valor_planejado='501')])]:
            s=io.StringIO();w=csv.DictWriter(s,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows);z.writestr(name,s.getvalue())
    return out.getvalue()


def test_budget_only_complements_existing_expenses():
    rows,_=prepare_backup(sample(),date(2026,11,1))
    assert len(rows)==3
    assert sum(r['expected_cents'] for r in rows)==50100
    assert [r['expected_cents'] for r in rows[1:]]==[20050,20050]
    assert all(r['category']=='Alimentação' for r in rows)


def test_reimport_does_not_duplicate(database):
    rows,_=prepare_backup(sample(),date(2026,11,1))
    with storage.connection(database) as conn:
        assert import_rows(conn,rows)==3
        assert import_rows(conn,rows)==0
        assert len(storage.list_entries(conn,2026,11))==3
