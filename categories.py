"""Classificação por finalidade, independente da forma de pagamento."""
CATEGORIES = ['Casa','Alimentação','Transporte','Saúde','Educação','Pets','Pessoal e lazer',
              'Apoio familiar','Trabalho e tributos','Dívidas e acordos','Reserva e investimentos',
              'Cartão — sem distribuição','A revisar']


def classify(category, subgroup, description):
    text = f'{subgroup} {description}'.casefold()
    if subgroup == 'Auxílios': return 'Apoio familiar'
    if 'acordo' in text or 'empréstimo' in text or 'juros' in text or category == 'Dívidas': return 'Dívidas e acordos'
    if subgroup == 'PJ' or any(x in text for x in ['digitadora','laudite','imposto pj','contador','plantão ']): return 'Trabalho e tributos'
    if subgroup == 'Moradia' or 'tv ' in text: return 'Casa'
    if subgroup == 'Educação' or 'pós ' in text: return 'Educação'
    if subgroup == 'Saúde' or 'farmácia' in text: return 'Saúde'
    if 'pet' in text: return 'Pets'
    if any(x in text for x in ['gasolina','motorista','uber','emplacamento','compass','toro']): return 'Transporte'
    if subgroup == 'Compras' or any(x in text for x in ['supermercado','padaria','frutas']): return 'Alimentação'
    if subgroup in ('Lazer','Assinaturas') or any(x in text for x in ['personal','viagem','ingresso','presente']): return 'Pessoal e lazer'
    return 'A revisar'
