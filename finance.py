"""Financial projections: pure functions, amounts quantized at the boundary."""
import datetime
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo
import pandas as pd

prioridades_map = {"Alta 🔴": 0, "Média 🟡": 1, "Baixa 🟢": 2}

def today_local():
    return datetime.datetime.now(ZoneInfo("America/Sao_Paulo")).date()

def cents(value):
    amount = Decimal(str(value))
    if not amount.is_finite():
        raise ValueError("Valor monetário inválido")
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def money(value):
    return Decimal(cents(value)) / 100

def split_total(total, count):
    if count < 1 or cents(total) < 0:
        raise ValueError("Parcelamento inválido")
    base, remainder = divmod(cents(total), count)
    return [Decimal(base + (i < remainder)) / 100 for i in range(count)]

def allocate_amount(total, weights):
    """Largest remainder allocation; never negative, exact sum in cents."""
    units = cents(total)
    if units < 0 or not weights:
        raise ValueError("Rateio inválido")
    w = [max(cents(v), 0) for v in weights]
    if not sum(w): w = [1] * len(w)
    denominator = sum(w)
    result = [units * x // denominator for x in w]
    order = sorted(range(len(w)), key=lambda i: (-(units*w[i] % denominator), i))
    for i in order[:units-sum(result)]: result[i] += 1
    return [Decimal(x) / 100 for x in result]

def int_seguro(valor, padrao=0):
    """Converte números vindos de pandas/SQL sem quebrar com NaN/None."""
    try:
        if valor is None or pd.isna(valor):
            return int(padrao)
        return int(valor)
    except (TypeError, ValueError, OverflowError):
        return int(padrao)

def float_seguro(valor, padrao=0.0):
    """Converte números financeiros sem propagar NaN para cálculos/UI."""
    try:
        if valor is None or pd.isna(valor):
            return float(padrao)
        return float(valor)
    except (TypeError, ValueError, OverflowError):
        return float(padrao)

def format_brl(valor):
    if pd.isna(valor): return "0,00"
    return f"{float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def _descricao_exibicao(r):
    if pd.notna(r.get('total_parcelas')) and float_seguro(r.get('total_parcelas')) > 1 and int_seguro(r.get('total_parcelas')) != 999:
        return f"{r['descricao']} ({int_seguro(r.get('parcela_atual'), 1)}/{int_seguro(r.get('total_parcelas'), 1)})"
    return str(r.get('descricao') or '')

def _consolidar_operacional(df, consolidar_cartao=False, hoje=None):
    hoje = hoje or today_local()
    """Prepara os lançamentos exibidos no uso diário.

    Na UX 2.0 a Home preserva apenas lançamentos reais e não cria faturas
    sintéticas. No Fluxo, porém, despesas explicitamente marcadas como Crédito
    podem ser consolidadas em uma única fatura operacional, porque é essa saída
    que o usuário efetivamente paga. As compras individuais continuam no banco
    para alimentar categorias, planejamento e histórico.
    """
    cols_saida = ['id_ui','tipo','categoria','descricao','valor','valor_pago','pago','data_vencimento','data_pagamento','prioridade','ids','consolidado','ordem_pri','atrasado','ordem_atraso']
    if df.empty: return pd.DataFrame(columns=cols_saida)
    base = df.copy()
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(0.0)
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(0.0)
    linhas = []
    if consolidar_cartao and 'forma_pagamento' in base.columns:
        mask_cred = (base['tipo'] == 'Despesa') & (base['forma_pagamento'] == 'Crédito')
    else:
        # UX diária: preserve cada despesa real exatamente como foi cadastrada.
        mask_cred = pd.Series(False, index=base.index)

    if mask_cred.any():
        credito = base[mask_cred].copy()
        credito['_fatura'] = credito.get('fatura_id', pd.Series(index=credito.index, dtype=object)).fillna('legado').astype(str)
        credito['_pagamento'] = pd.to_datetime(credito['data_pagamento'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('pendente')
        for chave, grp in credito.groupby(['_fatura', 'data_vencimento', 'pago', '_pagamento']):
            mes_fatura = '_'.join(str(x) for x in chave)
            nome_cartao = str(grp.iloc[0].get('cartao_nome') or 'Cartão não identificado')
            if nome_cartao == 'nan': nome_cartao = 'Cartão não identificado'
            all_paid = bool((grp['pago'] == 1).all())
            datas_pg = pd.to_datetime(grp['data_pagamento'], errors='coerce').dropna() if 'data_pagamento' in grp.columns else pd.Series(dtype='datetime64[ns]')
            data_pg = datas_pg.max().date() if all_paid and not datas_pg.empty else None
            linhas.append({
                'id_ui':f'cartao_{mes_fatura}', 'tipo':'Despesa', 'categoria':'Cartão de Crédito', 'descricao':f'💳 Fatura · {nome_cartao}',
                'valor':float(grp['valor'].sum()), 'valor_pago':float(grp['valor_pago'].sum()), 'pago':1 if all_paid else 0,
                'data_vencimento':pd.to_datetime(grp['data_vencimento']).min().date(), 'data_pagamento':data_pg, 'prioridade':'Alta 🔴',
                'ids':grp['id'].astype(int).tolist(), 'consolidado':True,
            })
    restante = base[~mask_cred].copy()
    mask_plant = (restante['tipo'] == 'Entrada') & restante['descricao'].str.match(r'(?i)^plant[ãa]o\s', na=False)
    plant = restante[mask_plant].copy()
    if not plant.empty:
        def _grupo_hospital(r):
            cat = str(r.get('categoria') or '').strip()
            sub = str(r.get('subgrupo') or '').strip()
            cat_norm = cat.lower().replace('õ','o').replace('ã','a')
            return sub if cat_norm in ('plantoes','plantao') and sub else cat
        plant['_grupo_hospital'] = plant.apply(_grupo_hospital, axis=1)
        plant['_pagamento'] = pd.to_datetime(plant['data_pagamento'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('pendente')
        for (hospital, dt, status, pagamento), grp in plant.groupby(['_grupo_hospital','data_vencimento','pago','_pagamento']):
            all_paid = bool((grp['pago'] == 1).all())
            datas_pg = pd.to_datetime(grp['data_pagamento'], errors='coerce').dropna() if 'data_pagamento' in grp.columns else pd.Series(dtype='datetime64[ns]')
            data_pg = datas_pg.max().date() if all_paid and not datas_pg.empty else None
            linhas.append({
                'id_ui':f"plant_{hospital}_{dt}_{status}_{pagamento}", 'tipo':'Entrada', 'categoria':hospital, 'descricao':f"🏥 {hospital}",
                'valor':float(grp['valor'].sum()), 'valor_pago':float(grp['valor_pago'].sum()), 'pago':1 if all_paid else 0,
                'data_vencimento':pd.to_datetime(dt).date(), 'data_pagamento':data_pg, 'prioridade':'Baixa 🟢',
                'ids':grp['id'].astype(int).tolist(), 'consolidado':True,
            })
    for _, r in restante[~mask_plant].iterrows():
        data_pg = pd.to_datetime(r.get('data_pagamento'), errors='coerce')
        linhas.append({
            'id_ui':str(r['id']), 'tipo':r['tipo'], 'categoria':r['categoria'], 'descricao':_descricao_exibicao(r),
            'valor':float(r['valor']), 'valor_pago':float_seguro(r.get('valor_pago')), 'pago':int_seguro(r.get('pago')),
            'data_vencimento':pd.to_datetime(r['data_vencimento']).date(), 'data_pagamento':data_pg.date() if pd.notna(data_pg) else None, 'prioridade':r['prioridade'],
            'ids':[int(r['id'])], 'consolidado':False,
        })
    out = pd.DataFrame(linhas) if linhas else pd.DataFrame(columns=cols_saida)
    if not out.empty:
        out['ordem_pri'] = out['prioridade'].map(prioridades_map).fillna(2)
        out['atrasado'] = (out['pago'] == 0) & (out['data_vencimento'] < hoje)
        out['ordem_atraso'] = (~out['atrasado']).astype(int)
        out = out.sort_values(['ordem_atraso','data_vencimento','ordem_pri']).reset_index(drop=True)
    if not out.empty:
        excluded = {int(r['id']): money(r['valor_pago']) for _,r in base.iterrows()
                    if r['tipo']=='Entrada' and int_seguro(r.get('pago'))==1 and r.get('desconsiderar_cobertura',False)}
        out['valor_desconsiderado'] = out['ids'].map(lambda ids: float(sum((excluded.get(int(i),Decimal('0')) for i in ids),Decimal('0'))))
    return out

def _valor_operacional(r):
    planejado = max(float_seguro(r.get('valor')), 0.0)
    realizado = max(float_seguro(r.get('valor_pago')), 0.0)
    return realizado if int_seguro(r.get('pago')) == 1 else planejado

def _data_operacional(r):
    if int_seguro(r.get('pago')) == 1:
        dp = pd.to_datetime(r.get('data_pagamento'), errors='coerce')
        if pd.notna(dp):
            return dp.date()
    dv = pd.to_datetime(r.get('data_vencimento'), errors='coerce')
    return dv.date() if pd.notna(dv) else today_local()

def _montar_plano_pagamentos(df_ops, ano, mes, hoje=None):
    hoje = hoje or today_local()
    """Cria uma agenda de caixa sem assumir saldo bancário externo ao app.

    As fontes recebidas são consumidas primeiro pelos pagamentos já realizados.
    Depois, o que sobra nelas e as entradas ainda previstas são alocados às contas
    pendentes em ordem de vencimento. Se a cobertura só aparece depois do vencimento,
    a conta recebe um alerta de risco.
    """
    resultado = {
        'fontes': [], 'contas': [], 'risco_contas': [], 'reserva_minima': Decimal("0.00"),
        'reserva_sugerida': Decimal("0.00"), 'recebido_nao_alocado': Decimal("0.00"),
        'recebido_total': Decimal("0.00"), 'previsto_total': Decimal("0.00"), 'uso_externo_historico': Decimal("0.00"),
    }
    if df_ops is None or df_ops.empty:
        return _numbers(resultado)

    base = df_ops.copy()
    base['valor'] = pd.to_numeric(base['valor'], errors='coerce').fillna(Decimal("0.00"))
    base['valor_pago'] = pd.to_numeric(base['valor_pago'], errors='coerce').fillna(Decimal("0.00"))

    fontes = []
    for _, r in base[base['tipo'] == 'Entrada'].iterrows():
        valor = money(_valor_operacional(r))
        if valor <= 0:
            continue
        fonte = {
            'id': str(r.get('id_ui')),
            'descricao': str(r.get('descricao') or r.get('categoria') or 'Entrada'),
            'categoria': str(r.get('categoria') or ''),
            'data': _data_operacional(r),
            'valor': round(valor, 2),
            'restante': round(valor, 2),
            'recebido': int_seguro(r.get('pago')) == 1,
            'atrasada': int_seguro(r.get('pago')) == 0 and _data_operacional(r) < hoje,
            'desconsiderado': min(valor,money(r.get('valor_desconsiderado',0))) if int_seguro(r.get('pago'))==1 else Decimal('0'),
            'compromissos': [],
        }
        fontes.append(fonte)
    fontes.sort(key=lambda x: (0 if x['recebido'] else 1, x['data'], x['descricao']))

    resultado['recebido_total'] = round(sum(f['valor'] for f in fontes if f['recebido']), 2)
    resultado['previsto_total'] = round(sum(f['valor'] for f in fontes if not f['recebido']), 2)

    despesas = []
    for _, r in base[base['tipo'] == 'Despesa'].iterrows():
        valor = money(_valor_operacional(r))
        if valor <= 0:
            continue
        despesas.append({
            'id': str(r.get('id_ui')),
            'descricao': str(r.get('descricao') or r.get('categoria') or 'Despesa'),
            'categoria': str(r.get('categoria') or ''),
            'data': _data_operacional(r),
            'vencimento': pd.to_datetime(r.get('data_vencimento'), errors='coerce').date(),
            'valor': round(valor, 2),
            'pago': int_seguro(r.get('pago')) == 1,
            'prioridade': str(r.get('prioridade') or ''),
            'alocacoes': [],
            'risco_valor': Decimal("0.00"),
            'descoberto': Decimal("0.00"),
        })

    pagos = sorted([d for d in despesas if d['pago']], key=lambda x: (x['data'], x['descricao']))
    pendentes = sorted([d for d in despesas if not d['pago']], key=lambda x: (x['vencimento'], prioridades_map.get(x['prioridade'], 2), x['descricao']))

    def alocar(conta, valor_restante, predicado, tipo_alocacao):
        for fonte in fontes:
            if valor_restante <= Decimal("0.004"):
                break
            if fonte['restante'] <= Decimal("0.004") or not predicado(fonte):
                continue
            uso = round(min(fonte['restante'], valor_restante), 2)
            if uso <= 0:
                continue
            fonte['restante'] = round(fonte['restante'] - uso, 2)
            valor_restante = round(valor_restante - uso, 2)
            conta['alocacoes'].append({'fonte_id': fonte['id'], 'fonte': fonte['descricao'], 'data': fonte['data'], 'valor': uso, 'tipo': tipo_alocacao, 'recebido': fonte['recebido']})
            fonte['compromissos'].append({'conta_id': conta['id'], 'descricao': conta['descricao'], 'vencimento': conta['vencimento'], 'valor': uso, 'pago': conta['pago']})
        return max(round(valor_restante, 2), Decimal("0.00"))

    # O que já foi pago consome apenas entradas que realmente já foram recebidas
    # até aquela data. Diferenças representam recursos trazidos de fora do mês/app.
    for conta in pagos:
        faltante = conta['valor']
        faltante = alocar(conta, faltante, lambda f, dt=conta['data']: f['recebido'] and f['data'] <= dt, 'historico')
        if faltante > Decimal("0.004"):
            resultado['uso_externo_historico'] += faltante
            conta['descoberto'] = faltante

    # Keep historical allocations; only the pending plan loses unavailable receipts.
    for fonte in fontes:
        if fonte['recebido']:
            fonte['restante'] = min(fonte['restante'],max(fonte['valor']-fonte['desconsiderado'],Decimal('0')))
    saldos_replanejados = {f['id']:f['restante'] for f in fontes if f['recebido']}

    # Contas futuras usam primeiro recursos que chegam até o vencimento; só depois
    # recorrem a entradas posteriores, que indicam risco de atraso sem reserva.
    for conta in pendentes:
        faltante = conta['valor']
        faltante = alocar(conta, faltante, lambda f, dt=conta['vencimento']: not f['atrasada'] and f['data'] <= max(dt, hoje), 'no_prazo')
        risco = faltante
        if faltante > Decimal("0.004"):
            faltante = alocar(conta, faltante, lambda f, dt=conta['vencimento']: not f['atrasada'] and f['data'] > max(dt, hoje), 'apos_vencimento')
        if faltante > Decimal('0.004'):
            faltante = alocar(conta, faltante, lambda f: f['atrasada'], 'renda_atrasada')
        conta['risco_valor'] = round(risco, 2)
        conta['descoberto'] = round(faltante, 2)
        if conta['risco_valor'] > Decimal("0.004"):
            resultado['risco_contas'].append(conta)

    # Reserva de virada: pior déficit acumulado do mês, partindo de zero.
    eventos = []
    for _, r in base.iterrows():
        if r['tipo'] == 'Entrada' and int_seguro(r.get('pago')) == 0 and _data_operacional(r) < hoje:
            continue
        valor = money(_valor_operacional(r))
        if valor <= 0:
            continue
        data_ev = _data_operacional(r)
        sinal = 1 if r['tipo'] == 'Entrada' else -1
        eventos.append((data_ev, 0 if sinal > 0 else 1, sinal * valor))
    if any(f['desconsiderado'] > 0 for f in fontes):
        eventos = [(hoje,0,v) for v in saldos_replanejados.values()]
        eventos += [(max(f['data'],hoje),0,f['valor']) for f in fontes if not f['recebido'] and not f['atrasada']]
        eventos += [(max(c['vencimento'],hoje),1,-c['valor']) for c in pendentes]
    eventos.sort(key=lambda x: (x[0], x[1]))
    acumulado = Decimal("0.00")
    minimo = Decimal("0.00")
    for _, _, valor in eventos:
        acumulado += valor
        minimo = min(minimo, acumulado)
    reserva = max(-minimo, Decimal("0.00"))
    resultado['reserva_minima'] = round(reserva, 2)
    resultado['reserva_sugerida'] = round(reserva * Decimal("1.10"), 2) if reserva > 0 else Decimal("0.00")

    resultado['recebido_nao_alocado'] = round(sum(max(f['restante'], Decimal("0.00")) for f in fontes if f['recebido']), 2)
    resultado['fontes'] = fontes
    resultado['contas'] = pagos + pendentes
    return _numbers(resultado)

def _fluxo2_texto_cobertura(conta):
    """Texto curto de casamento para uma conta pendente."""
    if not conta:
        return "", ""
    if float_seguro(conta.get('descoberto')) > 0.004:
        return "danger", f"Sem renda suficiente · faltam R$ {format_brl(conta['descoberto'])}"

    atrasadas = [a for a in conta.get('alocacoes', []) if a.get('tipo') == 'renda_atrasada']
    if atrasadas:
        return 'danger', 'Depende de renda atrasada: ' + ', '.join(dict.fromkeys(a['fonte'] for a in atrasadas))

    tardias = [a for a in conta.get('alocacoes', []) if a.get('tipo') == 'apos_vencimento']
    if tardias:
        a = min(tardias, key=lambda x: x['data'])
        dias = max((a['data'] - conta['vencimento']).days, 1)
        plural = "dia" if dias == 1 else "dias"
        return "warn", f"{a['fonte']} entra em {a['data'].strftime('%d/%m')} · {dias} {plural} depois"

    no_prazo = [a for a in conta.get('alocacoes', []) if a.get('tipo') in ('no_prazo', 'historico')]
    if no_prazo:
        nomes = []
        for a in no_prazo:
            nome = str(a['fonte'])
            if nome not in nomes:
                nomes.append(nome)
        if len(nomes) == 1:
            data_fonte = max(a['data'] for a in no_prazo if str(a['fonte']) == nomes[0])
            confirmado = all(a.get('recebido') for a in no_prazo)
            return ('ok' if confirmado else 'warn'), f"{'Coberto com recebimento confirmado' if confirmado else 'Cobertura prevista'} · {nomes[0]} · {data_fonte.strftime('%d/%m')}"
        confirmado = all(a.get('recebido') for a in no_prazo)
        return ('ok' if confirmado else 'warn'), ('Recebimentos confirmados: ' if confirmado else 'Cobertura prevista: ') + " + ".join(nomes[:2]) + (" + …" if len(nomes) > 2 else "")
    return "danger", "Sem fonte de renda associada"

def _fluxo2_resumo_proxima_renda(plano, ano, mes, hoje=None):
    hoje = hoje or today_local()
    fontes = plano.get('fontes', [])
    pendentes = [c for c in plano.get('contas', []) if not c.get('pago')]
    referencia = hoje if (ano == hoje.year and mes == hoje.month) else datetime.date(ano, mes, 1)
    candidatas = [f for f in fontes if (not f.get('recebido')) and f.get('data') >= referencia]
    if not candidatas:
        return None
    fonte = min(candidatas, key=lambda f: (f['data'], f['descricao']))
    contas_ate = [c for c in pendentes if c['vencimento'] <= fonte['data']]
    total = round(sum(float_seguro(c['valor']) for c in contas_ate), 2)
    risco = round(sum(float_seguro(c.get('risco_valor')) for c in contas_ate), 2)
    return {'fonte': fonte, 'contas': contas_ate, 'total': total, 'risco': risco, 'prevista': any(not a.get('recebido') for c in contas_ate for a in c.get('alocacoes', []))}


def _numbers(value):
    if isinstance(value, Decimal): return float(value)
    if isinstance(value, dict): return {k: _numbers(v) for k,v in value.items()}
    if isinstance(value, list): return [_numbers(v) for v in value]
    return value


def invoice_due(purchase_date, closing_day, due_day):
    """Purchase on closing day belongs to next cycle; user can override due date."""
    import calendar
    month = purchase_date.replace(day=1)
    close = month.replace(day=min(closing_day, calendar.monthrange(month.year, month.month)[1]))
    if purchase_date >= close:
        month = (month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    if due_day <= closing_day:
        month = (month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return month.replace(day=min(due_day, calendar.monthrange(month.year, month.month)[1]))
