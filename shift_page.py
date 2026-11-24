import streamlit as st
import pandas as pd
import plotly.express as px
import calendar,datetime,uuid,html
from database import fetch_dataframe,ui_transaction
from finance import format_brl,float_seguro,int_seguro
from operations import request_key,insert_shifts

def _inserir_plantoes(records):
    with ui_transaction() as cur:return insert_shifts(cur,records)

def render_shift_page(ano_selecionado,mes_selecionado,hoje,data_contexto_ativo,meses,render_periodo_topo,flash,parse_valor,aplicar_tema_grafico):
    top_back,top_title=st.columns([1.1,4.9],vertical_alignment="center")
    if top_back.button("← Rendas",key="back_rendas_v2",use_container_width=True): st.session_state.pop("rendas_fonte_filtro",None); st.session_state.menu_atual="💰 Rendas"; st.rerun()
    with top_title: st.markdown("<div class='income2-title' style='font-size:1.55rem'>Gestão de plantões</div><div class='income2-sub'>Modo especializado para escala, produção e previsão de pagamento.</div>",unsafe_allow_html=True)
    render_periodo_topo("plantoes")
    tab_cal,tab_add,tab_prod,tab_ger=st.tabs(["📅 Escala","➕ Adicionar","📊 Produção","⚙️ Gerenciar"])
    df_t=fetch_dataframe("SELECT l.* FROM agenda_eventos e JOIN lancamentos l ON l.compromisso_id=e.id WHERE e.tipo='Plantão' AND e.status='ativo'")
    fonte_especial=st.session_state.get('rendas_fonte_filtro')
    if not df_t.empty:
        df_t['d_p']=pd.to_datetime(df_t['data_competencia'].fillna(df_t['data_vencimento']),errors='coerce').dt.date
        if fonte_especial:
            df_t=df_t[df_t['subgrupo'].fillna('').astype(str)==str(fonte_especial)].copy()
            st.caption(f"Fonte ativa: {fonte_especial}")
    with tab_cal:
        c1,c2=st.columns(2); cal_mes=c1.selectbox('Mês',range(1,13),format_func=lambda x:meses[x-1],index=mes_selecionado-1,key='plant_cal_mes'); anos_cal=list(range(hoje.year-2,hoje.year+4)); cal_ano=c2.selectbox('Ano',anos_cal,index=anos_cal.index(ano_selecionado) if ano_selecionado in anos_cal else 2,key='plant_cal_ano')
        dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==cal_mes)&(pd.to_datetime(df_t['d_p']).dt.year==cal_ano)].copy() if not df_t.empty else pd.DataFrame()
        heads=st.columns(7)
        for i,dn in enumerate(['Seg','Ter','Qua','Qui','Sex','Sáb','Dom']): heads[i].markdown(f"<div style='text-align:center;color:var(--text-muted);font-size:.75rem;font-weight:600'>{dn}</div>",unsafe_allow_html=True)
        for week in calendar.monthcalendar(cal_ano,cal_mes):
            cols=st.columns(7)
            for i,day in enumerate(week):
                if day:
                    cd=datetime.date(cal_ano,cal_mes,day); shifts=dm[dm['d_p']==cd] if not dm.empty else pd.DataFrame(); txt='' if shifts.empty else '<br>'.join([f"🏥 {x}" for x in shifts['subgrupo'].fillna('Local').astype(str).tolist()[:3]])
                    cols[i].markdown(f"<div class='ux-card' style='min-height:88px;padding:.45rem'><b>{day}</b><br><span style='font-size:.7rem'>{txt}</span></div>",unsafe_allow_html=True)
        if not dm.empty:
            dias=sorted(dm['d_p'].unique()); dia_det=st.selectbox('Ver detalhes do dia',dias,format_func=lambda d:pd.to_datetime(d).strftime('%d/%m/%Y'))
            for _,r in dm[dm['d_p']==dia_det].iterrows():
                st.markdown(f"<div class='ux-row'>🏥 <b>{r['subgrupo']}</b> · R$ {format_brl(r['valor'])} · recebe {pd.to_datetime(r['data_vencimento']).strftime('%d/%m')}</div>",unsafe_allow_html=True)
    with tab_add:
        modo=st.radio('Modo',['Dia específico','Plantões fixos na semana'],horizontal=True)
        defs_plant=fetch_dataframe("SELECT subgrupo FROM categorias_personalizadas WHERE tipo='Entrada' AND COALESCE(is_producao_variavel,0)=1 AND COALESCE(subgrupo,'')<>'' ORDER BY subgrupo"); locais=sorted(defs_plant['subgrupo'].dropna().astype(str).unique().tolist()) if not defs_plant.empty else []
        if not locais: st.warning('Ative o modo Plantões em uma fonte de renda antes de cadastrar a escala.')
        else:
            loc_default=locais.index(fonte_especial) if fonte_especial in locais else 0; loc=st.selectbox('Local',locais,index=loc_default); defaults={'v':1000.0,'m':1,'d':10}; res=fetch_dataframe("SELECT valor_padrao,atraso_meses,dia_pagamento FROM categorias_personalizadas WHERE subgrupo=%s AND tipo='Entrada' LIMIT 1",(loc,))
            if not res.empty:
                if pd.notna(res.iloc[0]['valor_padrao']): defaults['v']=float(res.iloc[0]['valor_padrao'])
                if pd.notna(res.iloc[0]['atraso_meses']): defaults['m']=int(res.iloc[0]['atraso_meses'])
                if pd.notna(res.iloc[0]['dia_pagamento']): defaults['d']=int(res.iloc[0]['dia_pagamento'])
            a,b=st.columns(2); valor=a.number_input('Valor (R$)',value=defaults['v']); atraso=b.number_input('Recebe quantos meses depois?',0,6,defaults['m']); dia_pg=b.number_input('Dia do pagamento',1,31,defaults['d'])
            turno=st.selectbox('Turno',['Diurno','Noturno','Outro'])
            ini=st.time_input('Início do plantão',value=datetime.time(7) if turno=='Diurno' else datetime.time(19))
            fim=st.time_input('Fim do plantão',value=datetime.time(19) if turno=='Diurno' else datetime.time(7))
            nonce=st.session_state.setdefault('plant_draft',str(uuid.uuid4()))
            if st.button('Novo plantão'): st.session_state['plant_draft']=str(uuid.uuid4()); st.rerun()
            if modo=='Dia específico': data_p=a.date_input('Data do plantão',value=data_contexto_ativo); dias_sem=None; repetir=1
            else: dias_sem=a.multiselect('Dias da semana',range(7),format_func=lambda x:['Seg','Ter','Qua','Qui','Sex','Sáb','Dom'][x]); repetir=a.number_input('Repetir por meses',1,24,6); data_p=None
            if st.button('Registrar plantão',type='primary',use_container_width=True):
                src_def=fetch_dataframe("SELECT categoria FROM categorias_personalizadas WHERE tipo='Entrada' AND COALESCE(is_producao_variavel,0)=1 AND subgrupo=%s LIMIT 1",(loc,)); cat=(str(src_def.iloc[0]['categoria']) if not src_def.empty else 'Rendas'); regs=[]
                datas=[]
                if modo=='Dia específico': datas=[data_p]
                else:
                    for off in range(int(repetir)):
                        ma=(mes_selecionado+off-1)%12+1; aa=ano_selecionado+(mes_selecionado+off-1)//12
                        for d in range(1,calendar.monthrange(aa,ma)[1]+1):
                            dt=datetime.date(aa,ma,d)
                            if dt.weekday() in dias_sem: datas.append(dt)
                for dt in datas:
                    from zoneinfo import ZoneInfo
                    inicio=datetime.datetime.combine(dt,ini,ZoneInfo('America/Fortaleza')); final=datetime.datetime.combine(dt,fim,ZoneInfo('America/Fortaleza'))
                    if final<=inicio: final+=datetime.timedelta(days=1)
                    mf=(dt.month+int(atraso)-1)%12+1; af=dt.year+(dt.month+int(atraso)-1)//12; ds=min(int(dia_pg),calendar.monthrange(af,mf)[1]); venc=datetime.date(af,mf,ds)
                    regs.append(('Entrada',cat,loc,f"Plantão {loc} · {turno} ({dt.strftime('%d/%m/%Y')})",valor,venc,1,1,0,request_key(nonce,[loc,dt,turno,ini,fim]),'Outros','Baixa 🟢',0.0,dt,inicio,final))
                if regs: _inserir_plantoes(regs); flash('success','Escala processada. Plantões já existentes foram preservados.'); st.rerun()
    with tab_prod:
        dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==mes_selecionado)&(pd.to_datetime(df_t['d_p']).dt.year==ano_selecionado)].copy() if not df_t.empty else pd.DataFrame()
        if dm.empty: st.info('Sem plantões no período ativo.')
        else:
            dm['valor']=pd.to_numeric(dm['valor'],errors='coerce').fillna(0); x1,x2,x3=st.columns(3); x1.metric('Plantões',len(dm)); x2.metric('Produção prevista',f"R$ {format_brl(dm['valor'].sum())}"); x3.metric('Média por plantão',f"R$ {format_brl(dm['valor'].mean())}")
            by=dm.groupby('subgrupo')['valor'].agg(['count','sum']).sort_values('sum').reset_index(); fig=px.bar(by,x='sum',y='subgrupo',orientation='h',labels={'sum':'R$','subgrupo':''}); st.plotly_chart(aplicar_tema_grafico(fig),use_container_width=True,key="finance_chart_7")
    with tab_ger:
        with st.expander('📥 Importar CSV'):
            st.caption("Colunas: data (DD/MM/AAAA), local e valor opcional. Importar novamente não duplica a mesma descrição de plantão.")
            arq=st.file_uploader('CSV de plantões',type='csv',key='plant_csv')
            if arq is not None:
                try:
                    imp=pd.read_csv(arq); imp.columns=[c.strip().lower() for c in imp.columns]; cd=next((c for c in imp if c in ('data','data_plantao','date')),None); cl=next((c for c in imp if c in ('local','hospital','subgrupo')),None); cv=next((c for c in imp if c in ('valor','value')),None)
                    if not cd or not cl: st.error("O CSV precisa de 'data' e 'local'.")
                    else:
                        defs=fetch_dataframe("SELECT categoria,subgrupo,valor_padrao,atraso_meses,dia_pagamento FROM categorias_personalizadas WHERE tipo='Entrada' AND COALESCE(is_producao_variavel,0)=1"); exist=set(); novos=[]; problemas=[]
                        for _,r in imp.iterrows():
                            try: dt=pd.to_datetime(str(r[cd]).strip(),format='%d/%m/%Y').date()
                            except: problemas.append(str(r[cd])); continue
                            loc=str(r[cl]).strip(); inf=defs[defs['subgrupo'].fillna('').str.strip().str.lower()==loc.lower()]
                            if inf.empty: problemas.append(f'{loc} · local não cadastrado'); continue
                            inf=inf.iloc[0]; desc=f"Plantão {inf['subgrupo']} ({dt.strftime('%d/%m/%Y')})"
                            desc += ' · '+str(r.get('turno',''))+' '+str(r.get('inicio',''))
                            val=parse_valor(r[cv]) if cv and pd.notna(r.get(cv)) else float_seguro(inf.get('valor_padrao'))
                            if val<=0: problemas.append(f'{desc} · sem valor'); continue
                            am=int(inf['atraso_meses']) if pd.notna(inf['atraso_meses']) else 1; dp=int(inf['dia_pagamento'] or 10); mf=(dt.month+am-1)%12+1; af=dt.year+(dt.month+am-1)//12; venc=datetime.date(af,mf,min(dp,calendar.monthrange(af,mf)[1])); novos.append(('Entrada',inf['categoria'],inf['subgrupo'],desc,val,venc,1,1,0,(request_key('csv-id',[str(r['id'])]) if pd.notna(r.get('id')) else request_key('csv-event',[desc,str(_)])),'Outros','Baixa 🟢',0.0,dt)); exist.add(desc)
                        st.write(f"Novos: **{len(novos)}** · Problemas: **{len(problemas)}**")
                        if problemas: st.caption('Problemas: '+', '.join(problemas[:10]))
                        if novos and st.button('Confirmar importação',type='primary'): _inserir_plantoes(novos); flash('success','Importação concluída. Plantões já existentes foram preservados.'); st.rerun()
                except Exception as e: st.error(f'Erro no CSV: {e}')
        if df_t.empty: st.info('Sem plantões para gerenciar.')
        else:
            dm=df_t[(pd.to_datetime(df_t['d_p']).dt.month==mes_selecionado)&(pd.to_datetime(df_t['d_p']).dt.year==ano_selecionado)].copy()
            if dm.empty: st.info('Sem plantões no período ativo.')
            else:
                locais_ger=sorted(dm['subgrupo'].dropna().astype(str).unique().tolist())
                filtro_locais=st.multiselect('Filtrar por hospital/local',locais_ger,placeholder='Todos os locais',key='plant_ger_filtro')
                if filtro_locais: dm=dm[dm['subgrupo'].astype(str).isin(filtro_locais)].copy()
                dm=dm.sort_values('d_p').reset_index(drop=True); dm['Apagar']=False; dm['Data']=pd.to_datetime(dm['d_p']).dt.strftime('%d/%m/%Y'); ed=st.data_editor(dm[['id','Apagar','Data','subgrupo','valor']],hide_index=True,use_container_width=True,column_config={'id':st.column_config.NumberColumn(disabled=True),'valor':st.column_config.NumberColumn('Valor',format='R$ %.2f')})
                st.caption('Alterações de valor atualizam automaticamente a previsão de renda. Plantões já recebidos preservam seus valores.')
                if st.button('Salvar valores dos plantões',key='plant_save_values'):
                    from operations import update_shift_values
                    with ui_transaction() as cur:
                        changed=update_shift_values(cur,[(int(r['id']),r['valor']) for _,r in ed.iterrows()])
                    flash('success',f'{changed} plantão(ões) atualizado(s). A previsão de renda acompanha os novos valores.')
                    st.rerun()
                conf=st.checkbox('Confirmo a exclusão dos itens marcados',key='conf_del_plant')
                pb1,pb2=st.columns(2)
                if pb1.button('Excluir selecionados',disabled=not conf,use_container_width=True):
                    ids=[int(r['id']) for _,r in ed.iterrows() if r['Apagar']]
                    if ids:
                        with ui_transaction() as cur:
                            from operations import delete_entries
                            delete_entries(cur,ids)
                        flash('success',f'{len(ids)} plantão(ões) excluído(s).'); st.rerun()
                conf_todos=pb2.checkbox('Confirmar apagar tudo listado',key='plant_del_all_conf')
                if pb2.button('🚨 Apagar TUDO listado',disabled=not conf_todos,use_container_width=True):
                    ids_todos=dm['id'].astype(int).tolist()
                    if ids_todos:
                        with ui_transaction() as cur:
                            from operations import delete_entries
                            delete_entries(cur,ids_todos)
                        flash('success',f'{len(ids_todos)} plantão(ões) listado(s) apagado(s).'); st.rerun()
