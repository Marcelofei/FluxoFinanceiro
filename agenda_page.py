"""General commitments. Personal and professional events need no receipt."""
import datetime as dt
import uuid
from zoneinfo import ZoneInfo
import streamlit as st
from database import fetch_dataframe,ui_transaction
from agenda import create_event,cancel_event,update_event

def render_agenda(year,month,today):
    st.subheader('Agenda pessoal e profissional')
    start=dt.datetime(year,month,1,tzinfo=ZoneInfo('America/Fortaleza'))
    finish=(start.replace(day=28)+dt.timedelta(days=4)).replace(day=1)
    events=fetch_dataframe('SELECT * FROM agenda_eventos WHERE inicio<%s AND fim>%s AND status=%s ORDER BY inicio,id',(finish,start,'ativo'))
    if events.empty:st.info('Nenhum compromisso neste mês.')
    else:
        st.dataframe(events[['titulo','tipo','local','inicio','fim']],hide_index=True,use_container_width=True)
        selected=st.selectbox('Compromisso para cancelar',[None]+events['id'].tolist(),format_func=lambda x:'Selecione...' if x is None else events.loc[events.id==x,'titulo'].iloc[0],key='agenda_cancel')
        if selected and st.button('Cancelar compromisso'):
            with ui_transaction() as cur:cancel_event(cur,selected)
            st.rerun()
    if not events.empty:
        edit=st.selectbox('Editar compromisso',[None]+events['id'].tolist(),format_func=lambda x:'Selecione...' if x is None else events.loc[events.id==x,'titulo'].iloc[0])
        if edit:
            row=events[events.id==edit].iloc[0]; old=row['inicio'].to_pydatetime()
            with st.form(f'edit_event_{edit}'):
                title=st.text_input('Novo título',value=row['titulo'])
                date=st.date_input('Nova data',value=old.astimezone(ZoneInfo('America/Fortaleza')).date())
                if st.form_submit_button('Atualizar compromisso'):
                    start=dt.datetime.combine(date,old.astimezone(ZoneInfo('America/Fortaleza')).time(),ZoneInfo('America/Fortaleza'))
                    with ui_transaction() as cur:update_event(cur,edit,title,start,start+(row['fim'].to_pydatetime()-old),expected_start=old)
                    st.rerun()
    nonce=st.session_state.setdefault('agenda_nonce',str(uuid.uuid4()))
    if st.button('Novo compromisso'):st.session_state['agenda_nonce']=str(uuid.uuid4());st.rerun()
    with st.form('agenda_general_form'):
        title=st.text_input('Título do compromisso')
        kind=st.selectbox('Tipo do compromisso',['Pessoal','Profissional','Plantão'])
        location=st.text_input('Local do compromisso')
        day=st.date_input('Data do compromisso',value=today)
        a,b=st.columns(2)
        begin=a.time_input('Início',value=dt.time(8))
        end=b.time_input('Fim',value=dt.time(9))
        if st.form_submit_button('Salvar compromisso'):
            with ui_transaction() as cur:
                create_event(cur,title,dt.datetime.combine(day,begin,ZoneInfo('America/Fortaleza')),
                    dt.datetime.combine(day,end,ZoneInfo('America/Fortaleza')),kind,location,event_id=nonce)
            st.rerun()
