import streamlit as st
import os,time,hmac
from security import load_accounts,verify_password

@st.cache_resource
def _login_attempts():
    from threading import Lock
    return {}, Lock()


def check_password():
    try:
        accounts = load_accounts(os.environ.get('APP_USERS_JSON'))
    except ValueError:
        st.error('Configuração de acesso inválida. Contate o administrador.')
        return False
    # Existing private deployments continue to work; named accounts use isolated schemas.
    now = time.time()
    if st.session_state.get('password_correct'):
        account_now = accounts.get(st.session_state.get('actor'), {})
        expired = bool(accounts) and account_now.get('schema') != st.session_state.get('tenant_schema')
        expired |= now - st.session_state.get('last_activity', 0) > 1800
        expired |= now - st.session_state.get('login_time', 0) > 43200
        if not expired:
            st.session_state['last_activity'] = now
            return True
        st.session_state.clear()
        st.warning('Sua sessão expirou. Entre novamente.')
    if not accounts and not os.environ.get('APP_PASSWORD'):
        st.error('Configure o acesso antes de iniciar o app.')
        return False
    st.markdown('### 🔒 Acesso restrito')
    username = st.text_input('Usuário') if accounts else 'owner'
    password = st.text_input('Senha', type='password', key='login_password')
    if st.button('Entrar', type='primary'):
        attempts, lock = _login_attempts()
        with lock:
            recent = [t for t in attempts.get(username, []) if now-t < 300]
            if len(recent) >= 5:
                st.error('Muitas tentativas. Aguarde cinco minutos.')
                return False
            recent.append(now)
            attempts[username] = recent
        account = accounts.get(username, {})
        valid = verify_password(password, account.get('password_hash')) if accounts else hmac.compare_digest(password, os.environ.get('APP_PASSWORD',''))
        if valid:
            with lock: attempts.pop(username, None)
            st.session_state.clear()
            st.session_state.update(password_correct=True, actor=username, tenant_schema=account.get('schema','public'), last_activity=now, login_time=now)
            st.rerun()
        st.error('Usuário ou senha incorretos.')
    return False
