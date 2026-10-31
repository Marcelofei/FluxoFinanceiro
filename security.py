"""Acesso privado compatível com as variáveis já usadas no Railway."""
import base64
import hashlib
import hmac
import json
import os
import re


def validate_schema(value):
    if not isinstance(value, str) or not re.fullmatch(r'(public|tenant_[a-z0-9_]{1,48})', value):
        raise ValueError('Schema inválido.')
    return value


def verify_password(password, encoded):
    try:
        algorithm, rounds, salt, expected = encoded.split('$')
        rounds = int(rounds)
        if algorithm != 'pbkdf2_sha256' or not 100000 <= rounds <= 2000000:
            return False
        digest = hashlib.pbkdf2_hmac('sha256', password.encode(), base64.b64decode(salt), rounds)
        return hmac.compare_digest(digest, base64.b64decode(expected))
    except (ValueError, TypeError, AttributeError):
        return False


def accounts():
    result = json.loads(os.getenv('APP_USERS_JSON') or '{}')
    if not isinstance(result, dict):
        raise ValueError('Configuração de acesso inválida.')
    for user, account in result.items():
        if not user or not isinstance(account, dict):
            raise ValueError('Conta inválida.')
        if validate_schema(account.get('schema')) == 'public':
            raise ValueError('Conta nomeada exige schema próprio.')
        if not str(account.get('password_hash', '')).startswith('pbkdf2_sha256$'):
            raise ValueError('Configure hash de senha.')
    return result


def authenticate(user, password):
    configured = accounts()
    if configured:
        account = configured.get(user, {})
        return account.get('schema') if verify_password(password, account.get('password_hash')) else None
    secret = os.getenv('APP_PASSWORD', '')
    return 'public' if secret and hmac.compare_digest(password.encode(), secret.encode()) else None
