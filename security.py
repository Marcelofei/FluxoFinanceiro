"""Credential and tenant validation; no database access and no plaintext storage."""
import base64
import hashlib
import hmac
import json
import os
import re


def validate_schema(schema):
    if not isinstance(schema, str) or not re.fullmatch(r'(public|tenant_[a-z0-9_]{1,48})', schema):
        raise ValueError('Schema de cliente inválido')
    return schema


def new_password_hash(password, salt=None):
    if len(password) < 12:
        raise ValueError('Use uma senha com pelo menos 12 caracteres')
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000)
    return 'pbkdf2_sha256$600000$'+base64.b64encode(salt).decode()+'$'+base64.b64encode(digest).decode()


def verify_password(password, encoded):
    try:
        algorithm, iterations, salt, expected = encoded.split('$')
        if algorithm != 'pbkdf2_sha256' or not 100000 <= int(iterations) <= 2000000:
            return False
        result = hashlib.pbkdf2_hmac('sha256', password.encode(), base64.b64decode(salt), int(iterations))
        return hmac.compare_digest(result, base64.b64decode(expected))
    except (ValueError, TypeError, AttributeError):
        return False


def load_accounts(raw):
    accounts = json.loads(raw or '{}')
    if not isinstance(accounts, dict): raise ValueError('Configuração de contas inválida')
    for username, account in accounts.items():
        if not username or not isinstance(account, dict): raise ValueError('Conta inválida')
        schema = validate_schema(account.get('schema'))
        if schema == 'public': raise ValueError('Contas nomeadas exigem schema tenant_ dedicado')
        if not str(account.get('password_hash', '')).startswith('pbkdf2_sha256$'):
            raise ValueError('Configure hashes de senha, não senhas em texto')
    return accounts


if __name__ == '__main__':
    import getpass
    print(new_password_hash(getpass.getpass('Nova senha: ')))
