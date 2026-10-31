import base64
import hashlib
import json
import pytest
from security import authenticate, accounts, validate_schema, verify_password


def test_private_login_requires_configured_password(monkeypatch):
    monkeypatch.delenv('APP_USERS_JSON',raising=False)
    monkeypatch.delenv('APP_PASSWORD',raising=False)
    assert authenticate('owner','') is None
    monkeypatch.setenv('APP_PASSWORD','teste-seguro')
    assert authenticate('owner','wrong') is None
    assert authenticate('owner','teste-seguro') == 'public'


def test_named_account_cannot_fall_back_to_shared_password(monkeypatch):
    salt = b'test-salt'
    digest = hashlib.pbkdf2_hmac('sha256',b'correct',salt,100000)
    encoded = 'pbkdf2_sha256$100000$'+base64.b64encode(salt).decode()+'$'+base64.b64encode(digest).decode()
    monkeypatch.setenv('APP_USERS_JSON',json.dumps({'marcelo':{'schema':'tenant_marcelo','password_hash':encoded}}))
    monkeypatch.setenv('APP_PASSWORD','shared')
    assert authenticate('marcelo','correct')=='tenant_marcelo'
    assert authenticate('marcelo','shared') is None
    assert authenticate('unknown','correct') is None
    assert not verify_password('correct','bad-hash')
    monkeypatch.setenv('APP_USERS_JSON',json.dumps({'x':{'schema':'public','password_hash':encoded}}))
    with pytest.raises(ValueError): accounts()


@pytest.mark.parametrize('schema',['public;drop table x','tenant_a"','other',''])
def test_invalid_schema(schema):
    with pytest.raises(ValueError): validate_schema(schema)
