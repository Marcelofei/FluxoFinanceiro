import json
import pytest
from security import *


def test_hash_verification():
    hashed=new_password_hash('example-password-123')
    assert verify_password('example-password-123',hashed)
    assert not verify_password('wrong',hashed)
    assert not verify_password('x','malformed')


@pytest.mark.parametrize('name',['other','tenant_;DROP TABLE x','public,tenant_a','pg_catalog','tenant_A'])
def test_reject_schema_injection(name):
    with pytest.raises(ValueError): validate_schema(name)


def test_named_accounts_cannot_share_legacy_public():
    with pytest.raises(ValueError): load_accounts(json.dumps({'a':{'schema':'public','password_hash':'pbkdf2_sha256$x'}}))
