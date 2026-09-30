from operations import request_key


def test_retries_share_identity_but_new_draft_does_not():
    payload=['Despesa','Mercado',100,'2026-09-30']
    assert request_key('draft',payload)==request_key('draft',payload.copy())
    assert request_key('draft',payload)!=request_key('new-draft',payload)
    assert request_key('draft',payload)!=request_key('draft',payload+[2])
