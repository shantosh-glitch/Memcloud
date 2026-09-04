from memnode import security


def test_auth_roundtrip():
    secret = "test-secret"
    node_id = "abc123"
    nonce = security.new_nonce()
    auth = security.compute_auth(secret, node_id, nonce)
    assert security.verify_auth(secret, node_id, nonce, auth)


def test_auth_rejects_wrong_secret():
    node_id = "abc123"
    nonce = security.new_nonce()
    auth = security.compute_auth("right-secret", node_id, nonce)
    assert not security.verify_auth("wrong-secret", node_id, nonce, auth)


def test_auth_rejects_tampered_node_id():
    secret = "test-secret"
    nonce = security.new_nonce()
    auth = security.compute_auth(secret, "node-a", nonce)
    assert not security.verify_auth(secret, "node-b", nonce, auth)


def test_auth_rejects_replayed_auth_with_different_nonce():
    secret = "test-secret"
    node_id = "abc123"
    auth = security.compute_auth(secret, node_id, security.new_nonce())
    # same auth value, but checked against a fresh nonce -- must fail
    assert not security.verify_auth(secret, node_id, security.new_nonce(), auth)


def test_nonces_are_unique():
    assert security.new_nonce() != security.new_nonce()
