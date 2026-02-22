"""Tests for CredentialRepository (src/database/repos/credentials.py)."""


def test_save_and_get_credentials(tmp_db):
    """Save credentials, retrieve, verify all fields."""
    tmp_db.credentials.save_credentials("my_key", "my_secret", "http://localhost/cb")
    creds = tmp_db.credentials.get_credentials()

    assert creds is not None
    assert creds["api_key"] == "my_key"
    assert creds["api_secret"] == "my_secret"
    assert creds["redirect_uri"] == "http://localhost/cb"


def test_update_credentials(tmp_db):
    """Save once, save again with new values, verify updated."""
    tmp_db.credentials.save_credentials("key_v1", "secret_v1")
    tmp_db.credentials.save_credentials("key_v2", "secret_v2")

    creds = tmp_db.credentials.get_credentials()
    assert creds["api_key"] == "key_v2"
    assert creds["api_secret"] == "secret_v2"


def test_save_and_get_token(tmp_db):
    """Save credentials, save token, verify token in credentials."""
    tmp_db.credentials.save_credentials("key", "secret")
    tmp_db.credentials.save_token("tok_abc123", 9999999999.0)

    creds = tmp_db.credentials.get_credentials()
    assert creds["access_token"] == "tok_abc123"
    assert creds["token_expiry"] == 9999999999.0


def test_create_api_key(tmp_db):
    """Create key, verify key format starts with 'expt_'."""
    result = tmp_db.credentials.create_api_key("Test Key")

    assert result is not None
    assert result["api_key"].startswith("expt_")
    assert result["key_name"] == "Test Key"
    assert "id" in result
    assert "created_at" in result


def test_verify_api_key(tmp_db):
    """Create key, verify it's valid, verify unknown key returns None."""
    created = tmp_db.credentials.create_api_key("Verify Key")
    api_key = created["api_key"]

    verified = tmp_db.credentials.verify_api_key(api_key)
    assert verified is not None
    assert verified["key_name"] == "Verify Key"

    unknown = tmp_db.credentials.verify_api_key("expt_bogus_key_does_not_exist")
    assert unknown is None


def test_revoke_api_key(tmp_db):
    """Create key, revoke it, verify it can no longer be verified."""
    created = tmp_db.credentials.create_api_key("Revoke Key")
    key_id = created["id"]
    api_key = created["api_key"]

    # Should be verifiable before revoke
    assert tmp_db.credentials.verify_api_key(api_key) is not None

    tmp_db.credentials.revoke_api_key(key_id)

    # Should be None after revoke
    assert tmp_db.credentials.verify_api_key(api_key) is None
