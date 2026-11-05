import pytest
from cryptography.fernet import Fernet

from windrow import credentials


@pytest.fixture(autouse=True)
def _credential_key(monkeypatch):
    monkeypatch.setenv("WINDROW_CREDENTIAL_KEY", Fernet.generate_key().decode())


def test_encrypt_decrypt_round_trips():
    record = credentials.encrypt("admin", "sup3r-secret")
    assert credentials.decrypt(record) == "sup3r-secret"


def test_encrypted_record_never_contains_plaintext():
    record = credentials.encrypt("admin", "sup3r-secret")
    assert "sup3r-secret" not in record["password_ciphertext"]


def test_redact_strips_password_and_ciphertext():
    record = credentials.encrypt("admin", "sup3r-secret")
    redacted = credentials.redact(record)
    assert redacted == {"username": "admin", "password": None}
    assert "password_ciphertext" not in redacted


def test_redact_none_is_none():
    assert credentials.redact(None) is None


def test_missing_key_raises_on_encrypt(monkeypatch):
    monkeypatch.delenv("WINDROW_CREDENTIAL_KEY", raising=False)
    with pytest.raises(RuntimeError):
        credentials.encrypt("admin", "pw")


def test_missing_key_raises_on_decrypt(monkeypatch):
    record = credentials.encrypt("admin", "pw")
    monkeypatch.delenv("WINDROW_CREDENTIAL_KEY", raising=False)
    with pytest.raises(RuntimeError):
        credentials.decrypt(record)
