"""
Device-credential encryption at rest. Plaintext JSON is fine for a webhook
HMAC secret (see baler/webhooks.py) - it's a shared signing key sent
over HTTPS to an operator-chosen URL. It's the wrong bar for a real device
login a human types in, so this stores the password with Fernet (AES-128 +
HMAC, the standard boring choice for "encrypt a small blob with one
symmetric key") rather than plaintext. Not a vault/KMS integration - that's
over-engineering for this project's single-trusted-operator, localhost
deployment; a key in an env var matches how every other secret here
(POSTGRES_PASSWORD, etc.) is already handled.
"""


from cryptography.fernet import Fernet

from windrow import legacy


def _fernet() -> Fernet:
    key = legacy.env("WINDROW_CREDENTIAL_KEY")  # or pre-rename FBF_CREDENTIAL_KEY
    if not key:
        raise RuntimeError(
            "WINDROW_CREDENTIAL_KEY is not set - required to store or read a device credential "
            "(generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\")"
        )
    return Fernet(key.encode())


def encrypt(username: str, password: str) -> dict:
    """Returns the record stored on disk - password_ciphertext, never the
    plaintext. Called only from a credential-setting API handler."""
    return {"username": username, "password_ciphertext": _fernet().encrypt(password.encode()).decode()}


def decrypt(credential: dict) -> str:
    return _fernet().decrypt(credential["password_ciphertext"].encode()).decode()


def redact(credential: dict | None) -> dict | None:
    """What every list/get API response shows instead of the real record -
    password_ciphertext never leaves this process. Reused identically by
    every handler that serializes a device or connection record."""
    if credential is None:
        return None
    return {"username": credential["username"], "password": None}
