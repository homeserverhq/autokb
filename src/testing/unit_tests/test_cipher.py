"""PasswordCipher round-trip / legacy-compat / fail-closed checks (R4/R5).

Runnable directly: ``ENCRYPTION_KEY=... python /src/testing/unit_tests/test_cipher.py``.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.misc_utils import DecryptionError, PasswordCipher


def test_roundtrip_and_prefix():
    c = PasswordCipher()
    tok = c.encrypt("s3cret")
    assert tok.startswith("encv1:")
    assert c.decrypt(tok) == "s3cret"
    assert c.decrypt(c.encrypt("")) == ""


def test_legacy_compat():
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    import base64

    key_str = os.environ["ENCRYPTION_KEY"]
    c = PasswordCipher()  # derived from key_str (600k, resolved salt)
    # Simulate a token written by the OLD derivation: same key, 100k
    # iterations, public default salt.
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(), length=32,
        salt=b"autokb-fernet-salt-v1", iterations=100_000,
    )
    legacy_key = base64.urlsafe_b64encode(kdf.derive(key_str.encode()))
    old_token = Fernet(legacy_key).encrypt(b"legacy-secret").decode()
    assert c.decrypt(old_token) == "legacy-secret"


def test_fail_closed():
    c = PasswordCipher()
    tok = c.encrypt("s3cret")
    wrong = PasswordCipher(key="someotherkey")
    try:
        wrong.decrypt(tok)
    except DecryptionError:
        return
    raise AssertionError("wrong key must raise DecryptionError, not return ciphertext")


def main():
    for fn in (test_roundtrip_and_prefix, test_legacy_compat, test_fail_closed):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_cipher.py: ALL PASSED")


if __name__ == "__main__":
    main()