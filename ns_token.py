"""
ns_token.py — NetSuite Token-Based Authentication (TBA) token password generator.

Generates the token password string for SuiteAnalytics Connect (ODBC/ADO.NET)
per the NetSuite TBA procedure:
https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/article_163240164565.html

Token password format:
    {account_id}&{consumer_key}&{token_id}&{nonce}&{timestamp}&{base64_signature}&HMAC-SHA256

No network calls are made — this is pure local HMAC-SHA256 crypto.
Credentials are loaded once from config.json at import time.

Expected config.json keys under config["netsuite"]["prod"]:
    account_id              -- NetSuite account ID, e.g. "1234567"
    secret_consumer_key     -- Key Vault secret name for the Consumer key
    secret_consumer_secret  -- Key Vault secret name for the Consumer secret
    secret_token_id         -- Key Vault secret name for the Token ID
    secret_token_secret     -- Key Vault secret name for the Token secret

The actual TBA token values are resolved at call time via ns_utils.get_secret()
and cached in-process — never stored in config.json.

Usage:
    from ns_token import build_token_password

    pwd  = build_token_password()
    conn = pyodbc.connect(f"DSN=NetSuite;PWD={pwd}", autocommit=True)

    # Or just use connect_netsuite(config) in ns_utils — it calls this automatically for prod.
"""

import base64
import hashlib
import hmac
import secrets
import string
import time

from ns_utils import load_config

_creds = None


def _load_creds():
    global _creds
    if _creds is None:
        from ns_utils import get_secret   # deferred import — avoids circular at module top
        ns = load_config()["netsuite"]["prod"]
        _creds = (
            ns["account_id"],
            get_secret(ns["secret_consumer_key"]),
            get_secret(ns["secret_consumer_secret"]),
            get_secret(ns["secret_token_id"]),
            get_secret(ns["secret_token_secret"]),
        )
    return _creds


def build_token_password():
    """
    Generate a NetSuite TBA token password for SuiteAnalytics Connect.

    Steps (per NetSuite TBA procedure):
        1. Base string  — account_id&consumer_key&token_id&nonce&timestamp
        2. Signing key  — consumer_secret&token_secret
        3. Signature    — HMAC-SHA256(base_string, signing_key), Base64-encoded
        4. Token password — base_string&signature&HMAC-SHA256

    Returns
    -------
    str
        Token password to pass as PWD in the ODBC connection string.
        A new nonce and timestamp are generated on every call.

    Example
    -------
        pwd  = build_token_password()
        conn = pyodbc.connect(f"DSN=NetSuiteProd;PWD={pwd}", autocommit=True)

        # ADO.NET driver requires single quotes around the token password:
        # conn_str = f"DSN=NetSuiteProd;PWD='{pwd}'"
    """
    account_id, consumer_key, consumer_secret, token_id, token_secret = _load_creds()

    nonce     = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(20))
    timestamp = str(int(time.time()))

    base_string = "&".join([account_id, consumer_key, token_id, nonce, timestamp])
    signing_key = f"{consumer_secret}&{token_secret}"

    signature = base64.b64encode(
        hmac.new(
            signing_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")

    return f"{base_string}&{signature}&HMAC-SHA256"
