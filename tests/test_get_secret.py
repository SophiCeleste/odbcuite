"""
test_get_secret.py — unit tests for ns_utils.get_secret().

All tests mock either environment variables or the Azure SDK — no network calls.
No real credentials appear in this file (T-02-04): mock values only
("test-password", "sophia@screeninnovations.com", "sa_login", "value").

RED state note: ns_utils.get_secret does NOT exist yet, so every test below
fails with AttributeError. That is the expected pre-implementation state.
"""
import importlib
import pytest
from unittest.mock import patch, MagicMock

import ns_utils


def _reset_module_cache():
    """Reset module-level cache between tests to avoid bleed-over."""
    ns_utils._secret_client = None
    ns_utils._vault_url = None


# -----------------------------------------------------------------
# Env-var fallback path (no vault URL configured)
# -----------------------------------------------------------------

def test_env_fallback(monkeypatch):
    _reset_module_cache()
    monkeypatch.delenv("AZURE_KEYVAULT_URL", raising=False)
    monkeypatch.setenv("NETSUITE_UID", "sophia@screeninnovations.com")
    with patch.object(ns_utils, "_get_config_vault_url", return_value=None):
        result = ns_utils.get_secret("netsuite-uid")
    assert result == "sophia@screeninnovations.com"


def test_env_name_derivation(monkeypatch):
    """Confirm hyphen -> underscore uppercasing: azure-sql-prod-uid -> AZURE_SQL_PROD_UID."""
    _reset_module_cache()
    monkeypatch.delenv("AZURE_KEYVAULT_URL", raising=False)
    monkeypatch.setenv("AZURE_SQL_PROD_UID", "sa_login")
    with patch.object(ns_utils, "_get_config_vault_url", return_value=None):
        result = ns_utils.get_secret("azure-sql-prod-uid")
    assert result == "sa_login"


def test_no_vault_no_env(monkeypatch):
    """KeyError raised when no vault configured and env var absent."""
    _reset_module_cache()
    monkeypatch.delenv("AZURE_KEYVAULT_URL", raising=False)
    monkeypatch.delenv("NETSUITE_UID", raising=False)
    with patch.object(ns_utils, "_get_config_vault_url", return_value=None):
        with pytest.raises(KeyError):
            ns_utils.get_secret("netsuite-uid")


# -----------------------------------------------------------------
# Vault path (vault URL configured)
# -----------------------------------------------------------------

def test_vault_path(monkeypatch):
    """Returns .value from mocked SecretClient when vault URL set."""
    _reset_module_cache()
    monkeypatch.setenv("AZURE_KEYVAULT_URL", "https://my-vault.vault.azure.net/")

    mock_secret = MagicMock()
    mock_secret.value = "test-password"

    with patch("azure.keyvault.secrets.SecretClient") as MockClient, \
         patch("azure.identity.DefaultAzureCredential"):
        MockClient.return_value.get_secret.return_value = mock_secret
        result = ns_utils.get_secret("netsuite-uid")

    assert result == "test-password"


def test_vault_client_cached(monkeypatch):
    """SecretClient instantiated only once across multiple get_secret() calls."""
    _reset_module_cache()
    monkeypatch.setenv("AZURE_KEYVAULT_URL", "https://my-vault.vault.azure.net/")

    mock_secret = MagicMock()
    mock_secret.value = "value"

    with patch("azure.keyvault.secrets.SecretClient") as MockClient, \
         patch("azure.identity.DefaultAzureCredential"):
        MockClient.return_value.get_secret.return_value = mock_secret
        ns_utils.get_secret("netsuite-uid")
        ns_utils.get_secret("netsuite-pwd")

    assert MockClient.call_count == 1  # client created only once


def test_vault_auth_error_propagates(monkeypatch):
    """Key Vault auth failure is NOT swallowed — it propagates (D-04)."""
    from azure.core.exceptions import ClientAuthenticationError
    _reset_module_cache()
    monkeypatch.setenv("AZURE_KEYVAULT_URL", "https://my-vault.vault.azure.net/")

    with patch("azure.keyvault.secrets.SecretClient") as MockClient, \
         patch("azure.identity.DefaultAzureCredential"):
        MockClient.return_value.get_secret.side_effect = ClientAuthenticationError("auth failed")
        with pytest.raises(ClientAuthenticationError):
            ns_utils.get_secret("netsuite-uid")
