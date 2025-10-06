import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

import pytest

from phoenix.db import pg_token
from phoenix.db.pg_token import AzureTokenProvider, get_token


class _StubAccessToken:
    def __init__(self, token: str, expires_on: int) -> None:
        self.token = token
        self.expires_on = expires_on


def _install_stub_azure_identity(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a stub azure.identity module into sys.modules with tracing hooks."""
    ns = SimpleNamespace(last_scope=None, last_client_id=None)

    class DefaultAzureCredential:
        def __init__(self):
            self.kind = "default"

        def get_token(self, scope: str) -> _StubAccessToken:
            ns.last_scope = scope
            return _StubAccessToken("default_token", int(datetime.now(timezone.utc).timestamp()) + 1000)

    class ManagedIdentityCredential:
        def __init__(self, client_id=None):
            self.kind = "managed"
            ns.last_client_id = client_id

        def get_token(self, scope: str) -> _StubAccessToken:
            ns.last_scope = scope
            return _StubAccessToken("managed_token", int(datetime.now(timezone.utc).timestamp()) + 1000)

    identity_mod = ModuleType("azure.identity")
    identity_mod.DefaultAzureCredential = DefaultAzureCredential
    identity_mod.ManagedIdentityCredential = ManagedIdentityCredential

    azure_mod = ModuleType("azure")
    azure_mod.identity = identity_mod
    monkeypatch.setitem(sys.modules, "azure", azure_mod)
    monkeypatch.setitem(sys.modules, "azure.identity", identity_mod)
    return ns


def test_azure_default_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    ns = _install_stub_azure_identity(monkeypatch)
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")
    provider = AzureTokenProvider()
    token = provider.get_token()
    assert token.value == "default_token"
    assert ns.last_scope == "https://ossrdbms-aad.database.windows.net/.default"
    assert isinstance(token.expires_at, datetime)


def test_azure_custom_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    ns = _install_stub_azure_identity(monkeypatch)
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")
    monkeypatch.setenv("PHOENIX_AZURE_SCOPE", "https://example.com/.default")
    provider = AzureTokenProvider()
    token = provider.get_token()
    assert token.value in ("default_token", "managed_token")
    assert ns.last_scope == "https://example.com/.default"


def test_azure_managed_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    ns = _install_stub_azure_identity(monkeypatch)
    provider = AzureTokenProvider(auth_mode="managed")
    token = provider.get_token()
    assert token.value == "managed_token"
    assert ns.last_client_id is None


def test_azure_managed_identity_explicit_client_id(monkeypatch: pytest.MonkeyPatch) -> None:
    ns = _install_stub_azure_identity(monkeypatch)
    provider = AzureTokenProvider(auth_mode="managed", client_id="client-123")
    token = provider.get_token()
    assert token.value == "managed_token"
    assert ns.last_client_id == "client-123"


def test_missing_azure_identity_module(monkeypatch: pytest.MonkeyPatch) -> None:
    # Remove azure modules to simulate missing dependency
    monkeypatch.delenv("PHOENIX_POSTGRES_AUTH_MODE", raising=False)
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")
    monkeypatch.setitem(sys.modules, "azure", None)
    monkeypatch.setitem(sys.modules, "azure.identity", None)
    with pytest.raises(RuntimeError) as exc:
        AzureTokenProvider()
    assert "azure.identity is required" in str(exc.value)


def test_selection_via_get_token(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_stub_azure_identity(monkeypatch)
    # Reset singleton provider for isolation
    monkeypatch.setattr(pg_token, "_singleton_provider", None)
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")
    t = get_token()
    assert t.value in ("default_token", "managed_token")
