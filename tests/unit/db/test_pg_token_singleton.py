import pytest

from phoenix.db import pg_token
from phoenix.db.pg_token import Token, TokenProvider, clear_token_provider, get_token_value, set_token_provider


class _StaticProvider:
    def __init__(self, value: str) -> None:
        self.value = value

    def get_token(self) -> Token:
        return Token(value=self.value, expires_at=None)


def test_set_and_clear_token_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure starting from a clean singleton
    clear_token_provider()
    # Inject custom provider
    set_token_provider(_StaticProvider("injected"), skew_seconds=5)
    assert get_token_value() == "injected"

    # Clear and ensure rebuild uses default env provider path
    clear_token_provider()
    # Set env to use token-env
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "abc")
    monkeypatch.delenv("PHOENIX_POSTGRES_AUTH_MODE", raising=False)
    assert get_token_value() == "abc"
