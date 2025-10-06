import logging

import pytest

from phoenix.db import pg_token
from phoenix.db.pg_token import get_token_value


def test_default_env_provider_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    # Reset singleton and set env token without explicit auth mode
    monkeypatch.setattr(pg_token, "_singleton_provider", None)
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "abc123")
    monkeypatch.delenv("PHOENIX_POSTGRES_AUTH_MODE", raising=False)

    value = get_token_value()
    assert value == "abc123"


def test_warning_when_env_provider_has_no_token(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # Reset singleton and ensure no token is set
    monkeypatch.setattr(pg_token, "_singleton_provider", None)
    monkeypatch.delenv("PHOENIX_POSTGRES_TOKEN", raising=False)
    monkeypatch.delenv("PHOENIX_POSTGRES_AUTH_MODE", raising=False)

    caplog.set_level(logging.WARNING, logger="phoenix.db.pg_token")
    with pytest.raises(RuntimeError):
        # This will build the provider and log a warning, then fail on get_token
        pg_token.get_token()

    assert any("falling back to env provider" in rec.getMessage() for rec in caplog.records)

