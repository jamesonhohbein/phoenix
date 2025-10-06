import os
from datetime import datetime, timezone

import pytest

from phoenix.db.pg_token import EnvTokenProvider


def test_env_token_with_expires_at(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "abc")
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_EXPIRES_AT", "2025-01-01T12:34:56Z")

    provider = EnvTokenProvider()
    token = provider.get_token()

    assert token.value == "abc"
    assert token.expires_at is not None
    assert token.expires_at.tzinfo is not None
    # Expect UTC naive conversion equality
    expected = datetime(2025, 1, 1, 12, 34, 56, tzinfo=timezone.utc)
    assert token.expires_at == expected


def test_env_token_ttl_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "xyz")
    monkeypatch.delenv("PHOENIX_POSTGRES_TOKEN_EXPIRES_AT", raising=False)
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_TTL_SECONDS", "3600")

    provider = EnvTokenProvider()
    start = datetime.now(timezone.utc)
    token = provider.get_token()

    assert token.value == "xyz"
    assert token.expires_at is not None
    delta = (token.expires_at - start).total_seconds()
    assert 3590 <= delta <= 3610  # allow small timing variance


def test_env_token_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PHOENIX_POSTGRES_TOKEN", raising=False)
    provider = EnvTokenProvider()
    with pytest.raises(RuntimeError):
        provider.get_token()


def test_env_token_with_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "abc")
    # 14:00:00+02:00 should become 12:00:00Z
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_EXPIRES_AT", "2025-01-01T14:00:00+02:00")

    provider = EnvTokenProvider()
    token = provider.get_token()

    assert token.value == "abc"
    assert token.expires_at == datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_env_token_malformed_expiry(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN", "abc")
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_EXPIRES_AT", "not-a-date")

    provider = EnvTokenProvider()
    token = provider.get_token()
    assert token.value == "abc"
    assert token.expires_at is None
    assert any("Failed to parse token expiry" in rec.getMessage() for rec in caplog.records)
