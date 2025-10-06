import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from phoenix.db.pg_token import CommandTokenProvider


class _FakeCompleted:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "", kwargs=None) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.kwargs = kwargs or {}


def test_command_token_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"token": "tok", "expires_at": "2025-01-01T00:00:00Z"}

    def fake_run(args, capture_output=True, text=True, timeout=10):
        return _FakeCompleted(stdout=json.dumps(payload), kwargs={"timeout": timeout})

    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD", "fetch-token --json")
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD_TIMEOUT_SECONDS", "10")
    monkeypatch.setattr("subprocess.run", fake_run)

    provider = CommandTokenProvider()
    token = provider.get_token()

    assert token.value == "tok"
    assert token.expires_at == datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_command_token_plain_output_with_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args, capture_output=True, text=True, timeout=10):
        return _FakeCompleted(stdout="tok2", kwargs={"timeout": timeout})

    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD", "fetch-token")
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_TTL_SECONDS", "1800")
    monkeypatch.setattr("subprocess.run", fake_run)

    provider = CommandTokenProvider()
    start = datetime.now(timezone.utc)
    token = provider.get_token()

    assert token.value == "tok2"
    assert token.expires_at is not None
    delta = (token.expires_at - start).total_seconds()
    assert 1790 <= delta <= 1810


def test_command_token_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args, capture_output=True, text=True, timeout=10):
        return _FakeCompleted(returncode=1, stdout="", stderr="boom", kwargs={"timeout": timeout})

    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD", "fetch-token")
    monkeypatch.setattr("subprocess.run", fake_run)

    with pytest.raises(RuntimeError) as exc:
        provider = CommandTokenProvider()
        provider.get_token()
    assert "exit 1" in str(exc.value) or "boom" in str(exc.value)


def test_command_timeout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded = SimpleNamespace(timeout=None)

    def fake_run(args, capture_output=True, text=True, timeout=10):
        recorded.timeout = timeout
        return _FakeCompleted(stdout="tok3")

    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD", "fetch-token")
    monkeypatch.setenv("PHOENIX_POSTGRES_TOKEN_CMD_TIMEOUT_SECONDS", "5")
    monkeypatch.setattr("subprocess.run", fake_run)

    provider = CommandTokenProvider()
    provider.get_token()

    assert recorded.timeout == 5

