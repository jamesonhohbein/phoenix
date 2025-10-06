import logging
from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from phoenix.db.pg_token import CachedTokenProvider, Token


class _Provider:
    def __init__(self) -> None:
        self.count = 0

    def get_token(self) -> Token:
        self.count += 1
        now = datetime.now(timezone.utc)
        return Token(value=f"tok-{self.count}", expires_at=now + timedelta(seconds=120))


def test_logging_cache_hit_and_refresh(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="phoenix.db.pg_token")
    prov = _Provider()
    cache = CachedTokenProvider(prov, skew_seconds=60)

    with freeze_time("2025-01-01 00:00:00"):
        cache.get_token()
        assert any("Refreshed database token" in rec.getMessage() for rec in caplog.records)

    with freeze_time("2025-01-01 00:00:30"):
        cache.get_token()
        assert any("Using cached database token" in rec.getMessage() for rec in caplog.records)
