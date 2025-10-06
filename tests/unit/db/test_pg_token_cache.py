from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from phoenix.db.pg_token import CachedTokenProvider, Token, TokenProvider


class _CountingProvider:
    def __init__(self) -> None:
        self.count = 0

    def get_token(self) -> Token:
        self.count += 1
        # Each call returns a token valid for 120 seconds from now
        now = datetime.now(timezone.utc)
        return Token(value=f"tok-{self.count}", expires_at=now + timedelta(seconds=120))


def test_cached_provider_refresh_behavior() -> None:
    prov = _CountingProvider()
    cache = CachedTokenProvider(prov, skew_seconds=60)

    with freeze_time("2025-01-01 00:00:00"):
        t1 = cache.get_token()
        assert t1.value == "tok-1"
        # Advance 30s: still outside the 60s skew window → no refresh
        with freeze_time("2025-01-01 00:00:30"):
            t2 = cache.get_token()
            assert t2.value == "tok-1"
        # Advance 70s: within the 60s skew before expiry → refresh
        with freeze_time("2025-01-01 00:01:10"):
            t3 = cache.get_token()
            assert t3.value == "tok-2"


def test_cached_provider_thread_safety() -> None:
    # Provider that simulates a slow refresh to expose races if not locked
    import threading
    import time

    class SlowProvider:
        def __init__(self) -> None:
            self.count = 0

        def get_token(self) -> Token:
            time.sleep(0.05)
            self.count += 1
            now = datetime.now(timezone.utc)
            # Token that expires soon (force refresh path)
            return Token(value=f"tok-{self.count}", expires_at=now + timedelta(seconds=10))

    prov = SlowProvider()
    cache = CachedTokenProvider(prov, skew_seconds=9)  # within skew after creation

    # First call populates cache at baseline time
    with freeze_time("2025-01-01 00:00:00"):
        t1 = cache.get_token()
        assert t1.value == "tok-1"

    # Advance time into the skew window to trigger one refresh across threads
    results: list[str] = []

    def worker():
        results.append(cache.get_token().value)

    with freeze_time("2025-01-01 00:00:01"):
        threads = [threading.Thread(target=worker) for _ in range(10)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

    # Provider should have been called exactly once more (i.e., 2 total)
    assert prov.count == 2
    # All concurrent results should be the refreshed token
    assert all(val == "tok-2" for val in results)
