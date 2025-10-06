import os
from types import SimpleNamespace

import pytest
from sqlalchemy import URL, make_url

from phoenix.db import pg_token
from phoenix.db.pg_token import Token, TokenProvider
from phoenix.db.engines import aio_postgresql_engine


class _StaticProvider:
    def __init__(self, value: str) -> None:
        self.value = value

    def get_token(self) -> Token:
        return Token(value=self.value, expires_at=None)


@pytest.mark.asyncio
async def test_asyncpg_creator_uses_token(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: set token provider and auth mode to azure
    pg_token.clear_token_provider()
    pg_token.set_token_provider(_StaticProvider("secret"))
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")

    # Build URL without static password
    url = make_url("postgresql://user@localhost:5432/phoenix")
    async_url = URL.create(
        drivername="postgresql+asyncpg",
        username=None,
        password=None,
        host=url.host,
        port=url.port,
        database=url.database,
        # Include sslmode so get_pg_config produces SSL args
        query={"user": "user", "sslmode": "require"},
    )

    captured = {}

    def fake_create_async_engine(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(sync_engine=None, dialect=SimpleNamespace(name="postgresql"))

    async def fake_asyncpg_connect(**kwargs):
        captured["connect_kwargs"] = kwargs
        return SimpleNamespace()

    # Patch the symbol used by engines module for async engine factory
    monkeypatch.setattr("phoenix.db.engines.create_async_engine", fake_create_async_engine)
    import asyncpg as _asyncpg  # type: ignore

    monkeypatch.setattr(_asyncpg, "connect", fake_asyncpg_connect)

    # Act: create engine (migrate disabled to avoid psycopg path)
    engine = aio_postgresql_engine(async_url, migrate=False)

    # Assert: our async_creator exists and uses token when invoked
    assert "async_creator" in captured
    creator = captured["async_creator"]
    await creator()
    assert captured["connect_kwargs"]["password"] == "secret"
    assert captured["connect_kwargs"]["user"] == "user"
    # SSL propagated via get_pg_config
    assert "ssl" in captured["connect_kwargs"]


def test_psycopg_creator_uses_token(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    pg_token.clear_token_provider()
    pg_token.set_token_provider(_StaticProvider("secret"))
    monkeypatch.setenv("PHOENIX_POSTGRES_AUTH_MODE", "azure")

    url = make_url("postgresql://user@localhost:5432/phoenix")
    async_url = URL.create(
        drivername="postgresql+asyncpg",
        username=None,
        password=None,
        host=url.host,
        port=url.port,
        database=url.database,
        query={"user": "user", "sslmode": "require"},
    )

    captured = {}

    def fake_create_async_engine(**kwargs):
        # Return dummy engine
        return SimpleNamespace(sync_engine=None, dialect=SimpleNamespace(name="postgresql"))

    def fake_create_engine(**kwargs):
        # Call the creator immediately to capture connection args
        creator = kwargs.get("creator")
        assert callable(creator)
        # Patch psycopg.connect to record args
        import psycopg as _psycopg  # type: ignore

        def fake_psycopg_connect(**ckw):
            captured.update(ckw)
            return SimpleNamespace()

        monkeypatch.setattr(_psycopg, "connect", fake_psycopg_connect)
        creator()
        return SimpleNamespace()

    monkeypatch.setattr("sqlalchemy.ext.asyncio.create_async_engine", fake_create_async_engine)
    monkeypatch.setattr("sqlalchemy.create_engine", fake_create_engine)
    # Avoid running migrations: patch the symbol used inside engines module
    monkeypatch.setattr("phoenix.db.engines.migrate_in_thread", lambda *args, **kwargs: None)

    # Act
    aio_postgresql_engine(async_url, migrate=True)

    # Assert
    assert captured["password"] == "secret"
    assert captured["user"] == "user"
    # SSL args propagated for psycopg
    assert captured.get("sslmode") == "require"
