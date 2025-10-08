from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Optional, Protocol


logger = logging.getLogger(__name__)

from phoenix.config import get_env_postgres_auth_mode

# Environment variables
ENV_TOKEN_SKEW_SECONDS = "PHOENIX_POSTGRES_TOKEN_SKEW_SECONDS"
"""A safety buffer, in seconds, to proactively refresh a token before it expires.

This helps prevent connection failures due to network latency or clock skew by ensuring
the application does not use a token that is about to expire. For example, if set
to 60 (the default), a token will be considered expired 60 seconds before its
actual expiration time, triggering a refresh.
"""

# Azure-specific configuration
ENV_AZURE_AUTH_MODE = "PHOENIX_AZURE_AUTH_MODE"  # values: managed, default
ENV_AZURE_IAM_CLIENT_ID = "PHOENIX_AZURE_IAM_CLIENT_ID"  # for user-assigned MI
ENV_AZURE_SCOPE = "PHOENIX_AZURE_SCOPE"  # default: AAD for Azure PostgreSQL

_DEFAULT_AZURE_PG_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


@dataclass(frozen=True)
class Token:
    """Database auth token with expiry metadata."""

    value: str
    expires_at: Optional[datetime]

    def is_expired(self, skew_seconds: int = 60) -> bool:
        if self.expires_at is None:
            return False
        now = datetime.now(timezone.utc)
        # Refresh proactively if we are within skew window
        return now >= (self.expires_at - timedelta(seconds=skew_seconds))


class TokenProvider(Protocol):
    def get_token(self) -> Token:
        ...


class CachedTokenProvider:
    """Caches tokens and proactively refreshes before expiry."""

    def __init__(self, provider: TokenProvider, skew_seconds: int = 60) -> None:
        self._provider = provider
        self._skew_seconds = skew_seconds
        self._cached: Optional[Token] = None
        self._lock: Lock = Lock()

    def get_token(self) -> Token:
        # Fast path: return cached token if valid
        if self._cached and not self._cached.is_expired(self._skew_seconds):
            logger.debug("Using cached database token; not within skew window")
            return self._cached
        # Slow path: lock and re-check, then refresh if still needed
        with self._lock:
            if self._cached and not self._cached.is_expired(self._skew_seconds):
                logger.debug("Using cached database token after lock; not within skew window")
                return self._cached
            token = self._provider.get_token()
            self._cached = token
            logger.debug("Refreshed database token via %s", type(self._provider).__name__)
            return token


_singleton_provider: Optional[CachedTokenProvider] = None


def _build_provider() -> CachedTokenProvider:
    skew_env = os.getenv(ENV_TOKEN_SKEW_SECONDS)
    skew_seconds = int(skew_env) if skew_env and skew_env.isdigit() else 60
    base = AzureTokenProvider()
    logger.debug("Selected token provider: %s", type(base).__name__)
    return CachedTokenProvider(base, skew_seconds=skew_seconds)


def get_token() -> Token:
    """Return the current token, refreshing if near expiry.

    This is the public entry point for consumers (e.g., engines) to obtain
    a token suitable for database authentication.
    """
    global _singleton_provider
    if _singleton_provider is None:
        _singleton_provider = _build_provider()
    return _singleton_provider.get_token()


def get_token_value() -> str:
    """Convenience helper to return only the token string."""
    return get_token().value


# -----------------------------
# Azure implementation
# -----------------------------


def _get_azure_credential(mode: str = "default", client_id: Optional[str] = None) -> object:
    """Return an Azure credential based on mode and environment.

    - mode="managed": ManagedIdentityCredential (user-assigned if client_id provided).
    - mode="default": DefaultAzureCredential.
    """
    try:
        from azure.identity import DefaultAzureCredential, ManagedIdentityCredential  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "azure.identity is required for Azure token mode. Install with `pip install azure-identity`."
        ) from e

    mode = mode.strip().lower()
    if mode == "managed":
        return ManagedIdentityCredential(client_id=client_id)
    return DefaultAzureCredential()


class AzureTokenProvider:
    """Azure AD token provider for PostgreSQL using azure.identity.

    Fetches tokens for the configured scope and returns Token with expiry metadata.
    Caching is delegated to CachedTokenProvider which wraps this provider.
    """

    def __init__(
        self,
        scope: Optional[str] = None,
        credential: Optional[object] = None,
        auth_mode: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> None:
        self._scope = scope or os.getenv(ENV_AZURE_SCOPE, _DEFAULT_AZURE_PG_SCOPE)
        mode = (auth_mode or os.getenv(ENV_AZURE_AUTH_MODE) or "default").strip().lower()
        client_id = client_id or os.getenv(ENV_AZURE_IAM_CLIENT_ID) or None
        self._credential = credential or _get_azure_credential(mode, client_id)

    def get_token(self) -> Token:
        try:
            access_token = self._credential.get_token(self._scope)  # type: ignore[attr-defined]
            token_str = getattr(access_token, "token", None)
            if not isinstance(token_str, str) or not token_str:
                raise RuntimeError("Azure credential returned invalid token")
            # expires_on is epoch seconds (int)
            expires_on = getattr(access_token, "expires_on", None)
            expires_at: Optional[datetime]
            if isinstance(expires_on, (int, float)):
                expires_at = datetime.fromtimestamp(int(expires_on), tz=timezone.utc)
            else:
                expires_at = None
            return Token(value=token_str, expires_at=expires_at)
        except Exception as e:
            raise RuntimeError(f"Failed to obtain Azure AD token: {e}")
