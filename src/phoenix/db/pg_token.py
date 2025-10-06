from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Optional, Protocol


logger = logging.getLogger(__name__)

from phoenix.config import get_env_postgres_auth_mode

# Environment variables (kept local to this module to avoid coupling)
ENV_AUTH_MODE = "PHOENIX_POSTGRES_AUTH_MODE"  # values: token-env, token-cmd, azure
ENV_TOKEN = "PHOENIX_POSTGRES_TOKEN"
ENV_TOKEN_EXPIRES_AT = "PHOENIX_POSTGRES_TOKEN_EXPIRES_AT"  # ISO8601, e.g. 2025-01-01T12:34:56Z
ENV_TOKEN_TTL_SECONDS = "PHOENIX_POSTGRES_TOKEN_TTL_SECONDS"  # int
ENV_TOKEN_SKEW_SECONDS = "PHOENIX_POSTGRES_TOKEN_SKEW_SECONDS"  # int, default 60
ENV_TOKEN_CMD = "PHOENIX_POSTGRES_TOKEN_CMD"  # command to run
ENV_TOKEN_CMD_TIMEOUT_SECONDS = "PHOENIX_POSTGRES_TOKEN_CMD_TIMEOUT_SECONDS"  # int, default 10

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


def _parse_iso8601(value: str) -> Optional[datetime]:
    try:
        # Accept values like "2025-01-01T12:34:56Z" and with offsets
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        logger.warning("Failed to parse token expiry '%s': %s", value, e)
        return None


class EnvTokenProvider:
    """Reads token and expiry from environment variables.

    - PHOENIX_POSTGRES_TOKEN
    - PHOENIX_POSTGRES_TOKEN_EXPIRES_AT (ISO8601) or PHOENIX_POSTGRES_TOKEN_TTL_SECONDS
    """

    def get_token(self) -> Token:
        token_value = os.getenv(ENV_TOKEN)
        if not token_value:
            raise RuntimeError(
                f"Environment token not set. Please set {ENV_TOKEN}."
            )
        expires_at_str = os.getenv(ENV_TOKEN_EXPIRES_AT)
        if expires_at_str:
            expires_at = _parse_iso8601(expires_at_str)
        else:
            ttl = os.getenv(ENV_TOKEN_TTL_SECONDS)
            expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=int(ttl))
                if ttl and ttl.isdigit()
                else None
            )
        return Token(value=token_value, expires_at=expires_at)


class CommandTokenProvider:
    """Runs a command to fetch a token.

    Expected outputs:
      - JSON object with keys: {"token": "...", "expires_at": "ISO8601"} (expires_at optional)
      - Plain string token; expiry inferred from PHOENIX_POSTGRES_TOKEN_TTL_SECONDS
    """

    def __init__(self) -> None:
        cmd = os.getenv(ENV_TOKEN_CMD)
        if not cmd:
            raise RuntimeError(
                f"Token command not set. Please set {ENV_TOKEN_CMD}."
            )
        self._cmd = cmd
        timeout_env = os.getenv(ENV_TOKEN_CMD_TIMEOUT_SECONDS)
        self._timeout = int(timeout_env) if timeout_env and timeout_env.isdigit() else 10

    def get_token(self) -> Token:
        try:
            # Use shlex.split to avoid shell=True risks.
            logger.debug("Running token command: %s", self._cmd)
            result = subprocess.run(
                shlex.split(self._cmd),
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"Token command failed (exit {result.returncode}): {result.stderr.strip()}"
                )
            output = result.stdout.strip()
            # Try JSON first
            token_value: Optional[str] = None
            expires_at: Optional[datetime] = None
            if output.startswith("{"):
                try:
                    payload = json.loads(output)
                    token_value = payload.get("token")
                    if not isinstance(token_value, str) or not token_value:
                        raise ValueError("JSON payload missing 'token' string field")
                    expires_str = payload.get("expires_at")
                    if isinstance(expires_str, str):
                        expires_at = _parse_iso8601(expires_str)
                except Exception as e:
                    raise RuntimeError(f"Failed to parse JSON token: {e}")
            else:
                token_value = output

            if not token_value:
                raise RuntimeError("Token command produced no token value")

            if expires_at is None:
                ttl = os.getenv(ENV_TOKEN_TTL_SECONDS)
                expires_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=int(ttl))
                    if ttl and ttl.isdigit()
                    else None
                )
            return Token(value=token_value, expires_at=expires_at)
        except Exception as e:
            raise RuntimeError(f"Failed to obtain token via command: {e}")


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


def _build_provider_from_env() -> CachedTokenProvider:
    # Prefer Azure mode via centralized config toggle
    config_mode = get_env_postgres_auth_mode()
    mode = (os.getenv(ENV_AUTH_MODE) or "").strip().lower()
    skew_env = os.getenv(ENV_TOKEN_SKEW_SECONDS)
    skew_seconds = int(skew_env) if skew_env and skew_env.isdigit() else 60

    if config_mode == "azure" or mode in ("azure",):
        base = AzureTokenProvider()
    elif mode in ("token-env", "env"):
        base = EnvTokenProvider()
    elif mode in ("token-cmd", "cmd"):
        base = CommandTokenProvider()
    else:
        # Default to env-based if mode unspecified
        base = EnvTokenProvider()
        if not os.getenv(ENV_TOKEN):
            logger.warning(
                "%s not set and %s unspecified; falling back to env provider which will error if token is missing.",
                ENV_TOKEN,
                ENV_AUTH_MODE,
            )
    logger.debug(
        "Selected token provider: %s (config_mode=%s, env_mode=%s)",
        type(base).__name__,
        config_mode or "",
        mode or "",
    )
    return CachedTokenProvider(base, skew_seconds=skew_seconds)


def get_token() -> Token:
    """Return the current token, refreshing if near expiry.

    This is the public entry point for consumers (e.g., engines) to obtain
    a token suitable for database authentication.
    """
    global _singleton_provider
    if _singleton_provider is None:
        _singleton_provider = _build_provider_from_env()
    return _singleton_provider.get_token()


def get_token_value() -> str:
    """Convenience helper to return only the token string."""
    return get_token().value


def set_token_provider(provider: TokenProvider, *, skew_seconds: Optional[int] = None) -> None:
    """Override the global token provider with an explicit instance.

    Useful for dependency injection in tests or in multi-process environments
    where implicit globals are undesirable.
    """
    global _singleton_provider
    if skew_seconds is None:
        skew_env = os.getenv(ENV_TOKEN_SKEW_SECONDS)
        skew_seconds = int(skew_env) if skew_env and skew_env.isdigit() else 60
    _singleton_provider = CachedTokenProvider(provider, skew_seconds=skew_seconds)


def clear_token_provider() -> None:
    """Clear the global token provider, forcing rebuild on next access."""
    global _singleton_provider
    _singleton_provider = None


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
                # Fallback to TTL env or unknown expiry
                ttl_env = os.getenv(ENV_TOKEN_TTL_SECONDS)
                expires_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=int(ttl_env))
                    if ttl_env and ttl_env.isdigit()
                    else None
                )
            return Token(value=token_str, expires_at=expires_at)
        except Exception as e:
            raise RuntimeError(f"Failed to obtain Azure AD token: {e}")
