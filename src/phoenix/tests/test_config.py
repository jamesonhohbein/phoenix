import os
from typing import Optional
from unittest.mock import patch

import pytest

from phoenix.config import get_env_collector_endpoint


@pytest.mark.parametrize(
    "env,expected",
    [
        ({"PHOENIX_COLLECTOR_ENDPOINT": "http://localhost:6006"}, "http://localhost:6006"),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:6006"}, "http://localhost:6006"),
        (
            {
                "PHOENIX_COLLECTOR_ENDPOINT": "http://localhost:6006",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            },
            "http://localhost:6006",
        ),
        ({}, None),
    ],
)
def test_get_env_collector_endpoint(env: dict[str, str], expected: Optional[str]) -> None:
    with patch.dict(os.environ, env, clear=True):
        assert get_env_collector_endpoint() == expected


@pytest.mark.parametrize(
    "env,expected",
    [
        ({}, ""),
        ({"PHOENIX_POSTGRES_AUTH_MODE": "azure"}, "azure"),
        ({"PHOENIX_POSTGRES_AUTH_MODE": "AZURE"}, "azure"),
        ({"PHOENIX_POSTGRES_AUTH_MODE": "other"}, ""),
    ],
)
def test_get_env_postgres_auth_mode(env: dict[str, str], expected: str) -> None:
    with patch.dict(os.environ, env, clear=True):
        from phoenix.config import get_env_postgres_auth_mode

        assert get_env_postgres_auth_mode() == expected
