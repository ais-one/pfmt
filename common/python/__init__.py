"""Shared Python modules for backend services."""

__version__ = "0.1.0"

from common.python.config import env_bool, env_int, env_str, get_environment
from common.python.logger import get_logger
from common.python.models import (
    EchoRequest,
    EchoResponse,
    HealthResponse,
    ServiceInfoResponse,
)
from common.python.testing import api_url, create_test_client

__all__ = [
    "EchoRequest",
    "EchoResponse",
    "HealthResponse",
    "ServiceInfoResponse",
    "api_url",
    "create_test_client",
    "env_bool",
    "env_int",
    "env_str",
    "get_environment",
    "get_logger",
]
