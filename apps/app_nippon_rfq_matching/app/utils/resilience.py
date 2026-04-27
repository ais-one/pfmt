"""
Resilience Patterns for External API Calls

This module provides generic decorators and classes for handling external API calls
with circuit breaker and retry mechanisms.
"""

import functools
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit is open, blocking calls
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: float = 60.0  # Seconds to wait before trying again
    expected_exception: tuple[type[Exception], ...] = (Exception,)
    success_threshold: int = 2  # Successes needed to close circuit in half-open state


@dataclass
class RetryConfig:
    """Configuration for retry mechanism."""

    max_attempts: int = 3  # Maximum number of retry attempts
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 30.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff base
    jitter: bool = True  # Add randomness to delay
    expected_exception: tuple[type[Exception], ...] = (Exception,)


class CircuitBreaker:
    """
    Circuit breaker implementation for external service calls.

    Prevents cascading failures by stopping calls to failing services
    after a threshold of failures is reached.
    """

    def __init__(self, config: CircuitBreakerConfig):
        """
        Initialize circuit breaker.

        Args:
            config: Circuit breaker configuration
        """
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: datetime | None = None
        self._lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function through the circuit breaker.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function return value

        Raises:
            Exception: If circuit is open or function raises an exception
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info("Circuit breaker entering HALF_OPEN state")
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker is OPEN. Service has failed {self.failure_count} times. "
                        f"Retry after {self._get_remaining_time():.1f} seconds."
                    )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.config.expected_exception as e:
            self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True

        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.config.recovery_timeout

    def _get_remaining_time(self) -> float:
        """Get remaining time before retry attempt."""
        if self.last_failure_time is None:
            return 0.0

        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        remaining = self.config.recovery_timeout - elapsed
        return max(0.0, remaining)

    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info("Circuit breaker CLOSED after recovery")
            else:
                # Reset failure count on success in closed state
                self.failure_count = max(0, self.failure_count - 1)

    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.error("Circuit breaker OPEN after failure in HALF_OPEN state")
            elif self.failure_count >= self.config.failure_threshold:
                self.state = CircuitState.OPEN
                logger.error(
                    f"Circuit breaker OPEN after {self.failure_count} failures. "
                    f"Will retry after {self.config.recovery_timeout} seconds."
                )

    def reset(self):
        """Manually reset the circuit breaker."""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

    def get_state(self) -> dict[str, Any]:
        """Get current circuit breaker state."""
        with self._lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "last_failure_time": self.last_failure_time.isoformat()
                if self.last_failure_time
                else None,
                "remaining_time": self._get_remaining_time()
                if self.state == CircuitState.OPEN
                else 0.0,
            }


class RetryHandler:
    """
    Retry handler with exponential backoff for external service calls.

    Retries failed calls with increasing delays between attempts.
    """

    def __init__(self, config: RetryConfig):
        """
        Initialize retry handler.

        Args:
            config: Retry configuration
        """
        self.config = config

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function return value

        Raises:
            Exception: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                if attempt > 0:
                    logger.info(
                        f"Retry attempt {attempt + 1}/{self.config.max_attempts}"
                    )

                result = func(*args, **kwargs)

                if attempt > 0:
                    logger.info(f"Success after {attempt} retries")

                return result

            except self.config.expected_exception as e:
                last_exception = e

                if attempt < self.config.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt + 1}/{self.config.max_attempts} failed: {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"All {self.config.max_attempts} attempts failed. Last error: {e}"
                    )

        raise MaxRetriesExceededError(
            f"Max retries ({self.config.max_attempts}) exceeded. Last error: {last_exception}"
        ) from last_exception

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay before next retry."""
        delay = min(
            self.config.base_delay * (self.config.exponential_base**attempt),
            self.config.max_delay,
        )

        if self.config.jitter:
            # Add randomness to prevent thundering herd
            import random

            delay = delay * (0.5 + random.random() * 0.5)

        return delay


class ResilientCaller:
    """
    Combines circuit breaker and retry mechanisms for resilient external API calls.

    This provides a unified interface for handling external service calls with
    both retry logic and circuit breaker protection.
    """

    def __init__(
        self,
        service_name: str,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        retry_config: RetryConfig | None = None,
    ):
        """
        Initialize resilient caller.

        Args:
            service_name: Name of the service (for logging)
            circuit_breaker_config: Circuit breaker configuration
            retry_config: Retry configuration
        """
        self.service_name = service_name
        self.circuit_breaker_config = circuit_breaker_config or CircuitBreakerConfig()
        self.retry_config = retry_config or RetryConfig()

        self.circuit_breaker = CircuitBreaker(self.circuit_breaker_config)
        self.retry_handler = RetryHandler(self.retry_config)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function through circuit breaker and retry mechanisms.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function return value

        Raises:
            CircuitBreakerOpenError: If circuit is open
            MaxRetriesExceededError: If all retry attempts fail
        """
        logger.debug(f"Calling {self.service_name} with resilient caller")

        def wrapped_func():
            return self.retry_handler.call(func, *args, **kwargs)

        return self.circuit_breaker.call(wrapped_func)

    def get_state(self) -> dict[str, Any]:
        """Get current state of circuit breaker."""
        return {
            "service_name": self.service_name,
            "circuit_breaker": self.circuit_breaker.get_state(),
            "retry_config": {
                "max_attempts": self.retry_config.max_attempts,
                "base_delay": self.retry_config.base_delay,
                "max_delay": self.retry_config.max_delay,
            },
        }


# Custom exceptions
class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""

    pass


class MaxRetriesExceededError(Exception):
    """Raised when max retry attempts are exceeded."""

    pass


# Decorators for convenience
def with_circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    expected_exception: tuple[type[Exception], ...] = (Exception,),
):
    """
    Decorator to add circuit breaker to a function.

    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds to wait before trying again
        expected_exception: Exceptions that count as failures

    Returns:
        Decorated function

    Example:
        @with_circuit_breaker(failure_threshold=3, recovery_timeout=30)
        def call_external_api():
            ...
    """

    def decorator(func: Callable) -> Callable:
        circuit_breaker = CircuitBreaker(
            CircuitBreakerConfig(
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout,
                expected_exception=expected_exception,
            )
        )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return circuit_breaker.call(func, *args, **kwargs)

        # Attach circuit breaker to wrapper for inspection
        wrapper._circuit_breaker = circuit_breaker

        return wrapper

    return decorator


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    expected_exception: tuple[type[Exception], ...] = (Exception,),
):
    """
    Decorator to add retry logic to a function.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        expected_exception: Exceptions that trigger retry

    Returns:
        Decorated function

    Example:
        @with_retry(max_attempts=3, base_delay=1.0)
        def call_external_api():
            ...
    """

    def decorator(func: Callable) -> Callable:
        retry_handler = RetryHandler(
            RetryConfig(
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                expected_exception=expected_exception,
            )
        )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return retry_handler.call(func, *args, **kwargs)

        return wrapper

    return decorator


def with_resilience(
    service_name: str = "external_service",
    circuit_breaker_config: CircuitBreakerConfig | None = None,
    retry_config: RetryConfig | None = None,
):
    """
    Decorator to add both circuit breaker and retry to a function.

    Args:
        service_name: Name of the service (for logging)
        circuit_breaker_config: Circuit breaker configuration
        retry_config: Retry configuration

    Returns:
        Decorated function

    Example:
        @with_resilience(
            service_name="openai_api",
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=5),
            retry_config=RetryConfig(max_attempts=3)
        )
        def call_openai():
            ...
    """

    def decorator(func: Callable) -> Callable:
        resilient_caller = ResilientCaller(
            service_name=service_name,
            circuit_breaker_config=circuit_breaker_config,
            retry_config=retry_config,
        )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return resilient_caller.call(func, *args, **kwargs)

        # Attach resilient caller to wrapper for inspection
        wrapper._resilient_caller = resilient_caller

        return wrapper

    return decorator


# Singleton instances for common services
class ResilientCallers:
    """Singleton instances for common external services."""

    _instances: dict[str, ResilientCaller] = {}
    _lock = threading.Lock()

    @classmethod
    def get_openai_normalization(cls) -> ResilientCaller:
        """Get resilient caller for OpenAI normalization API."""
        with cls._lock:
            if "openai_normalization" not in cls._instances:
                cls._instances["openai_normalization"] = ResilientCaller(
                    service_name="openai_normalization",
                    circuit_breaker_config=CircuitBreakerConfig(
                        failure_threshold=5,
                        recovery_timeout=60.0,
                        expected_exception=(ConnectionError, TimeoutError),
                    ),
                    retry_config=RetryConfig(
                        max_attempts=2,  # 1 initial + 1 retry
                        base_delay=1.0,
                        max_delay=5.0,
                        expected_exception=(ConnectionError, TimeoutError),
                    ),
                )
            return cls._instances["openai_normalization"]

    @classmethod
    def get_openai_embedding(cls) -> ResilientCaller:
        """Get resilient caller for OpenAI embedding API."""
        with cls._lock:
            if "openai_embedding" not in cls._instances:
                cls._instances["openai_embedding"] = ResilientCaller(
                    service_name="openai_embedding",
                    circuit_breaker_config=CircuitBreakerConfig(
                        failure_threshold=5,
                        recovery_timeout=60.0,
                        expected_exception=(ConnectionError, TimeoutError),
                    ),
                    retry_config=RetryConfig(
                        max_attempts=2,
                        base_delay=1.0,
                        max_delay=5.0,
                        expected_exception=(ConnectionError, TimeoutError),
                    ),
                )
            return cls._instances["openai_embedding"]

    @classmethod
    def get_all_states(cls) -> dict[str, dict[str, Any]]:
        """Get state of all resilient callers."""
        with cls._lock:
            return {name: caller.get_state() for name, caller in cls._instances.items()}

    @classmethod
    def reset_all(cls):
        """Reset all circuit breakers."""
        with cls._lock:
            for caller in cls._instances.values():
                caller.circuit_breaker.reset()
            logger.info("All circuit breakers reset")
