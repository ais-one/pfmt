import functools
import logging
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from apps.app_nippon_rfq_matching.app.core.logging_config import (
    get_logger,
)

logger = get_logger(__name__)


def timed_operation(
    operation_name: str, logger: logging.Logger | None = None
) -> Callable:
    """Decorator to time and log function execution"""
    if logger is None:
        logger = get_logger(__name__)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            logger.info(f"Starting {operation_name}")

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f"Completed {operation_name} in {duration:.3f}s",
                    extra={"duration": duration},
                )
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"Failed {operation_name} after {duration:.3f}s",
                    extra={"duration": duration, "error": str(e)},
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator


@contextmanager
def operation_context(operation_name: str, logger: logging.Logger | None = None):
    """Context manager for timing operations"""
    if logger is None:
        logger = get_logger(__name__)

    start_time = time.time()
    logger.info(f"Starting {operation_name}")

    try:
        yield
        duration = time.time() - start_time
        logger.info(
            f"Completed {operation_name} in {duration:.3f}s",
            extra={"duration": duration},
        )
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"Failed {operation_name} after {duration:.3f}s",
            extra={"duration": duration, "error": str(e)},
            exc_info=True,
        )
        raise


def log_function_entry(func: Callable, *args, **kwargs) -> None:
    """Log function entry with arguments (excluding sensitive data)"""
    logger = get_logger(func.__module__)

    # Sanitize arguments to avoid logging sensitive data
    sanitized_args = []
    for arg in args:
        if (
            isinstance(arg, str | int | float | bool)
            and not isinstance(arg, str)
            or len(str(arg)) < 100
        ):
            sanitized_args.append(arg)
        else:
            sanitized_args.append("[sanitized]")

    sanitized_kwargs = {
        k: (
            v
            if isinstance(v, int | float | bool)
            or (isinstance(v, str) and len(v) < 100)
            else "[sanitized]"
        )
        for k, v in kwargs.items()
    }

    logger.debug(
        f"Entering function {func.__name__}",
        extra={
            "function": func.__name__,
            "args": sanitized_args,
            "kwargs": sanitized_kwargs,
        },
    )


def log_function_exit(func: Callable, result: Any, duration: float) -> None:
    """Log function exit with result and duration"""
    logger = get_logger(func.__module__)

    # Sanitize result if it's large or contains sensitive data
    if isinstance(result, list | dict) and len(result) > 10:
        result_str = f"[{type(result).__name__} with {len(result)} items]"
    elif isinstance(result, str) and len(result) > 100:
        result_str = f"[string with {len(result)} characters]"
    else:
        result_str = str(result)

    logger.debug(
        f"Exiting function {func.__name__}",
        extra={"function": func.__name__, "result": result_str, "duration": duration},
    )
