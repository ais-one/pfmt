import inspect
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def __init__(self):
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with detailed information"""

        # Get the caller information
        frame_info = inspect.currentframe()
        try:
            # Go up the call stack to find the actual caller
            caller_frame = frame_info.f_back.f_back
            filename = Path(caller_frame.f_code.co_filename).name
            lineno = caller_frame.f_lineno
            func_name = caller_frame.f_code.co_name
        except (AttributeError, TypeError):
            # Fallback if we can't get caller info
            filename = getattr(record, "filename", "unknown")
            lineno = record.lineno
            func_name = record.funcName
        finally:
            del frame_info

        # Create structured log data
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "source": {"file": filename, "function": func_name, "line": lineno},
            "module": record.module,
            "thread": record.thread,
            "thread_name": record.threadName,
            "process": record.process,
            "pathname": record.pathname,
        }

        # Add extra fields if they exist
        if hasattr(record, "extra_data"):
            log_data["extra_data"] = record.extra_data

        if hasattr(record, "query"):
            log_data["query"] = record.query

        if hasattr(record, "duration"):
            log_data["duration_ms"] = round(record.duration * 1000, 2)

        if hasattr(record, "correlation_id"):
            log_data["correlation_id"] = record.correlation_id

        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id

        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id

        if hasattr(record, "exception"):
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info else None,
                "message": str(record.exc_info[1]) if record.exc_info else None,
                "traceback": self.formatException(record.exc_info)
                if record.exc_info
                else None,
            }

        # Add traceback if present
        if record.exc_info:
            log_data["traceback"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class PerformanceFilter(logging.Filter):
    """Filter to add performance timing information"""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add duration to log record if performance decorator is used"""
        if hasattr(record, "start_time"):
            record.duration = time.time() - record.start_time
        return True


def setup_logging(log_level: str = "INFO", log_to_file: bool = False) -> None:
    """Setup centralized logging configuration"""

    # Convert string level to logging level
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    formatter = JSONFormatter()
    performance_filter = PerformanceFilter()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(performance_filter)
    root_logger.addHandler(console_handler)

    # File handler (optional)
    if log_to_file:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Main file handler
        file_handler = logging.FileHandler(
            log_dir / "app.log", encoding="utf-8", mode="a"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(performance_filter)
        root_logger.addHandler(file_handler)

        # Error file handler specifically for errors and exceptions
        error_handler = logging.FileHandler(
            log_dir / "error.log", encoding="utf-8", mode="a"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        error_handler.addFilter(performance_filter)
        root_logger.addHandler(error_handler)

    # Set up specific loggers
    setup_specific_loggers()


def setup_specific_loggers() -> None:
    """Setup specific loggers with appropriate levels"""

    # SQL Alchemy logger (for query logging)
    sqlalchemy_logger = logging.getLogger("sqlalchemy.engine")
    sqlalchemy_logger.setLevel(logging.DEBUG if __debug__ else logging.INFO)

    # Apply our JSON formatter to SQLAlchemy logs
    for handler in sqlalchemy_logger.handlers:
        handler.setFormatter(JSONFormatter())

    # SQL Alchemy connection pool logger
    connection_logger = logging.getLogger("sqlalchemy.pool")
    connection_logger.setLevel(logging.WARNING)

    # Apply our JSON formatter to connection logs
    for handler in connection_logger.handlers:
        handler.setFormatter(JSONFormatter())

    # FastAPI logger
    fastapi_logger = logging.getLogger("fastapi")
    fastapi_logger.setLevel(logging.WARNING)

    # Apply our JSON formatter to FastAPI logs
    for handler in fastapi_logger.handlers:
        handler.setFormatter(JSONFormatter())

    # Uvicorn logger
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.setLevel(logging.WARNING)

    # Apply our JSON formatter to Uvicorn logs
    for handler in uvicorn_logger.handlers:
        handler.setFormatter(JSONFormatter())

    # SQLAlchemy ORM logger
    orm_logger = logging.getLogger("sqlalchemy.orm")
    orm_logger.setLevel(logging.WARNING)

    # Apply our JSON formatter to ORM logs
    for handler in orm_logger.handlers:
        handler.setFormatter(JSONFormatter())

    # Suppress overly verbose loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("multipart").setLevel(logging.WARNING)

    # Apply JSON formatter to any remaining handlers
    for handler in logging.getLogger().handlers:
        if not isinstance(handler.formatter, JSONFormatter):
            handler.setFormatter(JSONFormatter())


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name"""
    return logging.getLogger(name)


def log_query(
    logger: logging.Logger, query: str, params: dict[str, Any] | None = None
) -> None:
    """Log a database query with parameters"""
    extra = {"query": query, "params": params or {}}
    logger.debug(f"Executing query: {query}", extra=extra)


def log_performance(logger: logging.Logger, operation: str) -> callable:
    """Decorator to log performance timing for operations"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Starting operation: {operation}")

            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f"Completed operation: {operation}", extra={"duration": duration}
                )
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"Failed operation: {operation}",
                    extra={"duration": duration, "exception": str(e)},
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator


def set_correlation_id(correlation_id: str | None = None) -> str:
    """Generate and set correlation ID for request tracing"""
    import uuid

    cid = correlation_id or str(uuid.uuid4())

    # Store in thread-local for request duration
    import threading

    if not hasattr(threading.local(), "correlation_id"):
        threading.local().correlation_id = cid

    return cid


def get_correlation_id() -> str | None:
    """Get current correlation ID"""
    import threading

    return getattr(threading.local(), "correlation_id", None)


class ContextFilter(logging.Filter):
    """Filter to add contextual information to logs"""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID and other context to log record"""
        correlation_id = get_correlation_id()
        if correlation_id:
            record.correlation_id = correlation_id
        return True


# Initialize the logging configuration
setup_logging()
