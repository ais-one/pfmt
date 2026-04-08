import os


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def get_environment(service_variable: str) -> str:
    return os.getenv(service_variable, os.getenv("APP_ENV", "development"))
