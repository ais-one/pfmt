from dataclasses import dataclass

from common.python.config import env_bool, env_int, env_str, get_environment


@dataclass(frozen=True)
class AppSampleSettings:
    service_name: str
    environment: str
    debug: bool
    host: str
    port: int
    api_prefix: str
    api_version: str
    docs_enabled: bool

    @property
    def api_base_path(self) -> str:
        return f"{self.api_prefix}/{self.api_version}"


def get_app_sample_settings() -> AppSampleSettings:
    return AppSampleSettings(
        service_name="app-sample",
        environment=get_environment("APP_SAMPLE_ENV"),
        debug=env_bool("APP_SAMPLE_DEBUG", env_bool("APP_DEBUG", False)),
        host=env_str("APP_SAMPLE_HOST", "127.0.0.1"),
        port=env_int("APP_SAMPLE_PORT", 8000),
        api_prefix=env_str("APP_SAMPLE_API_PREFIX", "/api/sample"),
        api_version=env_str("APP_SAMPLE_API_VERSION", "v1"),
        docs_enabled=env_bool("APP_SAMPLE_DOCS_ENABLED", True),
    )


settings = get_app_sample_settings()
