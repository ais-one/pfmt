from dataclasses import dataclass

from common.python.config import env_bool, env_int, env_str, get_environment


@dataclass(frozen=True)
class AppAdminSettings:
    service_name: str
    environment: str
    debug: bool
    host: str
    port: int
    api_prefix: str
    api_version: str
    admin_mode_enabled: bool

    @property
    def api_base_path(self) -> str:
        return f"{self.api_prefix}/{self.api_version}"


def get_app_admin_settings() -> AppAdminSettings:
    return AppAdminSettings(
        service_name="app-admin",
        environment=get_environment("APP_ADMIN_ENV"),
        debug=env_bool("APP_ADMIN_DEBUG", env_bool("APP_DEBUG", False)),
        host=env_str("APP_ADMIN_HOST", "127.0.0.1"),
        port=env_int("APP_ADMIN_PORT", 8001),
        api_prefix=env_str("APP_ADMIN_API_PREFIX", "/api/admin"),
        api_version=env_str("APP_ADMIN_API_VERSION", "v1"),
        admin_mode_enabled=env_bool("APP_ADMIN_MODE_ENABLED", True),
    )


settings = get_app_admin_settings()
