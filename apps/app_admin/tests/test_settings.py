from apps.app_admin.app.settings import get_app_admin_settings


def test_app_admin_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("APP_ADMIN_ENV", "staging")
    monkeypatch.setenv("APP_ADMIN_DEBUG", "true")
    monkeypatch.setenv("APP_ADMIN_MODE_ENABLED", "false")
    monkeypatch.setenv("APP_ADMIN_API_VERSION", "v2")

    settings = get_app_admin_settings()

    assert settings.environment == "staging"
    assert settings.debug is True
    assert settings.admin_mode_enabled is False
    assert settings.api_base_path == "/api/admin/v2"
