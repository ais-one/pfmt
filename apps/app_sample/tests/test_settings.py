from apps.app_sample.app.settings import get_app_sample_settings


def test_app_sample_settings_from_env(monkeypatch) -> None:
    monkeypatch.setenv("APP_SAMPLE_ENV", "test")
    monkeypatch.setenv("APP_SAMPLE_DEBUG", "true")
    monkeypatch.setenv("APP_SAMPLE_PORT", "8010")
    monkeypatch.setenv("APP_SAMPLE_API_PREFIX", "/api/custom-sample")

    settings = get_app_sample_settings()

    assert settings.environment == "test"
    assert settings.debug is True
    assert settings.port == 8010
    assert settings.api_base_path == "/api/custom-sample/v1"
