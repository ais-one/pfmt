from apps.app_sample.app.main import app
from apps.app_sample.app.settings import settings
from common.python import api_url, create_test_client

client = create_test_client(app)


def test_healthcheck() -> None:
    response = client.get(api_url(settings.api_base_path, "/health"))

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "app-sample",
        "environment": "development",
    }


def test_echo_message() -> None:
    response = client.post(
        api_url(settings.api_base_path, "/echo"), json={"message": "hello"}
    )

    assert response.status_code == 200
    assert response.json() == {
        "service": "app-sample",
        "environment": "development",
        "message": "hello",
    }
