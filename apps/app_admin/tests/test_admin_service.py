from apps.app_admin.app.main import app
from apps.app_admin.app.settings import settings
from common.python import api_url, create_test_client

client = create_test_client(app)


def test_healthcheck() -> None:
    response = client.get(api_url(settings.api_base_path, "/health"))

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "app-admin",
        "environment": "development",
    }


def test_service_info() -> None:
    response = client.get(api_url(settings.api_base_path, "/info"))

    assert response.status_code == 200
    assert response.json() == {
        "service": "app-admin",
        "environment": "development",
        "debug": False,
        "host": "127.0.0.1",
        "port": 8001,
        "api_base_path": "/api/admin/v1",
    }


def test_echo_message() -> None:
    response = client.post(api_url(settings.api_base_path, "/echo"), json={"message": "hello-admin"})

    assert response.status_code == 200
    assert response.json() == {
        "service": "app-admin",
        "environment": "development",
        "message": "hello-admin",
    }
