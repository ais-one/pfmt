from fastapi import FastAPI
from fastapi.testclient import TestClient


def create_test_client(app: FastAPI) -> TestClient:
    return TestClient(app)


def api_url(base_path: str, route: str) -> str:
    normalized_base = base_path.rstrip("/")
    normalized_route = route if route.startswith("/") else f"/{route}"
    return f"{normalized_base}{normalized_route}"
