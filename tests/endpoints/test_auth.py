from __future__ import annotations

import os

from fastapi.testclient import TestClient

from etl_core.main import app


def _request_token(client_secret: str, client_id: str | None = None) -> str:
    with TestClient(app) as client:
        response = client.post(
            "/auth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id or os.environ.get("ETL_AUTH_CLIENT_ID"),
                "client_secret": client_secret,
            },
        )
        assert response.status_code == 200, response.text
        return response.json()["access_token"]


def test_token_generation_returns_jwt() -> None:
    token = _request_token(os.environ["ETL_AUTH_CLIENT_SECRET"])
    assert token and token.count(".") == 2


def test_protected_endpoint_rejects_unauthenticated_calls() -> None:
    with TestClient(app) as client:
        response = client.get("/configs/job")
        assert response.status_code == 401


def test_protected_endpoint_accepts_valid_token() -> None:
    token = _request_token(os.environ["ETL_AUTH_CLIENT_SECRET"])
    with TestClient(app) as client:
        client.headers.update({"Authorization": f"Bearer {token}"})
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
