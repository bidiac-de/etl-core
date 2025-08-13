from src.main import app
from fastapi.testclient import TestClient


client = TestClient(app)


def test_setup_valid_key():
    response = client.get("/setup", params={"key": "VALID_KEY"})
    assert response.status_code == 200
    assert response.json() is True


def test_setup_invalid_key():
    response = client.get("/setup", params={"key": "INVALID"})
    assert response.status_code == 200
    assert response.json() is False
