from __future__ import annotations

import pytest
import requests

from etl_core.api.cli.adapters import _RestBase, PersistNotFoundError


def _build_response(
    status_code: int,
    url: str,
    method: str = "GET",
    reason: str | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response.reason = reason
    response._content = b""  # type: ignore[attr-defined]
    response.request = requests.Request(method=method, url=url).prepare()
    return response


def test_raise_for_status_404_translates_to_persist_not_found() -> None:
    client = _RestBase("https://api.example.com")
    response = _build_response(
        404,
        "https://user:pass@example.com/resource?id=42",
        reason="Not Found",
    )

    with pytest.raises(PersistNotFoundError) as excinfo:
        client._raise_for_status(response)

    message = str(excinfo.value)
    assert "Resource not found" in message
    assert "GET https://***@example.com/resource" in message


def test_raise_for_status_wraps_http_error_with_sanitized_url() -> None:
    client = _RestBase("https://api.example.com")
    response = _build_response(
        500,
        "https://example.com/path?token=secret",
        reason="Internal Server Error",
    )

    with pytest.raises(requests.HTTPError) as excinfo:
        client._raise_for_status(response)

    error = excinfo.value
    assert error.response is response
    assert error.request is response.request
    assert str(error) == "500 Internal Server Error for GET https://example.com/path"
