"""Tests for HTTP error handling functions."""

import pytest
from fastapi import HTTPException, status
from etl_core.api.http_errors import _payload, http_404, http_409, http_422, http_500


class TestPayload:
    """Tests for the _payload helper function."""

    def test_payload_basic(self):
        """Test basic payload creation."""
        result = _payload("TEST_CODE", "Test message")
        expected = {"code": "TEST_CODE", "message": "Test message"}
        assert result == expected

    def test_payload_with_extras(self):
        """Test payload creation with extra parameters."""
        result = _payload("TEST_CODE", "Test message", field1="value1", field2=42)
        expected = {
            "code": "TEST_CODE",
            "message": "Test message",
            "field1": "value1",
            "field2": 42,
        }
        assert result == expected

    def test_payload_empty_extra(self):
        """Test payload creation with no extra parameters."""
        result = _payload("CODE", "Message")
        expected = {"code": "CODE", "message": "Message"}
        assert result == expected


class TestHttpExceptions:
    """Tests for HTTP exception functions."""

    def test_http_404_basic(self):
        """Test basic 404 exception creation."""
        exc = http_404("NOT_FOUND", "Resource not found")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_404_NOT_FOUND
        assert exc.detail == {"code": "NOT_FOUND", "message": "Resource not found"}

    def test_http_404_with_extras(self):
        """Test 404 exception with extra fields."""
        exc = http_404("NOT_FOUND", "Resource not found", resource_id="123", type="job")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_404_NOT_FOUND
        assert exc.detail == {
            "code": "NOT_FOUND",
            "message": "Resource not found",
            "resource_id": "123",
            "type": "job",
        }

    def test_http_409_basic(self):
        """Test basic 409 exception creation."""
        exc = http_409("CONFLICT", "Resource already exists")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_409_CONFLICT
        assert exc.detail == {"code": "CONFLICT", "message": "Resource already exists"}

    def test_http_409_with_extras(self):
        """Test 409 exception with extra fields."""
        exc = http_409("CONFLICT", "Duplicate entry", duplicate_field="name")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_409_CONFLICT
        assert exc.detail == {
            "code": "CONFLICT",
            "message": "Duplicate entry",
            "duplicate_field": "name",
        }

    def test_http_422_basic(self):
        """Test basic 422 exception creation."""
        exc = http_422("VALIDATION_ERROR", "Invalid input data")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert exc.detail == {"code": "VALIDATION_ERROR", "message": "Invalid input data"}

    def test_http_422_with_extras(self):
        """Test 422 exception with extra fields."""
        exc = http_422("VALIDATION_ERROR", "Invalid input", field="email", expected="valid email")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert exc.detail == {
            "code": "VALIDATION_ERROR",
            "message": "Invalid input",
            "field": "email",
            "expected": "valid email",
        }

    def test_http_500_basic(self):
        """Test basic 500 exception creation."""
        exc = http_500("INTERNAL_ERROR", "An unexpected error occurred")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert exc.detail == {"code": "INTERNAL_ERROR", "message": "An unexpected error occurred"}

    def test_http_500_with_extras(self):
        """Test 500 exception with extra fields."""
        exc = http_500("INTERNAL_ERROR", "Database error", operation="insert", table="jobs")
        
        assert isinstance(exc, HTTPException)
        assert exc.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert exc.detail == {
            "code": "INTERNAL_ERROR",
            "message": "Database error",
            "operation": "insert",
            "table": "jobs",
        }


class TestAllHttpErrors:
    """Integration tests for all HTTP error functions."""

    def test_all_status_codes_are_different(self):
        """Test that all HTTP error functions return different status codes."""
        exc_404 = http_404("CODE", "Message")
        exc_409 = http_409("CODE", "Message")
        exc_422 = http_422("CODE", "Message")
        exc_500 = http_500("CODE", "Message")
        
        status_codes = [exc_404.status_code, exc_409.status_code, exc_422.status_code, exc_500.status_code]
        assert len(set(status_codes)) == 4  # All should be unique

    def test_all_preserve_payload_structure(self):
        """Test that all HTTP error functions preserve payload structure."""
        code = "TEST_CODE"
        message = "Test message"
        extra_field = "extra_value"
        
        exceptions = [
            http_404(code, message, extra=extra_field),
            http_409(code, message, extra=extra_field),
            http_422(code, message, extra=extra_field),
            http_500(code, message, extra=extra_field),
        ]
        
        expected_detail = {"code": code, "message": message, "extra": extra_field}
        
        for exc in exceptions:
            assert exc.detail == expected_detail