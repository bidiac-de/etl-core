from __future__ import annotations

from typing import Any, Dict, List, Optional


class ETLCoreError(Exception):
    """
    Base domain error for ETL Core.

    Carries canonical API metadata so routers/services can raise typed exceptions
    and let the global exception mapper build a stable error envelope.
    """

    default_code = "INTERNAL_ERROR"
    default_http_status = 500
    default_message = "Unexpected server error."

    def __init__(
        self,
        message: Optional[str] = None,
        *,
        code: Optional[str] = None,
        http_status: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
        details: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        resolved_message = message or self.default_message
        super().__init__(resolved_message)
        self.code = code or self.default_code
        self.http_status = http_status or self.default_http_status
        self.message = resolved_message
        self.context = context or {}
        self.details = details or []


class ConfigurationError(ETLCoreError):
    default_code = "CONFIGURATION_ERROR"
    default_http_status = 400
    default_message = "Invalid configuration."


class ContextResolutionError(ETLCoreError, ValueError):
    default_code = "CONTEXT_RESOLUTION_ERROR"
    default_http_status = 400
    default_message = "Failed to resolve context."


class CredentialResolutionError(ETLCoreError, ValueError):
    default_code = "CREDENTIAL_RESOLUTION_ERROR"
    default_http_status = 400
    default_message = "Failed to resolve credentials."


class TemplateResolutionError(ETLCoreError, ValueError):
    default_code = "TEMPLATE_RESOLUTION_ERROR"
    default_http_status = 400
    default_message = "Failed to resolve context template."


class ExecutionConflictError(ETLCoreError):
    default_code = "EXECUTION_CONFLICT"
    default_http_status = 409
    default_message = "Execution conflict."


class ExternalDependencyError(ETLCoreError):
    default_code = "EXTERNAL_DEPENDENCY_ERROR"
    default_http_status = 503
    default_message = "External dependency is unavailable."


class NotFoundError(ETLCoreError):
    default_code = "NOT_FOUND"
    default_http_status = 404
    default_message = "Requested resource was not found."


class ValidationError(ETLCoreError):
    default_code = "VALIDATION_ERROR"
    default_http_status = 422
    default_message = "Validation failed."
