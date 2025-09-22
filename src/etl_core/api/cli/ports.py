from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol

from etl_core.context.environment import Environment


class JobsPort(Protocol):
    def create(self, cfg: Dict[str, Any]) -> str: ...

    def get(self, job_id: str) -> Dict[str, Any]: ...

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str: ...

    def delete(self, job_id: str) -> None: ...

    def list_brief(self) -> List[Dict[str, Any]]: ...


class ExecutionPort(Protocol):
    def start(
        self, job_id: str, environment: Optional[Environment] = None
    ) -> Dict[str, Any]: ...

    def list_executions(
        self,
        *,
        job_id: Optional[str] = None,
        status: Optional[str] = None,
        environment: Optional[str] = None,
        started_after: Optional[str] = None,
        started_before: Optional[str] = None,
        sort_by: str = "started_at",
        order: str = "desc",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]: ...

    def get(self, execution_id: str) -> Dict[str, Any]: ...

    def attempts(self, execution_id: str) -> List[Dict[str, Any]]: ...


class ContextsPort(Protocol):
    def create_context(
        self, context: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]: ...

    def create_credentials(
        self, credentials: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]: ...

    def create_context_mapping(self, mapping_ctx: Dict[str, Any]) -> Dict[str, Any]: ...

    def list_providers(self) -> List[Dict[str, Any]]: ...

    def get_provider(self, provider_id: str) -> Dict[str, Any]: ...

    def delete_provider(self, provider_id: str) -> None: ...
