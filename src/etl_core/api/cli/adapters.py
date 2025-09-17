from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4
from urllib.parse import urlencode, urlsplit, urlunsplit

from etl_core.context.context import Context
from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import Environment
from etl_core.context.secure_context_adapter import SecureContextAdapter
from etl_core.context.secrets.keyring_provider import KeyringSecretProvider
from etl_core.persistence.errors import PersistNotFoundError
from etl_core.singletons import (
    job_handler as _jh_singleton,
    execution_handler as _eh_singleton,
    execution_records_handler as _erh_singleton,
    context_handler as _ch_singleton,
    credentials_handler as _crh_singleton,
)
from etl_core.api.cli.ports import ContextsPort, ExecutionPort, JobsPort
import requests


class LocalJobsClient(JobsPort):
    def __init__(self) -> None:
        self.job_handler = _jh_singleton()

    def create(self, cfg: Dict[str, Any]) -> str:
        from etl_core.persistence.configs.job_config import JobConfig

        row = self.job_handler.create_job_entry(JobConfig(**cfg))
        return row.id

    def get(self, job_id: str) -> Dict[str, Any]:
        job = self.job_handler.load_runtime_job(job_id)
        data = job.model_dump()
        data["id"] = job.id
        return data

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        from etl_core.persistence.configs.job_config import JobConfig

        row = self.job_handler.update(job_id, JobConfig(**cfg))
        return row.id

    def delete(self, job_id: str) -> None:
        self.job_handler.delete(job_id)

    def list_brief(self) -> List[Dict[str, Any]]:
        return self.job_handler.list_jobs_brief()


class LocalExecutionClient(ExecutionPort):
    def __init__(self) -> None:
        self.exec_handler = _eh_singleton()
        self.jobs = LocalJobsClient()
        self._records = _erh_singleton()

    def start(
        self, job_id: str, environment: Optional[Environment] = None
    ) -> Dict[str, Any]:
        runtime_job = self.jobs.job_handler.load_runtime_job(job_id)
        execution = self.exec_handler.execute_job(runtime_job, environment=environment)
        return {
            "job_id": job_id,
            "status": "started",
            "execution_id": execution.id,
            "max_attempts": execution.max_attempts,
            "environment": environment.value if environment else None,
        }

    @staticmethod
    def _row_to_exec_out(row: Any) -> Dict[str, Any]:
        return {
            "id": row.id,
            "job_id": row.job_id,
            "environment": row.environment,
            "status": row.status,
            "error": row.error,
            "started_at": row.started_at.isoformat(),
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }

    @staticmethod
    def _row_to_attempt_out(row: Any) -> Dict[str, Any]:
        return {
            "id": row.id,
            "execution_id": row.execution_id,
            "attempt_index": row.attempt_index,
            "status": row.status,
            "error": row.error,
            "started_at": row.started_at.isoformat(),
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        }

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
    ) -> Dict[str, Any]:
        sa = None
        sb = None
        if started_after:
            sa = self._parse_dt(started_after)
        if started_before:
            sb = self._parse_dt(started_before)

        rows, _ = self._records.list_executions(
            job_id=job_id,
            status=status,
            environment=environment,
            started_after=sa,
            started_before=sb,
            sort_by=sort_by,
            order=order,
            limit=limit,
            offset=offset,
        )
        return {"data": [self._row_to_exec_out(r) for r in rows]}

    def get(self, execution_id: str) -> Dict[str, Any]:
        row, attempts = self._records.get_execution(execution_id)
        if row is None:
            raise PersistNotFoundError(f"Execution {execution_id!r} not found")
        return {
            "execution": self._row_to_exec_out(row),
            "attempts": [self._row_to_attempt_out(a) for a in attempts],
        }

    def attempts(self, execution_id: str) -> List[Dict[str, Any]]:
        row, _ = self._records.get_execution(execution_id)
        if row is None:
            raise PersistNotFoundError(f"Execution {execution_id!r} not found")
        rows = self._records.list_attempts(execution_id)
        return [self._row_to_attempt_out(r) for r in rows]

    # keep tiny and readable; no heavy parsing here
    @staticmethod
    def _parse_dt(value: str):
        from datetime import datetime

        return datetime.fromisoformat(value)


class LocalContextsClient(ContextsPort):
    _DEFAULT_SERVICE = "sep-sose-2025/default"

    def __init__(self) -> None:
        self.ctx_handler = _ch_singleton()
        self.creds_handler = _crh_singleton()

    def _secret_store(self, override: Optional[str]) -> KeyringSecretProvider:
        service = override or self._DEFAULT_SERVICE
        return KeyringSecretProvider(service=service)

    @staticmethod
    def _non_secure_params(ctx: Context) -> Dict[str, Any]:
        return {k: p.value for k, p in ctx.parameters.items() if not p.is_secure}

    def create_context(
        self, context: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        ctx = Context(**context)
        context_id = str(uuid4())

        store = self._secret_store(keyring_service)
        SecureContextAdapter(
            provider_id=context_id,
            secret_store=store,
            context=ctx,
        ).bootstrap_to_store()

        non_secure = self._non_secure_params(ctx)
        secure_keys = [k for k, p in ctx.parameters.items() if p.is_secure]
        self.ctx_handler.upsert(
            context_id=context_id,
            name=ctx.name,
            environment=ctx.environment.value,
            non_secure_params=non_secure,
            secure_param_keys=secure_keys,
        )
        return {
            "id": context_id,
            "kind": "context",
            "environment": ctx.environment.value,
            "parameters_registered": len(secure_keys),
        }

    def create_credentials(
        self, credentials: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        creds = Credentials(**credentials)
        creds_id = str(uuid4())

        store = self._secret_store(keyring_service)
        SecureContextAdapter(
            provider_id=creds_id,
            secret_store=store,
            credentials=creds,
        ).bootstrap_to_store()

        creds.password = None
        self.creds_handler.upsert(creds, credentials_id=creds_id)
        return {
            "id": creds_id,
            "kind": "credentials",
            "environment": None,
            "parameters_registered": 1,
        }

    def create_context_mapping(self, mapping_ctx: Dict[str, Any]) -> Dict[str, Any]:
        cmc = CredentialsMappingContext(**mapping_ctx)
        context_id = str(uuid4())

        unknown = [
            cid
            for cid in cmc.credentials_ids.values()
            if self.creds_handler.get_by_id(cid) is None
        ]
        if unknown:
            raise PersistNotFoundError(
                f"Unknown credentials provider_id(s): {', '.join(sorted(unknown))}"
            )

        mapping = {}
        for env, cid in cmc.credentials_ids.items():
            mapping[env] = cid

        self.ctx_handler.upsert_credentials_mapping_context(
            context_id=context_id,
            name=cmc.name,
            environment=cmc.environment.value,
            mapping_env_to_credentials_id=mapping,
        )
        return {
            "id": context_id,
            "kind": "context",
            "environment": cmc.environment.value,
            "parameters_registered": len(mapping),
        }

    def list_providers(self) -> List[Dict[str, Any]]:
        """
        Return a combined list of all IDs (contexts + credentials).
        [{"id": "...","kind": "context"|"credentials"}, ...]
        """
        out: List[Dict[str, Any]] = []

        # These handlers are patched in tests, so keep the calls straightforward.
        for row in self.ctx_handler.list_all():
            # rows are SimpleNamespace or ORM rows with .id
            out.append({"id": row.id, "kind": "context"})
        for row in self.creds_handler.list_all():
            out.append({"id": row.id, "kind": "credentials"})

        return out

    def get_provider(self, provider_id: str) -> Dict[str, Any]:
        """
        Lookup a provider: prefer context, else credentials.
        Raise 404-style error if missing.
        """
        ctx_row = self.ctx_handler.get_by_id(provider_id)
        if ctx_row is not None:
            return {
                "id": provider_id,
                "kind": "context",
                "environment": getattr(ctx_row, "environment", None),
            }

        cred_row = self.creds_handler.get_by_id(provider_id)
        if cred_row is not None:
            return {"id": provider_id, "kind": "credentials"}

        raise PersistNotFoundError(f"Provider '{provider_id}' not found")

    def delete_provider(self, provider_id: str) -> None:
        """
        Best-effort delete on both handlers; swallow 'not found' on either.
        Tests assert both are attempted.
        """
        try:
            self.ctx_handler.delete_by_id(provider_id)
        except PersistNotFoundError:
            pass

        try:
            self.creds_handler.delete_by_id(provider_id)
        except PersistNotFoundError:
            pass


def _dedupe(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        pid = str(it.get("id"))
        if pid in seen:
            continue
        seen.add(pid)
        out.append(it)
    return out


def api_base_url() -> Optional[str]:
    """
    Resolve API base URL for CLI to talk to a running server.

    Reads ETL_API_BASE_URL; returns a sanitized base (no trailing slash).
    Returns None if not configured, signaling use of local clients.
    """
    raw = os.getenv("ETL_API_BASE_URL")
    if not raw:
        return None
    return raw.rstrip("/")


class _RestBase:
    def __init__(self, base_url: str) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()

    @staticmethod
    def _sanitize_url(raw_url: str | None) -> str:
        if not raw_url:
            return "<unknown>"
        try:
            parsed = urlsplit(raw_url)
        except ValueError:
            return "<invalid-url>"

        netloc = parsed.netloc
        if "@" in netloc:
            host = netloc.split("@", 1)[1]
            netloc = f"***@{host}"

        sanitized = parsed._replace(netloc=netloc, query="", fragment="")
        return urlunsplit(sanitized)

    def _raise_for_status(self, resp: requests.Response) -> None:
        sanitized_url = self._sanitize_url(getattr(resp.request, "url", None))
        method = getattr(resp.request, "method", "UNKNOWN")

        if resp.status_code == 404:
            raise PersistNotFoundError(
                f"Resource not found: {method} {sanitized_url}"
            )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - network errors rarely hit
            message = (
                f"{resp.status_code} {resp.reason} "
                f"for {method} {sanitized_url}"
            )
            raise requests.HTTPError(
                message,
                response=resp,
                request=resp.request,
            ) from exc


class RemoteJobsClient(_RestBase, JobsPort):
    def create(self, cfg: Dict[str, Any]) -> str:
        r = self.session.post(f"{self.base}/jobs/", json=cfg)
        self._raise_for_status(r)
        return r.json()

    def get(self, job_id: str) -> Dict[str, Any]:
        r = self.session.get(f"{self.base}/jobs/{job_id}")
        self._raise_for_status(r)
        return r.json()

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        r = self.session.put(f"{self.base}/jobs/{job_id}", json=cfg)
        self._raise_for_status(r)
        return r.json()

    def delete(self, job_id: str) -> None:
        r = self.session.delete(f"{self.base}/jobs/{job_id}")
        self._raise_for_status(r)

    def list_brief(self) -> List[Dict[str, Any]]:
        r = self.session.get(f"{self.base}/jobs/")
        self._raise_for_status(r)
        return r.json()


class RemoteExecutionClient(_RestBase, ExecutionPort):
    def start(
        self, job_id: str, environment: Optional[Environment] = None
    ) -> Dict[str, Any]:
        body = {"environment": environment.value} if environment else None
        r = self.session.post(f"{self.base}/execution/{job_id}", json=body)
        self._raise_for_status(r)
        return r.json()

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
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "sort_by": sort_by,
            "order": order,
            "limit": limit,
            "offset": offset,
        }
        if job_id:
            params["job_id"] = job_id
        if status:
            params["status"] = status
        if environment:
            params["environment"] = environment
        if started_after:
            params["started_after"] = started_after
        if started_before:
            params["started_before"] = started_before
        query = urlencode(params)
        r = self.session.get(f"{self.base}/execution/executions?{query}")
        self._raise_for_status(r)
        return r.json()

    def get(self, execution_id: str) -> Dict[str, Any]:
        r = self.session.get(f"{self.base}/execution/executions/{execution_id}")
        self._raise_for_status(r)
        return r.json()

    def attempts(self, execution_id: str) -> List[Dict[str, Any]]:
        r = self.session.get(
            f"{self.base}/execution/executions/{execution_id}/attempts"
        )
        self._raise_for_status(r)
        return r.json()


class RemoteContextsClient(_RestBase, ContextsPort):
    def create_context(
        self, context: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        payload = {"context": context, "keyring_service": keyring_service}
        r = self.session.post(f"{self.base}/contexts/context", json=payload)
        self._raise_for_status(r)
        return r.json()

    def create_credentials(
        self, credentials: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        payload = {"credentials": credentials, "keyring_service": keyring_service}
        r = self.session.post(f"{self.base}/contexts/credentials", json=payload)
        self._raise_for_status(r)
        return r.json()

    def create_context_mapping(self, mapping_ctx: Dict[str, Any]) -> Dict[str, Any]:
        payload = {"context": mapping_ctx}
        r = self.session.post(f"{self.base}/contexts/context-mapping", json=payload)
        self._raise_for_status(r)
        return r.json()

    def list_providers(self) -> List[Dict[str, Any]]:
        r = self.session.get(f"{self.base}/contexts/")
        self._raise_for_status(r)
        return r.json()

    def get_provider(self, provider_id: str) -> Dict[str, Any]:
        r = self.session.get(f"{self.base}/contexts/{provider_id}")
        self._raise_for_status(r)
        return r.json()

    def delete_provider(self, provider_id: str) -> None:
        r = self.session.delete(f"{self.base}/contexts/{provider_id}")
        self._raise_for_status(r)
