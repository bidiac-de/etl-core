from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import requests

from etl_core.context.context import Context
from etl_core.context.credentials import Credentials
from etl_core.context.credentials_mapping_context import CredentialsMappingContext
from etl_core.context.environment import Environment
from etl_core.context.secure_context_adapter import SecureContextAdapter
from etl_core.context.secrets.keyring_provider import KeyringSecretProvider
from etl_core.persistance.errors import PersistNotFoundError
from etl_core.persistance.handlers.context_handler import ContextHandler
from etl_core.persistance.handlers.credentials_handler import CredentialsHandler
from etl_core.singletons import (
    job_handler as _jh_singleton,
    execution_handler as _eh_singleton,
)
from etl_core.api.cli.ports import ContextsPort, ExecutionPort, JobsPort


class LocalJobsClient(JobsPort):
    def __init__(self) -> None:
        self.job_handler = _jh_singleton()

    def create(self, cfg: Dict[str, Any]) -> str:
        from etl_core.persistance.configs.job_config import JobConfig

        row = self.job_handler.create_job_entry(JobConfig(**cfg))
        return row.id

    def get(self, job_id: str) -> Dict[str, Any]:
        job = self.job_handler.load_runtime_job(job_id)
        data = job.model_dump()
        data["id"] = job.id
        return data

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        from etl_core.persistance.configs.job_config import JobConfig

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


class LocalContextsClient(ContextsPort):
    _DEFAULT_SERVICE = "sep-sose-2025/default"

    def __init__(self) -> None:
        self.ctx_handler = ContextHandler()
        self.creds_handler = CredentialsHandler()

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
        self.creds_handler.upsert(creds,credentials_id=creds_id)
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
        Return a combined list of all provider IDs (contexts + credentials).
        Shape mirrors the HTTP client:
        [{"id": "...","kind": "context"|"credentials"}, ...]
        """
        out: List[Dict[str, Any]] = []

        # These handlers are patched in tests, so keep the calls straightforward.
        for row in self.ctx_handler.list_all():
            # rows are SimpleNamespace or ORM rows with .provider_id
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
            # Provide a minimal response compatible with tests
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


# HTTP adapters


class HttpJobsClient(JobsPort):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def create(self, cfg: Dict[str, Any]) -> str:
        r = requests.post(f"{self.base_url}/jobs", json=cfg, timeout=30)
        r.raise_for_status()
        return r.json()

    def get(self, job_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base_url}/jobs/{job_id}", timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()

    def update(self, job_id: str, cfg: Dict[str, Any]) -> str:
        r = requests.put(f"{self.base_url}/jobs/{job_id}", json=cfg, timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()

    def delete(self, job_id: str) -> None:
        r = requests.delete(f"{self.base_url}/jobs/{job_id}", timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()

    def list_brief(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base_url}/jobs", timeout=30)
        r.raise_for_status()
        return r.json()


class HttpExecutionClient(ExecutionPort):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def start(
        self, job_id: str, environment: Optional[Environment] = None
    ) -> Dict[str, Any]:
        body = {"environment": environment.value} if environment else None
        r = requests.post(f"{self.base_url}/execution/{job_id}", json=body, timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Job {job_id!r} not found")
        r.raise_for_status()
        return r.json()


class HttpContextsClient(ContextsPort):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def create_context(
        self, context: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        payload = {"context": context, "keyring_service": keyring_service}
        r = requests.post(f"{self.base_url}/contexts/context", json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    def create_credentials(
        self, credentials: Dict[str, Any], keyring_service: Optional[str]
    ) -> Dict[str, Any]:
        payload = {"credentials": credentials, "keyring_service": keyring_service}
        r = requests.post(
            f"{self.base_url}/contexts/credentials", json=payload, timeout=30
        )
        r.raise_for_status()
        return r.json()

    def create_context_mapping(self, mapping_ctx: Dict[str, Any]) -> Dict[str, Any]:
        payload = {"context": mapping_ctx}
        r = requests.post(
            f"{self.base_url}/contexts/context-mapping", json=payload, timeout=30
        )
        r.raise_for_status()
        return r.json()

    def list_providers(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.base_url}/contexts/", timeout=30)
        r.raise_for_status()
        return r.json()

    def get_provider(self, provider_id: str) -> Dict[str, Any]:
        r = requests.get(f"{self.base_url}/contexts/{provider_id}", timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Provider '{provider_id}' not found")
        r.raise_for_status()
        return r.json()

    def delete_provider(self, provider_id: str) -> None:
        r = requests.delete(f"{self.base_url}/contexts/{provider_id}", timeout=30)
        if r.status_code == 404:
            raise PersistNotFoundError(f"Provider '{provider_id}' not found")
        r.raise_for_status()
