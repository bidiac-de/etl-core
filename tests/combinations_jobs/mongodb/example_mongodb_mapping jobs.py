from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials
from etl_core.persistence.handlers.credentials_handler import CredentialsHandler
from etl_core.persistence.handlers.context_handler import ContextHandler
from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from tests.helpers import runtime_job_from_config  # noqa: E402
from etl_core.api.helpers import autodiscover_components

BASE = Path(__file__).parent

CFG_ROW_NESTED_TO_NESTED = BASE / "mongo_row_nested_to_nested.json"
CFG_BULK_FLAT_TO_NESTED_UPSERT = BASE / "mongo_bulk_flat_to_nested_upsert.json"
CFG_BULK_NESTED_TO_FLAT = BASE / "mongo_bulk_nested_to_flat.json"
CFG_BULK_JOIN = BASE / "mongo_bulk_join_people_orders.json"

# Placeholder token used inside the JSON files
CONTEXT_ID_TOKEN = "${MONGO_EXAMPLE_CONTEXT_ID}"


def _persist_credentials_for_env() -> Credentials:
    """Persist credentials set to your MongoDB instance"""
    host = "placeholder_host"
    port = 27017
    database = "placeholder_db"
    user = "placeholder_user"
    password = "placeholder_password"

    creds = Credentials(
        credentials_id=str(uuid4()),
        name="mongo_example_creds",
        user=user,
        host=host,
        port=port,
        database=database,
        password=password or None,
        pool_max_size=10,
        pool_timeout_s=30,
    )
    CredentialsHandler().upsert(provider_id=creds.credentials_id, creds=creds)
    return creds


def _create_mapping_context(
    *, env: Environment, credentials_id: str, name: str = "mongo_example_ctx"
) -> str:
    """Create a mapping context (env --> credentials_id)."""
    provider_id = str(uuid4())
    ContextHandler().upsert_credentials_mapping_context(
        provider_id=provider_id,
        name=name,
        environment=env.value,
        mapping_env_to_credentials_id={env.value: credentials_id},
    )
    return provider_id


def _load_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as fh:
        return fh.read()


def _load_config_with_context(path: Path, context_id: str) -> Dict[str, Any]:
    """
    Read the JSON file as text, replace the placeholder with the real context_id,
    then parse JSON. The JSON must already contain the placeholder.
    """
    raw = _load_text(path)
    if CONTEXT_ID_TOKEN not in raw:
        raise RuntimeError(
            f"File '{path.name}' does not contain the context placeholder "
            f"{CONTEXT_ID_TOKEN}. Please update your config to include it."
        )
    materialized = raw.replace(CONTEXT_ID_TOKEN, context_id)
    return json.loads(materialized)


def run_job_from_json(cfg_path: Path, env: Environment = Environment.TEST) -> None:
    """
    Persist credentials --> create mapping context --> materialize config --> run job.
    """
    creds = _persist_credentials_for_env()
    ctx_id = _create_mapping_context(env=env, credentials_id=creds.credentials_id)
    cfg = _load_config_with_context(cfg_path, context_id=ctx_id)

    job = runtime_job_from_config(cfg)
    exec_handler = JobExecutionHandler()
    run_info = exec_handler.execute_job(job, environment=env)
    status = exec_handler.job_info.metrics_handler.get_job_metrics(run_info.id).status

    print(f"Job {cfg.get('name', cfg_path.name)}, finished with status: {status}")


if __name__ == "__main__":

    autodiscover_components("etl_core.components")
    # Run one of the example jobs with your own MongoDB credentials:
    run_job_from_json(CFG_BULK_FLAT_TO_NESTED_UPSERT)
