from __future__ import annotations

import json
from pathlib import Path

from etl_core.context.context import Context
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials

from etl_core.job_execution.job_execution_handler import JobExecutionHandler
from etl_core.components.runtime_state import RuntimeState
from tests.helpers import runtime_job_from_config

from etl_core.components.databases.mongodb.mongodb_write import MongoDBWrite  # noqa: F401
from etl_core.components.databases.mongodb.mongodb_read import MongoDBRead  # noqa: F401
from etl_core.components.data_operations.schema_mapping.schema_mapping_component import SchemaMappingComponent  # noqa: F401

BASE = Path(__file__).parent
NESTED_TO_NESTED = BASE / "mongo_row_nested_to_nested.json"
FLAT_TO_NESTED_UPSERT = BASE / "mongo_bulk_flat_to_nested_upsert.json"
NESTED_TO_FLAT = BASE / "mongo_bulk_nested_to_flat.json"
JOIN = BASE / "mongo_bulk_join_people_orders.json"


def make_mongo_context(*, cred_id: int = 9001) -> Context:
    """
    MongoDB Context with Credentials.
    Placeholders need to be replaced with real values.
    Auth DB can be set on components in JSON if different from data DB.
    Password and User can also be left empty to test no-auth connections.
    """

    host = "placeholder_host"
    port = 27017
    database = "placeholder_db"
    user = "placeholder_user"
    password = "placeholder_password"

    creds = Credentials(
        credentials_id=cred_id,
        name="mongo_user_placeholder",
        user=user,
        host=host,
        port=port,
        database=database,  # data DB
        password=password or None,
    )

    ctx = Context(
        id=1,
        name="mongodb_test_ctx",
        environment=Environment.DEV,
        parameters={},
    )
    ctx.add_credentials(creds)

    return ctx


def run_job_from_json(cfg_path: Path, cred_id: int = 9001) -> RuntimeState:
    """
    Load a job config, build the runtime job, set context on DB components if needed,
    and execute with JobExecutionHandler.
    """
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    job = runtime_job_from_config(cfg)

    ctx = make_mongo_context(cred_id=cred_id)

    for comp in getattr(job, "components", []):
        if hasattr(comp, "credentials_id") and hasattr(comp, "context"):
            comp.context = ctx

    exec_handler = JobExecutionHandler()
    run = exec_handler.execute_job(job)
    status = exec_handler.job_info.metrics_handler.get_job_metrics(run.id).status
    print(f"Job '{cfg.get('name')}' finished with status: {status}")
    return status


if __name__ == "__main__":

    # Swap between the different job configs to test them
    run_job_from_json(FLAT_TO_NESTED_UPSERT)
