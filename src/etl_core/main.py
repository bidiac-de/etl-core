from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from starlette.config import Config

from fastapi import FastAPI

from pathlib import Path
from .logger.logging_setup import setup_logging
from .api.routers import schemas, setup, jobs, execution, contexts, schedules
from .api.helpers import autodiscover_components
from .components.component_registry import (
    RegistryMode,
    set_registry_mode,
)
from .scheduling.scheduler_service import SchedulerService
from etl_core.persistence.db import ensure_schema

config = Config(".env")  # loads env and .env file if present


def _resolve_registry_mode() -> RegistryMode:
    raw = config("ETL_COMPONENT_MODE", cast=str)
    try:
        return RegistryMode(raw.lower())
    except ValueError:
        return RegistryMode.PRODUCTION


def _resolve_scheduler_sync_seconds() -> Optional[str]:
    return config("ETL_SCHEDULES_SYNC_SECONDS", default=None)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # ensure we load project logging config and respect LOG_DIR for file output
    setup_logging(
        default_path=str(
            Path(__file__).with_name("config").joinpath("logging_config.yaml")
        )
    )

    ensure_schema()

    set_registry_mode(_resolve_registry_mode())

    # Ensure all components are registered under the chosen mode
    autodiscover_components("etl_core.components")
    # Start scheduler and load jobs
    sync_override = _resolve_scheduler_sync_seconds()
    if sync_override is None:
        SchedulerService.instance().start()
    else:
        SchedulerService.instance().start(sync_override)
    yield
    # Graceful shutdown: pause and wait for running jobs
    await SchedulerService.instance().shutdown_gracefully()


app = FastAPI(lifespan=lifespan)
app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)
app.include_router(contexts.router)
app.include_router(schedules.router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("etl_core.main:app", host="127.0.0.1", port=8000, reload=True)
