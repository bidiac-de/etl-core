from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional, Tuple, List

from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from starlette.config import Config

from fastapi.middleware.cors import CORSMiddleware
from .api.helpers import autodiscover_components
from .api.routers import contexts, execution, jobs, schedules, schemas, setup
from .components.component_registry import RegistryMode, set_registry_mode
from .components.databases.pool_registry import ConnectionPoolRegistry
from .logger.logging_setup import setup_logging
from .scheduling.scheduler_service import SchedulerService
from etl_core.persistence.db import ensure_schema

config = Config(".env")

_EXAMPLE_JOB_ID = "__mongo_example_job__"
_LOG = logging.getLogger("etl_core.main")


def _resolve_registry_mode() -> RegistryMode:
    raw = config("ETL_COMPONENT_MODE", cast=str)
    try:
        return RegistryMode(raw.lower())
    except ValueError:
        return RegistryMode.PRODUCTION


def _resolve_scheduler_sync_seconds() -> Optional[str]:
    return config("ETL_SCHEDULES_SYNC_SECONDS", default=None)


def _resolve_mongo_client_config() -> Tuple[Optional[str], Dict[str, Any]]:
    uri = config("ETL_MONGO_URI", default=None)
    kwargs: Dict[str, Any] = {}
    if uri is None:
        return None, kwargs

    max_pool = config("ETL_MONGO_MAX_POOL_SIZE", cast=int, default=None)
    if max_pool is not None:
        kwargs["maxPoolSize"] = max_pool

    wait_ms = config("ETL_MONGO_WAIT_QUEUE_TIMEOUT_MS", cast=int, default=None)
    if wait_ms is not None:
        kwargs["waitQueueTimeoutMS"] = wait_ms

    return uri, kwargs


def _resolve_example_job_settings() -> (
    Tuple[Optional[int], Optional[str], Optional[str]]
):
    interval = config("ETL_MONGO_EXAMPLE_INTERVAL_SECONDS", cast=int, default=None)
    db_name = config("ETL_MONGO_EXAMPLE_DB", default=None)
    coll_name = config("ETL_MONGO_EXAMPLE_COLLECTION", default=None)
    return interval, db_name, coll_name


async def example_mongo_job(*, app: FastAPI) -> None:
    """Fetch a handful of documents to validate Mongo connectivity."""
    log = logging.getLogger("etl_core.example_jobs.mongo")

    client: Optional[AsyncIOMotorClient] = getattr(app.state, "mongo_client", None)
    if client is None:
        log.debug("Mongo example job skipped: client not configured")
        return

    db_name = getattr(app.state, "example_mongo_db", None)
    coll_name = getattr(app.state, "example_mongo_collection", None)
    if not db_name or not coll_name:
        log.debug("Mongo example job skipped: database/collection missing")
        return

    async with await client.start_session() as session:
        collection = client[db_name][coll_name]
        cursor = collection.find({}, session=session).limit(5)
        try:
            docs = await cursor.to_list(length=5)
        finally:
            await cursor.close()

        log.info(
            "Example MongoDB job fetched %s documents from %s.%s",
            len(docs),
            db_name,
            coll_name,
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    setup_logging(
        default_path=str(
            Path(__file__).with_name("config").joinpath("logging_config.yaml")
        )
    )

    ensure_schema()

    set_registry_mode(_resolve_registry_mode())
    autodiscover_components("etl_core.components")

    scheduler = SchedulerService.instance()
    sync_override = _resolve_scheduler_sync_seconds()
    if sync_override is None:
        scheduler.start()
    else:
        scheduler.start(sync_override)

    uri, client_kwargs = _resolve_mongo_client_config()
    client: Optional[AsyncIOMotorClient] = None
    if uri:
        client = AsyncIOMotorClient(uri, **client_kwargs)
        ConnectionPoolRegistry.instance().register_mongo_client(
            uri=uri,
            client=client,
            client_kwargs=client_kwargs,
        )
        _LOG.info("Mongo client initialised during startup")
    else:
        _LOG.info("Mongo client not configured; skipping initialisation")

    app.state.mongo_client = client
    app.state.mongo_client_uri = uri
    app.state.mongo_client_kwargs = client_kwargs

    interval, db_name, coll_name = _resolve_example_job_settings()
    app.state.example_mongo_db = db_name
    app.state.example_mongo_collection = coll_name

    if client and interval and interval > 0 and db_name and coll_name:
        scheduler.add_internal_job(
            job_id=_EXAMPLE_JOB_ID,
            func=example_mongo_job,
            trigger=IntervalTrigger(seconds=interval),
            kwargs={"app": app},
        )
        _LOG.info(
            "Example Mongo job scheduled every %s seconds for %s.%s",
            interval,
            db_name,
            coll_name,
        )
    else:
        _LOG.debug(
            "Example Mongo job disabled (client missing or configuration incomplete)"
        )

    try:
        # Hand control to the application
        yield
    finally:
        await SchedulerService.instance().shutdown_gracefully()

        client_close: Optional[AsyncIOMotorClient] = getattr(
            app.state, "mongo_client", None
        )
        if client_close is not None:
            client_close.close()
            app.state.mongo_client = None
            _LOG.info("Mongo client closed during shutdown")


app = FastAPI(lifespan=lifespan)

# CORS: handle both simple and preflight requests properly
allowed_origins = _parse_origins(config("CORS_ALLOW_ORIGINS", cast=str, default="*"))

# If "*" is used, credentials cannot be allowed by browsers
allow_credentials = allowed_origins != ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
    max_age=600,  # cache preflight for 10 minutes
)

# Routers
app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)
app.include_router(contexts.router)
app.include_router(schedules.router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("etl_core.main:app", host="127.0.0.1", port=8000, reload=True)
