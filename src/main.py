from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .logger.logging_setup import setup_logging
from .api.routers import schemas, setup, jobs, execution
from .api.helpers import autodiscover_components
from .components.component_registry import (
    RegistryMode,
    set_registry_mode,
)


def _resolve_registry_mode() -> RegistryMode:
    raw = os.getenv("ETL_COMPONENT_MODE", RegistryMode.PRODUCTION.value).lower()
    try:
        return RegistryMode(raw)
    except ValueError:
        # Fall back safely if misconfigured
        return RegistryMode.PRODUCTION


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging()

    set_registry_mode(_resolve_registry_mode())

    # Ensure all components are registered under the chosen mode
    autodiscover_components("src.components")
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.main:app", host="127.0.0.1", port=8000, reload=True)
