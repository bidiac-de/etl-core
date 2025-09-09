from __future__ import annotations

from contextlib import asynccontextmanager
from starlette.config import Config


from fastapi import FastAPI, Request

from .logger.logging_setup import setup_logging
from .api.routers import schemas, setup, jobs, execution, contexts
from .api.helpers import autodiscover_components
from .components.component_registry import (
    RegistryMode,
    set_registry_mode,
)

config = Config(".env")  # loads env and .env file if present


def _resolve_registry_mode() -> RegistryMode:
    raw = config("ETL_COMPONENT_MODE", cast=str)
    try:
        return RegistryMode(raw.lower())
    except ValueError:
        return RegistryMode.PRODUCTION


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging()

    set_registry_mode(_resolve_registry_mode())

    # Ensure all components are registered under the chosen mode
    autodiscover_components("etl_core.components")
    yield


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def _add_access_control_allow_origin(request: Request, call_next):
    """
    Add 'Access-Control-Allow-Origin: *' to every response without changing
    endpoint behavior. If the header is already present (e.g., from another
    middleware), we leave it untouched.
    """
    response = await call_next(request)
    if response.headers.get("Access-Control-Allow-Origin") is None:
        response.headers["Access-Control-Allow-Origin"] = "*"
    return response


app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)
app.include_router(contexts.router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("etl_core.main:app", host="127.0.0.1", port=8000, reload=True)
