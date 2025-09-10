from __future__ import annotations

from contextlib import asynccontextmanager
from typing import List

from starlette.config import Config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .logger.logging_setup import setup_logging
from .api.routers import schemas, setup, jobs, execution, contexts
from .api.helpers import autodiscover_components
from .components.component_registry import RegistryMode, set_registry_mode

config = Config(".env")  # loads env and .env file if present


def _resolve_registry_mode() -> RegistryMode:
    raw = config("ETL_COMPONENT_MODE", cast=str)
    try:
        return RegistryMode(raw.lower())
    except ValueError:
        return RegistryMode.PRODUCTION


def _parse_origins(raw: str | None) -> List[str]:
    """
    Accept comma-separated origins from env. Use "*" to allow all origins.
    Examples:
      CORS_ALLOW_ORIGINS="http://localhost:5173,http://127.0.0.1:5173"
      CORS_ALLOW_ORIGINS="*"
    """
    if not raw or raw.strip() == "*":
        return ["*"]
    items = [p.strip() for p in raw.split(",")]
    return [x for x in items if x]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging()
    set_registry_mode(_resolve_registry_mode())
    # Ensure all components are registered under the chosen mode
    autodiscover_components("etl_core.components")
    yield


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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("etl_core.main:app", host="127.0.0.1", port=8000, reload=True)
