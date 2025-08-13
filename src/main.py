from fastapi import FastAPI
from contextlib import asynccontextmanager

from .logger.logging_setup import setup_logging
from .api.routers import schemas, setup, jobs, execution
from .api.helpers import autodiscover_components
from .persistance.handlers.job_handler import JobHandler
from .job_execution.job_execution_handler import JobExecutionHandler

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    autodiscover_components("src.components")

    # Create exactly one instance of each handler for the whole app
    app.state.job_handler = JobHandler()
    app.state.execution_handler = JobExecutionHandler()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(schemas.router)
app.include_router(setup.router)
app.include_router(jobs.router)
app.include_router(execution.router)


if __name__ == "__main__":
    import uvicorn

    setup_logging()

    uvicorn.run("src.main:app", host="127.0.0.1", port=8000, reload=True)
