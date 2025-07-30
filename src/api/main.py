from fastapi import FastAPI
from src.logging.logging_setup import setup_logging
from src.api.routers.schemas import component_schemas, job_schema
from src.api.routers.schemas import dataclass_schemas

app = FastAPI()
app.include_router(job_schema.router)
app.include_router(component_schemas.router)
app.include_router(dataclass_schemas.router)


if __name__ == "__main__":
    import uvicorn

    setup_logging()

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
