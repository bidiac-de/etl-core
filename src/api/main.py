from fastapi import FastAPI
from src.logging.logging_setup import setup_logging
from src.api.routers import schemas
from src.api.routers import setup
from src.api.helpers import autodiscover_components

autodiscover_components("src.components")
app = FastAPI()
app.include_router(schemas.router)
app.include_router(setup.router)

if __name__ == "__main__":
    import uvicorn

    setup_logging()

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
