from fastapi import FastAPI

from src.logging.logging_setup import setup_logging
from src.api.routers.contexts import router as contexts_router

app = FastAPI()
app.include_router(contexts_router)


if __name__ == "__main__":
    import uvicorn

    setup_logging()

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
