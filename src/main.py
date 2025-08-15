from fastapi import FastAPI

from .logging.logging_setup import setup_logging

app = FastAPI()


if __name__ == "__main__":
    import uvicorn

    setup_logging()

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
