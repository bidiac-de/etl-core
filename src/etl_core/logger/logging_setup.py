import os
import yaml
import logging
import logging.config
from pathlib import Path


def resolve_log_dir() -> Path:
    """Resolve the directory used for log files.

    Prefers `LOG_DIR`, then `ETL_LOG_DIR`, falling back to ``./logs``.
    """

    candidates = (os.getenv("LOG_DIR"), os.getenv("ETL_LOG_DIR"))
    for raw in candidates:
        if raw:
            return Path(raw).expanduser().resolve()
    return Path("logs").resolve()


def setup_logging(
    default_path="logging_config.yaml",
    default_level=logging.INFO,
    env_key="LOG_CFG",
):
    """
    Load logging configuration from YAML file, falling back to basicConfig.
    """
    path = os.getenv(env_key, default_path)
    if os.path.exists(path):
        with open(path, "rt", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        log_dir = resolve_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers = config.get("handlers", {})
        file_handler = handlers.get("file")
        if file_handler and "filename" in file_handler:
            filename = Path(file_handler["filename"]).name
            file_handler["filename"] = str(log_dir / filename)

        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
