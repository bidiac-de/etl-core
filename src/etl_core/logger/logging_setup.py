import os
import yaml
import logging
import logging.config
from pathlib import Path


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

        # Resolve file handler path to configured log directory
        log_dir = Path(os.getenv(" ", "logs")).expanduser().resolve()
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers = config.get("handlers", {})
        file_handler = handlers.get("file")
        if file_handler and "filename" in file_handler:
            filename = Path(file_handler["filename"]).name
            file_handler["filename"] = str(log_dir / filename)

        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
