import os
import yaml
import logging
import logging.config

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
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)