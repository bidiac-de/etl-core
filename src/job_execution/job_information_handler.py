import logging
from pathlib import Path
from src.metrics.job_metrics import JobMetrics

class JobInformationHandler:
    """
    Handles job information and metrics for optional logging
    """
    def __init__(self, filepath:Path, job_name: str):
        self.metrics_handler = MetricsHandler()
        self.logging_handler = LoggingHandler(filepath, job_name)

class MetricsHandler:
    """
    Handles metrics collection and storage
    """
    def __init__(self):
        self.job_metrics = []            # list[JobMetrics]
        self.component_metrics = {}      # dict[job_id, list[ComponentMetrics]]

    def add_job_metrics(self, job_id: str, metrics: JobMetrics):
        self.job_metrics.append((job_id, metrics))

    def add_component_metrics(self, job_id: str, name: str, metrics):
        self.component_metrics.setdefault(job_id, []).append((name, metrics))

class LoggingHandler:
    """
    Configures Python logging to write everything to a job-specific logfile,
    and allows the job name to be updated at runtime.
    """
    def __init__(self, base_path: Path, job_name: str):
        self.base_path = base_path
        self.job_name = job_name

        # create or get a named logger
        self.logger = logging.getLogger(f"job.{job_name}")
        self.logger.setLevel(logging.DEBUG)

        # ensure single FileHandler per logger
        self._add_file_handler()

    def _add_file_handler(self) -> None:
        log_path = Path(self.base_path) / f"{self.job_name}.log"

        # avoid duplicate handlers
        existing = [
            h for h in self.logger.handlers
            if isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path)
        ]
        if existing:
            return

        fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        fmt = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(fmt)
        self.logger.addHandler(fh)

    def log(self, information) -> None:
        """
        log a metric object at INFO level
        """
        self.logger.info("%r", information)

    def update_job_name(self, job_name: str) -> None:
        """
        Switch this handler to a new job name (and logfile).
        """
        # remove all existing FileHandlers in this Handler
        for h in list(self.logger.handlers):
            if isinstance(h, logging.FileHandler):
                h.close()
                self.logger.removeHandler(h)

        self.job_name = job_name

        # reassign logger name & level
        self.logger = logging.getLogger(f"job.{job_name}")
        self.logger.setLevel(logging.DEBUG)

        # add a fresh FileHandler
        self._add_file_handler()
