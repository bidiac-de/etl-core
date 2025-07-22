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
    Configures Python logging to write everything to a job-specific logfile
    """

    def __init__(self, base_path: Path, job_name: str):
        # Build the logfile path
        log_path = Path(base_path) / f"{job_name}.log"

        # Create or get a named logger for this job
        self.logger = logging.getLogger(f"job.{job_name}")
        self.logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers if re‐instantiated
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path)
                   for h in self.logger.handlers):
            # Create and configure the FileHandler
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            fmt = logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            fh.setFormatter(fmt)
            self.logger.addHandler(fh)

    def log(self, information):
        """
        log a metric object at INFO level
        """
        self.logger.info("%r", information)