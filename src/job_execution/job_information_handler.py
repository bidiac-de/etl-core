import logging
from src.metrics.job_metrics import JobMetrics
import datetime
from pathlib import Path


class JobInformationHandler:
    """
    Handles job information and metrics for optional logging
    """

    def __init__(self, job_name: str):
        self.metrics_handler = MetricsHandler()
        self.logging_handler = LoggingHandler(job_name)


class MetricsHandler:
    """
    Handles metrics collection and storage
    """

    def __init__(self):
        self.job_metrics = []  # list[JobMetrics]
        self.component_metrics = {}  # dict[job_id, list[ComponentMetrics]]

    def add_job_metrics(self, job_id: str, metrics: JobMetrics):
        self.job_metrics.append((job_id, metrics))

    def add_component_metrics(self, job_id: str, name: str, metrics):
        self.component_metrics.setdefault(job_id, []).append((name, metrics))


class LoggingHandler:
    """
    Writes all logs for one job into its own sub-dir under logs/,
    and creates a fresh timestamped file on each execution
    """

    def __init__(
        self,
        job_name: str,
        base_log_dir: Path = Path("logs"),
    ):
        self.base_log_dir = base_log_dir
        self.job_name = job_name
        self.logger = logging.getLogger(f"job.{job_name}")
        self.logger.setLevel(logging.DEBUG)
        self._configure_handler()

    def _configure_handler(self) -> None:
        """
        (Re)configure the FileHandler for current execution
        """
        # ensure base/job directory exists
        job_dir = self.base_log_dir / self.job_name
        job_dir.mkdir(parents=True, exist_ok=True)

        # timestamped logfile so each run is separate
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = job_dir / f"{ts}.log"

        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt)

        # clear out old handlers so we don't double-log
        for h in list(self.logger.handlers):
            self.logger.removeHandler(h)

        self.logger.addHandler(fh)

    def log(self, information) -> None:
        """
        Log a metric object at INFO level
        """
        self.logger.info("%r", information)

    def update_job_name(self, new_job_name: str) -> None:
        """
        Switch to a new job name (and log directory) and start a new file
        """
        self.job_name = new_job_name
        self.logger = logging.getLogger(f"job.{new_job_name}")
        self.logger.setLevel(logging.DEBUG)
        self._configure_handler()
