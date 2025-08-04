import logging
from src.metrics.job_metrics import JobMetrics
import datetime
from pathlib import Path


class JobInformationHandler:
    """
    Handles job information and metrics for optional logging
    """

    _metrics_handler: "MetricsHandler"
    _logging_handler: "LoggingHandler"

    def __init__(self, job_name: str):
        self._metrics_handler = MetricsHandler()
        self._logging_handler = LoggingHandler(job_name)

    @property
    def metrics_handler(self) -> "MetricsHandler":
        return self._metrics_handler

    @property
    def logging_handler(self) -> "LoggingHandler":
        return self._logging_handler


class MetricsHandler:
    """
    Handles metrics collection and storage
    """

    _job_metrics: list
    _component_metrics: dict  # job_id -> list of (name, metrics)

    def __init__(self):
        self._job_metrics = []
        self._component_metrics = {}

    @property
    def job_metrics(self) -> list:
        return self._job_metrics

    @property
    def component_metrics(self) -> dict:
        return self._component_metrics

    def add_job_metrics(self, job_id: str, metrics: JobMetrics):
        self._job_metrics.append((job_id, metrics))

    def add_component_metrics(self, job_id: str, name: str, metrics):
        self._component_metrics.setdefault(job_id, []).append((name, metrics))


class LoggingHandler:
    """
    Writes all logs for one job into its own sub-dir under logs/,
    and creates a fresh timestamped file on each execution
    """

    _base_log_dir: Path
    _job_name: str
    _logger: logging.Logger

    def __init__(
        self,
        job_name: str,
        base_log_dir: Path = Path("logs"),
    ):
        self._base_log_dir = base_log_dir
        self._job_name = job_name
        self._logger = logging.getLogger(f"job.{job_name}")
        self._logger.setLevel(logging.DEBUG)
        self._configure_handler()

    def _configure_handler(self) -> None:
        """
        (Re)configure the FileHandler for current execution
        """
        # ensure base/job directory exists
        job_dir = self._base_log_dir / self.job_name
        job_dir.mkdir(parents=True, exist_ok=True)

        # timestamped logfile so each run is separate
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = job_dir / f"{ts}.log"

        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt)

        # clear out old handlers so we don't double-log
        for h in list(self._logger.handlers):
            self._logger.removeHandler(h)

        self._logger.addHandler(fh)

    def log(self, information) -> None:
        """
        Log a metric object at INFO level
        """
        self._logger.info("%r", information)

    def update_job_name(self, new_job_name: str) -> None:
        """
        Switch to a new job name (and log directory) and start a new file
        """
        self._job_name = new_job_name
        self._logger = logging.getLogger(f"job.{new_job_name}")
        self._logger.setLevel(logging.DEBUG)
        self._configure_handler()

    @property
    def base_log_dir(self) -> Path:
        return self._base_log_dir

    @base_log_dir.setter
    def base_log_dir(self, value: Path) -> None:
        if not isinstance(value, Path):
            raise ValueError("base_log_dir must be a Path object")
        self._base_log_dir = value
        self._configure_handler()

    @property
    def job_name(self) -> str:
        return self._job_name

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @logger.setter
    def logger(self, value: logging.Logger) -> None:
        if not isinstance(value, logging.Logger):
            raise ValueError("logger must be an instance of logging.Logger")
        self._logger = value
        self._configure_handler()
