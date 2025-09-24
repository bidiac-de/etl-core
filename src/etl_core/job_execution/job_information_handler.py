import datetime
import logging
from pathlib import Path
from typing import Dict, Tuple, Type, Union, Optional

from etl_core.logger.logging_setup import resolve_log_dir
from etl_core.metrics.component_metrics.component_metrics import ComponentMetrics
from etl_core.metrics.execution_metrics import ExecutionMetrics


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
    Single source of truth for every metrics object.
    """

    def __init__(self) -> None:
        self._job_metrics: Dict[str, ExecutionMetrics] = {}
        self._component_metrics: Dict[Tuple[str, str, str], ComponentMetrics] = {}

    def create_job_metrics(self, execution_id: str) -> ExecutionMetrics:
        """
        Lazily create or return the JobMetrics for the given execution.
        """
        if execution_id not in self._job_metrics:
            self._job_metrics[execution_id] = ExecutionMetrics()
        return self._job_metrics[execution_id]

    def get_job_metrics(self, execution_id: str) -> ExecutionMetrics:
        """
        Retrieve existing JobMetrics; KeyError if missing.
        """
        return self._job_metrics[execution_id]

    def create_component_metrics(
        self,
        execution_id: str,
        attempt_id: str,
        component_id: str,
        metrics_cls: Type[ComponentMetrics] = ComponentMetrics,
    ) -> ComponentMetrics:
        """
        Lazily create or return the metrics object for this component.
        Allows passing a subclass of ComponentMetrics for concrete components.
        """
        key = (execution_id, attempt_id, component_id)
        if key not in self._component_metrics:
            self._component_metrics[key] = metrics_cls()
        return self._component_metrics[key]

    def get_comp_metrics(
        self,
        execution_id: str,
        attempt_id: str,
        component_id: str,
    ) -> ComponentMetrics:
        """
        Retrieve existing ComponentMetrics; KeyError if missing.
        """
        return self._component_metrics[(execution_id, attempt_id, component_id)]

    def all_job_metrics(self) -> Dict[str, ExecutionMetrics]:
        """
        Access all recorded job metrics.
        """
        return dict(self._job_metrics)

    def all_comp_metrics(self) -> Dict[Tuple[str, str, str], ComponentMetrics]:
        """
        Access all recorded component metrics.
        """
        return dict(self._component_metrics)


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
        base_log_dir: Optional[Union[Path, str]] = None,
    ):
        resolved_base = (
            resolve_log_dir()
            if base_log_dir is None
            else Path(base_log_dir).expanduser()
        )
        self._base_log_dir = resolved_base
        self._job_name = job_name
        self._logger = logging.getLogger(f"job.{job_name}")
        self._logger.setLevel(logging.DEBUG)
        self._handler_configured = False

    def _ensure_configured(self) -> None:
        if not self._handler_configured:
            self._configure_handler()

    def _configure_handler(self) -> None:
        """
        (Re)configure the FileHandler for current execution
        """
        job_dir = self._base_log_dir / self.job_name
        job_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = job_dir / f"{ts}.log"

        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt)

        for h in self._logger.handlers:
            self._logger.removeHandler(h)

        self._logger.addHandler(fh)
        self._handler_configured = True

    def log(self, information) -> None:
        """
        Log a metric object at INFO level
        """
        self._ensure_configured()
        self._logger.info("%r", information)

    def update_job_name(self, new_job_name: str) -> None:
        """
        Switch to a new job name (and log directory) and start a new file
        """
        self._job_name = new_job_name
        self._logger = logging.getLogger(f"job.{new_job_name}")
        self._logger.setLevel(logging.DEBUG)
        self._handler_configured = False
        self._configure_handler()

    @property
    def base_log_dir(self) -> Path:
        return self._base_log_dir

    @base_log_dir.setter
    def base_log_dir(self, value: Union[Path, str]) -> None:
        self._base_log_dir = Path(value).expanduser()
        self._handler_configured = False
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
