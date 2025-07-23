import logging
from src.metrics.job_metrics import JobMetrics


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
    Configures Python logging to write everything to a job-specific logfile,
    and allows the job name to be updated at runtime.
    """

    def __init__(self, job_name: str):
        # create or get a named logger
        self.logger = logging.getLogger(f"job.{job_name}")
        self.logger.setLevel(logging.DEBUG)


    def log(self, information) -> None:
        """
        log a metric object at INFO level
        """
        self.logger.info("%r", information)

    def update_job_name(self, new_job_name: str) -> None:
        """
        Switch this handler to a new job name and logfile
        """

        # reassign logger name & level
        self.logger = logging.getLogger(f"job.{new_job_name}")
        self.logger.setLevel(logging.DEBUG)
