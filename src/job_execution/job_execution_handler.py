from typing import Dict,Any
from pathlib import Path
from src.job_execution.job import Job, JobStatus, JobExecution
from src.components.base import Component, RuntimeState
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler
from src.strategies.bigdata_strategy import BigDataExecutionStrategy
from src.strategies.bulk_strategy import BulkExecutionStrategy
from src.strategies.row_strategy import RowExecutionStrategy
from src.metrics.job_metrics import JobMetrics

import datetime
import concurrent.futures
import logging

log_dir = Path("etl-core/logs")
log_path = log_dir / "execution.log"
logger = logging.getLogger("job.ExecutionHandler")
logger.setLevel(logging.DEBUG)

if not any(isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path)
           for h in logger.handlers):
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)


class JobExecutionHandler:
    """
    Handles the execution of ETL jobs by managing component execution.
    Preserves command pattern while supporting parallel execution.
    """
    def __init__(self, component_registry: Dict[str, type]):
        """
        :param component_registry: dict with all existing component types
        """
        self.component_registry = component_registry
        self.job_information_handler = JobInformationHandler(filepath= Path("etl-core/logs"))
        self.system_metrics_handler = SystemMetricsHandler()

    def create_job(self, config: Dict, user_id: int):
        """
        Create a job based on the provided config.
        :param config: Configuration dictionary for the job, derived from JSON.
        :param user_id: ID of the user creating the job.
        :return: An instance of the job class.
        """
        return Job(config,user_id)

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        """
        Execute a job with support for parallel processing.
        """
        try:
            job.status = JobStatus.RUNNING.value
            job.started_at = datetime.datetime.now()
            exception_count = 0

            components = self.build_components(job.config)

            # Find root components (components with no predecessors)
            roots = [c for c in components.values() if not c.prev_components]

            # Initialize component states
            pending_components = set(components.values())
            completed_components = set()

            # Use ThreadPoolExecutor for parallel execution
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Start with root components
                futures = {}
                for root in roots:
                    futures[executor.submit(self._execute_component, root, None)] = root

                # Process components as they complete
                while futures:
                    done, _ = concurrent.futures.wait(
                        futures,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    for future in done:
                        component = futures.pop(future)
                        try:
                            result = future.result()
                        except Exception:
                            exception_count += 1

                        # Mark as completed
                        pending_components.remove(component)
                        completed_components.add(component)

                        # Find next components to execute
                        for next_component in component.next_components:
                            # Check if all prerequisites are completed
                            prerequisites_met = all(
                                prev in completed_components
                                for prev in next_component.prev_components
                            )

                            if prerequisites_met and next_component in pending_components:
                                # Submit next component for execution
                                futures[executor.submit(
                                    self._execute_component, next_component, result
                                )] = next_component

            # Update job status
            job.status = JobStatus.COMPLETED.value
            job.completed_at = datetime.datetime.now()

            # Build and record JobMetrics:
            job_processing_time = job.completed_at - job.started_at
            lines_processed = sum(c.metrics.lines_received for c in components.values())
            throughput = (lines_processed / job_processing_time.total_seconds()
                          if job_processing_time.total_seconds() > 0 else 0.0)

            jm = JobMetrics(
                started_at=job.started_at,
                processing_time=job_processing_time,
                error_count=exception_count,
                throughput=throughput,
                job_status=job.status
            )
            # store in handler
            self.job_information_handler.metrics_handler.add_job_metrics(job.id, jm)

            # Build and record each components metrics:
            for cname, comp in components.items():
                self.job_information_handler.metrics_handler.add_component_metrics(
                    job.id,
                    cname,
                    comp.metrics
                )

            # Optionally log all metrics to file
            if job.fileLogging:
                self.job_information_handler.logging_handler.log(jm)
                for _, cmetrics in self.job_information_handler.metrics_handler.component_metrics[job.id]:
                    self.job_information_handler.logging_handler.log(cmetrics)

            # preserve JobExecution object
            job_metrics = jm
            component_metrics = {
                cname: comp.metrics
                for cname, comp in components.items()
            }
            job.executions.append(JobExecution(job, job_metrics, component_metrics))

            return job

        except Exception as e:
            logger.exception(f"Job execution failed: {str(e)}")
            job.status = JobStatus.FAILED.value
            job.completed_at = datetime.datetime.now()
            job.error = str(e)
            return job

    def _execute_component(self, component: Component, data: Any) -> Any:
        """
        Execute a single component
        !!note: will only work when component is implemented !!
        :param component: The component to execute.
        :param data: Input data for the component.
        :return: Result of the component execution.
        """
        try:
            logger.info(f"Executing component: {component.name}")
            component.status = RuntimeState.RUNNING

            # Execute the component
            result = component.execute(data)

            component.status = RuntimeState.SUCCESS
            logger.info(f"Component {component.name} completed successfully")
            return result

        except Exception as e:
            logger.exception(f"Component {component.name} failed: {str(e)}")
            component.status = RuntimeState.FAILED
            raise

    def build_components(self, config: Dict):
        components = {}

        # Create components
        for comp_config in config.get("components", []):
            component_type = comp_config.get("type")
            component_name = comp_config.get("name")

            if component_type not in self.component_registry:
                raise ValueError(f"Unknown component type: {component_type}")

            strategy_name = comp_config.get("strategy")
            match strategy_name:
                case "bigdata":
                    strategy = BigDataExecutionStrategy()
                case "row":
                    strategy = RowExecutionStrategy()
                case _:
                    strategy = BulkExecutionStrategy()

            component_class = self.component_registry[component_type]
            component = component_class(
                config=comp_config.get("config", {}),
                strategy=strategy
            )

            components[component_name] = component


        return components
