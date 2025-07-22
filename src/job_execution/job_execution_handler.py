from typing import Dict, Any
from pathlib import Path

from src.job_execution.job import Job, JobStatus, JobExecution
from src.components.base_component import Component, RuntimeState
from src.components.schemas import ComponentInfo
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler
from src.metrics.job_metrics import JobMetrics

import datetime
import concurrent.futures
import logging

log_dir = Path("etl-core/logs")
log_path = log_dir / "execution.log"
logger = logging.getLogger("job.ExecutionHandler")
logger.setLevel(logging.DEBUG)

if not any(
    isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path)
    for h in logger.handlers
):
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
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
        :param component_registry: dict with all existing concrete component types
        """
        self.component_registry = component_registry
        self.job_information_handler = JobInformationHandler(
            filepath=Path("etl-core/logs"), job_name="no_job_assigned"
        )
        self.system_metrics_handler = SystemMetricsHandler()

    def create_job(self, config: Dict, user_id: int):
        """
        Create a job based on the provided config.
        :param config: Configuration dictionary for the job, derived from JSON.
        :param user_id: ID of the user creating the job.
        :return: An instance of the job class.
        """
        return Job(config, user_id)

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        """
        Execute a given job, specified by the job object
        """
        self.job_information_handler.logging_handler.update_job_name(job.name)
        retry_attempts = 0

        while retry_attempts <= job.num_of_retries:
            try:
                job.status = JobStatus.RUNNING.value
                job.started_at = datetime.datetime.now()
                exception_count = 0

                components = self.build_components(job.config)
                roots = [c for c in components.values() if not c.prev_components]

                pending_components = set(components.values())
                completed_components = set()
                failed_components = set()
                skipped_components = set()

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    futures = {}
                    for root in roots:
                        futures[
                            executor.submit(self._execute_component, root, None)
                        ] = root

                    while futures:
                        done, _ = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.FIRST_COMPLETED
                        )

                        for future in done:
                            component = futures.pop(future)
                            try:
                                result = future.result()
                                component.status = RuntimeState.SUCCESS
                            except Exception:
                                exception_count += 1
                                failed_components.add(component)
                                component.status = RuntimeState.FAILED

                            pending_components.discard(component)
                            completed_components.add(component)

                            for next_component in component.next_components:
                                if (
                                    next_component in completed_components
                                    or next_component in skipped_components
                                ):
                                    continue

                                # Check if any predecessor failed or was skipped
                                if any(
                                    p.status
                                    in {RuntimeState.FAILED, RuntimeState.SKIPPED}
                                    for p in next_component.prev_components
                                ):
                                    next_component.status = RuntimeState.SKIPPED
                                    skipped_components.add(next_component)
                                    pending_components.discard(next_component)
                                    continue

                                # Check if all predecessors are completed
                                prerequisites_met = all(
                                    p in completed_components
                                    for p in next_component.prev_components
                                )
                                if (
                                    prerequisites_met
                                    and next_component in pending_components
                                ):
                                    futures[
                                        executor.submit(
                                            self._execute_component,
                                            next_component,
                                            result,
                                        )
                                    ] = next_component

                # Mark job as completed or failed based on state
                if failed_components or skipped_components:
                    raise RuntimeError(
                        "One or more components failed or were skipped "
                        "due to dependency failure."
                    )

                job.status = JobStatus.COMPLETED.value
                job.completed_at = datetime.datetime.now()
                job_processing_time = job.completed_at - job.started_at
                lines_processed = sum(
                    c.metrics.lines_received for c in components.values()
                )
                throughput = (
                    lines_processed / job_processing_time.total_seconds()
                    if job_processing_time.total_seconds() > 0
                    else 0.0
                )

                jm = JobMetrics(
                    started_at=job.started_at,
                    processing_time=job_processing_time,
                    error_count=exception_count,
                    throughput=throughput,
                    job_status=job.status,
                )

                self.job_information_handler.metrics_handler.add_job_metrics(job.id, jm)
                for cname, comp in components.items():
                    self.job_information_handler.metrics_handler.add_component_metrics(
                        job.id, cname, comp.metrics
                    )

                if job.fileLogging:
                    self.job_information_handler.logging_handler.log(jm)
                    for (
                        _,
                        cmetrics,
                    ) in self.job_information_handler.metrics_handler.component_metrics[
                        job.id
                    ]:
                        self.job_information_handler.logging_handler.log(cmetrics)

                job.executions.append(
                    JobExecution(
                        job,
                        jm,
                        {cname: comp.metrics for cname, comp in components.items()},
                    )
                )
                return job

            except Exception as e:
                retry_attempts += 1
                logger.warning(
                    f"Attempt {retry_attempts} for job '{job.name}' failed: {e}"
                )
                if retry_attempts > job.num_of_retries:
                    logger.exception(
                        f"Job execution failed after "
                        f"{retry_attempts} attempts: {str(e)}"
                    )
                    job.status = JobStatus.FAILED.value
                    job.completed_at = datetime.datetime.now()
                    job.error = str(e)
                    return job

    def _execute_component(self, component: Component, data: Any) -> Any:
        """
        Execute a single component
        !!note: will only work when component is a concrete class !!
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
        """
        Build and connects components from the provided job configuration
        """
        components = {}

        # Create components
        for comp_config in config["components"]:
            info = ComponentInfo.parse_obj(comp_config)
            comp_type = info.type

            if comp_type not in self.component_registry:
                raise ValueError(f"Unknown component type: {comp_type}")

            component_class = self.component_registry[comp_type]
            # Create the component instance by unpacking the info dict
            component = component_class(**info.dict())

            components[info.name] = component

        # build component relationships
        for comp_conf in config["components"]:
            src = components[comp_conf["name"]]
            for next_comp in comp_conf.get("next", []):
                src.add_next(components[next_comp])
                components[next_comp].add_prev(src)

        return components
