import datetime
import logging
import concurrent.futures
from typing import Any

from src.job_execution.job import Job, JobStatus
from src.components.base_component import Component, RuntimeState
from src.job_execution.job import JobExecution
from src.metrics.job_metrics import JobMetrics
from src.job_execution.job_information_handler import JobInformationHandler
from src.metrics.system_metrics import SystemMetricsHandler

logger = logging.getLogger("job.ExecutionHandler")


class JobExecutionHandler:
    """
    Handles the execution of ETL jobs by managing component execution
    Preserves command pattern while supporting parallel execution
    """

    def __init__(self):
        """
        Initialize the JobExecutionHandler with the component registry and logging
        """
        self.job_information_handler = JobInformationHandler(job_name="no_job_assigned")
        self.system_metrics_handler = SystemMetricsHandler()

    def create_job(self, config: dict, user_id: int):
        """
        Create a job based on the provided config
        :param config: Configuration dictionary for the job, derived from JSON
        :param user_id: ID of the user creating the job
        :return: An instance of the job class
        """
        return Job(config, user_id)

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        """
        Execute a given job, specified by the job object
        """
        self.job_information_handler.logging_handler.update_job_name(job.name)
        retry_attempts = 0
        job_execution = JobExecution(job=job, status=JobStatus.RUNNING.value)
        job.executions.append(job_execution)
        started_at = datetime.datetime.now()

        while retry_attempts <= job.num_of_retries:
            try:
                exception_count = 0

                components = job.components
                roots = [c for c in components.values() if not c.prev_components]

                pending_components = list(components.values())
                completed_components = []
                failed_components = []
                skipped_components = []

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
                                failed_components.append(component)
                                component.status = RuntimeState.FAILED

                            if component in pending_components:
                                pending_components.remove(component)
                            completed_components.append(component)

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
                                    skipped_components.append(next_component)
                                    if next_component in pending_components:
                                        pending_components.remove(next_component)
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

                status = JobStatus.COMPLETED.value
                completed_at = datetime.datetime.now()
                job_processing_time = completed_at - started_at
                lines_processed = sum(
                    c.metrics.lines_received for c in components.values()
                )
                throughput = (
                    lines_processed / job_processing_time.total_seconds()
                    if job_processing_time.total_seconds() > 0
                    else 0.0
                )

                jm = JobMetrics(
                    started_at=started_at,
                    processing_time=job_processing_time,
                    error_count=exception_count,
                    throughput=throughput,
                    job_status=status,
                )

                self.job_information_handler.metrics_handler.add_job_metrics(job.id, jm)
                for cname, comp in components.items():
                    self.job_information_handler.metrics_handler.add_component_metrics(
                        job.id, cname, comp.metrics
                    )

                if job.file_logging:
                    self.job_information_handler.logging_handler.log(jm)
                    for (
                        _,
                        cmetrics,
                    ) in self.job_information_handler.metrics_handler.component_metrics[
                        job.id
                    ]:
                        self.job_information_handler.logging_handler.log(cmetrics)

                job_execution.job_metrics = jm
                job_execution.component_metrics = {
                    cname: comp.metrics for cname, comp in components.items()
                }
                job_execution.status = status
                job_execution.completed_at = completed_at
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
                    job_execution.status = JobStatus.FAILED.value
                    job_execution.completed_at = datetime.datetime.now()
                    job_execution.error = str(e)
                    return job

    def _execute_component(self, component: Component, data: Any) -> Any:
        """
        Execute a single component
        !!note: will only work when component is a concrete class !!
        :param component: The component to execute
        :param data: Input data for the component
        :return: Result of the component execution
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
