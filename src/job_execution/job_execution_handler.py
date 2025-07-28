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

    def execute_job(self, job: Job, max_workers: int = 4) -> Job:
        """
        Execute a given job, specified by the job object
        Components only start when all predecessors have succeeded.
        """
        # high-level start log
        logger.info("Starting execution of job '%s'", job.name)
        # update file handler context
        self.job_information_handler.logging_handler.update_job_name(job.name)
        file_logger = self.job_information_handler.logging_handler.logger

        retry_attempts = 0
        job_execution = JobExecution(job=job, status=JobStatus.RUNNING.value)
        job.executions.append(job_execution)
        started_at = datetime.datetime.now()
        job_execution.started_at = started_at
        job_execution.job_metrics = JobMetrics(
            started_at=started_at, processing_time=0, error_count=0
        )

        # initialize component metrics container
        job_execution.component_metrics = {}

        while retry_attempts <= job.num_of_retries:
            try:
                logger.debug("Attempt %d for job '%s'", retry_attempts + 1, job.name)
                file_logger.debug(
                    "Starting attempt %d for job '%s'", retry_attempts + 1, job.name
                )

                exception_count = 0
                total_lines_processed = 0
                components = job.components
                roots = [c for c in components.values() if not c.prev_components]

                # track states by component ID
                succeeded_ids = set()
                failed_ids = set()
                skipped_ids = set()
                pending_ids = set(components.keys())

                logger.info(
                    "Job '%s': %d root components to schedule", job.name, len(roots)
                )
                file_logger.debug("Root components: %s", [c.name for c in roots])

                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    futures: dict[concurrent.futures.Future, Component] = {}

                    # schedule root components
                    for root in roots:
                        file_logger.debug(
                            "Submitting root component '%s' to executor", root.name
                        )
                        futures[
                            executor.submit(self._execute_component, root, None)
                        ] = root
                        pending_ids.discard(root.id)

                    # process as components finish
                    while futures:
                        done, _ = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.FIRST_COMPLETED
                        )

                        for future in done:
                            component = futures.pop(future)
                            cid = component.id
                            try:
                                result = future.result()
                                component.status = RuntimeState.SUCCESS
                                succeeded_ids.add(cid)
                                file_logger.debug(
                                    "Component '%s' SUCCESS", component.name
                                )
                                try:
                                    job_execution.component_metrics[cid] = (
                                        component.metrics
                                    )
                                    total_lines_processed += (
                                        component.metrics.lines_received
                                    )
                                    job_execution.processing_time = (
                                        datetime.datetime.now() - started_at
                                    )
                                    job_execution.job_metrics.calc_throughput(
                                        total_lines_processed
                                    )
                                except AttributeError:
                                    file_logger.warning(
                                        "Component '%s' has no metrics set",
                                        component.name,
                                    )

                            except Exception:
                                exception_count += 1
                                component.status = RuntimeState.FAILED
                                failed_ids.add(cid)
                                file_logger.error(
                                    "Component '%s' FAILED",
                                    component.name,
                                    exc_info=True,
                                )

                            # evaluate successors
                            for nxt in component.next_components:
                                nid = nxt.id
                                if nid in succeeded_ids | failed_ids | skipped_ids:
                                    continue
                                # check if all predecessors succeeded
                                prev_ids = {p.id for p in nxt.prev_components}
                                if prev_ids.issubset(succeeded_ids):
                                    file_logger.debug(
                                        "All predecessors passed for '%s'", nxt.name
                                    )
                                    futures[
                                        executor.submit(
                                            self._execute_component, nxt, result
                                        )
                                    ] = nxt
                                    pending_ids.discard(nid)

                                # check if any predecessor failed or was skipped
                                elif prev_ids & (failed_ids | skipped_ids):
                                    nxt.status = RuntimeState.SKIPPED
                                    skipped_ids.add(nid)
                                    pending_ids.discard(nid)
                                    file_logger.warning(
                                        "Component '%s' set to SKIPPED", nxt.name
                                    )

                                # predecessors still pending
                                else:
                                    file_logger.debug(
                                        "Component '%s' has pending predecessors",
                                        nxt.name,
                                    )

                # after executor
                # any components still pending must be downstream
                # of a skipped/failed node → skip them
                for pid in list(pending_ids):
                    comp = components[pid]
                    comp.status = RuntimeState.SKIPPED
                    skipped_ids.add(pid)
                    file_logger.warning(
                        "Component '%s' set to SKIPPED (no runnable path)", comp.name
                    )
                pending_ids.clear()
                logger.info(
                    "Execution summary for job '%s': %d succeeded,"
                    " %d failed, %d skipped",
                    job.name,
                    len(succeeded_ids),
                    len(failed_ids),
                    len(skipped_ids),
                )
                file_logger.debug("Succeeded IDs: %s", succeeded_ids)
                file_logger.debug("Failed IDs: %s", failed_ids)
                file_logger.debug("Skipped IDs: %s", skipped_ids)

                if failed_ids:
                    msg = "One or more components failed; dependent components skipped"
                    logger.error(msg)
                    file_logger.error(msg)
                    raise RuntimeError(msg)

                # finalize job success
                job_time = datetime.datetime.now() - started_at
                job_execution.job_metrics.processing_time = job_time
                job_execution.job_metrics.error_count = retry_attempts
                job_execution.job_metrics.status = JobStatus.COMPLETED.value

                logger.info(
                    "Job '%s' completed in %s with %d errors",
                    job.name,
                    job_time,
                    exception_count,
                )
                file_logger.debug("JobMetrics: %s", job_execution.job_metrics)

                # add metrics to job information handler
                self.job_information_handler.metrics_handler.add_job_metrics(
                    job.id, job_execution.job_metrics
                )
                for c_id, comp in components.items():
                    self.job_information_handler.metrics_handler.add_component_metrics(
                        job.id, c_id, comp.metrics
                    )

                # optional file logging of metrics
                if job.file_logging:
                    self.job_information_handler.logging_handler.log(
                        job_execution.job_metrics
                    )
                    for (
                        cm
                    ) in self.job_information_handler.metrics_handler.component_metrics[
                        job.id
                    ]:
                        self.job_information_handler.logging_handler.log(cm)

                job_execution.status = JobStatus.COMPLETED.value
                return job

            except Exception as e:
                retry_attempts += 1
                job_execution.job_metrics.error_count = retry_attempts
                logger.warning(
                    "Attempt %d for job '%s' failed: %s", retry_attempts, job.name, e
                )
                file_logger.warning(
                    "Attempt %d failed with error: %s", retry_attempts, e
                )
                if retry_attempts > job.num_of_retries:
                    logger.exception(
                        "Job '%s' failed after %d attempts", job.name, retry_attempts
                    )
                    file_logger.exception(
                        "Final failure after %d attempts", retry_attempts
                    )
                    job_execution.status = JobStatus.FAILED.value
                    job_execution.job_metrics.status = JobStatus.FAILED.value
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
