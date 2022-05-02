import json
import os
import threading
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional
from typing import Tuple

from django.conf import settings
from django.db import connection
from django.db.models import F
from django.utils import timezone

from automation.models import Job

logger = settings.LOGGER


@dataclass
class RunResult:
    success: bool = False
    message: Optional[str] = None
    data: dict = None


class AutomationBase(object):
    # Uniquely identifies a job type
    key = None

    # retry_interval is only for retry cases. Set this to None if no retry is required
    # should be used together with the times_to_retry
    retry_interval = 30
    times_to_retry = 3

    model = None
    model_pk_name = "record_id"

    # The interval in at which this worker repeats
    # When this is not none, the class becomes a cron-like background
    # Task that repeats itself
    repeat_interval: timedelta = None

    def __init__(self, job: Job) -> None:
        self.job = job

    def execute(self):
        # IMPORTANT: Update the status and start time
        Job.objects.filter(pk=self.job.pk).update(
            status=Job.STATUS_RUNNING,
            started_at=timezone.now(),
        )

        try:
            results = self.on_execute()
        except Exception as ex:
            logger.exception(ex)
            results = RunResult(message=f"Error: {ex}")

        new_run_time = self.job.run_at
        run_status = (
            Job.STATUS_COMPLETED if results.success else Job.STATUS_FAILED_WITH_RETRY
        )
        fail_reason = results.message if not results.success else None

        if not results.success:
            logger.warn(
                f"Job ID: {self.job.pk} Type: {self.job.type_key} failed. Process: {os.getpid()} Thread ID: {threading.get_ident()}"
            )

            if self.job.times_executed >= self.times_to_retry:
                run_status = Job.STATUS_FAILED
            else:
                new_run_time = timezone.now() + timedelta(seconds=self.retry_interval)

        # IMPORTANT: Update the status and finish time
        Job.objects.filter(pk=self.job.pk).update(
            status=run_status,
            finished_at=timezone.now(),
            run_at=new_run_time,
            fail_reason=fail_reason,
            times_executed=F("times_executed") + 1,
        )

    def on_execute(self) -> RunResult:
        raise NotImplementedError()

    @classmethod
    def schedule_job(cls, delay: timedelta = None, **kwargs) -> Job:

        delay = delay or timedelta(seconds=1)
        model = cls.model

        metadata = None
        if "metadata" in kwargs:
            metadata = kwargs.pop("metadata")
        else:
            metadata = {}

        if model:
            job_model = model.objects.create(**kwargs)
            metadata[cls.model_pk_name] = job_model.pk

        metadata_json = json.dumps(metadata)
        start_time = timezone.now() + delay

        job = Job.objects.create(
            type_key=cls.key, run_at=start_time, _metadata=metadata_json
        )

        return job


class Worker(AutomationBase):

    # The interval in at which this worker repeats
    # When this is not none, the class becomes a cron-like background
    # Task that repeats itself
    repeat_interval: timedelta = timedelta(minutes=5)

    def execute(self):
        results = super().execute()
        if self.repeat_interval and self.repeat_interval is timedelta:
            self.job.reschedule(timezone.now() + self.repeat_interval)

        return results

    @classmethod
    def start(cls):
        """
        Creates a new instance of this worker Job if none exists already
        """
        if Job.objects.filter(
            type_key=cls.key,
            status__in=(
                Job.STATUS_FAILED_WITH_RETRY,
                Job.STATUS_NEW,
                Job.STATUS_RUNNING,
            ),
        ).exists():
            return

        Worker.schedule_job(cls.repeat_interval)


def _find_processor_class_by_key(job_type_key: str) -> AutomationBase:

    # IMPORTANT: We are lazily loading this module to avoid circular dependency
    from automation.processors.factory import get_all_automation_jobs

    mapping = get_all_automation_jobs()
    return mapping.get(job_type_key)


def initialize_workers():
    # IMPORTANT: We are lazily loading this module to avoid circular dependency
    from automation.processors.factory import get_all_automation_jobs

    workers: Tuple[Worker] = tuple(
        w for w in get_all_automation_jobs().values() if w is Worker
    )
    for worker in workers:
        worker.start()
        logger.info(f"{worker.key} initialized!")

    logger.info(f"{len(workers)} Found and initialized!")


def run_automations():
    from automation.management.commands.run_job import CONFIG

    time_now = timezone.now()
    maximum_jobs = int(CONFIG.get("JOB_BATCH_SIZE", "100"))

    jobs = Job.objects.filter(
        status__in=(Job.STATUS_NEW, Job.STATUS_FAILED_WITH_RETRY),
        run_at__lte=time_now,
    ).order_by("id")[:maximum_jobs]

    for job in jobs:
        JobProcessorClass = _find_processor_class_by_key(job.type_key)
        if JobProcessorClass is None:
            logger.critical(f"Undefined job type key = {job.type_key}")
            continue

        job_processor = JobProcessorClass(job)
        try:
            job_processor.execute()
        except Exception as ex:
            print(ex)
            logger.exception(ex)

    # IMPORTANT: Close connection as django will not do it if we are not in a request
    connection.close()
