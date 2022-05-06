import importlib
import json
import os
import pkgutil
import threading
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Dict, Optional, cast
from typing import Tuple

from django.conf import settings
from django.db import connection
from django.db.models import F
from django.utils import timezone

from .models import Job

logger = settings.LOGGER
job_package = getattr(settings,'JOBS_PACKAGE','jobs')

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
        if self.job.pk:
            self.job.refresh_from_db()
            
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

        if self.job.pk:
            self.job.refresh_from_db()

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

    # Silent workers will not record their runs in the database
    # When this flag is True, the database logs are suppressed
    run_silent = False

    @classmethod
    def next_run(cls):
        return timezone.now() + cls.repeat_interval

    def execute(self):
        super().execute()
        if (self.job.status in (Job.STATUS_COMPLETED, Job.STATUS_FAILED) 
            and not self.run_silent and self.repeat_interval 
            and isinstance(self.repeat_interval,timedelta)
        ):
            self.job.repeat_schedule(self.next_run())


    @classmethod
    def start(cls):
        """
        Creates a new instance of this worker Job if none exists already
        """

        if cls.run_silent or Job.objects.filter(
            type_key=cls.key,
            status__in=(
                Job.STATUS_FAILED_WITH_RETRY,
                Job.STATUS_NEW,
                Job.STATUS_RUNNING,
            ),
        ).exists():
            return

        cls.schedule_job(cls.next_run()-timezone.now())


class WorkerEngine:
    def __init__(self) -> None:
        self.schedules:Dict[Worker, datetime] = {}
        self.jobs = {}

    def __subclasses_recursive(self, cls):
        direct = cls.__subclasses__()
        indirect = []
        for subclass in direct:
            indirect.extend(self.__subclasses_recursive(subclass))
        return set(direct + indirect)

    def __get_all_automation_jobs(self, path:str, parent=""):
        for (_, name, ispkg) in pkgutil.iter_modules([path]):
            if ispkg:
                self.__get_all_automation_jobs(os.path.join(path, name), parent=os.path.basename(path))
            else:
                packs = []
                if parent:
                    packs.append(parent)
                packs =[*packs, os.path.basename(path), name]
                module_name = ".".join(packs)
                importlib.import_module(module_name, __package__)
        
        job_clasess = self.__subclasses_recursive(AutomationBase)
        return {cls.key:cls for cls in job_clasess if cls.key}


    def _find_processor_class_by_key(self,job_type_key: str) -> AutomationBase:
        return self.jobs.get(job_type_key)

    def initialize_jobs(self,job_package=job_package) -> Dict[str,AutomationBase]:
        pkg_dir = os.path.join(settings.BASE_DIR, job_package)
        self.jobs = self.__get_all_automation_jobs(pkg_dir)
        self.initialize_workers()

    def initialize_workers(self):
        workers: Tuple[Worker] = tuple(
            w for w in self.jobs.values() if issubclass(w,Worker)
        )
        for worker in workers:
            worker.start()
            logger.info(f"{worker.key} initialized!")

        logger.info(f"{len(workers)} Found and initialized!")


    def __run_silent_workers(self):
        silent_workers:Tuple[Worker] = tuple(job for job in self.jobs.values() 
            if issubclass(job,Worker) and cast(Worker,job).run_silent is True)

        for worker in silent_workers:
            worker_obj:Worker = worker(Job(pk=0))
            next_run = self.schedules.get(worker,None)
            if next_run and next_run <= timezone.now():
                worker_obj.execute()
            elif next_run is not None:
                continue
            
            self.schedules[worker] = worker.next_run()
            
            


    def run_automations(self):

        # Run the silent workers in a separate thread
        silent_worker_thread = threading.Thread(target=self.__run_silent_workers)
        silent_worker_thread.start()

        time_now = timezone.now()
        maximum_jobs = 100

        jobs = Job.objects.filter(
            status__in=(Job.STATUS_NEW, Job.STATUS_FAILED_WITH_RETRY),
            run_at__lte=time_now,
        ).order_by("id")[:maximum_jobs]

        for job in jobs:
            JobProcessorClass = self._find_processor_class_by_key(job.type_key)
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

        silent_worker_thread.join()
