import json
from datetime import datetime

from django.db import models


class Job(models.Model):
    STATUS_NEW = "new"
    STATUS_RUNNING = "running"
    STATUS_COMPLETED = "done"
    STATUS_FAILED = "failed"
    STATUS_FAILED_WITH_RETRY = "retry"

    JOB_STATUSES = (
        (STATUS_NEW, "New"),
        (STATUS_RUNNING, "Processing"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
        (STATUS_FAILED_WITH_RETRY, "Retry"),
    )
    created_at = models.DateTimeField(auto_now=True)
    run_at = models.DateTimeField()
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    type_key = models.CharField(max_length=15)
    status = models.CharField(max_length=15, choices=JOB_STATUSES, default=STATUS_NEW)
    fail_reason = models.CharField(max_length=250, null=True, blank=True)
    times_executed = models.SmallIntegerField(default=0)
    _metadata = models.TextField(max_length=512)

    def __str__(self):
        return f"{self.type_key} job ({self.status})"

    @property
    def metadata(self) -> dict:
        if self._metadata:
            return json.loads(self._metadata)
        else:
            return {}

    @metadata.setter
    def metadata(self, value: dict):
        self._metadata = json.dumps(value)

    def repeat_schedule(self, time: datetime):
        Job.objects.create(
            run_at=time, type_key=self.type_key, _metadata=self._metadata
        )

