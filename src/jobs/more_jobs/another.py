from datetime import datetime, timedelta

from django.utils import timezone
from automation.common import AutomationBase, RunResult, Worker


class AnotherJob(Worker):
    key = "another"

    def on_execute(self) -> RunResult:
        print("AnotherJob executed!")
        return RunResult(success=True)

    @classmethod
    def next_run(cls):
        return (timezone.now() + timedelta(seconds=10))