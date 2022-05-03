
from datetime import timedelta
from automation.common import RunResult, Worker
class TestSilentWorker(Worker):
    key="silently-silent"
    run_silent = True
    repeat_interval = timedelta(seconds=30)
    def on_execute(self) -> RunResult:
        print(f"Executung Silently {self.job}")
        return RunResult(success=True)