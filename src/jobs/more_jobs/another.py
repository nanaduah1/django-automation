from automation.common import AutomationBase, RunResult


class AnotherJob(AutomationBase):
    key = "another"

    def on_execute(self) -> RunResult:
        print("AnotherJob executed!")
        return RunResult(success=True)