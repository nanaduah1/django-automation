from datetime import timedelta

from automation.common import AutomationBase
from automation.common import RunResult
from core import delivery


class CancelOrderDelivery(AutomationBase):
    key = "cancel-delivery"

    def on_execute(self) -> RunResult:
        delivery_token = self.job.metadata.get("delivery_token")
        reason = self.job.metadata.get("reason")
        response = delivery.cancel_delivery(
            delivery_token=delivery_token, reason=reason
        )
        return RunResult(success=response.success, message=response.message)
