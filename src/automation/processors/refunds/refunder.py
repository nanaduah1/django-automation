from django.conf import settings
from django.contrib.auth.models import User

from automation.common import AutomationBase
from automation.common import RunResult
from core.models import Refund
from core.services import refunds

CONFIG = settings.CONFIG_PROVIDER


class RefunderBot(AutomationBase):
    key = "approve-refund"
    retry_interval = 300
    times_to_retry = 2
    model_pk_name = "refundId"

    def on_execute(self) -> RunResult:
        refund = Refund.objects.filter(
            pk=self.job.metadata.get(self.model_pk_name)
        ).first()
        if not refund:
            return RunResult(message="Record with id not found")

        approver = User.objects.get(username=CONFIG.get("REFUND_APPROVER", "admin"))
        response = refunds.approve_refund(
            refund_id=refund.pk,
            current_user_id=approver.pk,
            approval_code=refund.approval_code,
            method=refund.method,
        )

        if (
            refund.method == Refund.METHOD_ELECTRONIC
            and not response.success
            and self.job.times_executed == self.times_to_retry
        ):

            # We failed to refund electronically
            # Switch to manual and prompt support
            response = refunds.approve_refund(
                refund_id=refund.pk,
                current_user_id=approver.pk,
                approval_code=refund.approval_code,
                method=Refund.METHOD_MANUAL,
            )

        return RunResult(success=response.success, message=response.message)
