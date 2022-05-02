from django.conf import settings

from automation.common import AutomationBase
from automation.common import RunResult
from automation.models import Message
from core import messaging
from core.models import Order

from .slack import SlackAlertJob
from .slack import SlackAlertTypes

CONFIG = settings.CONFIG_PROVIDER

logger = settings.LOGGER


class SendSmsJob(AutomationBase):
    key = "send-sms"
    retry_interval = 300
    times_to_retry = 2
    model = Message
    model_pk_name = "messageId"

    def on_execute(self) -> RunResult:
        message = Message.objects.filter(
            pk=self.job.metadata.get(self.model_pk_name),
        ).first()
        result = self.send_sms(
            sender=message.sender_name,
            message=message.body,
            recipient=message.recipient,
        )

        if result.success:
            # IMPORTANT: create update message status if job
            logger.info(
                f"Message sent starting job to track deliverability for message ID {self.job.metadata[self.model_pk_name]}"
            )
            SMSStatusJob.schedule_job(
                metadata=dict(
                    orderId=self.job.metadata.get("orderId"),
                    messageId=self.job.metadata.get(self.model_pk_name),
                    externalTrackingId=result.data.get("id"),
                )
            )
            # check balance
            check_sms_balance(result.data.get("sms_credit"))
            Message.objects.filter(
                pk=self.job.metadata.get(self.model_pk_name),
            ).update(external_tracking_token=result.data.get("id"))
            return RunResult(success=True)

        return result

    def send_sms(self, sender: str, message: str, recipient: str):
        result = messaging.send_sms(
            message=message, phone_number=recipient, sender=sender
        )
        return RunResult(
            success=result.success, message=result.message, data=result.data
        )


class SMSStatusJob(AutomationBase):
    key = "sms-status"
    retry_interval = 30
    times_to_retry = 3

    def on_execute(self) -> RunResult:
        result = self.check_message_status(
            external_tracking_id=self.job.metadata.get("externalTrackingId")
        )
        if result.success:
            Order.objects.filter(pk=self.job.metadata.get("orderId")).update(
                acknowledged=True
            )
            Message.objects.filter(pk=self.job.metadata.get("messageId")).update(
                is_delivered=True
            )
            return RunResult(success=True)
        return result

    def check_message_status(self, external_tracking_id: str):
        result = messaging.check_message_status(
            message_id=external_tracking_id, medium=messaging.SMS
        )
        return RunResult(
            success=result.success, message=result.message, data=result.data
        )


def check_sms_balance(balance):
    if balance <= int(CONFIG.get("SMS_MINIMUM_BALANCE", "100")):
        SlackAlertJob.schedule_job(
            metadata=dict(
                name="Sms Blanace Alert",
                message=f"Sms Balance is running low!! Balance is {balance}.",
                type=SlackAlertTypes.GENERAL,
            )
        )
        logger.critical(f"Sms Balance is low!! Balance is {balance}")
