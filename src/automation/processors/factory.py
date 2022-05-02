from typing import Any
from typing import Dict

from automation.processors.refunds.refunder import RefunderBot
from automation.processors.slack import SlackAlertJob
from automation.processors.sms import SendSmsJob
from automation.processors.sms import SMSStatusJob
from automation.processors.workers.deliverysync import CancelOrderDelivery


def get_all_automation_jobs():

    # IMPORTANT: Register your automation classes here
    job_registry: Dict[str, Any] = {
        SendSmsJob.key: SendSmsJob,
        SMSStatusJob.key: SMSStatusJob,
        SlackAlertJob.key: SlackAlertJob,
        RefunderBot.key: RefunderBot,
        CancelOrderDelivery.key: CancelOrderDelivery,
    }

    return job_registry
