import queue
import threading
from typing import Optional

import requests
from django.conf import settings

from automation.common import AutomationBase
from automation.common import RunResult

CONFIG = settings.CONFIG_PROVIDER

logger = settings.LOGGER


class SlackChannels:
    OPERATIONS = "operations"
    FINANCE = "finance"
    SECURITY = "security"
    PRODUCTION_SUPPORT = "ops"


SLACK_CHANNELS_CONFIG = {
    SlackChannels.OPERATIONS: CONFIG.get("SLACK_OPERATIONS_CHANNEL_URL"),
    SlackChannels.FINANCE: CONFIG.get("SLACK_FINANCE_CHANNEL_URL"),
    SlackChannels.SECURITY: CONFIG.get("SLACK_SECURITY_CHANNEL_URL"),
    SlackChannels.PRODUCTION_SUPPORT: CONFIG.get(
        "SLACK_PRODUCTION_SUPPORT_CHANNEL_URL"
    ),
}


class SlackAlertTypes:
    NEW_ORDER_ALERT = "new-order-received"
    ORDER_STATUS_CHANGED = "order-status-changed"
    GENERAL = "text-general"


class SlackAlertJob(AutomationBase):
    key = "announce-slack"
    retry_interval = 30
    times_to_retry = 3

    def on_execute(self) -> RunResult:
        announcer = SlackAnnouncer()
        success = False
        channel = self.job.metadata.get("channel")
        alert_type = self.job.metadata.get("type")
        message = None
        if alert_type == SlackAlertTypes.NEW_ORDER_ALERT:
            message = NewOrderMessageBuilder(order=self.job.metadata)
        elif alert_type == SlackAlertTypes.ORDER_STATUS_CHANGED:
            message = OrderStatusMessageBuilder(order_status=self.job.metadata)
        elif alert_type == SlackAlertTypes.GENERAL:
            message = SimpleTextMessage(
                message=self.job.metadata["message"], channel=channel
            )
        else:
            return RunResult(message="Undefined slack message type")

        if message:
            announcer.announce(message, channel=channel)
            success = True

        return RunResult(success=success)


class MessageBuilderBase(object):
    def __init__(self, channel: Optional[str] = None) -> None:
        self.channel = channel
        super().__init__()

    def build(self) -> dict:
        raise NotImplementedError()


class SlackAnnouncer(object):
    def __init__(self) -> None:
        self.queue = queue.SimpleQueue()

        def message_announcer():
            while True:
                message = self.queue.get()
                self.announce(message)

        threading.Thread(target=message_announcer, daemon=True).start()

    def announce(self, message_builder: MessageBuilderBase, channel=None):
        print(f"Announcing {message_builder}")
        if settings.ENVIRONMENT == "test":
            return

        SLACK_API_URL = SLACK_CHANNELS_CONFIG.get(channel or SlackChannels.OPERATIONS)
        if not SLACK_API_URL:
            SLACK_API_URL = settings.CONFIG_PROVIDER.get(
                "SLACK_WEB_HOOK_URL",
                "https://hooks.slack.com/services/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",
            )

        try:
            requests.post(url=SLACK_API_URL, json=message_builder.build())
        except Exception as ex:
            logger.error("Slack announcement failed", detail=f" Error: {ex}")


class SimpleTextMessage(MessageBuilderBase):
    def __init__(self, message: str, channel: Optional[str] = None) -> None:
        super().__init__(channel=channel)
        self.message = message

    def build(self) -> dict:
        return dict(
            blocks=[
                dict(
                    type="section",
                    text=dict(
                        type="mrkdwn",
                        text=self.message,
                    ),
                )
            ]
        )


class NewOrderMessageBuilder(MessageBuilderBase):
    def __init__(self, order: dict) -> None:
        self.order = order
        self.channel = SlackChannels.OPERATIONS
        super().__init__()

    def build(self) -> dict:
        return self._build_message_for_order(self.order)

    def _build_message_for_order(self, order: dict):
        message = f"*{order.get('first_name')}* has ordered *{order.get('product')}* from *{order.get('shop_name')}. Order #{order.get('order_id')}* GHC{order.get('amount')}"
        return SimpleTextMessage(message=message, channel=self.channel).build()


class OrderStatusMessageBuilder(MessageBuilderBase):
    def __init__(self, order_status: dict) -> None:
        super().__init__()
        self.order_status = order_status
        self.channel = SlackChannels.OPERATIONS

    def build(self) -> dict:
        return self._build_slack_message_for_order_status_update(
            order_status=self.order_status
        )

    def _build_slack_message_for_order_status_update(self, order_status: dict = None):
        if order_status.get("status", None):
            message = f"*{order_status.get('name')}* has updated order *#{order_status.get('order_id'):05d}* status to *{order_status.get('status')}*"
        else:
            message = f"*{order_status.get('name')}* has *confirmed* order *#{order_status.get('order_id'):05d}*"
        return SimpleTextMessage(message=message, channel=self.channel).build()
