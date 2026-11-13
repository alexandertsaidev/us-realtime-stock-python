# /app/src/notify/slack_notifier.py

import logging
from typing import Any

from slack_sdk.errors import SlackApiError
from slack_sdk.webhook import WebhookClient

logger = logging.getLogger(__name__)


class SlackNotifier:
    """透過 slack_sdk 的 WebhookClient 發送通知。"""

    def __init__(self, webhook_url: str, timeout: int = 5) -> None:
        self.client = WebhookClient(webhook_url, timeout=timeout)

    def send(
        self,
        text: str,
        blocks: list[dict[str, Any]] | None = None,
    ) -> bool:
        try:
            resp = self.client.send(text=text, blocks=blocks)
        except (SlackApiError, OSError) as e:
            logger.error("Slack 通知發送失敗: %s", e)
            return False

        if resp.status_code != 200 or resp.body != "ok":
            logger.error(
                "Slack 通知發送失敗: status=%s body=%s",
                resp.status_code,
                resp.body,
            )
            return False
        return True

    def info(self, message: str) -> bool:
        return self.send(f"{message}")

    def success(self, message: str) -> bool:
        return self.send(f"✅success: {message}")

    def warning(self, message: str) -> bool:
        return self.send(f"⚠️warning: {message}")

    def error(self, message: str) -> bool:
        return self.send(f"❌error: {message}")


# import asyncio
# import os


# async def main():
#     notifier = AsyncSlackNotifier(os.environ["SLACK_WEBHOOK_URL"])
#     await notifier.success("部署完成")
#     await notifier.error("任務失敗:資料庫連線逾時")


# asyncio.run(main())
