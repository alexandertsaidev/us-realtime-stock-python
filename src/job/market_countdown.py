# /app/src/job/market_countdown.py

import logging
from datetime import timedelta

from common.eastern_clock import EasternClock
from market.calendar import DayDetail
from notify.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)


class MarketCloseCountdown:
    """計算今日距離開盤 / 收盤的剩餘時間並發送到 Slack。"""

    def __init__(
        self,
        notifier: SlackNotifier,
        detail: DayDetail,
        next_detail: DayDetail,
    ) -> None:
        self.notifier = notifier
        self.detail = detail
        self.next_detail: DayDetail = next_detail

    @staticmethod
    def _format(seconds: int) -> str:
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{h} 小時 {m} 分 {s} 秒"

    def remaining(self) -> str:
        now = EasternClock.now()

        if (
            self.detail.open_at is None
            or self.detail.close_at is None
            or self.next_detail.open_at is None
            or self.next_detail.close_at is None
        ):
            raise ValueError("休市日不應建立倒數")

        elif now < self.detail.open_at:
            seconds = int((self.detail.open_at - now).total_seconds())
            return f"⏳ 距離 🗽開盤 剩餘 {self._format(seconds)}"

        elif now < self.detail.open_at + timedelta(minutes=1):
            seconds = int((self.detail.close_at - now).total_seconds())
            return (
                f"🔔 現在 🗽已開盤, 今日收盤時間: {self.detail.close_at.strftime('%H:%M')}\n"
                f"⏳ 距離 🗽收盤 剩餘 {self._format(seconds)}"
            )

        elif now < self.detail.close_at:
            seconds = int((self.detail.close_at - now).total_seconds())
            return f"⏳ 距離 🗽收盤 剩餘 {self._format(seconds)}"

        return (
            f"🔕 今日 🗽已收盤: {self.detail.close_at.strftime('%H:%M')}\n"
            f"👉 下一個開盤日: {self.next_detail.day.strftime('%Y-%m-%d')}\n"
            f"      屬性: {self.next_detail.status.value}\n"
            f"      開盤時間: {self.next_detail.open_at.strftime('%H:%M')}\n"
            f"      收盤時間: {self.next_detail.close_at.strftime('%H:%M')}"
        )

    def notify(self) -> bool:
        return self.notifier.info(self.remaining())

    def show(self) -> None:
        logger.info(self.remaining())
