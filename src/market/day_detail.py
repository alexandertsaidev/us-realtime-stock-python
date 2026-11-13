# /app/src/market/day_detail.py

from dataclasses import dataclass
from datetime import date, datetime

from common.eastern_clock import EasternClock
from market.day_type import DayType


@dataclass(frozen=True)
class DayDetail:
    """某一天的交易時段資訊（時間皆為 ET）。"""

    day: date
    status: DayType
    open_at: datetime | None   # CLOSE 時為 None
    close_at: datetime | None  # CLOSE 時為 None

    def is_before_close(self) -> bool:
        """當下時間是否早於收盤時間。休市(close_at 為 None)回傳 False。"""
        if self.close_at is None:
            return False
        return EasternClock.now() < self.close_at
