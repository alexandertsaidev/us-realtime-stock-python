# /app/src/common/eastern_clock.py

from datetime import date, datetime, time
from zoneinfo import ZoneInfo


class EasternClock:
    """只負責提供美東時間（ET）。"""

    TZ_NAME = "America/New_York"         
    TZ = ZoneInfo(TZ_NAME)

    @classmethod
    def now(cls) -> datetime:
        return datetime.now(cls.TZ)            # 日期 + 時間（完整資料）

    @classmethod
    def today(cls) -> date:
        return cls.now().date()                # 只取日期

    @classmethod
    def current_time(cls) -> time:
        return cls.now().time()                # 只取時間
