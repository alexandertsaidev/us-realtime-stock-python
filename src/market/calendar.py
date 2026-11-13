# /app/src/market/calendar.py

from datetime import date, datetime, timedelta

import pandas_market_calendars as mcal  # pyright: ignore[reportMissingTypeStubs]

from common.eastern_clock import EasternClock
from market.day_detail import DayDetail
from market.day_type import DayType


class Calendar:
    """只負責回答「某天美股是否開盤、幾點開收」。"""

    # 往後最多搜尋的天數(連假最長也不會超過這個範圍)
    _LOOKAHEAD_DAYS = 14

    def __init__(self, exchange: str = "NYSE") -> None:
        self._cal = mcal.get_calendar(exchange)

    def session_of(self, day: date | None = None) -> DayDetail:
        day = day or EasternClock.today()

        sched = self._cal.schedule(
            start_date=day,
            end_date=day,
            tz=EasternClock.TZ,
        )

        # 沒有任何列 = 當天不是交易日
        if sched.empty:
            return DayDetail(day, DayType.CLOSE, None, None)

        row = sched.iloc[0]
        is_early = not self._cal.early_closes(sched).empty
        status = \
            DayType.ADJUST if is_early else DayType.REGULAR

        open_at: datetime = row["market_open"].to_pydatetime()
        close_at: datetime = row["market_close"].to_pydatetime()

        return DayDetail(
            day=day,
            status=status,
            open_at=open_at,
            close_at=close_at,
        )

    def next_session_of(
        self,
        day: date | None = None,
        *,
        include_today: bool = False,
    ) -> DayDetail:
        """找 day 之後(預設不含當天)第一個有開盤的日子。

        include_today=True 時,若 day 本身是交易日就直接回傳當天。
        """
        day = day or EasternClock.today()
        start = day if include_today else day + timedelta(days=1)

        sched = self._cal.schedule(
            start_date=start,
            end_date=start + timedelta(days=self._LOOKAHEAD_DAYS),
            tz=EasternClock.TZ,
        )

        if sched.empty:
            raise RuntimeError(
                f"{start} 之後 {self._LOOKAHEAD_DAYS} 天內找不到交易日"
            )

        next_day: date = sched.index[0].date()
        # 重用 session_of,確保 DayDetail 的組裝邏輯只有一份
        return self.session_of(next_day)
