# /app/src/market/day_type.py

from enum import Enum


class DayType(Enum):
    REGULAR = "REGULAR"   # 正常開盤
    ADJUST = "ADJUST"     # 提早收盤
    CLOSE = "CLOSE"       # 休市
