# /app/src/market/session_type.py

from enum import Enum


class SessionType(Enum):
    PRE_OPEN = "PRE_OPEN"   # 開盤前
    OPENING = "OPENING"     # 盤中
    CLOSED = "CLOSED"       # 已收盤
