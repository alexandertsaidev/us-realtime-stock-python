# /app/src/storage/jsonl_writer.py

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol

from common.eastern_clock import EasternClock
from market.day_detail import DayDetail

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

type Trade = dict[str, Any]
type GroupKey = tuple[str, str]  # (symbol, trade_date)


class KafkaRecord(Protocol):
    """只描述本 class 用到的 Kafka record 欄位。"""

    @property
    def value(self) -> Trade: ...
    @property
    def partition(self) -> int: ...
    @property
    def offset(self) -> int: ...


@dataclass(frozen=True)
class JsonlWriter:
    """把 Kafka records 依 (symbol, 交易日) 分組,過濾盤中資料後,以 jsonl 寫入 MinIO。"""

    client: "S3Client"
    bucket: str
    day_detail_of: Callable[[date], DayDetail]
    prefix: str = "stock/real-time/prices/bronze"

    # ---------- 時間 ----------
    @staticmethod
    def _to_et(ts_ms: int) -> datetime:
        return datetime.fromtimestamp(ts_ms / 1000, tz=EasternClock.TZ)

    @staticmethod
    def _in_session(et: datetime, detail: DayDetail) -> bool:
        if detail.open_at is None or detail.close_at is None:
            return False
        return detail.open_at <= et <= detail.close_at

    # ---------- 分組 ----------
    def _group(self, buf: Iterable[KafkaRecord]) -> dict[GroupKey, list[KafkaRecord]]:
        groups: defaultdict[GroupKey, list[KafkaRecord]] = defaultdict(list)
        details: dict[date, DayDetail] = {}

        for record in buf:
            v = record.value
            et = self._to_et(int(v["t"]))
            d = et.date()

            if d not in details:
                details[d] = self.day_detail_of(d)
            if not self._in_session(et, details[d]):
                continue

            groups[(str(v["s"]), d.isoformat())].append(record)
        return groups

    # ---------- key / bytes ----------
    def _build_key(
        self, sym: str, trade_date: str, msgs: Sequence[KafkaRecord]
    ) -> str:
        first, last = msgs[0], msgs[-1]
        return (
            f"{self.prefix}/{sym}/{trade_date}/"
            f"p{first.partition}_{first.offset}-{last.offset}.jsonl"
        )

    @staticmethod
    def _to_jsonl_bytes(records: Sequence[Trade]) -> bytes:
        return "\n".join(json.dumps(r) for r in records).encode("utf-8")

    # ---------- 對外介面 ----------
    def write_batch(self, buf: Iterable[KafkaRecord]) -> list[str]:
        uploaded: list[str] = []
        for (sym, trade_date), msgs in self._group(buf).items():
            data = self._to_jsonl_bytes([m.value for m in msgs])
            key = self._build_key(sym, trade_date, msgs)

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType="application/x-ndjson",
            )
            uploaded.append(key)
        return uploaded
