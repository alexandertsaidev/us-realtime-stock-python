# /app/src/kafka_io/kafka_consumer.py

import json
import logging
import time
from collections.abc import Callable
from typing import Any, Literal, Self

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

logger = logging.getLogger(__name__)

Message = dict[str, Any]
BatchHandler = Callable[[list[Any]], object]  # 傳入整批 records,回傳值忽略
OffsetReset = Literal["earliest", "latest"]


class KafkaConsumerClient:
    """at-least-once 批次 consumer:累積 records,handler 整批成功後才 commit。"""

    def __init__(
        self,
        topics: list[str],
        handler: BatchHandler,
        bootstrap_servers: list[str] | None = None,
        group_id: str = "my-group",
        auto_offset_reset: OffsetReset = "earliest",
        flush_size: int = 2000,          # 累積幾筆就處理一次
        flush_interval_s: float = 5.0,   # 或每幾秒處理一次
        poll_timeout_ms: int = 1000,
        max_poll_records: int = 500,
        **extra_configs: Any,            # 其他 KafkaConsumer 參數(SASL、SSL 等)
    ) -> None:
        if not topics:
            raise ValueError("topics 不可為空")

        self.topics = topics
        self.handler = handler
        self.bootstrap_servers = bootstrap_servers or ["localhost:9092"]
        self.group_id = group_id
        self.auto_offset_reset: OffsetReset = auto_offset_reset
        self.flush_size = flush_size
        self.flush_interval_s = flush_interval_s
        self.poll_timeout_ms = poll_timeout_ms
        self.max_poll_records = max_poll_records
        self.extra_configs = extra_configs

        self._consumer: KafkaConsumer | None = None
        self._running = False
        self._buf: list[Any] = []
        self._last_flush = time.monotonic()

    # ---- 反序列化 ----
    @staticmethod
    def _deserialize(raw: bytes | None) -> Message:
        if raw is None:  # tombstone 訊息(value 為 None)
            return {}
        return json.loads(raw.decode("utf-8"))

    # ---- 生命週期 ----
    def connect(self) -> KafkaConsumer:
        consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,    # 一律手動 commit,才能保證 at-least-once
            max_poll_records=self.max_poll_records,
            value_deserializer=self._deserialize,
            **self.extra_configs,
        )
        self._consumer = consumer
        logger.info("Consumer 已連線: %s", self.topics)
        return consumer

    def start(self) -> None:
        """開始消費(會阻塞,直到 stop()、Ctrl+C 或批次處理失敗)"""
        consumer = self._consumer or self.connect()
        self._running = True
        self._last_flush = time.monotonic()
        try:
            while self._running:
                polled = consumer.poll(timeout_ms=self.poll_timeout_ms)
                for records in polled.values():
                    self._buf.extend(records)

                if not self._buf:
                    self._last_flush = time.monotonic()
                    continue

                due = time.monotonic() - self._last_flush >= self.flush_interval_s
                if len(self._buf) >= self.flush_size or due:
                    if not self._flush():
                        break  # 失敗:不 commit,結束,重啟後從上次 commit 處重來
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def _flush(self) -> bool:
        """整批交給 handler,成功才 commit。回傳 False 代表失敗。"""
        if not self._buf or self._consumer is None:
            return True

        try:
            self.handler(self._buf)
        except Exception:
            logger.exception("批次處理失敗 (%d 筆),不 commit", len(self._buf))
            return False

        # 每個 partition 取最後一筆 offset + 1(= 下一筆要讀的位置)
        offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        for msg in self._buf:
            offsets[TopicPartition(msg.topic, msg.partition)] = OffsetAndMetadata(
                offset=msg.offset + 1, metadata="", leader_epoch=-1
            )

        try:
            self._consumer.commit(offsets=offsets)
        except KafkaError:
            # 資料已處理,commit 失敗最多造成重送,不會漏資料
            logger.warning("commit 失敗,下次 commit 會一併涵蓋", exc_info=True)

        logger.info("已處理 %d 筆", len(self._buf))
        self._buf.clear()
        self._last_flush = time.monotonic()
        return True

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._flush()  # 關閉前把剩餘的處理完
            except Exception:
                logger.exception("關閉前 flush 失敗")
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer 已關閉")

    # ---- 支援 with 語法 ----
    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     KafkaMessageConsumer(
#         topics=["ticks"],
#         group_id="ticker-group",
#         handler=print_message,
#     ).start()
