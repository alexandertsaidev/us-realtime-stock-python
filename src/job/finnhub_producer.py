# /app/src/job/finnhub_producer.py

import json
import logging
import threading
from collections.abc import Sequence
from typing import Any

from pydantic import SecretStr

from finnhub.finnhub_client import FinnhubClient
from job.managed_task import ManagedTask
from kafka_io.kafka_producer import KafkaProducerClient

logger = logging.getLogger(__name__)


class FinnhubProducerTask(ManagedTask):
    def __init__(
        self,
        api_keys: Sequence[SecretStr],
        ticker_groups: Sequence[Sequence[str]],
        kafka_prod: KafkaProducerClient,
        name: str = "finnhub_producer",
        join_timeout: float = 5.0,
    ) -> None:
        self.name = name
        self._pairs = list(zip(api_keys, ticker_groups, strict=True))
        self._kafka_prod = kafka_prod
        self._join_timeout = join_timeout
        self._clients: list[FinnhubClient] = []
        self._threads: list[threading.Thread] = []

    # ── 訊息處理 ──────────────────────────────────────────
    def _handle_message(self, message: str) -> None:
        """處理一則 websocket 訊息:解析 JSON,trade 才送進 Kafka。"""
        try:
            data: dict[str, Any] = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("invalid JSON: %s", message)
            return

        if data.get("type") != "trade":
            return

        trades: list[dict[str, Any]] = data.get("data", [])
        for trade in trades:
            self._kafka_prod.send(value=trade, key=trade.get("s"))

    # ── 開啟 / 關閉 ───────────────────────────────────────
    def start(self) -> None:
        if self.is_running:
            logger.warning("[%s] 已在運作,略過 start", self.name)
            return

        self._clients.clear()
        self._threads.clear()

        for i, (key, symbols) in enumerate(self._pairs, start=1):
            thread_name = f"{self.name}_{i}"
            client = FinnhubClient(
                token=key.get_secret_value(),
                symbols=symbols,
                on_data=self._handle_message,
            )
            thread = threading.Thread(target=client.run, name=thread_name)
            self._clients.append(client)
            self._threads.append(thread)
            thread.start()

        logger.info("[%s] 已啟動 %d 個 FinnhubProducer thread", self.name, len(self._threads))

    def stop(self) -> None:
        """先關閉所有 client,再等 thread 結束(最多 join_timeout 秒)。"""
        logger.info("[%s] 停止中", self.name)

        # 1. 先通知全部 client 停止,讓它們同時開始收尾
        for client in self._clients:
            client.stop()

        # 2. 再逐一等 thread 結束
        for thread in self._threads:
            thread.join(timeout=self._join_timeout)
            if thread.is_alive():
                logger.warning(
                    "[%s] thread %s 未在 %.0f 秒內結束",
                    self.name, thread.name, self._join_timeout,
                )

        logger.info("[%s] 已停止,剩 %d 個 FinnhubProducer thread 仍在運作", self.name, self.alive_count)

    @property
    def is_running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    @property
    def alive_count(self) -> int:
        return sum(t.is_alive() for t in self._threads)

    @property
    def total_count(self) -> int:
        return len(self._threads)
