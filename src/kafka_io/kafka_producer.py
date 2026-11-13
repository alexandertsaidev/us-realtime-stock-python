# /app/src/kafka_io/kafka_producer.py

from __future__ import annotations

import json
import logging
from types import TracebackType
from typing import Any, Literal, Self

from kafka import KafkaProducer  # pyright: ignore[reportMissingTypeStubs]

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        topic: str,
        *,
        acks: int | Literal["all"] = "all",
        enable_idempotence: bool = True,
        retries: int = 5,
        max_in_flight_requests_per_connection: int = 5,
        linger_ms: int = 5,
        # compression_type: Literal["gzip", "snappy", "lz4", "zstd"] | None = None,
    ) -> None:
        self.topic: str = topic
        self._producer: Any = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self._serialize_value,
            acks=acks,
            enable_idempotence=enable_idempotence,
            retries=retries,
            max_in_flight_requests_per_connection=max_in_flight_requests_per_connection,
            linger_ms=linger_ms,
            # compression_type=compression_type,
        )

    @staticmethod
    def _on_error(exc: BaseException) -> None:
        logger.error("Kafka send failed: %s", exc)

    @staticmethod
    def _serialize_value(value: object) -> bytes:
        return json.dumps(value, ensure_ascii=False).encode("utf-8")

    def send(
        self,
        value: dict[str, Any],
        key: str | None = None,
        topic: str | None = None,
    ) -> None:
        key_bytes: bytes | None = key.encode("utf-8") if key is not None else None
        future: Any = self._producer.send(
            topic or self.topic, key=key_bytes, value=value
        )
        future.add_errback(self._on_error)


    
    def flush(self, timeout: float | None = None) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
