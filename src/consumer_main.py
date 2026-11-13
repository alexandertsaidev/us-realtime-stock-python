# /app/src/consumer_main.py

import logging
import signal
import sys
import threading
from datetime import timedelta
from types import FrameType

from common.eastern_clock import EasternClock
from config.settings import Settings
from kafka_io.kafka_consumer import KafkaConsumerClient
from market.calendar import Calendar
from market.day_type import DayType
from storage.jsonl_writer import JsonlWriter
from storage.minio_s3_factory import MinioS3Factory

logger = logging.getLogger(__name__)

TOPIC = "us-stock-trade"
GROUP_ID = "bronze-writer"
STOP_GRACE = timedelta(minutes=15)  # 收盤後多跑一段時間,消化 Kafka 裡延遲的資料


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    settings = Settings()  # pyright: ignore[reportCallIssue]

    # 1. 今天是否開盤
    calendar = Calendar()
    detail = calendar.session_of()
    tz_name = EasternClock.TZ_NAME

    if detail.status is DayType.CLOSE or detail.close_at is None:
        logger.info("%s, %s 為休市,已結束", tz_name, detail.day)
        return 0

    # 2. consumer 跑到「收盤 + 緩衝時間」才停
    stop_at = detail.close_at + STOP_GRACE
    remaining_s = (stop_at - EasternClock.now()).total_seconds()
    if remaining_s <= 0:
        logger.info("%s, %s 已超過 %s,已結束", tz_name, detail.day, stop_at.strftime("%H:%M"))
        return 0

    label = "調整收盤" if detail.status is DayType.ADJUST else "正常收盤"
    logger.info(
        "%s, %s %s,收盤 %s,consumer 預計 %s 停止",
        tz_name, detail.day, label,
        detail.close_at.strftime("%H:%M"), stop_at.strftime("%H:%M"),
    )

    # 3. 組裝
    s3 = MinioS3Factory.from_settings(settings).create_client()
    writer = JsonlWriter(
        client=s3,
        bucket=settings.minio_bucket,
        day_detail_of=calendar.session_of,
    )
    consumer = KafkaConsumerClient(
        topics=[TOPIC],
        handler=writer.write_batch,
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",")],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",  # 第一次啟動從頭讀,避免漏資料
    )

    # 4. 到點自動停止;Docker stop 送來的 SIGTERM 也走同一條路
    timer = threading.Timer(remaining_s, consumer.stop)
    timer.daemon = True
    timer.start()

    def _on_signal(_signum: int, _frame: FrameType | None) -> None:
        consumer.stop()

    # 統一走 consumer.stop()，確保緩衝資料寫完並 commit 
    signal.signal(signal.SIGTERM, _on_signal)  # docker stop / kill：外部要求關閉
    signal.signal(signal.SIGINT, _on_signal)   # Ctrl+C：手動中斷，走同一套收尾

    try:
        consumer.start()  # 阻塞,結束前會把緩衝的資料寫完並 commit
    finally:
        timer.cancel()

    return 0


if __name__ == "__main__":
    sys.exit(main())
