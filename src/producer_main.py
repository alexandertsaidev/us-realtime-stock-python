# /app/src/producer_main.py

import logging
import signal
import sys
import threading
from types import FrameType

import schedule

from common.eastern_clock import EasternClock
from config.settings import Settings
from job.finnhub_producer import FinnhubProducerTask
from job.market_countdown import MarketCloseCountdown
from kafka_io.kafka_producer import KafkaProducerClient
from market.calendar import Calendar
from market.day_type import DayType
from notify.slack_notifier import SlackNotifier
from storage.parquet_reader import ParquetReader
from tickers.ticker_splitter import TickerSplitter

logger = logging.getLogger(__name__)


def main() -> int:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 1. 載入設定：缺少必填欄位時，這裡會直接丟 ValidationError
    settings = Settings()  # pyright: ignore[reportCallIssue]
    api_keys = settings.finnhub_api_keys

    if not api_keys:
        logger.error("沒有可用的 API key，結束")
        return 1

    # 2. 檢查今天是否開盤
    tz_name= EasternClock.TZ_NAME
    detail = Calendar().session_of()
    is_before = detail.is_before_close()

    # next_detail=Calendar().next_session_of(detail.day)
    # print(next_detail.day, next_detail.status.value, next_detail.open_at, next_detail.close_at)

    # 3. 獲取 開/收盤時間字串
    open_str = detail.open_at.strftime("%H:%M") if detail.open_at else "未知"
    close_str = detail.close_at.strftime("%H:%M") if detail.close_at else "未知"

    if detail.status is DayType.CLOSE :
        logger.info("%s, %s 為休市，已結束", tz_name, detail.day)
        return 0

    if is_before :
        if detail.status is DayType.ADJUST :
            logger.info("%s, %s 調整收盤，收盤時間: %s", tz_name, detail.day, close_str)

        else :
            logger.info("%s, %s 正常收盤，收盤時間: %s", tz_name, detail.day, close_str)
    else:
        logger.info("%s, %s 此時已經收盤，收盤時間: %s", tz_name, detail.day, close_str)
        return 0

    # 4. 載入 tickers 並分組，每個 API key 負責一組以分散請求量
    try:
        with ParquetReader(settings) as reader:
            tickers = reader.recent_tickers(
                "stock/screening/us_all_co_screen.parquet"
            )

    except Exception:
        logger.exception("讀取 us_all_co_screen.parquet 失敗")
        return 1

    splitter = TickerSplitter(tickers, len(api_keys))
    ticker_groups = splitter.split()

    with KafkaProducerClient(
        bootstrap_servers= settings.kafka_bootstrap_servers, 
        topic= "us-stock-trade"
    ) as kafka_prod:

        fnhb_prod_task = FinnhubProducerTask(
            api_keys=api_keys,
            ticker_groups=ticker_groups,
            kafka_prod=kafka_prod,
        )

        countdown = MarketCloseCountdown(
            notifier=SlackNotifier(settings.slack_rt_pipe_webhook_url.get_secret_value()), 
            detail=detail,
            next_detail=Calendar().next_session_of(detail.day),
        )

        if detail.close_at is None:
            raise ValueError("休市日不應建立倒數")

        # 轉成本機時間、去掉時區
        close_at_local = detail.close_at.astimezone().replace(tzinfo=None)

        # 每小時 :30 通知距離開盤 or 收盤的剩餘時間，收盤後不再執行（Slack + log）
        schedule.every().hour.at(":30").until(close_at_local).do(countdown.notify)  # pyright: ignore[reportUnknownMemberType]
        schedule.every().hour.at(":30").until(close_at_local).do(countdown.show)    # pyright: ignore[reportUnknownMemberType]

        # 每天在收盤時間，通知已收盤（Slack + log）
        schedule.every().day.at(close_str, tz_name).do(countdown.notify)  # pyright: ignore[reportUnknownMemberType]
        schedule.every().day.at(close_str, tz_name).do(countdown.show)    # pyright: ignore[reportUnknownMemberType]

        # 每天在收盤時間，停止 Finnhub & producer 任務
        schedule.every().day.at(close_str, tz_name).do(fnhb_prod_task.stop) # pyright: ignore[reportUnknownMemberType]

        done = threading.Event()

        def stop_all() -> type[schedule.CancelJob]:
            fnhb_prod_task.stop()
            done.set()
            return schedule.CancelJob   # 只執行一次

        def _on_signal(signum: int, _frame: FrameType | None) -> None:
            logger.info("收到訊號 %s，準備關閉", signal.Signals(signum).name)
            done.set()                  # 只設旗標，收尾交給 finally

        signal.signal(signal.SIGTERM, _on_signal)  # docker stop / kill：外部要求關閉
        signal.signal(signal.SIGINT, _on_signal)   # Ctrl+C：手動中斷，走同一套收尾

        schedule.every().day.at(open_str, tz_name).do(fnhb_prod_task.start) # pyright: ignore[reportUnknownMemberType]
        schedule.every().day.at(close_str, tz_name).do(stop_all) # pyright: ignore[reportUnknownMemberType]

        # fnhb_prod_task.start()

        try:
            while not done.is_set():
                schedule.run_pending()
                done.wait(1)

        finally:
            fnhb_prod_task.stop()      # 確保離開 with 之前 thread 已停止

    return 0

if __name__ == "__main__":
    sys.exit(main())
