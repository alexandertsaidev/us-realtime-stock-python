# mypy
# mypy: ignore-errors

# pyright / Pylance
# pyright: basic
# pyright: reportGeneralTypeIssues=false
# pyright: reportAttributeAccessIssue=false

import os
import sys
import threading

from config.settings import Settings
from finnhub.finnhub_client import FinnhubClient
from tickers.ticker_splitter import TickerSplitter


def main():
    settings = Settings() # pyright: ignore[reportCallIssue]
    api_keys = settings.finnhub_api_keys     # list[str],3 把 key

    # tickers = ["AAPL","AMZN","SPY","QQQ","BINANCE:ETHUSDT","BINANCE:BTCUSDT"]
    # splitter = TickerSplitter(tickers, len(api_keys))
    # groups = splitter.split()

    # ticker 組,彼此不重疊
    groups: dict[str, list[str]] = {
        "stock": ["AAPL", "AMZN"],
        "crypto": ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"],
        "forex": ["IC MARKETS:1"],
    }

    # 數量要一致,否則 zip 會默默少掉一組
    if len(api_keys) != len(groups):
        raise ValueError(f"key 有 {len(api_keys)} 把,但 group 有 {len(groups)} 組")

    clients: list[FinnhubClient] = []
    threads: list[threading.Thread] = []

    for (name, symbols), key in zip(groups.items(), api_keys):
        client = FinnhubClient(
            token=key.get_secret_value(),                                   # 每組用自己的 key
            symbols=symbols,
            on_data=lambda msg, n=name: print(f"[{n}] {msg}"),
        )
        t = threading.Thread(target=client.run, name=name)
        clients.append(client)
        threads.append(t)
        t.start()

    def stop_all() -> None:
        print(">>> 10 秒到,關閉全部")
        for c in clients:
            c.stop()

    timer = threading.Timer(10, stop_all)
    timer.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        timer.cancel()
        stop_all()
        for t in threads:
            t.join()

    print("全部已關閉")

if __name__ == "__main__":
    sys.exit(main())
