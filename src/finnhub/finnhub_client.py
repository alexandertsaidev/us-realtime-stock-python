# /app/src/finnhub/finnhub_client.py

import json
import logging
import random
import time
from collections.abc import Callable, Iterable

import websocket

logger = logging.getLogger(__name__)

class FinnhubClient:
    URL: str = "wss://ws.finnhub.io"

    def __init__(
        self,
        token: str,
        symbols: Iterable[str],
        on_data: Callable[[str], None] | None = None,
    ) -> None:
        self.token: str = token
        self.symbols: list[str] = list(symbols)
        self.on_data: Callable[[str], None] = on_data or self._default_handler
        self.ws: websocket.WebSocketApp = websocket.WebSocketApp(
            f"{self.URL}?token={self.token}",
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

    @staticmethod
    def _default_handler(message: str) -> None:
        logger.info(message)

    # ---- callbacks ----
    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        for symbol in self.symbols:
            self.subscribe(symbol)
            time.sleep(random.uniform(0, 0.02))

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        self.on_data(message)

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        logger.info(f"[error] {error}")

    def _on_close(
        self,
        ws: websocket.WebSocketApp,
        close_status_code: int | None = None,
        close_msg: str | None = None,
    ) -> None:
        logger.info(f"### closed ### ({close_status_code}) {close_msg}")

    def subscribe(self, symbol: str) -> None:
        self.ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def run(self, trace: bool = False) -> None:
        websocket.enableTrace(trace)
        self.ws.run_forever( # pyright: ignore[reportUnknownMemberType]
            reconnect=15, ping_interval=60, ping_timeout=20
        )

    def stop(self) -> None:
        self.ws.close()


# if __name__ == "__main__":
#     client = FinnhubClient(
#         token=os.environ["FINNHUB_TOKEN"],
#         symbols=["AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"],
#     )
#     client.run(trace=True)
