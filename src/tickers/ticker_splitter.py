# /app/src/tickers/ticker_splitter.py

from dataclasses import dataclass


@dataclass
class TickerSplitter:
    tickers: list[str]
    n: int

    def __post_init__(self):
        if self.n <= 0:
            raise ValueError("n 必須 > 0")

    def split(self) -> list[list[str]]:
        k, m = divmod(len(self.tickers), self.n)
        return [
            self.tickers[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]
            for i in range(self.n)
        ]

# 舉例
# splitter = TickerSplitter(["AAPL", "MSFT", "GOOG", "AMZN", "NVDA"], 3)
# print(splitter.split())
# [['AAPL', 'MSFT'], ['GOOG', 'AMZN'], ['NVDA']]
