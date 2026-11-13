# mypy
# mypy: ignore-errors

# pyright / Pylance
# pyright: basic
# pyright: reportGeneralTypeIssues=false
# pyright: reportAttributeAccessIssue=false

import threading
import time
import requests


class ApiClient:
    def __init__(self, name, url):
        self.name = name
        self.url = url
        self.result = None
        self.error = None
        self.stop_event = threading.Event()

    def run(self):
        # 1. 只呼叫一次
        print(f"[{self.name}] 呼叫 {self.url}")
        try:
            resp = requests.get(self.url, timeout=3)
            resp.raise_for_status()
            self.result = resp.json()
        except Exception as e:
            self.error = str(e)
        print(f"[{self.name}] 呼叫完成,等待關閉...")

        # 2. 等待外部關閉(不耗 CPU,set() 後立刻醒來)
        self.stop_event.wait()
        print(f"[{self.name}] 已關閉")

    def stop(self):
        self.stop_event.set()


if __name__ == "__main__":
    apis = [
        ("天氣", "https://api.example.com/weather"),
        ("匯率", "https://api.example.com/rate"),
        ("新聞", "https://api.example.com/news"),
    ]

    clients = [ApiClient(name, url) for name, url in apis]
    threads = [threading.Thread(target=c.run) for c in clients]

    for t in threads:
        t.start()

    time.sleep(10)               # 模擬:外部在 5 秒後下指令

    for c in clients:           # 外部關閉
        c.stop()
    for t in threads:
        t.join()

    print("全部已關閉")
