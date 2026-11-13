import threading
import time

stop_event = threading.Event()  # 計時器用來控制兩個 thread 的開關


def worker(name): # type: ignore
    i =0 
    while not stop_event.is_set():
        print(f"[{name}] 運作中, 已運行 {i} s")
        time.sleep(1)
        i = i+1
    print(f"[{name}] 已停止")


def timer(seconds): # type: ignore
    for remaining in range(seconds, 0, -1): # type: ignore
        print(f"倒數 {remaining} 秒")
        time.sleep(1)
    print("時間到！通知所有 thread 停止")
    stop_event.set()


if __name__ == "__main__":
    t1 = threading.Thread(target=worker, args=("Thread-A",)) # type: ignore
    t2 = threading.Thread(target=worker, args=("Thread-B",)) # type: ignore
    tm = threading.Thread(target=timer, args=(10,)) # type: ignore

    t1.start()
    t2.start()
    tm.start()

    tm.join()
    t1.join()
    t2.join()
    print("全部結束")
