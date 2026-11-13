# /app/src/job/managed_task.py

from abc import ABC, abstractmethod


class ManagedTask(ABC):
    """可被排程開關的任務。"""

    name: str

    @abstractmethod
    def start(self) -> None:
        """啟動任務。不可阻塞,內部要自己開 thread 後立刻返回。"""

    @abstractmethod
    def stop(self) -> None:
        """通知任務停止。不等待結束,立刻返回。"""

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """任務目前是否在運作。"""

