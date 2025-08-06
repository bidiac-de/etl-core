import os
import shutil
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import psutil


class SystemMetrics:
    """
    Snapshot of system performance metrics.
    """

    _id: str
    _timestamp: datetime
    _cpu_usage: float
    _memory_usage: float
    _disk_usage: float

    def __init__(
        self,
        timestamp: datetime,
        cpu_usage: float,
        memory_usage: float,
        disk_usage: float,
    ) -> None:
        self._id = str(uuid4())
        self._timestamp = timestamp
        self._cpu_usage = cpu_usage
        self._memory_usage = memory_usage
        self._disk_usage = disk_usage

    @property
    def id(self) -> str:
        return self._id

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value: datetime) -> None:
        if not isinstance(value, datetime):
            raise ValueError("timestamp must be a datetime object")
        self._timestamp = value

    @property
    def cpu_usage(self) -> float:
        return self._cpu_usage

    @cpu_usage.setter
    def cpu_usage(self, value: float) -> None:
        if not isinstance(value, (int, float)):
            raise ValueError("cpu_usage must be a number")
        if value < 0 or value > 100:
            raise ValueError("cpu_usage must be between 0 and 100")
        self._cpu_usage = value

    @property
    def memory_usage(self) -> float:
        return self._memory_usage

    @memory_usage.setter
    def memory_usage(self, value: float) -> None:
        if not isinstance(value, (int, float)):
            raise ValueError("memory_usage must be a number")
        if value < 0 or value > 100:
            raise ValueError("memory_usage must be between 0 and 100")
        self._memory_usage = value

    @property
    def disk_usage(self) -> float:
        return self._disk_usage

    @disk_usage.setter
    def disk_usage(self, value: float) -> None:
        if not isinstance(value, (int, float)):
            raise ValueError("disk_usage must be a number")
        if value < 0 or value > 100:
            raise ValueError("disk_usage must be between 0 and 100")
        self._disk_usage = value

    def __repr__(self) -> str:
        return (
            f"SystemMetrics(id={self.id!r}, timestamp={self.timestamp!r}, "
            f"cpu_usage={self.cpu_usage:.2f}%, "
            f"memory_usage={self.memory_usage:.2f}%, "
            f"disk_usage={self.disk_usage:.2f}%)"
        )


class SystemMetricsHandler:
    """
    Handler that collects and stores SystemMetrics entries.
    """

    _system_metrics: List[SystemMetrics]

    def __init__(self) -> None:
        self._system_metrics = []
        # Capture initial metrics at startup
        self.new_metrics_entry()

    def new_metrics_entry(self) -> SystemMetrics:
        """
        Captures current system metrics and stores them.
        """
        timestamp = datetime.now()
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        memory_usage = memory.percent
        total, used, _ = shutil.disk_usage(os.getcwd())
        disk_usage = used / total * 100

        metrics = SystemMetrics(
            timestamp=timestamp,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
            disk_usage=disk_usage,
        )
        self._system_metrics.append(metrics)
        return metrics

    @property
    def system_metrics(self) -> List[SystemMetrics]:
        """
        Returns the list of collected system metrics.
        """
        return self._system_metrics

    def get_latest_system_metrics(self) -> Optional[SystemMetrics]:
        """
        Returns the most recent system metrics entry.
        """
        return self._system_metrics[-1] if self._system_metrics else None
