# system_metrics.py
import os
import shutil
from datetime import datetime
from typing import List, Optional
from uuid import uuid4

import psutil
from pydantic import BaseModel, Field


class SystemMetrics(BaseModel):
    """
    Snapshot of system performance metrics.
    """

    id: str = Field(default_factory=lambda: str(uuid4()), exclude=True)
    timestamp: datetime = Field(default_factory=datetime.now)
    cpu_usage: float
    memory_usage: float
    disk_usage: float

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

    def __init__(self) -> None:
        self.system_metrics: List[SystemMetrics] = []
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
        self.system_metrics.append(metrics)
        return metrics

    def get_all_system_metrics(self) -> Optional[List[SystemMetrics]]:
        """
        Returns all collected system metrics.
        """
        return self.system_metrics or None

    def get_latest_system_metrics(self) -> Optional[SystemMetrics]:
        """
        Returns the most recent system metrics entry.
        """
        return self.system_metrics[-1] if self.system_metrics else None
