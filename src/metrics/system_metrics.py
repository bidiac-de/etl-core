from datetime import datetime
from typing import List, Optional
import psutil


class SystemMetrics:
    """
    class to represent a snapshot of system performance metrics
    """

    def __init__(
        self,
        timestamp: datetime,
        cpu_usage: float,
        memory_usage: float,
        disk_usage: float,
    ):
        self.timestamp = timestamp
        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage
        self.disk_usage = disk_usage

    def __repr__(self):
        return (
            f"SystemMetrics(timestamp={self.timestamp!r}, "
            f"cpu_usage={self.cpu_usage:.2f}%, "
            f"memory_usage={self.memory_usage:.2f}%, "
            f"disk_usage={self.disk_usage:.2f}%)"
        )


class SystemMetricsHandler:
    """
    Handler that collects and stores SystemMetrics entries
    """

    def __init__(self):
        self.system_metrics: List[SystemMetrics] = []
        # Capture initial metrics at startup
        self.new_metrics_entry()

    def new_metrics_entry(self) -> SystemMetrics:
        """
        Captures current system metrics and stores them
        """
        timestamp = datetime.now()
        # CPU usage over a 1-second interval
        cpu_usage = psutil.cpu_percent(interval=1)
        # Memory usage percentage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent
        # Disk usage percentage for the root partition
        disk = psutil.disk_usage("/")
        disk_usage = disk.percent

        metrics = SystemMetrics(timestamp, cpu_usage, memory_usage, disk_usage)
        self.system_metrics.append(metrics)
        return metrics

    def get_all_system_metrics(self) -> Optional[List[SystemMetrics]]:
        """
        Returns all collected system metrics
        """
        return self.system_metrics if self.system_metrics else None

    def get_latest_system_metrics(self) -> Optional[SystemMetrics]:
        """
        Returns the most recent system metrics entry
        """
        return self.system_metrics[-1] if self.system_metrics else None
