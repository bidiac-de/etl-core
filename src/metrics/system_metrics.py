from datetime import datetime

class SystemMetricsHandler:
    def __init__(self):
        self.system_metrics = []

    def get_all_system_metrics(self):
        if self.system_metrics:
            return self.system_metrics
        return None

    def get_latest_system_metrics(self):
        if self.system_metrics:
            return self.system_metrics[-1]
        return None

    def new_metrics_entry(self):
        # insert system metrics scraping behavior here
        self.system_metrics.append(SystemMetrics(0,0,0))

class SystemMetrics:
    """
    A class to represent system metrics.
    """

    def __init__(self,system_time: datetime,  cpu_usage: float, memory_usage: float, disk_usage: float):
        self.system_time = system_time
        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage
        self.disk_usage = disk_usage

    def __repr__(self):
        return (f"SystemMetrics(system_time={self.system_time}, cpu_usage={self.cpu_usage}, "
                f"memory_usage={self.memory_usage}, disk_usage={self.disk_usage})")