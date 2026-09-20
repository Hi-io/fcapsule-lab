"""Small independent host-memory admission check for the disposable lab."""

import os
from urllib.request import urlopen


def memory_snapshot():
    url = os.environ.get("NODE_EXPORTER_URL", "http://prometheus-prometheus-node-exporter.monitoring:9100/metrics")
    with urlopen(url, timeout=3) as response:
        text = response.read(4_000_000).decode()
    values = {}
    for line in text.splitlines():
        if line.startswith(("node_memory_MemAvailable_bytes ", "node_memory_MemTotal_bytes ")):
            name, value = line.split()
            values[name] = int(float(value))
    available = values["node_memory_MemAvailable_bytes"]
    total = values["node_memory_MemTotal_bytes"]
    if not 0 < available <= total:
        raise ValueError("Invalid node memory measurement")
    return {"available_bytes": available, "total_bytes": total, "source": "node-exporter /proc/meminfo"}


def lease_seconds(value):
    seconds = int(value)
    if not 30 <= seconds <= 300:
        raise ValueError("Duration must be between 30 and 300 seconds")
    return seconds
