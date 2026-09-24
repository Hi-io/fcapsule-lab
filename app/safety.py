"""Small independent host-memory admission check for the disposable lab."""

import os
import time
from urllib.parse import urlsplit
from urllib.request import urlopen


def memory_snapshot():
    url = os.environ.get("NODE_EXPORTER_URL", "http://prometheus-prometheus-node-exporter.monitoring:9100/metrics")
    expected_node_ip = os.environ.get("NODE_IP", "").strip()
    endpoint_host = urlsplit(url).hostname
    if expected_node_ip and endpoint_host != expected_node_ip:
        raise ValueError("Node exporter endpoint does not match the Lab controller's scheduled node")
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
    return {
        "available_bytes": available,
        "total_bytes": total,
        "source": "node-exporter /proc/meminfo",
        "source_host": endpoint_host,
        "expected_node_ip": expected_node_ip or None,
        "node_identity_verified": bool(expected_node_ip and endpoint_host == expected_node_ip),
        "sampled_at": time.time(),
    }


def lease_seconds(value):
    seconds = int(value)
    if not 30 <= seconds <= 300:
        raise ValueError("Duration must be between 30 and 300 seconds")
    return seconds
