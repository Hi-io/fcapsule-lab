"""Deploy the disposable lab sequentially without a rolling-surge memory spike."""

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from urllib.request import urlopen

import yaml


ROOT = Path(__file__).resolve().parents[1]
KUBECTL = shutil.which("kubectl") or "/snap/bin/kubectl"


def kubectl(*args, input=None):
    return subprocess.run([KUBECTL, *args], input=input, text=True, check=True, capture_output=True).stdout


def memory_bytes(url):
    with urlopen(url.rstrip("/") + "/metrics", timeout=5) as response:
        text = response.read(4_000_000).decode()
    return int(float(next(line.split()[1] for line in text.splitlines() if line.startswith("node_memory_MemAvailable_bytes "))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-exporter", required=True)
    parser.add_argument("--revision", help="Published lab commit SHA; defaults to the local HEAD")
    args = parser.parse_args()
    if memory_bytes(args.node_exporter) < 1024 ** 3:
        raise SystemExit("Deployment blocked: node MemAvailable is below 1 GiB")
    revision = args.revision or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise SystemExit("Use a full published commit SHA")
    documents = list(yaml.safe_load_all(kubectl("kustomize", str(ROOT / "deploy/kubernetes"))))
    deployments = []
    for item in documents:
        if item["kind"] == "Deployment":
            deployments.append(item)
        else:
            print(kubectl("apply", "-f", "-", input=json.dumps(item)), end="", flush=True)
    traffic = next(item for item in deployments if item["metadata"]["name"] == "traffic-generator")
    try:
        try:
            kubectl("scale", "deployment/traffic-generator", "-n", "fcapsule-lab", "--replicas=0")
        except subprocess.CalledProcessError:
            pass  # First deployment has no traffic generator yet.
        for item in sorted(deployments, key=lambda item: item["metadata"]["name"] == "traffic-generator"):
            name = item["metadata"]["name"]
            if memory_bytes(args.node_exporter) < 1024 ** 3:
                raise RuntimeError("Deployment stopped: node MemAvailable fell below 1 GiB")
            for init in item["spec"]["template"]["spec"].get("initContainers", []):
                if init["name"] == "install-source":
                    init["command"][-1] = f"https://github.com/Hi-io/fcapsule-lab/archive/{revision}.zip"
            print(kubectl("apply", "-f", "-", input=json.dumps(item)), end="", flush=True)
            print(kubectl("rollout", "status", f"deployment/{name}", "-n", "fcapsule-lab", "--timeout=240s"), end="", flush=True)
    finally:
        try:
            kubectl("scale", "deployment/traffic-generator", "-n", "fcapsule-lab", f"--replicas={traffic['spec']['replicas']}")
        except subprocess.CalledProcessError:
            pass


if __name__ == "__main__":
    main()
