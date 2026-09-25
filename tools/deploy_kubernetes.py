"""Deploy the disposable lab sequentially without a rolling-surge memory spike."""

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.request import urlopen

import yaml


ROOT = Path(__file__).resolve().parents[1]
KUBECTL = None
CODE_DEPLOYMENTS = {
    "inventory-api",
    "orders-api",
    "traffic-generator",
    "lab-worker",
    "lab-control",
    "cnfc-edge-a",
    "cnfc-edge-b",
}


def resolve_kubectl(explicit=None):
    """Resolve kubectl from an explicit path, environment, PATH, or user install."""
    for configured in (explicit, os.environ.get("KUBECTL")):
        if configured:
            resolved = shutil.which(configured)
            if resolved:
                return resolved
            raise SystemExit(f"kubectl executable not found: {configured}")

    resolved = shutil.which("kubectl")
    if resolved:
        return resolved

    local_install = str(Path.home() / ".local" / "bin" / "kubectl")
    resolved = shutil.which(local_install)
    if resolved:
        return resolved

    raise SystemExit("kubectl not found; add it to PATH, set KUBECTL, or pass --kubectl PATH")


def kubectl(*args, input=None):
    executable = KUBECTL or resolve_kubectl()
    return subprocess.run([executable, *args], input=input, text=True, check=True, capture_output=True).stdout


def memory_bytes(url):
    with urlopen(url.rstrip("/") + "/metrics", timeout=5) as response:
        text = response.read(4_000_000).decode()
    return int(float(next(line.split()[1] for line in text.splitlines() if line.startswith("node_memory_MemAvailable_bytes "))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-exporter", required=True)
    parser.add_argument("--revision", help="Published lab commit SHA; defaults to the local HEAD")
    parser.add_argument("--kubectl", help="kubectl executable path; defaults to KUBECTL, PATH, or ~/.local/bin/kubectl")
    parser.add_argument(
        "--apps-only",
        action="store_true",
        help="update source-installed application Deployments only; leave config, database, services, and monitoring unchanged",
    )
    args = parser.parse_args()
    global KUBECTL
    KUBECTL = resolve_kubectl(args.kubectl)
    if memory_bytes(args.node_exporter) < 1024 ** 3:
        raise SystemExit("Deployment blocked: node MemAvailable is below 1 GiB")
    revision = args.revision or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise SystemExit("Use a full published commit SHA")
    documents = list(yaml.safe_load_all(kubectl("kustomize", str(ROOT / "deploy/kubernetes"))))
    deployments = []
    traffic = next(
        item for item in documents
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "traffic-generator"
    )
    live_traffic_replicas = kubectl(
        "get",
        "deployment/traffic-generator",
        "-n",
        "fcapsule-lab",
        "--ignore-not-found=true",
        "-o",
        "jsonpath={.spec.replicas}",
    ).strip()
    traffic_exists = bool(live_traffic_replicas)
    traffic_replicas = int(live_traffic_replicas) if live_traffic_replicas else int(traffic["spec"].get("replicas", 1))
    traffic["spec"]["replicas"] = traffic_replicas

    for item in documents:
        if item["kind"] == "Deployment":
            if args.apps_only and item["metadata"]["name"] not in CODE_DEPLOYMENTS:
                continue
            deployments.append(item)
        elif not args.apps_only:
            print(kubectl("apply", "-f", "-", input=json.dumps(item)), end="", flush=True)

    try:
        if traffic_exists:
            kubectl("scale", "deployment/traffic-generator", "-n", "fcapsule-lab", "--replicas=0")
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
        if traffic_exists:
            kubectl("scale", "deployment/traffic-generator", "-n", "fcapsule-lab", f"--replicas={traffic_replicas}")


if __name__ == "__main__":
    main()
