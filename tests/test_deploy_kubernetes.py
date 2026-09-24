import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import yaml

from tools import deploy_kubernetes


class DeployKubernetesTests(unittest.TestCase):
    def test_resolves_explicit_kubectl_path_before_other_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            explicit = Path(directory) / "kubectl-explicit"
            configured = Path(directory) / "kubectl-env"
            for executable in (explicit, configured):
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(0o755)

            with patch.dict(os.environ, {"KUBECTL": str(configured)}):
                self.assertEqual(deploy_kubernetes.resolve_kubectl(str(explicit)), str(explicit))

    def test_resolves_user_local_kubectl_when_path_is_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            executable = home / ".local" / "bin" / "kubectl"
            executable.parent.mkdir(parents=True)
            executable.write_text("#!/bin/sh\nexit 0\n")
            executable.chmod(0o755)

            with patch.dict(os.environ, {}, clear=True), patch.object(
                deploy_kubernetes.Path, "home", return_value=home
            ), patch.object(
                deploy_kubernetes.shutil,
                "which",
                side_effect=lambda value: str(executable) if value == str(executable) else None,
            ):
                self.assertEqual(deploy_kubernetes.resolve_kubectl(), str(executable))

    def test_fails_clearly_when_kubectl_is_unavailable(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(
            deploy_kubernetes.shutil, "which", return_value=None
        ), patch.object(deploy_kubernetes.Path, "home", return_value=Path("/missing")):
            with self.assertRaisesRegex(SystemExit, "kubectl not found"):
                deploy_kubernetes.resolve_kubectl()

    def test_deploy_modes_preserve_live_traffic_replicas(self):
        revision = "a" * 40
        documents = [
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "lab-runtime"}},
            self.deployment("mysql", source=False),
            self.deployment("mysql-exporter", source=False),
            self.deployment("inventory-api"),
            self.deployment("orders-api"),
            self.deployment("lab-worker"),
            self.deployment("lab-control"),
            self.deployment("traffic-generator", replicas=1),
            {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule", "metadata": {"name": "alerts"}},
        ]
        rendered = yaml.safe_dump_all(documents)

        for apps_only in (False, True):
            for live_replicas in (None, 0, 1):
                with self.subTest(apps_only=apps_only, live_replicas=live_replicas):
                    applied = []
                    scales = []
                    rollouts = []

                    def fake_kubectl(*args, input=None):
                        if args[0] == "kustomize":
                            return rendered
                        if args[0] == "get":
                            self.assertIn("--ignore-not-found=true", args)
                            return "" if live_replicas is None else str(live_replicas)
                        if args[0] == "scale":
                            scales.append(args[-1])
                            return "scaled\n"
                        if args[0] == "apply":
                            applied.append(json.loads(input))
                            return "configured\n"
                        if args[0] == "rollout":
                            rollouts.append(args[1])
                            return "rolled out\n"
                        self.fail(f"Unexpected kubectl call: {args}")

                    argv = [
                        "deploy_kubernetes.py",
                        "--node-exporter",
                        "http://worker-1:9100",
                        "--revision",
                        revision,
                        "--kubectl",
                        "/fake/kubectl",
                    ]
                    if apps_only:
                        argv.append("--apps-only")
                    with patch("sys.argv", argv), patch.object(
                        deploy_kubernetes, "resolve_kubectl", return_value="/fake/kubectl"
                    ), patch.object(deploy_kubernetes, "memory_bytes", return_value=2 * 1024**3), patch.object(
                        deploy_kubernetes, "kubectl", side_effect=fake_kubectl
                    ), redirect_stdout(io.StringIO()):
                        deploy_kubernetes.main()

                    applied_deployments = {
                        item["metadata"]["name"]: item
                        for item in applied
                        if item["kind"] == "Deployment"
                    }
                    expected = {"inventory-api", "orders-api", "lab-worker", "lab-control", "traffic-generator"}
                    if not apps_only:
                        expected |= {"mysql", "mysql-exporter"}
                    self.assertEqual(set(applied_deployments), expected)
                    replicas_to_preserve = 1 if live_replicas is None else live_replicas
                    self.assertEqual(applied_deployments["traffic-generator"]["spec"]["replicas"], replicas_to_preserve)
                    expected_scales = [] if live_replicas is None else ["--replicas=0", f"--replicas={live_replicas}"]
                    self.assertEqual(scales, expected_scales)
                    self.assertEqual(len(rollouts), 5 if apps_only else 7)
                    for item in applied_deployments.values():
                        init_containers = item["spec"]["template"]["spec"].get("initContainers", [])
                        if init_containers:
                            install_source = next(container for container in init_containers if container["name"] == "install-source")
                            self.assertEqual(
                                install_source["command"][-1],
                                f"https://github.com/Hi-io/fcapsule-lab/archive/{revision}.zip",
                            )
                    static_kinds = {item["kind"] for item in applied if item["kind"] != "Deployment"}
                    if apps_only:
                        self.assertFalse(static_kinds)
                    else:
                        self.assertEqual(static_kinds, {"ConfigMap", "PrometheusRule"})

    @staticmethod
    def deployment(name, replicas=1, source=True):
        spec = {"replicas": replicas, "template": {"spec": {}}}
        if source:
            spec["template"]["spec"]["initContainers"] = [
                {
                    "name": "install-source",
                    "command": ["python", "-m", "pip", "install", "https://github.com/Hi-io/fcapsule-lab/archive/refs/heads/main.zip"],
                }
            ]
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name},
            "spec": spec,
        }


if __name__ == "__main__":
    unittest.main()
