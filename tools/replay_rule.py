"""Compare a rule revision against retained real Prometheus samples, read-only."""

import argparse
import json
import subprocess
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import yaml


def expression(text, name):
    objects = yaml.safe_load_all(text)
    return next(rule["expr"] for obj in objects if obj and obj.get("kind") == "PrometheusRule"
                for group in obj["spec"]["groups"] for rule in group["rules"] if rule["alert"] == name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alert", required=True)
    parser.add_argument("--time", required=True)
    parser.add_argument("--reference", required=True, help="Earlier Git revision of the rule")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    path = "deploy/kubernetes/observability.yaml"
    prior = subprocess.check_output(["git", "show", args.reference + ":" + path], cwd=root, text=True)
    result = {"alert": args.alert, "time": args.time, "reference": args.reference,
              "limitation": "Historical expression replay, not a new live firing or a replay of alert pending duration."}
    for name, content in (("reference", prior), ("current", (root / path).read_text())):
        query = expression(content, args.alert)
        with urlopen(args.prometheus + "/api/v1/query?" + urlencode({"query": query, "time": args.time}), timeout=20) as response:
            data = json.loads(response.read())
        if data.get("status") != "success":
            raise RuntimeError("Prometheus query failed")
        result[name + "_observation"] = {"expression": query, "result": data["data"]["result"]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: len(result[key + "_observation"]["result"]) for key in ("reference", "current")}))


if __name__ == "__main__":
    main()
