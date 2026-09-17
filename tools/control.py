"""Switch the inventory failure mode from a host terminal."""

from __future__ import annotations

import json
import sys
from urllib.request import Request, urlopen


VALID_MODES = {"normal", "lock-contention", "bad-database-config"}


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    if len(argv) != 1 or argv[0] not in VALID_MODES:
        print(f"Usage: python3 tools/control.py <{'|'.join(sorted(VALID_MODES))}>", file=sys.stderr)
        return 2
    request = Request(
        "http://127.0.0.1:8081/control/failure",
        data=json.dumps({"mode": argv[0]}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:
        print(response.read().decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
