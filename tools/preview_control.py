"""Local UI preview with real Lab status and a hard block on all mutations."""

import argparse
from http.server import ThreadingHTTPServer
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.control import handler
from app.demo_catalog import public_demos
from tools.run_scenarios import request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--port", type=int, default=8768)
    parser.add_argument("--unavailable", action="store_true", help="Explicit local UI failure-state fixture; not demo evidence")
    args = parser.parse_args()

    class ReadOnlyState:
        def status(self):
            data = request(args.lab.rstrip("/") + "/api/status")
            data.update(demos=public_demos(), prometheus_url="http://192.168.0.102:30090", read_only_preview=True)
            return data

    class Preview(handler(ReadOnlyState())):
        def do_GET(self):
            if args.unavailable and self.path == "/api/status":
                self.send_json(503, {"error": "Local unavailable-state fixture"})
                return
            try:
                super().do_GET()
            except OSError:
                self.send_json(503, {"error": "Live Lab status unavailable"})

        def do_POST(self):
            self.send_json(405, {"error": "Read-only local preview; no cluster mutation performed"})

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Preview)
    print(f"Read-only Lab UI: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
