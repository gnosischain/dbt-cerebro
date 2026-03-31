"""Lightweight observability server for dbt-cerebro.

Replaces the bare `python -m http.server 8000` with a server that exposes
/health, /metrics (Prometheus), and serves static report/log files.

Modeled on cerebro-mcp's observability pattern but much simpler — this pod
only serves files and exposes scrape targets; it does not run dbt or edr.
"""

import json
import os
import sys
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

RUNTIME_DATA_DIR = os.environ.get("RUNTIME_DATA_DIR", "/data")
APP_VERSION = os.environ.get("APP_VERSION", "unknown")

dbt_cerebro_info = Gauge(
    "dbt_cerebro_info",
    "Static metadata about the running dbt-cerebro instance",
    ("version",),
)
dbt_cerebro_info.labels(version=APP_VERSION).set(1)

dbt_cerebro_server_up = Gauge(
    "dbt_cerebro_server_up",
    "Always 1 when the observability server is responsive",
)
dbt_cerebro_server_up.set(1)


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------


class ObservabilityHandler(SimpleHTTPRequestHandler):
    """Routes /health, /metrics, and static file serving for /logs and /reports."""

    # Suppress per-request access logs (too noisy for probes)
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._serve_health()
        elif self.path == "/metrics":
            self._serve_metrics()
        elif self.path.startswith("/logs/") or self.path.startswith("/reports/"):
            # Delegate to SimpleHTTPRequestHandler for static files
            super().do_GET()
        elif self.path == "/":
            self._serve_index()
        else:
            self.send_error(404)

    def _serve_health(self):
        body = json.dumps({"status": "ok"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_metrics(self):
        body = generate_latest() + self._semantic_metrics_payload()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_index(self):
        body = json.dumps(
            {
                "service": "dbt-cerebro",
                "version": APP_VERSION,
                "endpoints": ["/health", "/metrics", "/logs/", "/reports/"],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _semantic_metrics_payload(self) -> bytes:
        metric_paths = []
        seen = set()
        for directory in (
            Path(RUNTIME_DATA_DIR),
            Path(RUNTIME_DATA_DIR) / "metrics",
            Path(RUNTIME_DATA_DIR) / "target",
        ):
            try:
                if not directory.exists():
                    continue
            except PermissionError:
                continue
            for path in sorted(directory.glob("*.prom")):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                metric_paths.append(path)

        chunks = []
        for path in metric_paths:
            try:
                content = path.read_bytes().strip()
            except OSError:
                continue
            if content:
                chunks.append(content)

        if not chunks:
            return b""
        return b"\n" + b"\n".join(chunks) + b"\n"


def main():
    port = int(os.environ.get("PORT", "8000"))

    # Serve static files from /app/www (which has symlinks to /data/logs and /data/reports)
    www_dir = Path("/app/www")
    if not www_dir.exists():
        www_dir = Path(".")

    handler = partial(ObservabilityHandler, directory=str(www_dir))
    server = HTTPServer(("0.0.0.0", port), handler)

    print(f"dbt-cerebro observability server listening on :{port}", file=sys.stderr)
    print(f"  version={APP_VERSION}", file=sys.stderr)
    print(f"  runtime_data_dir={RUNTIME_DATA_DIR}", file=sys.stderr)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down", file=sys.stderr)
        server.shutdown()


if __name__ == "__main__":
    main()
