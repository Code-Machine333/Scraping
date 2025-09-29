from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

METRICS_FILE = Path("data/cache/metrics_last.json")


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        content = self._render_prom()
        data = content.encode("utf-8")
        

    def log_message(self, fmt, *args):
        # silence default stderr logging
        return  

    def _render_prom(self) -> str:
        metrics = {}
        if METRICS_FILE.exists():
            try:
                metrics = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
            except Exception:
                metrics = {}
        lines = []
        dur = metrics.get("durations", {}) or {}
        for k, v in dur.items():
            lines.append(f"etl_step_duration_seconds{{step=\"{k}\"}} {float(v):.3f}")
        # Simple gauges can be extended to include queue_depth etc. if desired
        return "\n".join(lines) + "\n"


def serve(host: str = "127.0.0.1", port: int = 9109):
    httpd = HTTPServer((host, port), MetricsHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    serve()


