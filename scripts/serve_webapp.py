"""Serve webapp/ locally with browser caching turned off.

`python -m http.server` sends no cache headers, so browsers may reuse old
copies of some JS files after an edit. A mix of old and new modules fails
to load and leaves the page empty. Sending `no-store` makes every reload
fetch fresh files.

Usage, from the project root:
    python -m scripts.serve_webapp [port]
"""

import errno
import functools
import http.server
import sys
from pathlib import Path

WEBAPP_DIR = Path(__file__).resolve().parent.parent / "webapp"


class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    """Static file handler that tells the browser never to cache."""

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    handler = functools.partial(NoCacheHandler, directory=str(WEBAPP_DIR))
    try:
        server = http.server.ThreadingHTTPServer(("", port), handler)
    except OSError as err:
        if err.errno != errno.EADDRINUSE:
            raise
        sys.exit(
            f"Port {port} is already in use (maybe this server is already "
            f"running?). Open http://localhost:{port}, or pick another port: "
            f"python -m scripts.serve_webapp {port + 1}"
        )
    with server:
        print(f"Serving {WEBAPP_DIR} at http://localhost:{port} (Ctrl+C to stop)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")


if __name__ == "__main__":
    main()
