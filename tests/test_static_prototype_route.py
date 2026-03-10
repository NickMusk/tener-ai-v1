from __future__ import annotations

import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import gettempdir
from typing import Dict, Tuple
from urllib import error, request

# Prevent import-time bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_static_prototype_bootstrap.sqlite3"))

from tener_ai import main as api_main


class _NoRedirectHandler(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


class StaticPrototypeRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services

    def _request_raw(self, path: str) -> Tuple[int, bytes, Dict[str, str]]:
        req = request.Request(url=f"{self.base_url}{path}", method="GET")
        try:
            with request.urlopen(req, timeout=20) as resp:
                return int(resp.status), resp.read(), {str(k): str(v) for k, v in resp.headers.items()}
        except error.HTTPError as exc:
            return int(exc.code), exc.read(), {str(k): str(v) for k, v in exc.headers.items()}

    def test_zalando_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/zalando", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/zalando/")

    def test_zalando_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/zalando/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Enterprise AI Sourcing for Zalando", raw.decode("utf-8"))

    def test_zalando_linked_pages_are_served(self) -> None:
        status, raw, headers = self._request_raw("/zalando/searching.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Sourcing in Progress", raw.decode("utf-8"))

    def test_zalando_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/zalando/../README.md")
        self.assertEqual(status, 404)

    def test_liveramp_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/liveramp", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/liveramp/")

    def test_liveramp_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/liveramp/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Enterprise AI Sourcing for LiveRamp", raw.decode("utf-8"))

    def test_liveramp_linked_pages_are_served(self) -> None:
        status, raw, headers = self._request_raw("/liveramp/searching.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Offensive Security Search", raw.decode("utf-8"))

    def test_liveramp_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/liveramp/../README.md")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
