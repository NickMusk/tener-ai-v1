from __future__ import annotations

import json
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

    def test_it_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/it", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/it/")

    def test_it_index_is_served_without_diversity_copy(self) -> None:
        status, raw, headers = self._request_raw("/it/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        body = raw.decode("utf-8")
        self.assertIn("top 0.01% IT talent", body)
        self.assertNotIn("Diversity", body)
        self.assertNotIn("Zalando", body)
        self.assertNotIn("Women in Tech", body)

    def test_it_linked_pages_are_served_without_diversity_copy(self) -> None:
        status, raw, headers = self._request_raw("/it/searching.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        body = raw.decode("utf-8")
        self.assertIn("High-Skill Search", body)
        self.assertNotIn("Diversity", body)
        self.assertNotIn("Zalando", body)

        status, raw, headers = self._request_raw("/it/results.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        body = raw.decode("utf-8")
        self.assertIn("12 Candidates Ready for Review", body)
        self.assertNotIn("Diversity", body)
        self.assertNotIn("Zalando", body)
        self.assertNotIn("Women in Tech", body)

    def test_it_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/it/../README.md")
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

    def test_fiverr_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/fiverr", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/fiverr/")

    def test_fiverr_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/fiverr/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Fiverr Elite", raw.decode("utf-8"))

    def test_fiverr_linked_pages_are_served(self) -> None:
        status, raw, headers = self._request_raw("/fiverr/searching.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Start talent search", raw.decode("utf-8"))

    def test_fiverr_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/fiverr/../README.md")
        self.assertEqual(status, 404)

    def test_skilled_trades_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/skilled-trades", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/skilled-trades/")

    def test_skilled_trades_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/skilled-trades/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Skilled Trades Staffing", raw.decode("utf-8"))

    def test_skilled_trades_landing_and_linked_pages_are_served(self) -> None:
        status, raw, headers = self._request_raw("/skilled-trades/landing.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("48 Hours Guaranteed", raw.decode("utf-8"))

        status, raw, headers = self._request_raw("/skilled-trades/search.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("View Shortlist", raw.decode("utf-8"))

    def test_skilled_trades_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/skilled-trades/../README.md")
        self.assertEqual(status, 404)

    def test_agents_office_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/agents-office", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/agents-office/")

    def test_agents_office_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/agents-office/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Tener Agent Office", raw.decode("utf-8"))

    def test_agents_office_supporting_files_are_served(self) -> None:
        status, raw, headers = self._request_raw("/agents-office/scenario.json")
        self.assertEqual(status, 200)
        self.assertIn("application/json", str(headers.get("Content-Type") or ""))
        self.assertIn("Industrial Electrician, Midland TX", raw.decode("utf-8"))

        status, raw, headers = self._request_raw("/agents-office/AI_TOWN_CREDITS.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("AI Town Credits", raw.decode("utf-8"))

    def test_agents_office_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/agents-office/../README.md")
        self.assertEqual(status, 404)

    def test_agents_office_public_jobs_api_returns_catalog(self) -> None:
        status, raw, headers = self._request_raw("/api/demo/agents-office/jobs")
        self.assertEqual(status, 200)
        self.assertIn("application/json", str(headers.get("Content-Type") or ""))
        payload = json.loads(raw.decode("utf-8"))
        self.assertIn("items", payload)
        self.assertTrue(isinstance(payload["items"], list))
        self.assertTrue(payload["items"])
        first = payload["items"][0]
        self.assertIn("title", first)
        self.assertIn("market_leads", first)
        self.assertIn("live_threads", first)
        self.assertIn("buyer_finalists", first)

    def test_toptal_root_redirects_to_trailing_slash(self) -> None:
        opener = request.build_opener(_NoRedirectHandler)
        req = request.Request(url=f"{self.base_url}/toptal", method="GET")
        with self.assertRaises(error.HTTPError) as ctx:
            opener.open(req, timeout=20)
        self.assertEqual(ctx.exception.code, 301)
        self.assertEqual(str(ctx.exception.headers.get("Location") or ""), "/toptal/")

    def test_toptal_index_is_served(self) -> None:
        status, raw, headers = self._request_raw("/toptal/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Toptal AI", raw.decode("utf-8"))

    def test_toptal_linked_pages_are_served(self) -> None:
        status, raw, headers = self._request_raw("/toptal/searching.html")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        self.assertIn("Start expert search", raw.decode("utf-8"))

    def test_toptal_path_traversal_is_blocked(self) -> None:
        status, _, _ = self._request_raw("/toptal/../README.md")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
