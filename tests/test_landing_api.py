from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_landing_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.auth import AuthRepository, AuthService
from tener_ai.db import Database
from tener_ai.landing import LandingService


class LandingApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.db = Database(str(tmp / "landing_api.sqlite3"))
        self.db.init_schema()

        auth_repo = AuthRepository(
            backend="sqlite",
            sqlite_path=str(tmp / "auth.sqlite3"),
        )
        auth_repo.init_schema()
        org_id = auth_repo.create_organization(name="Tener QA")
        user_id = auth_repo.create_user(email="qa-admin@tener.local", full_name="QA Admin")
        auth_repo.upsert_membership(org_id=org_id, user_id=user_id, role="admin", is_active=True)

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "landing": LandingService(self.db),
            "auth": AuthService(enabled=True, repository=auth_repo, legacy_admin_token=""),
        }

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services
        self._tmp.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, bytes, Dict[str, str]]:
        req_headers = dict(headers or {})
        data = None
        if payload is not None:
            req_headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"{self.base_url}{path}", method=method, data=data, headers=req_headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                return int(resp.status), resp.read(), {str(k): str(v) for k, v in resp.headers.items()}
        except error.HTTPError as exc:
            return int(exc.code), exc.read(), {str(k): str(v) for k, v in exc.headers.items()}

    def test_root_page_is_public_and_contains_functional_forms(self) -> None:
        status, raw, headers = self._request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", str(headers.get("Content-Type") or ""))
        body = raw.decode("utf-8")
        self.assertIn("Subscribe to News", body)
        self.assertIn('id="newsletter-form"', body)
        self.assertIn('id="contact-form"', body)
        self.assertIn("/favicon.png", body)

        dashboard_status, dashboard_raw, dashboard_headers = self._request("GET", "/dashboard")
        self.assertEqual(dashboard_status, 200)
        self.assertIn("text/html", str(dashboard_headers.get("Content-Type") or ""))
        self.assertIn("Tener", dashboard_raw.decode("utf-8"))

        protected_status, protected_raw, _ = self._request("GET", "/api/jobs")
        self.assertEqual(protected_status, 401)
        self.assertEqual(json.loads(protected_raw.decode("utf-8")).get("error"), "auth_required")

    def test_favicon_is_public(self) -> None:
        status, raw, headers = self._request("GET", "/favicon.ico")
        self.assertEqual(status, 200)
        self.assertGreater(len(raw), 10)
        self.assertEqual(str(headers.get("Content-Type") or ""), "image/png")

    def test_newsletter_endpoint_persists_and_handles_duplicates(self) -> None:
        status_created, raw_created, _ = self._request(
            "POST",
            "/api/landing/newsletter",
            {"email": "Pilot@Example.com", "full_name": "Pilot User"},
        )
        self.assertEqual(status_created, 201)
        created_payload = json.loads(raw_created.decode("utf-8"))
        self.assertEqual(created_payload.get("status"), "subscribed")

        status_duplicate, raw_duplicate, _ = self._request(
            "POST",
            "/api/landing/newsletter",
            {"email": "pilot@example.com", "company_name": "Acme"},
        )
        self.assertEqual(status_duplicate, 200)
        duplicate_payload = json.loads(raw_duplicate.decode("utf-8"))
        self.assertEqual(duplicate_payload.get("status"), "already_subscribed")

        rows = self.db.list_newsletter_subscriptions(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "pilot@example.com")
        self.assertEqual(rows[0]["company_name"], "Acme")

    def test_contact_endpoint_validates_and_persists(self) -> None:
        bad_status, bad_raw, _ = self._request(
            "POST",
            "/api/landing/contact",
            {"full_name": "", "work_email": "wrong", "company_name": "", "hiring_need": ""},
        )
        self.assertEqual(bad_status, 400)
        bad_payload = json.loads(bad_raw.decode("utf-8"))
        self.assertEqual(bad_payload.get("error"), "validation_failed")
        self.assertIn("work_email", bad_payload.get("field_errors") or {})

        good_status, good_raw, _ = self._request(
            "POST",
            "/api/landing/contact",
            {
                "full_name": "Jane Founder",
                "work_email": "jane@company.com",
                "company_name": "Acme Labs",
                "job_title": "Senior ML Engineer",
                "hiring_need": "Need a shortlist for an onsite ML platform lead within 30 days.",
            },
        )
        self.assertEqual(good_status, 201)
        good_payload = json.loads(good_raw.decode("utf-8"))
        self.assertEqual(good_payload.get("status"), "received")

        rows = self.db.list_contact_requests(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["work_email"], "jane@company.com")
        self.assertEqual(rows[0]["job_title"], "Senior ML Engineer")


if __name__ == "__main__":
    unittest.main()
