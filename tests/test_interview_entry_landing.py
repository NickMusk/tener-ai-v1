from __future__ import annotations

import os
import threading
import unittest
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from types import SimpleNamespace
from urllib import error, request
from urllib.parse import urlparse

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_interview_entry_bootstrap.sqlite3"))

from tener_interview import http_api
from tener_interview.db import InterviewDatabase, InterviewPostgresDatabase
from tener_interview.providers.hireflix_mock import HireflixMockAdapter
from tener_interview.scoring import InterviewScoringEngine
from tener_interview.service import InterviewService
from tener_interview.token_service import InterviewTokenService


class _SourceCatalog:
    def get_job(self, job_id: int):
        return {
            "id": int(job_id),
            "title": "Senior Backend Engineer",
            "company": "TechCorp",
            "jd_text": (
                "Build and scale backend systems for a product used by high-growth teams.\n\n"
                "- Own core services\n"
                "- Improve system reliability\n"
                "- Work closely with product and design"
            ),
            "location": "Remote",
            "seniority": "senior",
            "salary_min": 150000,
            "salary_max": 200000,
            "salary_currency": "USD",
            "preferred_languages": ["en"],
            "must_have_skills": ["python", "aws", "postgresql"],
            "nice_to_have_skills": ["docker"],
        }


class _NoRedirectHandler(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


class InterviewEntryLandingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        db_path = str(Path(self.tmp.name) / "interview.sqlite3")
        self.db = InterviewDatabase(db_path=db_path)
        self.db.init_schema()
        self.provider = HireflixMockAdapter()
        self.service = InterviewService(
            db=self.db,
            provider=self.provider,
            token_service=InterviewTokenService(secret="entry-test-secret"),
            scoring_engine=InterviewScoringEngine(),
            source_catalog=_SourceCatalog(),
            default_ttl_hours=72,
            public_base_url="http://127.0.0.1:8090",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_entry_landing_preview_keeps_session_invited(self) -> None:
        started = self.service.start_session(
            job_id=11,
            candidate_id=42,
            candidate_name="Jane Candidate",
            conversation_id=7,
            language="en",
        )
        token = Path(urlparse(started["entry_url"]).path).name

        preview = self.service.get_entry_landing(token)

        self.assertEqual(preview["status"], "invited")
        self.assertEqual(preview["landing"]["job"]["title"], "Senior Backend Engineer")
        self.assertEqual(preview["landing"]["job"]["company"], "TechCorp")
        self.assertEqual(preview["landing"]["candidate"]["name"], "Jane Candidate")
        self.assertEqual(preview["landing"]["job"]["salary_text"], "$150,000 - $200,000")
        self.assertTrue(preview["landing"]["job"]["skills"])

        session = self.db.get_session(started["session_id"])
        assert session is not None
        self.assertEqual(session["status"], "invited")

    def test_http_entry_routes_render_preview_and_redirect_on_start(self) -> None:
        started = self.service.start_session(
            job_id=12,
            candidate_id=77,
            candidate_name="Alex Candidate",
            conversation_id=9,
            language="en",
        )
        token = Path(urlparse(started["entry_url"]).path).name

        previous_services = http_api.SERVICES
        http_api.SERVICES = {
            "config": SimpleNamespace(public_base_url="", host="127.0.0.1", port=0),
            "db": self.db,
            "source_db": _SourceCatalog(),
            "provider_name": "hireflix",
            "provider_error": "",
            "interview": self.service,
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), http_api.InterviewRequestHandler)
        base_url = f"http://127.0.0.1:{server.server_port}"
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        try:
            status_html, raw_html, headers_html = self._request("GET", f"{base_url}/i/{token}")
            self.assertEqual(status_html, 200)
            self.assertIn("text/html", str(headers_html.get("Content-Type") or ""))
            self.assertIn("Preparing your interview", raw_html.decode("utf-8"))

            status_api, raw_api, headers_api = self._request("GET", f"{base_url}/api/interviews/entry/{token}")
            self.assertEqual(status_api, 200)
            self.assertIn("application/json", str(headers_api.get("Content-Type") or ""))
            self.assertIn("Senior Backend Engineer", raw_api.decode("utf-8"))
            self.assertIn(f"/i/{token}/start", raw_api.decode("utf-8"))

            opener = request.build_opener(_NoRedirectHandler)
            req = request.Request(url=f"{base_url}/i/{token}/start", method="GET")
            with self.assertRaises(error.HTTPError) as ctx:
                opener.open(req, timeout=20)
            self.assertEqual(ctx.exception.code, 302)
            self.assertIn("app.hireflix.com", str(ctx.exception.headers.get("Location") or ""))

            session = self.db.get_session(started["session_id"])
            assert session is not None
            self.assertEqual(session["status"], "in_progress")
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=3)
            http_api.SERVICES = previous_services

    def test_postgres_row_to_dict_decodes_landing_json(self) -> None:
        parsed = InterviewPostgresDatabase._row_to_dict(
            {
                "entry_context_json": '{"job":{"title":"Manual QA","company":"Tener"}}',
                "meta_json": '{"categories":{"hard_skills":3}}',
                "created_at": datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc),
            }
        )

        self.assertEqual(parsed["entry_context_json"]["job"]["title"], "Manual QA")
        self.assertEqual(parsed["entry_context_json"]["job"]["company"], "Tener")
        self.assertEqual(parsed["meta_json"]["categories"]["hard_skills"], 3)
        self.assertEqual(parsed["created_at"], "2026-03-11T00:00:00+00:00")

    @staticmethod
    def _request(method: str, url: str):
        req = request.Request(url=url, method=method)
        try:
            with request.urlopen(req, timeout=20) as resp:
                return int(resp.status), resp.read(), {str(k): str(v) for k, v in resp.headers.items()}
        except error.HTTPError as exc:
            return int(exc.code), exc.read(), {str(k): str(v) for k, v in exc.headers.items()}


if __name__ == "__main__":
    unittest.main()
