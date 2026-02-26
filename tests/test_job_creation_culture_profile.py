from __future__ import annotations

import json
import os
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_job_culture_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database


class _CultureService:
    def generate(self, company_name: str, website_url: str) -> Dict[str, Any]:
        return {
            "company_name": company_name,
            "website": website_url,
            "search_queries": [f"{company_name} culture values"],
            "sources": [{"url": website_url, "domain": "example.com", "source_kind": "official"}],
            "warnings": [],
            "profile": {
                "culture_values": ["ownership", "candor", "high standards"],
                "culture_interview_questions": [
                    "Tell us about a time you challenged a decision with evidence",
                ],
                "summary_200_300_words": "High ownership team with fast iteration and explicit accountability.",
            },
        }


class JobCreationCultureProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "jobs_culture.sqlite3"))
        self.db.init_schema()
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "company_culture": _CultureService(),
            "interview_api_base": "",
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

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        data = None
        headers: Dict[str, str] = {}
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"{self.base_url}{path}", method=method, data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8")
        if raw:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {"raw_text": raw}
        else:
            body = {}
        return status, body

    def test_create_job_rejects_invalid_company_website(self) -> None:
        status, body = self._request(
            "POST",
            "/api/jobs",
            {
                "title": "Senior Backend Engineer",
                "jd_text": "Need Python and AWS",
                "company_website": "ftp://internal-system",
            },
        )
        self.assertEqual(status, 400)
        self.assertIn("company_website", str(body.get("error") or ""))

    def test_create_job_generates_culture_profile_and_exposes_it_on_job(self) -> None:
        status, created = self._request(
            "POST",
            "/api/jobs",
            {
                "title": "Senior Backend Engineer",
                "company": "Tener",
                "jd_text": "Need Python and AWS",
                "company_website": "https://www.tener.ai",
            },
        )
        self.assertEqual(status, 201)
        job_id = int(created.get("job_id") or 0)
        self.assertGreater(job_id, 0)
        self.assertEqual(str((created.get("company_culture_profile") or {}).get("status") or ""), "pending")

        deadline = time.time() + 3.0
        profile = None
        while time.time() < deadline:
            profile = self.db.get_job_culture_profile(job_id)
            if isinstance(profile, dict) and str(profile.get("status") or "") == "ready":
                break
            time.sleep(0.05)
        self.assertIsInstance(profile, dict)
        self.assertEqual(str((profile or {}).get("status") or ""), "ready")

        status_job, job = self._request("GET", f"/api/jobs/{job_id}")
        self.assertEqual(status_job, 200)
        self.assertEqual(str(job.get("company_website") or ""), "https://tener.ai/")
        culture = job.get("company_culture_profile") if isinstance(job.get("company_culture_profile"), dict) else {}
        self.assertGreater(len(culture.get("culture_interview_questions") or []), 0)


if __name__ == "__main__":
    unittest.main()
