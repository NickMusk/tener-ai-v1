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

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_job_source_filters_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.agents import SourcingAgent
from tener_ai.db import Database


class _SourceProvider:
    def search_profiles(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        return []


class _MatchingEngineStub:
    def build_job_requirements(self, job: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "title": str(job.get("title") or ""),
            "target_seniority": str(job.get("seniority") or "middle"),
            "must_have_skills": ["python", "django", "aws"],
            "nice_to_have_skills": ["docker"],
            "questionable_skills": ["recruiting"],
            "location": job.get("location"),
            "preferred_languages": job.get("preferred_languages") or [],
        }

    def build_core_profile(self, job: Dict[str, Any], max_skills: int = 6) -> Dict[str, Any]:
        return {
            "title": str(job.get("title") or ""),
            "target_seniority": str(job.get("seniority") or "middle"),
            "core_skills": ["python", "django", "aws"],
            "location": job.get("location"),
            "preferred_languages": job.get("preferred_languages") or [],
        }


class _WorkflowStub:
    def __init__(self) -> None:
        self.sourcing_agent = SourcingAgent(_SourceProvider(), matching_engine=_MatchingEngineStub())


class JobSourceFiltersApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "job_source_filters.sqlite3"))
        self.db.init_schema()
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "workflow": _WorkflowStub(),
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
        body = json.loads(raw) if raw else {}
        return status, body

    def test_job_source_filters_preview_returns_structured_search_preview(self) -> None:
        job_id = self.db.insert_job(
            title="Senior Backend Engineer",
            company="Tener",
            jd_text="Need Python, Django, AWS, Docker and PostgreSQL for backend microservices.",
            location="Germany",
            preferred_languages=["en", "de"],
            seniority="senior",
        )

        status, payload = self._request("GET", f"/api/jobs/{job_id}/source-filters")
        self.assertEqual(status, 200)
        self.assertEqual(int(payload.get("job_id") or 0), job_id)
        self.assertEqual(str(payload.get("job_title") or ""), "Senior Backend Engineer")
        self.assertEqual(str(payload.get("job_company") or ""), "Tener")

        filters = payload.get("filters") or {}
        self.assertEqual(str(filters.get("title") or ""), "Senior Backend Engineer")
        self.assertEqual(str(filters.get("primary_query") or ""), "Senior Backend Engineer")
        self.assertEqual(str(filters.get("location") or ""), "Germany")
        self.assertEqual(str(filters.get("seniority") or ""), "senior")
        self.assertEqual(filters.get("preferred_languages") or [], ["en", "de"])
        self.assertEqual(filters.get("filters") or {}, {
            "location": "Germany",
            "skills": ["python", "django", "aws"],
            "profile_language": ["en", "de"],
        })
        self.assertEqual(filters.get("must_have_skills") or [], ["python", "django", "aws"])
        self.assertEqual(filters.get("nice_to_have_skills") or [], ["docker"])
        self.assertEqual(filters.get("questionable_skills") or [], ["recruiting"])
        self.assertEqual(filters.get("fallback_queries") or [], [
            "Senior Backend Engineer",
            "Senior Backend Engineer Germany",
            "Senior Backend Engineer python",
            "Senior Backend Engineer Germany python",
            "Senior Backend Engineer django",
            "Senior Backend Engineer Germany django",
            "Senior Backend Engineer aws",
            "Senior Backend Engineer Germany aws",
        ])

    def test_job_source_filters_preview_returns_not_found_for_unknown_job(self) -> None:
        status, payload = self._request("GET", "/api/jobs/999/source-filters")
        self.assertEqual(status, 404)
        self.assertIn("job not found", str(payload.get("error") or ""))


if __name__ == "__main__":
    unittest.main()
