from __future__ import annotations

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

import os

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_job_archiving_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database
from tener_ai.db_dual import DualWriteDatabase


class JobArchivingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.db = Database(str(tmp / "job_archiving.sqlite3"))
        self.db.init_schema()
        self.manual_job_id = self.db.insert_job(
            title="Manual QA Engineer",
            jd_text="Manual QA",
            location="Remote",
            preferred_languages=["en"],
            seniority="junior",
        )
        self.backend_job_id = self.db.insert_job(
            title="Senior Backend Engineer",
            jd_text="Python",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        self.frontend_job_id = self.db.insert_job(
            title="Frontend Engineer",
            jd_text="React",
            location="Remote",
            preferred_languages=["en"],
            seniority="middle",
        )

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "read_db": self.db,
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
        headers = {}
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"{self.base_url}{path}", method=method, headers=headers, data=data)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8")
        body = json.loads(raw) if raw else {}
        return status, body

    def test_bulk_archive_excludes_manual_qa_and_hides_archived_from_default_list(self) -> None:
        result = self.db.archive_jobs(exclude_titles=["Manual QA Engineer"])
        self.assertEqual(int(result.get("updated") or 0), 2)

        visible_jobs = self.db.list_jobs(limit=10)
        self.assertEqual([int(job.get("id") or 0) for job in visible_jobs], [self.manual_job_id])

        archived_backend = self.db.get_job(self.backend_job_id) or {}
        self.assertTrue(bool(archived_backend.get("is_archived")))
        self.assertTrue(str(archived_backend.get("archived_at") or "").strip())

    def test_jobs_api_archive_bulk_archives_all_except_manual_qa(self) -> None:
        status_archive, payload_archive = self._request(
            "POST",
            "/api/jobs/archive-bulk",
            {"exclude_titles": ["Manual QA Engineer"]},
        )
        self.assertEqual(status_archive, 200)
        self.assertEqual(int(payload_archive.get("updated") or 0), 2)

        status_jobs, payload_jobs = self._request("GET", "/api/jobs")
        self.assertEqual(status_jobs, 200)
        items = payload_jobs.get("items") or []
        self.assertEqual(len(items), 1)
        self.assertEqual(str(items[0].get("title") or ""), "Manual QA Engineer")

    def test_jobs_api_archive_bulk_archives_specific_job_ids(self) -> None:
        status_archive, payload_archive = self._request(
            "POST",
            "/api/jobs/archive-bulk",
            {"job_ids": [self.backend_job_id]},
        )
        self.assertEqual(status_archive, 200)
        self.assertEqual(int(payload_archive.get("updated") or 0), 1)

        status_jobs, payload_jobs = self._request("GET", "/api/jobs")
        self.assertEqual(status_jobs, 200)
        visible_titles = [str(item.get("title") or "") for item in (payload_jobs.get("items") or [])]
        self.assertEqual(visible_titles, ["Frontend Engineer", "Manual QA Engineer"])

    def test_dual_write_archive_jobs_mirrors_archived_at(self) -> None:
        class _Mirror:
            def __init__(self) -> None:
                self.rows = []

            def upsert_job(self, row: Dict[str, Any]) -> None:
                self.rows.append(dict(row))

        mirror = _Mirror()
        dual = DualWriteDatabase(primary=self.db, mirror=mirror, strict=True)

        result = dual.archive_jobs(exclude_titles=["Manual QA Engineer"])
        self.assertEqual(int(result.get("updated") or 0), 2)
        archived_titles = {
            str(row.get("title") or "")
            for row in mirror.rows
            if str(row.get("archived_at") or "").strip()
        }
        self.assertIn("Senior Backend Engineer", archived_titles)
        self.assertIn("Frontend Engineer", archived_titles)


if __name__ == "__main__":
    unittest.main()
