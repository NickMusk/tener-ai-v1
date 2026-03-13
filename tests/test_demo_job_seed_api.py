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

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_demo_job_seed_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.demo_jobs import MAIN_DASHBOARD_DEMO_SEED_KEY, MainDashboardDemoJobSeeder
from tener_ai.db import Database
from tener_ai.pre_resume_service import PreResumeCommunicationService


EXPECTED_ATS = {
    "queued": 92,
    "queued_delivery": 12,
    "connect_sent": 28,
    "dialogue": 26,
    "cv_received": 18,
    "interview_pending": 9,
    "completed": 15,
}


class DemoJobSeedApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self._previous_admin_token = os.environ.get("TENER_ADMIN_API_TOKEN")
        os.environ["TENER_ADMIN_API_TOKEN"] = "seed-admin-token"
        self.db = Database(str(tmp_path / "demo_job_seed.sqlite3"))
        self.db.init_schema()
        self.pre_resume = PreResumeCommunicationService(
            templates_path=str(self.root / "config" / "outreach_templates.json"),
        )
        self.seeder = MainDashboardDemoJobSeeder(db=self.db, pre_resume_service=self.pre_resume)
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "pre_resume": self.pre_resume,
            "postgres_dsn": "",
        }
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = "http://127.0.0.1:%s" % self.server.server_port
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services
        if self._previous_admin_token is None:
            os.environ.pop("TENER_ADMIN_API_TOKEN", None)
        else:
            os.environ["TENER_ADMIN_API_TOKEN"] = self._previous_admin_token
        self._tmp.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        include_auth: bool = True,
    ) -> Tuple[int, Dict[str, Any]]:
        headers: Dict[str, str] = {}
        data = None
        if include_auth:
            headers["Authorization"] = "Bearer seed-admin-token"
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url="%s%s" % (self.base_url, path), method=method, data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=30) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8")
        return status, json.loads(raw) if raw else {}

    def test_seed_demo_job_creates_full_dashboard_dataset(self) -> None:
        payload = self.seeder.ensure_seeded()
        self.assertTrue(bool(payload.get("seeded")))
        self.assertEqual(str(payload.get("seed_key") or ""), MAIN_DASHBOARD_DEMO_SEED_KEY)

        job_id = int(payload.get("job_id") or 0)
        self.assertGreater(job_id, 0)
        job = self.db.get_job(job_id) or {}
        self.assertEqual(str(job.get("title") or ""), "Middle JS Developer")
        self.assertEqual(str(job.get("company") or ""), "Interexy")
        self.assertEqual(str(job.get("location") or ""), "Warsaw, Poland / Hybrid")

        progress = self.db.list_job_step_progress(job_id)
        by_step = {str(item.get("step") or ""): item for item in progress}
        self.assertEqual(str((by_step.get("workflow") or {}).get("status") or ""), "success")
        self.assertEqual(str((by_step.get("outreach") or {}).get("status") or ""), "success")
        interview_output = (by_step.get("interview_assessment") or {}).get("output_json") or {}
        self.assertEqual(len(interview_output.get("questions") or []), 5)

        candidates = self.db.list_candidates_for_job(job_id)
        self.assertEqual(len(candidates), 200)
        selected_candidate = next(
            (item for item in candidates if str(item.get("full_name") or "") == "Aleksandra Wisniewska"),
            None,
        )
        self.assertIsNotNone(selected_candidate)
        self.assertEqual(str((selected_candidate or {}).get("current_status_key") or ""), "interview_passed")

        ats_payload = api_main.TenerRequestHandler._build_outreach_ats_board(  # type: ignore[attr-defined]
            db=self.db,
            job_id=job_id,
            limit=600,
        )
        self.assertEqual((ats_payload.get("summary") or {}), {"total_candidates": 200, **EXPECTED_ATS, "delivery_blocked": 0})

    def test_seed_demo_job_is_idempotent(self) -> None:
        first = self.seeder.ensure_seeded()
        second = self.seeder.ensure_seeded()
        self.assertTrue(bool(first.get("created")))
        self.assertFalse(bool(second.get("created")))
        self.assertFalse(bool(second.get("seeded")))
        self.assertEqual(int(first.get("job_id") or 0), int(second.get("job_id") or 0))
        self.assertEqual(len(self.db.list_candidates_for_job(int(first.get("job_id") or 0))), 200)

    def test_admin_seed_endpoint_requires_auth(self) -> None:
        status, body = self._request("POST", "/api/admin/seeds/full-demo-job", include_auth=False)
        self.assertEqual(status, 401)
        self.assertEqual(str(body.get("error") or ""), "admin auth required")

    def test_admin_seed_endpoint_triggers_reseed(self) -> None:
        status, created = self._request("POST", "/api/admin/seeds/full-demo-job", {})
        self.assertEqual(status, 200)
        self.assertTrue(bool(created.get("seeded")))
        job_id = int(created.get("job_id") or 0)
        self.assertGreater(job_id, 0)

        status, reseeded = self._request("POST", "/api/admin/seeds/full-demo-job", {"force_reseed": True})
        self.assertEqual(status, 200)
        self.assertTrue(bool(reseeded.get("seeded")))
        self.assertEqual(int(reseeded.get("job_id") or 0), job_id)

        ats_payload = api_main.TenerRequestHandler._build_outreach_ats_board(  # type: ignore[attr-defined]
            db=self.db,
            job_id=job_id,
            limit=600,
        )
        self.assertEqual((ats_payload.get("summary") or {}), {"total_candidates": 200, **EXPECTED_ATS, "delivery_blocked": 0})


if __name__ == "__main__":
    unittest.main()
