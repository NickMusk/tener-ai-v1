from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch
from urllib import error, request

# Prevent import-time default service bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_read_db_routing_bootstrap.sqlite3"))

from tener_ai import main as api_main


class _BusyLock:
    def acquire(self, blocking: bool = False) -> bool:
        return False

    def release(self) -> None:
        return None

    def locked(self) -> bool:
        return True


class _FailWriteDB:
    def __init__(self) -> None:
        self.strict = False

    def list_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        raise AssertionError("primary db list_jobs should not be called")

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        raise AssertionError("primary db get_job should not be called")

    def list_job_step_progress(self, job_id: int) -> List[Dict[str, Any]]:
        raise AssertionError("primary db list_job_step_progress should not be called")

    def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        raise AssertionError("primary db list_logs should not be called")

    def list_candidates_for_job(self, job_id: int) -> List[Dict[str, Any]]:
        raise AssertionError("primary db list_candidates_for_job should not be called")

    def list_conversations_overview(self, limit: int = 200, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
        raise AssertionError("primary db list_conversations_overview should not be called")

    def log_operation(
        self,
        operation: str,
        status: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        return None

    def set_strict_mode(self, strict: bool) -> Dict[str, Any]:
        self.strict = bool(strict)
        return {
            "enabled": True,
            "strict": self.strict,
            "mirror_errors": 0,
            "mirror_success": 0,
            "last_error": None,
        }


class _ReadDBStub:
    def list_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return [{"id": 501, "title": "ReadDB Job", "company": "Tener"}]

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        if int(job_id) != 501:
            return None
        return {"id": 501, "title": "ReadDB Job", "company": "Tener"}

    def list_job_step_progress(self, job_id: int) -> List[Dict[str, Any]]:
        return [{"job_id": int(job_id), "step": "source", "status": "success", "output_json": {"total": 3}}]

    def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return [{"id": 9001, "operation": "read.db.test", "status": "ok", "details": {"source": "read_db"}}]

    def list_candidates_for_job(self, job_id: int) -> List[Dict[str, Any]]:
        return [
            {
                "candidate_id": 77,
                "full_name": "Read Candidate",
                "status": "verified",
                "score": 0.91,
                "agent_assessments": [],
                "agent_scorecard": {},
            }
        ]

    def list_conversations_overview(self, limit: int = 200, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
        return [
            {
                "conversation_id": 3001,
                "job_id": int(job_id or 501),
                "candidate_id": 77,
                "conversation_status": "active",
                "candidate_name": "Read Candidate",
            }
        ]


class ReadDbRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": _FailWriteDB(),
            "read_db": _ReadDBStub(),
            "db_primary_path": str(Path(gettempdir()) / "read_db_routing.sqlite3"),
            "postgres_dsn": "",
            "db_backend": "dual",
            "db_runtime_mode": "sqlite_primary_postgres_mirror",
            "db_read_status": {"status": "ok", "source": "postgres"},
            "postgres_migration_status": {"status": "ok"},
            "scoring_formula": None,
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
            return status, json.loads(raw)
        return status, {}

    def test_jobs_routes_use_read_db(self) -> None:
        status_jobs, payload_jobs = self._request("GET", "/api/jobs")
        self.assertEqual(status_jobs, 200)
        self.assertEqual(int((payload_jobs.get("items") or [])[0]["id"]), 501)

        status_job, payload_job = self._request("GET", "/api/jobs/501")
        self.assertEqual(status_job, 200)
        self.assertEqual(int(payload_job.get("id") or 0), 501)

        status_progress, payload_progress = self._request("GET", "/api/jobs/501/progress")
        self.assertEqual(status_progress, 200)
        self.assertEqual(str((payload_progress.get("items") or [])[0].get("step") or ""), "source")

    def test_logs_route_uses_read_db(self) -> None:
        status, payload = self._request("GET", "/api/logs")
        self.assertEqual(status, 200)
        first = (payload.get("items") or [])[0]
        self.assertEqual(str(first.get("operation") or ""), "read.db.test")

    def test_candidates_and_chats_routes_use_read_db(self) -> None:
        status_candidates, payload_candidates = self._request("GET", "/api/jobs/501/candidates")
        self.assertEqual(status_candidates, 200)
        first_candidate = (payload_candidates.get("items") or [])[0]
        self.assertEqual(int(first_candidate.get("candidate_id") or 0), 77)

        status_chats, payload_chats = self._request("GET", "/api/chats/overview?job_id=501")
        self.assertEqual(status_chats, 200)
        first_chat = (payload_chats.get("items") or [])[0]
        self.assertEqual(int(first_chat.get("conversation_id") or 0), 3001)

    def test_db_parity_requires_postgres_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("GET", "/api/db/parity")
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("status") or ""), "error")
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_parity_deep_requires_postgres_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("GET", "/api/db/parity?deep=1&sample_limit=3")
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("status") or ""), "error")
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_backfill_run_requires_postgres_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("POST", "/api/db/backfill/run", payload={})
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("status") or ""), "error")
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_backfill_run_returns_result(self) -> None:
        class _FakeBackfillResult:
            def to_dict(self) -> Dict[str, Any]:
                return {
                    "sqlite_path": "/tmp/fake.sqlite3",
                    "copied_total": 12,
                    "failed_total": 0,
                    "skipped_total": 1,
                    "tables": [{"table": "jobs", "copied": 4, "failed": 0, "skipped": 0}],
                }

        with patch("tener_ai.main.backfill_sqlite_to_postgres", return_value=_FakeBackfillResult()) as mock_backfill:
            status, payload = self._request(
                "POST",
                "/api/db/backfill/run",
                payload={
                    "postgres_dsn": "postgres://example",
                    "batch_size": 250,
                    "truncate_first": True,
                    "tables": ["jobs", "candidates"],
                },
            )
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("status") or ""), "ok")
        self.assertEqual(int(payload.get("copied_total") or 0), 12)
        self.assertEqual(int(payload.get("batch_size") or 0), 250)
        self.assertTrue(bool(payload.get("truncate_first")))
        self.assertEqual(payload.get("tables_requested"), ["jobs", "candidates"])
        self.assertTrue(mock_backfill.called)

    def test_db_read_source_rejects_invalid_source(self) -> None:
        status, payload = self._request("POST", "/api/db/read-source", payload={"source": "unknown"})
        self.assertEqual(status, 400)
        self.assertIn("source must be sqlite or postgres", str(payload.get("error") or ""))

    def test_db_read_source_postgres_requires_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("POST", "/api/db/read-source", payload={"source": "postgres", "postgres_dsn": ""})
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_read_source_switches_to_postgres(self) -> None:
        class _FakePgReadDB:
            def __init__(self, dsn: str) -> None:
                self.dsn = dsn

        with patch("tener_ai.main.PostgresReadDatabase", _FakePgReadDB):
            status, payload = self._request(
                "POST",
                "/api/db/read-source",
                payload={"source": "postgres", "postgres_dsn": "postgres://example"},
            )
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("source") or ""), "postgres")
        self.assertEqual(str((payload.get("db_read_status") or {}).get("source") or ""), "postgres")

    def test_db_read_source_switches_to_sqlite(self) -> None:
        status, payload = self._request("POST", "/api/db/read-source", payload={"source": "sqlite"})
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("source") or ""), "sqlite")
        self.assertEqual(str((payload.get("db_read_status") or {}).get("source") or ""), "sqlite")

    def test_db_cutover_status_returns_payload(self) -> None:
        status, payload = self._request("GET", "/api/db/cutover/status")
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("status") or ""), "ok")
        self.assertIn("cutover", payload)
        self.assertIn("in_progress", payload)

    def test_db_cutover_preflight_requires_postgres_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("GET", "/api/db/cutover/preflight")
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_cutover_preflight_returns_ok_when_report_ok(self) -> None:
        api_main.SERVICES["postgres_dsn"] = "postgres://example"
        with patch.object(
            api_main.TenerRequestHandler,
            "_build_cutover_preflight_report",
            return_value={"status": "ok", "checks": {"postgres_connected": True}},
        ):
            status, payload = self._request("GET", "/api/db/cutover/preflight")
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("status") or ""), "ok")

    def test_db_cutover_run_requires_postgres_dsn(self) -> None:
        with patch.dict(os.environ, {"TENER_DB_DSN": ""}, clear=False):
            status, payload = self._request("POST", "/api/db/cutover/run", payload={"postgres_dsn": ""})
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("reason") or ""), "postgres_dsn_missing")

    def test_db_cutover_run_rejected_when_in_progress(self) -> None:
        api_main.SERVICES["db_cutover_lock"] = _BusyLock()
        status, payload = self._request("POST", "/api/db/cutover/run", payload={"postgres_dsn": "postgres://example"})
        self.assertEqual(status, 409)
        self.assertEqual(str(payload.get("reason") or ""), "cutover_in_progress")

    def test_db_cutover_run_success_with_switch(self) -> None:
        class _FakeBackfillResult:
            def to_dict(self) -> Dict[str, Any]:
                return {
                    "sqlite_path": "/tmp/fake.sqlite3",
                    "copied_total": 20,
                    "failed_total": 0,
                    "skipped_total": 0,
                    "tables": [],
                }

        class _FakePgReadDB:
            def __init__(self, dsn: str) -> None:
                self.dsn = dsn

        parity_ok = {
            "status": "ok",
            "sqlite_path": "/tmp/fake.sqlite3",
            "tables_checked": ["jobs"],
            "sqlite_counts": {"jobs": 1},
            "postgres_counts": {"jobs": 1},
            "mismatch_count": 0,
            "mismatches": [],
            "deep": {"enabled": True, "status": "ok", "checks": [], "mismatch_count": 0, "skipped_count": 0, "sample_limit": 20},
        }

        with patch("tener_ai.main.backfill_sqlite_to_postgres", return_value=_FakeBackfillResult()), patch(
            "tener_ai.main.build_parity_report", return_value=parity_ok
        ), patch("tener_ai.main.PostgresReadDatabase", _FakePgReadDB):
            status, payload = self._request(
                "POST",
                "/api/db/cutover/run",
                payload={
                    "postgres_dsn": "postgres://example",
                    "execute_backfill": True,
                    "auto_switch_read_source": True,
                    "set_dual_strict_on_success": False,
                    "deep": True,
                },
            )
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("status") or ""), "ok")
        self.assertEqual(str(((payload.get("switch_read_source") or {}).get("source") or "")), "postgres")

    def test_db_cutover_run_blocks_on_strict_parity_mismatch(self) -> None:
        parity_mismatch = {
            "status": "mismatch",
            "sqlite_path": "/tmp/fake.sqlite3",
            "tables_checked": ["jobs"],
            "sqlite_counts": {"jobs": 1},
            "postgres_counts": {"jobs": 2},
            "mismatch_count": 1,
            "mismatches": [{"table": "jobs", "sqlite_count": 1, "postgres_count": 2, "delta": 1}],
            "deep": {"enabled": True, "status": "mismatch", "checks": [], "mismatch_count": 1, "skipped_count": 0, "sample_limit": 20},
        }
        with patch("tener_ai.main.build_parity_report", return_value=parity_mismatch):
            status, payload = self._request(
                "POST",
                "/api/db/cutover/run",
                payload={
                    "postgres_dsn": "postgres://example",
                    "strict_parity": True,
                    "auto_switch_read_source": True,
                },
            )
        self.assertEqual(status, 409)
        self.assertEqual(str(payload.get("status") or ""), "blocked")
        self.assertEqual(str(payload.get("reason") or ""), "parity_mismatch")
        self.assertIsNone(payload.get("switch_read_source"))

    def test_db_dual_write_strict_toggle(self) -> None:
        status_on, payload_on = self._request("POST", "/api/db/dual-write/strict", payload={"strict": True})
        self.assertEqual(status_on, 200)
        self.assertEqual(str(payload_on.get("status") or ""), "ok")
        self.assertTrue(bool(payload_on.get("strict")))

        status_off, payload_off = self._request("POST", "/api/db/dual-write/strict", payload={"strict": False})
        self.assertEqual(status_off, 200)
        self.assertEqual(str(payload_off.get("status") or ""), "ok")
        self.assertFalse(bool(payload_off.get("strict")))

    def test_db_cutover_rollback_switches_read_source(self) -> None:
        status, payload = self._request("POST", "/api/db/cutover/rollback", payload={"disable_dual_strict": True})
        self.assertEqual(status, 200)
        self.assertEqual(str(payload.get("status") or ""), "ok")
        switch = payload.get("switch_read_source") or {}
        self.assertEqual(str(switch.get("source") or ""), "sqlite")

    def test_db_cutover_rollback_rejected_when_in_progress(self) -> None:
        api_main.SERVICES["db_cutover_lock"] = _BusyLock()
        status, payload = self._request("POST", "/api/db/cutover/rollback", payload={})
        self.assertEqual(status, 409)
        self.assertEqual(str(payload.get("reason") or ""), "cutover_in_progress")


if __name__ == "__main__":
    unittest.main()
