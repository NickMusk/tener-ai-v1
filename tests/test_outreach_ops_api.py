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

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_outreach_ops_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database, utc_now_iso


class OutreachOpsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "outreach_ops_api.sqlite3"))
        self.db.init_schema()

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {"db": self.db}

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

    def test_outreach_ops_aggregates_sent_failed_and_stuck_metrics(self) -> None:
        account_1 = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-ops-1",
            status="connected",
            connected_at=utc_now_iso(),
        )
        account_2 = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-ops-2",
            status="connected",
            connected_at=utc_now_iso(),
        )
        job_id = self.db.insert_job(
            title="Manual QA Engineer",
            jd_text="Need QA experience.",
            location="Remote",
            preferred_languages=["en"],
            seniority="junior",
        )
        candidate_1 = self.db.upsert_candidate(
            {
                "linkedin_id": "ops-ln-1",
                "full_name": "Ops Candidate 1",
                "headline": "QA",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["qa"],
                "years_experience": 4,
                "raw": {},
            },
            source="linkedin",
        )
        conversation_1 = self.db.create_conversation(job_id=job_id, candidate_id=candidate_1, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=conversation_1, account_id=account_1)
        self.db.update_conversation_status(conversation_id=conversation_1, status="active")
        self.db.add_message(
            conversation_id=conversation_1,
            direction="outbound",
            content="Checking in",
            candidate_language="en",
            meta={"delivery_status": "sent"},
        )
        self.db.upsert_pre_resume_session(
            session_id="pre-ops-1",
            conversation_id=conversation_1,
            job_id=job_id,
            candidate_id=candidate_1,
            state={"status": "awaiting_reply", "language": "en"},
            instruction="",
        )
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE conversations SET last_message_at = ? WHERE id = ?",
                ("2025-01-01T00:00:00+00:00", int(conversation_1)),
            )
            conn.execute(
                """
                INSERT INTO operation_logs (operation, entity_type, entity_id, status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "agent.outreach.send",
                    "conversation",
                    str(conversation_1),
                    "ok",
                    json.dumps({"delivery_status": "sent", "linkedin_account_id": int(account_1)}),
                    utc_now_iso(),
                ),
            )
            conn.execute(
                """
                INSERT INTO operation_logs (operation, entity_type, entity_id, status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "agent.outreach.send",
                    "conversation",
                    "999",
                    "error",
                    json.dumps({"delivery_status": "failed", "linkedin_account_id": int(account_2), "error": "api_error"}),
                    utc_now_iso(),
                ),
            )

        status, payload = self._request("GET", "/api/outreach/ops?stale_minutes=30")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("sent_24h") or 0), 1)
        self.assertEqual(int(summary.get("failed_24h") or 0), 1)
        self.assertEqual(int(summary.get("stuck_threads") or 0), 1)

        accounts = payload.get("accounts") or []
        self.assertEqual(len(accounts), 2)
        by_id = {int(item.get("account_id") or 0): item for item in accounts}
        self.assertEqual(int((by_id.get(account_1) or {}).get("sent_24h") or 0), 1)
        self.assertEqual(int((by_id.get(account_2) or {}).get("failed_24h") or 0), 1)


if __name__ == "__main__":
    unittest.main()
