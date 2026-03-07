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
        full_error = (
            "Unipile HTTP error 403: "
            "{\"status\":403,\"type\":\"errors/subscription_required\","
            "\"title\":\"Subscription required\","
            "\"detail\":\"The action you're trying to achieve requires a subscription to provider's services.\"}"
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
                    json.dumps({"delivery_status": "failed", "linkedin_account_id": int(account_2), "error": full_error}),
                    utc_now_iso(),
                ),
            )

        status, payload = self._request("GET", "/api/outreach/ops?stale_minutes=30")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(str(summary.get("delivery_health") or ""), "warning")
        self.assertEqual(str(summary.get("backlog_health") or ""), "warning")
        self.assertEqual(str(summary.get("health") or ""), "warning")
        self.assertEqual(int(summary.get("sent_24h") or 0), 1)
        self.assertEqual(int(summary.get("failed_24h") or 0), 1)
        self.assertEqual(int(summary.get("stuck_threads") or 0), 1)

        accounts = payload.get("accounts") or []
        self.assertEqual(len(accounts), 2)
        by_id = {int(item.get("account_id") or 0): item for item in accounts}
        self.assertEqual(int((by_id.get(account_1) or {}).get("sent_24h") or 0), 1)
        self.assertEqual(int((by_id.get(account_2) or {}).get("failed_24h") or 0), 1)
        self.assertEqual(str((by_id.get(account_1) or {}).get("delivery_health") or ""), "ok")
        self.assertEqual(str((by_id.get(account_1) or {}).get("backlog_health") or ""), "warning")
        self.assertEqual(str((by_id.get(account_1) or {}).get("dispatch_state") or ""), "ready")
        self.assertEqual(str((by_id.get(account_2) or {}).get("dispatch_state") or ""), "blocked_subscription")
        self.assertEqual(str((by_id.get(account_2) or {}).get("last_error") or ""), full_error)
        stuck_candidates = (by_id.get(account_1) or {}).get("stuck_candidates") or []
        self.assertEqual(len(stuck_candidates), 1)
        self.assertEqual(str(stuck_candidates[0].get("candidate_name") or ""), "Ops Candidate 1")
        self.assertEqual(int(stuck_candidates[0].get("conversation_id") or 0), conversation_1)

        backlog = payload.get("backlog") or {}
        backlog_summary = backlog.get("summary") or {}
        self.assertEqual(int(backlog_summary.get("stuck_replies") or 0), 1)
        backlog_items = backlog.get("items") or []
        self.assertTrue(any(str(item.get("queue_type") or "") == "stuck_reply" for item in backlog_items))

    def test_outreach_ops_counts_active_accounts_from_conversation_mapping(self) -> None:
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-ops-active",
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
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "ops-ln-active",
                "full_name": "Ops Candidate Active",
                "headline": "QA",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["qa"],
                "years_experience": 4,
                "raw": {},
            },
            source="linkedin",
        )
        conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        self.db.upsert_pre_resume_session(
            session_id="pre-ops-active",
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state={"status": "awaiting_reply", "language": "en"},
            instruction="",
        )

        status, payload = self._request("GET", "/api/outreach/ops?stale_minutes=45")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("active_accounts") or 0), 1)
        self.assertEqual(int(summary.get("active_conversations") or 0), 1)

    def test_outreach_ops_deduplicates_stuck_people_per_account(self) -> None:
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-ops-dedupe",
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
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "ops-ln-dedupe",
                "full_name": "Forced Test Candidate (olena-bachek-b8523121a)",
                "headline": "QA",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["qa"],
                "years_experience": 4,
                "raw": {},
            },
            source="linkedin",
        )
        conversation_ids = []
        for idx in range(3):
            conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            self.db.upsert_pre_resume_session(
                session_id=f"pre-ops-dedupe-{idx}",
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state={"status": "awaiting_reply", "language": "en"},
                instruction="",
            )
            conversation_ids.append(conversation_id)
        with self.db.transaction() as conn:
            for conversation_id in conversation_ids:
                conn.execute(
                    "UPDATE conversations SET last_message_at = ? WHERE id = ?",
                    ("2025-01-01T00:00:00+00:00", int(conversation_id)),
                )

        status, payload = self._request("GET", "/api/outreach/ops?stale_minutes=30")
        self.assertEqual(status, 200)

        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("stuck_threads") or 0), 1)

        accounts = payload.get("accounts") or []
        by_id = {int(item.get("account_id") or 0): item for item in accounts}
        account_row = by_id.get(account_id) or {}
        self.assertEqual(int(account_row.get("stuck_threads") or 0), 1)
        stuck_candidates = account_row.get("stuck_candidates") or []
        self.assertEqual(len(stuck_candidates), 1)
        self.assertEqual(
            str(stuck_candidates[0].get("candidate_name") or ""),
            "Forced Test Candidate (olena-bachek-b8523121a)",
        )

        backlog = payload.get("backlog") or {}
        backlog_summary = backlog.get("summary") or {}
        self.assertEqual(int(backlog_summary.get("stuck_replies") or 0), 1)
        stuck_items = [item for item in (backlog.get("items") or []) if str(item.get("queue_type") or "") == "stuck_reply"]
        self.assertEqual(len(stuck_items), 1)

    def test_outreach_ops_lists_new_thread_backlog_for_latest_auto_jobs_only(self) -> None:
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-ops-planned",
            status="connected",
            connected_at=utc_now_iso(),
            label="Planned Sender",
        )
        class _PreviewWorkflow:
            def preview_linkedin_account_sequence_for_new_threads(self, *, job_id: int, slots: int) -> Dict[str, Any]:
                return {
                    "items": [
                        {
                            "account_id": account_id,
                            "label": "Planned Sender",
                            "provider_account_id": "acc-ops-planned",
                            "daily_cap": 15,
                            "projected_new_threads_sent": idx + 1,
                        }
                        for idx in range(int(slots or 0))
                    ],
                    "reason": "ok",
                }

        api_main.SERVICES["workflow"] = _PreviewWorkflow()
        old_job_id = self.db.insert_job(
            title="Old Auto Job",
            jd_text="Need QA experience.",
            location="Remote",
            preferred_languages=["en"],
            seniority="junior",
            linkedin_routing_mode="auto",
        )
        manual_job_id = self.db.insert_job(
            title="Manual Job",
            jd_text="Need QA experience.",
            location="Remote",
            preferred_languages=["en"],
            seniority="junior",
            linkedin_routing_mode="manual",
        )
        new_job_id = self.db.insert_job(
            title="New Auto Job",
            jd_text="Need QA experience.",
            location="Remote",
            preferred_languages=["en"],
            seniority="junior",
            linkedin_routing_mode="auto",
        )

        def _add_match(job_id: int, suffix: str, score: float) -> None:
            candidate_id = self.db.upsert_candidate(
                {
                    "linkedin_id": f"ops-backlog-{suffix}",
                    "full_name": f"Backlog Candidate {suffix}",
                    "headline": "QA",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["qa"],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            self.db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=score,
                status="verified",
                verification_notes={},
            )

        _add_match(old_job_id, "old", 0.71)
        _add_match(manual_job_id, "manual", 0.99)
        _add_match(new_job_id, "high", 0.93)
        _add_match(new_job_id, "low", 0.62)

        status, payload = self._request("GET", "/api/outreach/ops?stale_minutes=45")
        self.assertEqual(status, 200)
        backlog = payload.get("backlog") or {}
        jobs = backlog.get("jobs") or []
        self.assertGreaterEqual(len(jobs), 2)
        self.assertEqual(int(jobs[0].get("job_id") or 0), new_job_id)
        self.assertTrue(all(int(item.get("job_id") or 0) != manual_job_id for item in jobs))

        items = [item for item in (backlog.get("items") or []) if str(item.get("queue_type") or "") == "new_thread"]
        self.assertTrue(items)
        self.assertEqual(int(items[0].get("job_id") or 0), new_job_id)
        self.assertEqual(str(items[0].get("candidate_name") or ""), "Backlog Candidate high")
        self.assertEqual(int(items[0].get("likely_account_id") or 0), account_id)
        self.assertEqual(str(items[0].get("likely_account_label") or ""), "Planned Sender")
        self.assertEqual(str(items[0].get("queue_reason") or ""), "new_thread")
        self.assertEqual(str(items[0].get("planned_action_kind") or ""), "connect_request")
        self.assertEqual(str(items[0].get("planned_action_label") or ""), "Connect planned")


if __name__ == "__main__":
    unittest.main()
