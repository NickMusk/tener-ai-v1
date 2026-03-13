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
from tener_ai.matching import MatchingEngine


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
            "matching_engine": MatchingEngine(
                rules_path=str(api_main.project_root() / "config" / "matching_rules.json")
            ),
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

    def test_pause_job_keeps_job_visible_and_resume_restores_active_state(self) -> None:
        paused = self.db.pause_job(job_id=self.backend_job_id, reason="ops")
        self.assertEqual(int(paused.get("updated") or 0), 1)
        paused_job = paused.get("job") if isinstance(paused.get("job"), dict) else {}
        self.assertEqual(str(paused_job.get("job_state") or ""), "paused")
        self.assertTrue(bool(paused_job.get("is_paused")))
        self.assertFalse(bool(paused_job.get("is_archived")))
        self.assertTrue(str(paused_job.get("paused_at") or "").strip())

        visible_jobs = self.db.list_jobs(limit=10)
        visible_ids = {int(job.get("id") or 0) for job in visible_jobs}
        self.assertIn(self.backend_job_id, visible_ids)

        resumed = self.db.resume_job(job_id=self.backend_job_id)
        self.assertEqual(int(resumed.get("updated") or 0), 1)
        resumed_job = resumed.get("job") if isinstance(resumed.get("job"), dict) else {}
        self.assertEqual(str(resumed_job.get("job_state") or ""), "active")
        self.assertFalse(bool(resumed_job.get("is_paused")))
        self.assertEqual(resumed_job.get("paused_at"), None)

    def test_jobs_api_pause_and_resume_job(self) -> None:
        status_pause, payload_pause = self._request(
            "POST",
            f"/api/jobs/{self.backend_job_id}/pause",
            {"reason": "ops"},
        )
        self.assertEqual(status_pause, 200)
        paused_job = payload_pause.get("job") if isinstance(payload_pause.get("job"), dict) else {}
        self.assertEqual(str(paused_job.get("job_state") or ""), "paused")

        status_jobs, payload_jobs = self._request("GET", "/api/jobs")
        self.assertEqual(status_jobs, 200)
        listed = {
            int(item.get("id") or 0): str(item.get("job_state") or "")
            for item in (payload_jobs.get("items") or [])
        }
        self.assertEqual(listed.get(self.backend_job_id), "paused")

        status_resume, payload_resume = self._request(
            "POST",
            f"/api/jobs/{self.backend_job_id}/resume",
            {},
        )
        self.assertEqual(status_resume, 200)
        resumed_job = payload_resume.get("job") if isinstance(payload_resume.get("job"), dict) else {}
        self.assertEqual(str(resumed_job.get("job_state") or ""), "active")

    def test_pause_job_hides_backlog_reads_and_account_workload_until_resume(self) -> None:
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-pause-1",
            status="connected",
            connected_at="2025-01-01T00:00:00+00:00",
        )
        active_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "pause-active-candidate",
                "full_name": "Pause Active Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 6,
            }
        )
        self.db.create_candidate_match(
            job_id=self.backend_job_id,
            candidate_id=active_candidate_id,
            score=0.91,
            status="outreach_sent",
            verification_notes={},
        )
        active_conversation_id = self.db.create_conversation(
            job_id=self.backend_job_id,
            candidate_id=active_candidate_id,
            channel="linkedin",
        )
        self.db.set_conversation_linkedin_account(conversation_id=active_conversation_id, account_id=account_id)
        self.db.update_conversation_status(conversation_id=active_conversation_id, status="active")
        self.db.create_outbound_action(
            job_id=self.backend_job_id,
            candidate_id=active_candidate_id,
            conversation_id=active_conversation_id,
            action_type="outreach_message",
            payload={"message": "Hello"},
            account_id=account_id,
        )

        backlog_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "pause-backlog-candidate",
                "full_name": "Pause Backlog Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
            }
        )
        self.db.create_candidate_match(
            job_id=self.backend_job_id,
            candidate_id=backlog_candidate_id,
            score=0.95,
            status="verified",
            verification_notes={},
        )

        recovery_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "pause-recovery-candidate",
                "full_name": "Pause Recovery Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
            }
        )
        self.db.create_candidate_match(
            job_id=self.backend_job_id,
            candidate_id=recovery_candidate_id,
            score=0.78,
            status="needs_resume",
            verification_notes={},
        )
        recovery_conversation_id = self.db.create_conversation(
            job_id=self.backend_job_id,
            candidate_id=recovery_candidate_id,
            channel="linkedin",
        )
        self.db.update_conversation_status(conversation_id=recovery_conversation_id, status="waiting_connection")
        self.db.upsert_pre_resume_session(
            session_id=f"pause-recovery-{recovery_conversation_id}",
            conversation_id=recovery_conversation_id,
            job_id=self.backend_job_id,
            candidate_id=recovery_candidate_id,
            state={"status": "awaiting_reply", "language": "en"},
            instruction="follow up",
        )

        before = self.db.summarize_linkedin_account_workload([account_id])[account_id]
        self.assertEqual(int(before.get("active_conversations") or 0), 1)
        self.assertEqual(int(before.get("assigned_actions") or 0), 1)
        self.assertTrue(self.db.list_job_outreach_candidates(job_id=self.backend_job_id, limit=20))
        self.assertTrue(self.db.list_unassigned_outreach_conversations(job_id=self.backend_job_id, limit=20))
        self.assertTrue(self.db.list_pending_outbound_actions(limit=20, job_id=self.backend_job_id))

        paused = self.db.pause_job(job_id=self.backend_job_id, reason="ops")
        self.assertEqual(int(paused.get("updated") or 0), 1)
        after_pause = self.db.summarize_linkedin_account_workload([account_id])[account_id]
        self.assertEqual(int(after_pause.get("total_load") or 0), 0)
        self.assertEqual(self.db.list_job_outreach_candidates(job_id=self.backend_job_id, limit=20), [])
        self.assertEqual(self.db.list_unassigned_outreach_conversations(job_id=self.backend_job_id, limit=20), [])
        self.assertEqual(self.db.list_pending_outbound_actions(limit=20, job_id=self.backend_job_id), [])

        resumed = self.db.resume_job(job_id=self.backend_job_id)
        self.assertEqual(int(resumed.get("updated") or 0), 1)
        after_resume = self.db.summarize_linkedin_account_workload([account_id])[account_id]
        self.assertEqual(int(after_resume.get("active_conversations") or 0), 1)
        self.assertEqual(int(after_resume.get("assigned_actions") or 0), 1)
        self.assertTrue(self.db.list_pending_outbound_actions(limit=20, job_id=self.backend_job_id))

    def test_jobs_api_updates_explicit_requirements(self) -> None:
        status_update, payload_update = self._request(
            "POST",
            f"/api/jobs/{self.manual_job_id}/requirements",
            {
                "must_have_skills": ["manual testing", "api testing", "regression"],
                "nice_to_have_skills": ["sql", "postman"],
            },
        )
        self.assertEqual(status_update, 200)
        self.assertEqual(payload_update.get("must_have_skills"), ["manual testing", "api testing", "regression"])
        self.assertEqual(payload_update.get("nice_to_have_skills"), ["sql", "postman"])
        self.assertEqual(payload_update.get("questionable_skills"), [])

        job = self.db.get_job(self.manual_job_id) or {}
        self.assertEqual(job.get("must_have_skills"), ["manual testing", "api testing", "regression"])
        self.assertEqual(job.get("nice_to_have_skills"), ["sql", "postman"])
        self.assertEqual(job.get("questionable_skills"), [])

    def test_jobs_api_auto_extracts_requirements_from_jd(self) -> None:
        status_create, payload_create = self._request(
            "POST",
            "/api/jobs",
            {
                "title": "Manual QA Engineer",
                "company": "Tener",
                "jd_text": (
                    "About Tener.ai recruiting platform. Requirements: manual testing, api testing, regression testing. "
                    "Nice to have: ci/cd. We also mention go and recruiting in company copy."
                ),
                "location": "Eastern Europe",
                "preferred_languages": ["en"],
                "seniority": "middle",
            },
        )
        self.assertEqual(status_create, 201)
        requirements = payload_create.get("requirements") if isinstance(payload_create.get("requirements"), dict) else {}
        self.assertIn("manual testing", requirements.get("must_have_skills") or [])
        self.assertIn("api testing", requirements.get("must_have_skills") or [])
        self.assertIn("regression testing", requirements.get("must_have_skills") or [])
        self.assertIn("ci/cd", requirements.get("nice_to_have_skills") or [])
        self.assertIn("go", requirements.get("questionable_skills") or [])

    def test_archive_job_stops_pending_outreach_and_pre_resume_followups(self) -> None:
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "archive-candidate-1",
                "full_name": "Archive Candidate",
                "headline": "QA",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["testing"],
                "years_experience": 2,
            }
        )
        self.db.create_candidate_match(
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            score=0.91,
            status="needs_resume",
            verification_notes={},
        )
        conversation_id = self.db.create_conversation(job_id=self.backend_job_id, candidate_id=candidate_id)
        action_id = self.db.create_outbound_action(
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            action_type="outreach_message",
            payload={"message": "Hello"},
        )
        self.db.upsert_pre_resume_session(
            session_id=f"pre-{conversation_id}",
            conversation_id=conversation_id,
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            state={
                "status": "awaiting_reply",
                "language": "en",
                "followups_sent": 1,
                "next_followup_at": "2026-03-09T10:00:00+00:00",
            },
            instruction="follow up",
        )

        result = self.db.archive_jobs(job_ids=[self.backend_job_id])
        self.assertEqual(int(result.get("updated") or 0), 1)

        action_row = self.db.get_outbound_action(action_id) or {}
        self.assertEqual(str(action_row.get("status") or ""), "failed")
        self.assertEqual(str(action_row.get("last_error") or ""), "job_archived")

        pending_for_archived = self.db.list_pending_outbound_actions(limit=20, job_id=self.backend_job_id)
        self.assertEqual(pending_for_archived, [])

        pre_resume_row = self.db.get_pre_resume_session(f"pre-{conversation_id}") or {}
        self.assertEqual(str(pre_resume_row.get("status") or ""), "stalled")
        self.assertEqual(pre_resume_row.get("next_followup_at"), None)
        state_json = pre_resume_row.get("state_json") if isinstance(pre_resume_row.get("state_json"), dict) else {}
        self.assertEqual(str(state_json.get("status") or ""), "stalled")
        self.assertEqual(state_json.get("next_followup_at"), None)

    def test_archive_job_hides_backlog_reads_for_archived_job(self) -> None:
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "archive-backlog-candidate-1",
                "full_name": "Archive Backlog Candidate",
                "headline": "QA",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["testing"],
                "years_experience": 3,
            }
        )
        self.db.create_candidate_match(
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            score=0.95,
            status="verified",
            verification_notes={},
        )
        conversation_id = self.db.create_conversation(
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            channel="linkedin",
        )
        self.db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")
        self.db.upsert_pre_resume_session(
            session_id=f"pre-backlog-{conversation_id}",
            conversation_id=conversation_id,
            job_id=self.backend_job_id,
            candidate_id=candidate_id,
            state={"status": "awaiting_reply", "language": "en"},
            instruction="follow up",
        )

        self.assertTrue(self.db.list_job_outreach_candidates(job_id=self.backend_job_id, limit=20))
        self.assertTrue(self.db.list_unassigned_outreach_conversations(job_id=self.backend_job_id, limit=20))
        self.assertTrue(self.db.list_conversations_overview(job_id=self.backend_job_id, limit=20))

        result = self.db.archive_jobs(job_ids=[self.backend_job_id])
        self.assertEqual(int(result.get("updated") or 0), 1)

        self.assertEqual(self.db.list_job_outreach_candidates(job_id=self.backend_job_id, limit=20), [])
        self.assertEqual(self.db.list_unassigned_outreach_conversations(job_id=self.backend_job_id, limit=20), [])
        self.assertEqual(self.db.list_conversations_overview(job_id=self.backend_job_id, limit=20), [])

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
