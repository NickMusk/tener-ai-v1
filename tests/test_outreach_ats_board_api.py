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

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_outreach_ats_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database, utc_now_iso


class OutreachAtsBoardApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "outreach_ats_board_api.sqlite3"))
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

    def _create_job(self, title: str) -> int:
        return self.db.insert_job(
            title=title,
            jd_text=f"{title} JD",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )

    def _create_candidate(self, suffix: str, name: str) -> int:
        return self.db.upsert_candidate(
            {
                "linkedin_id": f"ats-ln-{suffix}",
                "full_name": name,
                "headline": "Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            },
            source="linkedin",
        )

    def _attach_match(
        self,
        *,
        job_id: int,
        candidate_id: int,
        score: float = 0.81,
        status: str = "verified",
        verification_notes: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=score,
            status=status,
            verification_notes=verification_notes or {},
        )

    def test_outreach_ats_board_groups_candidates_by_pipeline_stage(self) -> None:
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="ats-account-1",
            status="connected",
            connected_at=utc_now_iso(),
        )
        job_id = self._create_job("Manual QA Engineer")

        queued_candidate = self._create_candidate("queued", "Queued Candidate")
        self._attach_match(job_id=job_id, candidate_id=queued_candidate, status="verified")

        connect_candidate = self._create_candidate("connect", "Connect Candidate")
        self._attach_match(job_id=job_id, candidate_id=connect_candidate, status="outreach_pending_connection")
        connect_conversation = self.db.create_conversation(job_id=job_id, candidate_id=connect_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=connect_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=connect_conversation, status="waiting_connection")

        dialogue_candidate = self._create_candidate("dialogue", "Dialogue Candidate")
        self._attach_match(job_id=job_id, candidate_id=dialogue_candidate, status="outreach_sent")
        dialogue_conversation = self.db.create_conversation(job_id=job_id, candidate_id=dialogue_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=dialogue_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=dialogue_conversation, status="active")
        self.db.add_message(
            conversation_id=dialogue_conversation,
            direction="inbound",
            content="Happy to chat",
            candidate_language="en",
            meta={},
        )

        queued_delivery_candidate = self._create_candidate("queued-delivery", "Queued Delivery Candidate")
        self._attach_match(job_id=job_id, candidate_id=queued_delivery_candidate, status="outreached")
        queued_delivery_conversation = self.db.create_conversation(job_id=job_id, candidate_id=queued_delivery_candidate, channel="linkedin")
        self.db.update_candidate_match_status(
            job_id=job_id,
            candidate_id=queued_delivery_candidate,
            status="outreached",
        )
        self.db.create_outbound_action(
            job_id=job_id,
            candidate_id=queued_delivery_candidate,
            conversation_id=queued_delivery_conversation,
            action_type="send_message",
            payload={"delivery_mode": "message_first", "planned_action_kind": "message"},
            priority=50,
            not_before=utc_now_iso(),
        )

        cv_candidate = self._create_candidate("cv", "CV Candidate")
        self._attach_match(job_id=job_id, candidate_id=cv_candidate, status="resume_received")
        cv_conversation = self.db.create_conversation(job_id=job_id, candidate_id=cv_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=cv_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=cv_conversation, status="active")
        self.db.upsert_pre_resume_session(
            session_id="ats-pre-cv",
            conversation_id=cv_conversation,
            job_id=job_id,
            candidate_id=cv_candidate,
            state={"status": "resume_received", "language": "en"},
            instruction="",
        )

        interview_candidate = self._create_candidate("interview", "Interview Candidate")
        self._attach_match(
            job_id=job_id,
            candidate_id=interview_candidate,
            status="interview_in_progress",
            verification_notes={"interview_status": "in_progress"},
        )

        passed_candidate = self._create_candidate("passed", "Passed Candidate")
        self._attach_match(
            job_id=job_id,
            candidate_id=passed_candidate,
            status="interview_scored",
            verification_notes={"interview_status": "scored", "interview_total_score": 84.0},
        )

        failed_candidate = self._create_candidate("failed", "Failed Candidate")
        self._attach_match(
            job_id=job_id,
            candidate_id=failed_candidate,
            status="interview_scored",
            verification_notes={"interview_status": "scored", "interview_total_score": 44.8},
        )

        closed_candidate = self._create_candidate("closed", "Closed Candidate")
        self._attach_match(job_id=job_id, candidate_id=closed_candidate, status="rejected")

        status, payload = self._request("GET", "/api/outreach/ats-board")
        self.assertEqual(status, 200)

        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("total_candidates") or 0), 9)
        self.assertEqual(int(summary.get("queued") or 0), 1)
        self.assertEqual(int(summary.get("connect_sent") or 0), 1)
        self.assertEqual(int(summary.get("queued_delivery") or 0), 1)
        self.assertEqual(int(summary.get("dialogue") or 0), 1)
        self.assertEqual(int(summary.get("cv_received") or 0), 1)
        self.assertEqual(int(summary.get("interview_pending") or 0), 1)
        self.assertEqual(int(summary.get("interview_passed") or 0), 1)
        self.assertEqual(int(summary.get("interview_failed") or 0), 1)
        self.assertEqual(int(summary.get("closed") or 0), 1)

        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("queued") or {}).get("items", [])],
            ["Queued Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("connect_sent") or {}).get("items", [])],
            ["Connect Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("queued_delivery") or {}).get("items", [])],
            ["Queued Delivery Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("dialogue") or {}).get("items", [])],
            ["Dialogue Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("cv_received") or {}).get("items", [])],
            ["CV Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("interview_pending") or {}).get("items", [])],
            ["Interview Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("interview_passed") or {}).get("items", [])],
            ["Passed Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("interview_failed") or {}).get("items", [])],
            ["Failed Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("closed") or {}).get("items", [])],
            ["Closed Candidate"],
        )

    def test_outreach_ats_board_filters_by_job_id(self) -> None:
        job_a = self._create_job("Manual QA Engineer")
        job_b = self._create_job("Senior Backend Engineer")

        candidate_a = self._create_candidate("job-a", "Manual QA Candidate")
        self._attach_match(job_id=job_a, candidate_id=candidate_a, status="verified")

        candidate_b = self._create_candidate("job-b", "Backend Candidate")
        self._attach_match(job_id=job_b, candidate_id=candidate_b, status="verified")

        status, payload = self._request("GET", f"/api/outreach/ats-board?job_id={job_a}")
        self.assertEqual(status, 200)
        self.assertEqual(int(payload.get("job_id") or 0), job_a)

        all_names = [
            str(item.get("candidate_name") or "")
            for column in (payload.get("columns") or [])
            for item in (column.get("items") or [])
        ]
        self.assertIn("Manual QA Candidate", all_names)
        self.assertNotIn("Backend Candidate", all_names)

    def test_outreach_ats_board_hides_forced_test_candidates_for_normal_jobs(self) -> None:
        job_id = self._create_job("Manual QA Engineer")

        forced_candidate = self._create_candidate("forced", "Forced Test Candidate (olena-bachek-b8523121a)")
        self._attach_match(
            job_id=job_id,
            candidate_id=forced_candidate,
            status="outreached",
            verification_notes={
                "forced_test_candidate": True,
                "forced_test_identifier": "olena-bachek-b8523121a",
            },
        )

        normal_candidate = self._create_candidate("normal", "Normal Candidate")
        self._attach_match(job_id=job_id, candidate_id=normal_candidate, status="verified")

        status, payload = self._request("GET", f"/api/outreach/ats-board?job_id={job_id}")
        self.assertEqual(status, 200)

        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("total_candidates") or 0), 1)
        self.assertEqual(int(summary.get("queued") or 0), 1)

        all_names = [
            str(item.get("candidate_name") or "")
            for column in (payload.get("columns") or [])
            for item in (column.get("items") or [])
        ]
        self.assertIn("Normal Candidate", all_names)
        self.assertNotIn("Forced Test Candidate (olena-bachek-b8523121a)", all_names)

    def test_outreach_ats_board_groups_terminal_states_into_closed(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        terminal_statuses = [
            ("not-interest", "Not Interested Candidate", "needs_resume", {"pre_resume_status": "not_interested"}),
            ("unreachable", "Unreachable Candidate", "needs_resume", {"pre_resume_status": "unreachable"}),
            ("failed", "Interview Failed Candidate", "interview_failed", {"interview_status": "failed"}),
        ]

        for suffix, name, status_key, notes in terminal_statuses:
            candidate_id = self._create_candidate(suffix, name)
            self._attach_match(job_id=job_id, candidate_id=candidate_id, status=status_key)
            if notes.get("pre_resume_status"):
                conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
                self.db.upsert_pre_resume_session(
                    session_id=f"ats-pre-{suffix}",
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state={"status": notes["pre_resume_status"], "language": "en"},
                    instruction="",
                )
            elif notes:
                self.db.update_candidate_match_status(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    status=status_key,
                    extra_notes=notes,
                )

        status, payload = self._request("GET", "/api/outreach/ats-board")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("closed") or 0), 2)
        self.assertEqual(int(summary.get("interview_failed") or 0), 1)
        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        closed_names = {str(item.get("candidate_name") or "") for item in (columns.get("closed") or {}).get("items", [])}
        self.assertEqual(
            closed_names,
            {"Not Interested Candidate", "Unreachable Candidate"},
        )
        failed_names = {str(item.get("candidate_name") or "") for item in (columns.get("interview_failed") or {}).get("items", [])}
        self.assertEqual(failed_names, {"Interview Failed Candidate"})

    def test_outreach_ats_board_surfaces_delivery_blocked_identity_separately_from_closed(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        candidate_id = self._create_candidate("delivery-blocked", "Delivery Blocked Candidate")
        self._attach_match(job_id=job_id, candidate_id=candidate_id, status="needs_resume")
        conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
        self.db.upsert_pre_resume_session(
            session_id="ats-pre-delivery-blocked",
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state={
                "status": "delivery_blocked_identity",
                "language": "en",
                "last_error": "invalid_candidate_identity",
            },
            instruction="",
        )

        status, payload = self._request("GET", "/api/outreach/ats-board")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("delivery_blocked") or 0), 1)
        self.assertEqual(int(summary.get("closed") or 0), 0)

        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        blocked_names = {
            str(item.get("candidate_name") or "")
            for item in (columns.get("delivery_blocked") or {}).get("items", [])
        }
        self.assertEqual(blocked_names, {"Delivery Blocked Candidate"})


if __name__ == "__main__":
    unittest.main()
