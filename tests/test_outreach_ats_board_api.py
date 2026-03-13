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


class _ReconcileWorkflowStub:
    def __init__(self) -> None:
        self.calls: list[Dict[str, Any]] = []

    def reconcile_waiting_connection_match_statuses(
        self,
        *,
        job_id: Optional[int] = None,
        limit: int = 200,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "job_id": job_id,
            "limit": limit,
            "dry_run": dry_run,
        }
        self.calls.append(payload)
        return {
            "status": "ok",
            "job_id": job_id,
            "dry_run": dry_run,
            "candidates_total": 1,
            "updated": 0 if dry_run else 1,
            "items": [payload],
        }


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

        sourced_candidate = self._create_candidate("sourced", "Sourced Candidate")
        self._attach_match(job_id=job_id, candidate_id=sourced_candidate, status="verified")

        connect_candidate = self._create_candidate("connect", "Connect Candidate")
        self._attach_match(job_id=job_id, candidate_id=connect_candidate, status="outreach_pending_connection")
        connect_conversation = self.db.create_conversation(job_id=job_id, candidate_id=connect_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=connect_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=connect_conversation, status="waiting_connection")

        responded_candidate = self._create_candidate("responded", "Responded Candidate")
        self._attach_match(job_id=job_id, candidate_id=responded_candidate, status="in_dialogue")
        responded_conversation = self.db.create_conversation(job_id=job_id, candidate_id=responded_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=responded_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=responded_conversation, status="active")
        self.db.add_message(
            conversation_id=responded_conversation,
            direction="inbound",
            content="Happy to chat",
            candidate_language="en",
            meta={},
        )

        sent_candidate = self._create_candidate("connect-sent", "Connect Sent Candidate")
        self._attach_match(job_id=job_id, candidate_id=sent_candidate, status="outreach_sent")
        self.db.create_conversation(job_id=job_id, candidate_id=sent_candidate, channel="linkedin")
        self.db.update_candidate_match_status(
            job_id=job_id,
            candidate_id=sent_candidate,
            status="outreach_sent",
        )

        must_have_candidate = self._create_candidate("must-have", "Must-Have Candidate")
        self._attach_match(job_id=job_id, candidate_id=must_have_candidate, status="must_have_approved")
        must_have_conversation = self.db.create_conversation(job_id=job_id, candidate_id=must_have_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=must_have_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=must_have_conversation, status="active")
        self.db.upsert_pre_resume_session(
            session_id="ats-pre-must-have",
            conversation_id=must_have_conversation,
            job_id=job_id,
            candidate_id=must_have_candidate,
            state={"status": "ready_for_cv", "prescreen_status": "ready_for_cv", "language": "en"},
            instruction="",
        )

        cv_candidate = self._create_candidate("cv", "CV Candidate")
        self._attach_match(job_id=job_id, candidate_id=cv_candidate, status="resume_received_pending_must_have")
        cv_conversation = self.db.create_conversation(job_id=job_id, candidate_id=cv_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=cv_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=cv_conversation, status="active")
        self.db.upsert_pre_resume_session(
            session_id="ats-pre-cv",
            conversation_id=cv_conversation,
            job_id=job_id,
            candidate_id=cv_candidate,
            state={"status": "cv_received_pending_answers", "prescreen_status": "cv_received_pending_answers", "cv_received": True, "language": "en"},
            instruction="",
        )

        approved_cv_candidate = self._create_candidate("approved-cv", "Approved CV Candidate")
        self._attach_match(job_id=job_id, candidate_id=approved_cv_candidate, status="resume_received")
        approved_cv_conversation = self.db.create_conversation(job_id=job_id, candidate_id=approved_cv_candidate, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=approved_cv_conversation, account_id=account_id)
        self.db.update_conversation_status(conversation_id=approved_cv_conversation, status="active")
        self.db.upsert_pre_resume_session(
            session_id="ats-pre-approved-cv",
            conversation_id=approved_cv_conversation,
            job_id=job_id,
            candidate_id=approved_cv_candidate,
            state={"status": "resume_received", "prescreen_status": "ready_for_interview", "cv_received": True, "language": "en"},
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
            status="interview_passed",
            verification_notes={"interview_status": "scored", "interview_total_score": 84.0},
        )

        failed_candidate = self._create_candidate("failed", "Failed Candidate")
        self._attach_match(
            job_id=job_id,
            candidate_id=failed_candidate,
            status="interview_failed",
            verification_notes={"interview_status": "scored", "interview_total_score": 44.8},
        )

        closed_candidate = self._create_candidate("closed", "Closed Candidate")
        self._attach_match(job_id=job_id, candidate_id=closed_candidate, status="rejected")

        status, payload = self._request("GET", "/api/outreach/ats-board")
        self.assertEqual(status, 200)

        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("total_candidates") or 0), 11)
        self.assertEqual(int(summary.get("sourced") or 0), 1)
        self.assertEqual(int(summary.get("connect_sent") or 0), 2)
        self.assertEqual(int(summary.get("responded") or 0), 1)
        self.assertEqual(int(summary.get("must_have_approved") or 0), 1)
        self.assertEqual(int(summary.get("cv_received") or 0), 2)
        self.assertEqual(int(summary.get("interview_pending") or 0), 1)
        self.assertEqual(int(summary.get("completed") or 0), 3)

        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("sourced") or {}).get("items", [])],
            ["Sourced Candidate"],
        )
        self.assertEqual(
            {
                str(item.get("candidate_name") or "")
                for item in (columns.get("connect_sent") or {}).get("items", [])
            },
            {"Connect Candidate", "Connect Sent Candidate"},
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("responded") or {}).get("items", [])],
            ["Responded Candidate"],
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("must_have_approved") or {}).get("items", [])],
            ["Must-Have Candidate"],
        )
        self.assertEqual(
            {
                str(item.get("candidate_name") or "")
                for item in (columns.get("cv_received") or {}).get("items", [])
            },
            {"CV Candidate", "Approved CV Candidate"},
        )
        self.assertEqual(
            [str(item.get("candidate_name") or "") for item in (columns.get("interview_pending") or {}).get("items", [])],
            ["Interview Candidate"],
        )
        completed_names = {
            str(item.get("candidate_name") or "")
            for item in (columns.get("completed") or {}).get("items", [])
        }
        self.assertEqual(
            completed_names,
            {"Passed Candidate", "Failed Candidate", "Closed Candidate"},
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

    def test_outreach_ats_board_skips_agent_scorecard_hydration(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        candidate_id = self._create_candidate("light-read", "Light Read Candidate")
        self._attach_match(job_id=job_id, candidate_id=candidate_id, status="verified")
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="interview_evaluation",
            agent_name="Jordan AI",
            stage_key="summary",
            score=82.5,
            status="completed",
        )

        def _unexpected_assessment_read(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("ATS board should not build full agent scorecards")

        self.db._list_candidate_assessments_grouped = _unexpected_assessment_read  # type: ignore[method-assign]

        status, payload = self._request("GET", f"/api/outreach/ats-board?job_id={job_id}")
        self.assertEqual(status, 200)

        queued_names = {
            str(item.get("candidate_name") or "")
            for column in (payload.get("columns") or [])
            if str(column.get("key") or "") == "sourced"
            for item in (column.get("items") or [])
        }
        self.assertEqual(queued_names, {"Light Read Candidate"})

    def test_outreach_ats_board_uses_dedicated_query_not_candidate_list_builder(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        candidate_id = self._create_candidate("dedicated-query", "Dedicated Query Candidate")
        self._attach_match(job_id=job_id, candidate_id=candidate_id, status="verified")

        def _unexpected_candidate_list(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("ATS board should not route through list_candidates_for_job")

        self.db.list_candidates_for_job = _unexpected_candidate_list  # type: ignore[method-assign]

        status, payload = self._request("GET", f"/api/outreach/ats-board?job_id={job_id}")
        self.assertEqual(status, 200)

        all_names = [
            str(item.get("candidate_name") or "")
            for column in (payload.get("columns") or [])
            for item in (column.get("items") or [])
        ]
        self.assertIn("Dedicated Query Candidate", all_names)

    def test_outreach_ats_board_hides_forced_test_candidates_for_normal_jobs(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        api_main.SERVICES["workflow"] = type(
            "_WorkflowStub",
            (),
            {
                "_load_forced_test_identifiers": staticmethod(lambda: (_ for _ in ()).throw(AssertionError("ATS board should not load forced test ids"))),
                "_build_forced_identifier_lookup": staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ATS board should not build forced lookup"))),
                "_effective_test_mode": staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ATS board should not resolve test mode"))),
                "_is_non_test_forced_candidate": staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("ATS board should not run forced candidate checks"))),
            },
        )()

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
        self.assertEqual(int(summary.get("sourced") or 0), 1)

        all_names = [
            str(item.get("candidate_name") or "")
        for column in (payload.get("columns") or [])
            for item in (column.get("items") or [])
        ]
        self.assertIn("Normal Candidate", all_names)
        self.assertNotIn("Forced Test Candidate (olena-bachek-b8523121a)", all_names)

    def test_outreach_ats_board_groups_terminal_states_into_completed(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        terminal_statuses = [
            ("rejected", "Rejected Candidate", "rejected", {}),
            ("stalled", "Stalled Candidate", "stalled", {}),
            ("failed", "Interview Failed Candidate", "interview_failed", {"interview_status": "failed"}),
        ]

        for suffix, name, status_key, notes in terminal_statuses:
            candidate_id = self._create_candidate(suffix, name)
            self._attach_match(job_id=job_id, candidate_id=candidate_id, status=status_key)
            if notes:
                self.db.update_candidate_match_status(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    status=status_key,
                    extra_notes=notes,
                )

        status, payload = self._request("GET", "/api/outreach/ats-board")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("completed") or 0), 3)
        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        completed_names = {
            str(item.get("candidate_name") or "")
            for item in (columns.get("completed") or {}).get("items", [])
        }
        self.assertEqual(
            completed_names,
            {"Rejected Candidate", "Stalled Candidate", "Interview Failed Candidate"},
        )

    def test_outreach_ats_board_ignores_delivery_blocked_identity_for_stage_assignment(self) -> None:
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
        self.assertEqual(int(summary.get("delivery_blocked") or 0), 0)
        self.assertEqual(int(summary.get("completed") or 0), 0)
        self.assertEqual(int(summary.get("sourced") or 0), 1)

        columns = {str(item.get("key") or ""): item for item in (payload.get("columns") or [])}
        queued_names = {
            str(item.get("candidate_name") or "")
            for item in (columns.get("sourced") or {}).get("items", [])
        }
        self.assertEqual(queued_names, {"Delivery Blocked Candidate"})

    def test_outreach_ats_board_summary_counts_all_candidates_even_when_cards_are_limited(self) -> None:
        job_id = self._create_job("Manual QA Engineer")
        for index in range(55):
            candidate_id = self._create_candidate(f"queued-{index}", f"Queued Candidate {index}")
            self._attach_match(job_id=job_id, candidate_id=candidate_id, status="verified")
        for index in range(3):
            candidate_id = self._create_candidate(f"complete-{index}", f"Completed Candidate {index}")
            self._attach_match(
                job_id=job_id,
                candidate_id=candidate_id,
                status="interview_scored",
                verification_notes={"interview_status": "scored", "interview_total_score": 85.0 if index == 0 else 50.0},
            )

        status, payload = self._request("GET", "/api/outreach/ats-board?limit=5")
        self.assertEqual(status, 200)
        summary = payload.get("summary") or {}
        self.assertEqual(int(summary.get("total_candidates") or 0), 58)
        self.assertEqual(int(summary.get("completed") or 0), 3)
        self.assertEqual(int(summary.get("sourced") or 0), 55)
        self.assertTrue(bool(payload.get("limited")))
        self.assertEqual(int(payload.get("displayed_candidates") or 0), 50)

    def test_reconcile_waiting_connection_endpoint_passes_through_payload(self) -> None:
        workflow = _ReconcileWorkflowStub()
        api_main.SERVICES = {"db": self.db, "workflow": workflow}

        status, payload = self._request(
            "POST",
            "/api/outreach/reconcile-waiting-connection",
            payload={"job_id": 27, "limit": 25, "dry_run": False},
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            workflow.calls,
            [{"job_id": 27, "limit": 25, "dry_run": False}],
        )
        self.assertEqual(int(payload.get("updated") or 0), 1)
        self.assertFalse(bool(payload.get("dry_run")))


if __name__ == "__main__":
    unittest.main()
