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

# Prevent import-time bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_candidate_profile_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.candidate_profile import CandidateProfileService
from tener_ai.candidate_scoring import CandidateScoringPolicy
from tener_ai.db import AGENT_DEFAULT_NAMES, Database
from tener_ai.matching import MatchingEngine


class CandidateProfileApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "candidate_profile_api.sqlite3"))
        self.db.init_schema()
        matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))
        scoring = CandidateScoringPolicy(str(self.root / "config" / "candidate_scoring_formula.json"))
        self.profile_service = CandidateProfileService(
            db=self.db,
            matching_engine=matching,
            scoring_policy=scoring,
            llm_responder=None,
        )
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "candidate_profile": self.profile_service,
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

    def _seed_candidate_context(self) -> Tuple[int, int]:
        job_id = self.db.insert_job(
            title="Senior Backend Engineer",
            company="Tener",
            jd_text="Need Python, AWS, Docker, communication, ownership",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "cand-profile-api-1",
                "full_name": "Candidate Profile API",
                "headline": "Backend Engineer",
                "location": "Poland",
                "languages": ["en"],
                "skills": ["python", "aws", "docker", "postgresql"],
                "years_experience": 6,
                "raw": {},
            },
            source="manual",
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.82,
            status="verified",
            verification_notes={
                "required_skills": ["python", "aws", "docker"],
                "matched_skills": ["python", "aws", "docker"],
                "components": {"location_match": 0.9, "language_match": 1.0},
                "interview_status": "scored",
                "interview_total_score": 85,
            },
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="sourcing_vetting",
            agent_name=AGENT_DEFAULT_NAMES["sourcing_vetting"],
            stage_key="vetting",
            score=88,
            status="qualified",
            reason="Strong skills match",
            details={"matched_required_skills": ["python", "aws", "docker"]},
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="communication",
            agent_name=AGENT_DEFAULT_NAMES["communication"],
            stage_key="dialogue",
            score=77,
            status="in_dialogue",
            reason="Responsive and clear communication",
            details={"quality_signals": {"turns": 4, "filler_count": 0}},
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="interview_evaluation",
            agent_name=AGENT_DEFAULT_NAMES["interview_evaluation"],
            stage_key="interview_results",
            score=85,
            status="scored",
            reason="Strong interview results",
            details={"technical_score": 84, "soft_skills_score": 83},
        )
        conversation_id = self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
        self.db.add_message(
            conversation_id=conversation_id,
            direction="inbound",
            content="Sharing my resume https://example.com/candidate-cv.pdf",
            candidate_language="en",
            meta={},
        )
        state = {
            "session_id": f"pre-{conversation_id}",
            "status": "resume_received",
            "resume_links": ["https://example.com/candidate-cv.pdf"],
            "updated_at": "2026-02-20T10:00:00+00:00",
        }
        self.db.upsert_pre_resume_session(
            session_id=state["session_id"],
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state=state,
            instruction="test",
        )
        self.db.insert_pre_resume_event(
            session_id=state["session_id"],
            conversation_id=conversation_id,
            event_type="inbound_processed",
            intent="resume_shared",
            inbound_text="attached resume",
            outbound_text="thanks",
            state_status="resume_received",
            details={"reason": "resume_detected"},
        )
        self.db.log_operation(
            operation="candidate.profile.test",
            status="ok",
            entity_type="candidate",
            entity_id=str(candidate_id),
            details={"job_id": job_id, "note": "seed"},
        )
        return job_id, candidate_id

    def test_get_candidate_profile_with_audit(self) -> None:
        job_id, candidate_id = self._seed_candidate_context()

        status, payload = self._request(
            "GET",
            f"/api/candidates/{candidate_id}/profile?job_id={job_id}&audit=1&explain=0",
        )
        self.assertEqual(status, 200)
        self.assertEqual(int(payload["candidate"]["id"]), candidate_id)
        self.assertEqual(int(payload["selected_job_id"]), job_id)
        self.assertIn("audit", payload)
        jobs = payload.get("jobs")
        self.assertIsInstance(jobs, list)
        self.assertGreater(len(jobs), 0)
        first = jobs[0]
        self.assertIsNotNone((first.get("overall_scoring") or {}).get("overall_score"))
        self.assertEqual((first.get("fit_explanation") or {}).get("source"), "fallback")
        kinds = {str(x.get("kind") or "") for x in (first.get("signals_timeline") or [])}
        self.assertIn("assessment_signal", kinds)
        self.assertIn("pre_resume_event", kinds)
        self.assertIn("operation_log", kinds)

    def test_resume_preview_returns_candidate_link(self) -> None:
        _, candidate_id = self._seed_candidate_context()

        status, preview = self._request("GET", f"/api/candidates/{candidate_id}/resume-preview")
        self.assertEqual(status, 200)
        self.assertTrue(bool(preview.get("available")))
        self.assertEqual(preview.get("url"), "https://example.com/candidate-cv.pdf")

        status_bad, body_bad = self._request(
            "GET",
            f"/api/candidates/{candidate_id}/resume-preview?url=https://example.com/other.pdf",
        )
        self.assertEqual(status_bad, 400)
        self.assertIn("error", body_bad)

    def test_demo_profile_endpoint_seeds_profile(self) -> None:
        status, created = self._request("POST", "/api/candidates/demo-profile", {})
        self.assertEqual(status, 201)
        candidate_id = int(created.get("candidate_id") or 0)
        self.assertGreater(candidate_id, 0)
        self.assertIn(f"/candidate/{candidate_id}", str(created.get("profile_path") or ""))

        profile_status, profile = self._request("GET", f"/api/candidates/{candidate_id}/profile")
        self.assertEqual(profile_status, 200)
        self.assertEqual(int(profile["candidate"]["id"]), candidate_id)

    def test_candidate_page_route_returns_html(self) -> None:
        status, body = self._request("GET", "/candidate/1")
        self.assertEqual(status, 200)
        self.assertIn("<!doctype html>", str(body.get("raw_text") or "").lower())


if __name__ == "__main__":
    unittest.main()
