from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _Provider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "chat_id": "chat-1"}


class _InterviewClient:
    def __init__(self) -> None:
        self.sessions = [
            {
                "session_id": "iv_fallback_1",
                "job_id": 1,
                "candidate_id": 1,
                "status": "scored",
                "scored_at": "2026-02-24T13:41:44.841415+00:00",
            }
        ]

    def list_sessions(self, job_id: int | None = None, status: str | None = None, limit: int = 100) -> Dict[str, Any]:
        return {"items": [x for x in self.sessions if int(x.get("job_id") or 0) == int(job_id or 0)]}

    def refresh_session(self, session_id: str, force: bool = False) -> Dict[str, Any]:
        return {
            "session_id": session_id,
            "status": "scored",
            "summary": {"total_score": 84.0},
        }

    def get_session(self, session_id: str) -> Dict[str, Any]:
        return self.refresh_session(session_id=session_id, force=False)

    def get_scorecard(self, session_id: str) -> Dict[str, Any]:
        return {"session_id": session_id, "scorecard": {"total_score": 84.0}}


class InterviewSyncFallbackTests(unittest.TestCase):
    def test_sync_updates_candidate_without_local_session_id(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "interview_sync_fallback.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_Provider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                interview_client=_InterviewClient(),
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-fallback-1",
                    "full_name": "Fallback Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 5,
                }
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.75,
                status="resume_received",
                verification_notes={},
            )

            out = workflow.sync_interview_progress(job_id=job_id, limit=20, force_refresh=True)
            self.assertEqual(out["processed"], 1)
            self.assertEqual(out["updated"], 1)

            row = db.list_candidates_for_job(job_id)[0]
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            self.assertEqual(row["status"], "interview_scored")
            self.assertEqual(str(notes.get("interview_session_id")), "iv_fallback_1")
            self.assertAlmostEqual(float(notes.get("interview_total_score")), 84.0, places=2)

    def test_sync_uses_scorecard_when_summary_missing(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "interview_sync_scorecard.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            interview = _InterviewClient()

            def _refresh_no_summary(session_id: str, force: bool = False) -> Dict[str, Any]:
                return {"session_id": session_id, "status": "scored", "summary": {"total_score": None}}

            interview.refresh_session = _refresh_no_summary  # type: ignore[assignment]
            interview.get_session = _refresh_no_summary  # type: ignore[assignment]

            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_Provider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                interview_client=interview,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-fallback-2",
                    "full_name": "Fallback Candidate Two",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": [],
                    "years_experience": 5,
                }
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.75,
                status="resume_received",
                verification_notes={},
            )

            out = workflow.sync_interview_progress(job_id=job_id, limit=20, force_refresh=True)
            self.assertEqual(out["processed"], 1)
            self.assertEqual(out["updated"], 1)

            row = db.list_candidates_for_job(job_id)[0]
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            self.assertEqual(row["status"], "interview_scored")
            self.assertAlmostEqual(float(notes.get("interview_total_score")), 84.0, places=2)


if __name__ == "__main__":
    unittest.main()
