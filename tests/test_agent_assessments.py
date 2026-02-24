from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.instructions import AgentEvaluationPlaybook
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _Provider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "chat_id": "chat-agent-score"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-agent-score"}


class AgentAssessmentsTests(unittest.TestCase):
    def test_candidate_contains_agent_scorecard_for_all_agent_roles(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "agent_assessments.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_Provider()),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
                ),
                agent_evaluation_playbook=AgentEvaluationPlaybook(
                    str(root / "config" / "agent_evaluation_instructions.json")
                ),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and track status"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-agent-score-1",
                "unipile_profile_id": "ln-agent-score-1",
                "attendee_provider_id": "ln-agent-score-1",
                "full_name": "Agent Score Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python", "aws"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.82, "status": "verified", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])
            workflow.process_inbound_message(conversation_id=conversation_id, text="Tell me more")

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(rows), 1)
            scorecard = rows[0].get("agent_scorecard") or {}

            self.assertIn("sourcing_vetting", scorecard)
            self.assertIn("communication", scorecard)
            self.assertIn("interview_evaluation", scorecard)

            self.assertEqual(scorecard["sourcing_vetting"].get("latest_stage"), "vetting")
            self.assertEqual(scorecard["communication"].get("latest_stage"), "dialogue")
            self.assertEqual(scorecard["interview_evaluation"].get("latest_status"), "not_started")


if __name__ == "__main__":
    unittest.main()
