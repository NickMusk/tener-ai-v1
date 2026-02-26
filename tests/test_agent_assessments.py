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

    def test_scores_are_na_before_candidate_dialogue_starts(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "agent_assessments_pre_dialogue.sqlite3"))
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
                "linkedin_id": "ln-agent-pre-dialogue",
                "unipile_profile_id": "ln-agent-pre-dialogue",
                "attendee_provider_id": "ln-agent-pre-dialogue",
                "full_name": "Pre Dialogue Candidate",
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

            workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(rows), 1)
            scorecard = rows[0].get("agent_scorecard") or {}

            communication = scorecard.get("communication") or {}
            interview = scorecard.get("interview_evaluation") or {}
            self.assertEqual(str(communication.get("latest_stage") or ""), "outreach")
            self.assertIsNone(communication.get("latest_score"))
            self.assertIsNone(interview.get("latest_score"))

    def test_communication_score_varies_with_candidate_message_quality(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "agent_assessments_quality.sqlite3"))
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
            profile_short = {
                "linkedin_id": "ln-agent-score-short",
                "unipile_profile_id": "ln-agent-score-short",
                "attendee_provider_id": "ln-agent-score-short",
                "full_name": "Short Reply Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python", "aws"],
                "years_experience": 4,
                "raw": {},
            }
            profile_rich = {
                "linkedin_id": "ln-agent-score-rich",
                "unipile_profile_id": "ln-agent-score-rich",
                "attendee_provider_id": "ln-agent-score-rich",
                "full_name": "Rich Reply Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python", "aws"],
                "years_experience": 6,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[
                    {"profile": profile_short, "score": 0.82, "status": "verified", "notes": {}},
                    {"profile": profile_rich, "score": 0.82, "status": "verified", "notes": {}},
                ],
            )
            candidate_ids = [int(item["candidate_id"]) for item in added["added"]]

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=candidate_ids)
            by_candidate_id = {int(item["candidate_id"]): int(item["conversation_id"]) for item in outreach["items"]}
            short_candidate_id = int(
                next(
                    item["candidate_id"]
                    for item in added["added"]
                    if str((item.get("profile") or {}).get("linkedin_id")) == "ln-agent-score-short"
                )
            )
            rich_candidate_id = int(
                next(
                    item["candidate_id"]
                    for item in added["added"]
                    if str((item.get("profile") or {}).get("linkedin_id")) == "ln-agent-score-rich"
                )
            )
            workflow.process_inbound_message(conversation_id=by_candidate_id[short_candidate_id], text="ok")
            workflow.process_inbound_message(
                conversation_id=by_candidate_id[rich_candidate_id],
                text=(
                    "Thanks for reaching out. I am interested and can share examples of scaling Python services. "
                    "Could you share next steps and timeline?"
                ),
            )

            rows = db.list_candidates_for_job(job_id)
            by_linkedin = {str(row.get("linkedin_id")): row for row in rows}
            short_entry = (by_linkedin["ln-agent-score-short"].get("agent_scorecard") or {}).get("communication") or {}
            rich_entry = (by_linkedin["ln-agent-score-rich"].get("agent_scorecard") or {}).get("communication") or {}
            short_score = short_entry.get("latest_score")
            rich_score = rich_entry.get("latest_score")

            self.assertIsInstance(short_score, (int, float))
            self.assertIsInstance(rich_score, (int, float))
            self.assertGreater(float(rich_score), float(short_score))

    def test_interview_score_loaded_from_notes_when_assessment_score_missing(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "agent_assessments_interview_fallback.sqlite3"))
            db.init_schema()

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-agent-interview-fallback",
                    "full_name": "Interview Score Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python", "aws"],
                    "years_experience": 5,
                }
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.81,
                status="interview_completed",
                verification_notes={
                    "interview_session_id": "iv_123",
                    "interview_status": "scored",
                    "interview_total_score": 82.5,
                },
            )
            db.upsert_candidate_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="interview_evaluation",
                agent_name="Jordan AI (Lead Interviewer)",
                stage_key="interview_results",
                score=None,
                status="invited",
                reason="Interview invite created.",
                details={"session_id": "iv_123"},
            )

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(rows), 1)
            interview = (rows[0].get("agent_scorecard") or {}).get("interview_evaluation") or {}
            self.assertAlmostEqual(float(interview.get("latest_score")), 82.5, places=2)
            self.assertEqual(str(interview.get("latest_status")), "scored")


if __name__ == "__main__":
    unittest.main()
