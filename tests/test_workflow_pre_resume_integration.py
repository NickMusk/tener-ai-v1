import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.linkedin_provider import MockLinkedInProvider
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class WorkflowPreResumeIntegrationTests(unittest.TestCase):
    def test_needs_resume_chat_flow_until_resume_received(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "workflow_pre_resume.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = MockLinkedInProvider(str(root / "data" / "mock_linkedin_profiles.json"))
            pre_resume = PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=pre_resume,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and drive candidate to resume"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-pre-resume-1",
                "full_name": "Candidate One",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 2,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.42, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            session_id = outreach["items"][0]["pre_resume_session_id"]
            self.assertTrue(session_id)
            conversation_id = int(outreach["items"][0]["conversation_id"])

            prs = db.get_pre_resume_session_by_conversation(conversation_id)
            self.assertIsNotNone(prs)
            self.assertEqual(prs["status"], "awaiting_reply")

            first_reply = workflow.process_inbound_message(conversation_id=conversation_id, text="What is salary range?")
            self.assertEqual(first_reply["mode"], "pre_resume")
            self.assertIn(first_reply["intent"], {"salary", "default"})
            self.assertIn("CV", first_reply["reply"])

            second_reply = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Here is my resume https://example.com/candidate-one-resume.pdf",
            )
            self.assertEqual(second_reply["mode"], "pre_resume")
            self.assertEqual(second_reply["state"]["status"], "resume_received")

            match_rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(match_rows), 1)
            self.assertEqual(match_rows[0]["status"], "resume_received")

            events = db.list_pre_resume_events(limit=20, session_id=session_id)
            event_types = {x["event_type"] for x in events}
            self.assertIn("session_started", event_types)
            self.assertIn("inbound_processed", event_types)


if __name__ == "__main__":
    unittest.main()
