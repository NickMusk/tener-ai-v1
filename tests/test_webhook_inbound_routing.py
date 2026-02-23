import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _WebhookStubProvider:
    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {
            "sent": True,
            "provider": "stub",
            "chat_id": "chat-123",
            "candidate": candidate_profile.get("linkedin_id"),
        }


class WebhookInboundRoutingTests(unittest.TestCase):
    def test_process_provider_inbound_routes_by_chat_id_and_updates_status(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "webhook_route.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _WebhookStubProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and track status"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-webhook-1",
                "full_name": "Webhook Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 2,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.4, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])

            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])

            conversation = db.get_conversation(conversation_id)
            self.assertEqual(conversation.get("external_chat_id"), "chat-123")

            routed = workflow.process_provider_inbound_message(
                external_chat_id="chat-123",
                text="Here is my resume https://example.com/webhook-cv.pdf",
            )
            self.assertTrue(routed["processed"])
            self.assertEqual(routed["conversation_id"], conversation_id)
            self.assertEqual(routed["result"]["mode"], "pre_resume")
            self.assertEqual(routed["result"]["state"]["status"], "resume_received")

            matches = db.list_candidates_for_job(job_id)
            self.assertEqual(matches[0]["status"], "resume_received")

            fallback = workflow.process_provider_inbound_message(
                external_chat_id="",
                sender_provider_id="ln-webhook-1",
                text="What is the process?",
            )
            self.assertTrue(fallback["processed"])


if __name__ == "__main__":
    unittest.main()
