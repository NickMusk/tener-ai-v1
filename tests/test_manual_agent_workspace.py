import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _ManualWorkspaceProvider:
    def __init__(self) -> None:
        self.send_calls = 0

    def search_profiles(self, job: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.send_calls += 1
        return {"sent": True, "provider": "stub", "chat_id": "stub-chat"}


class ManualAgentWorkspaceTests(unittest.TestCase):
    def test_manual_account_chat_flow_uses_manual_delivery(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "manual_agent_workspace.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _ManualWorkspaceProvider()
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

            added = workflow.add_manual_test_account(
                job_id=job_id,
                full_name="Manual Candidate",
                language="en",
            )
            conversation_id = int(added["conversation_id"])
            candidate_id = int(added["candidate_id"])

            conversation = db.get_conversation(conversation_id)
            self.assertIsNotNone(conversation)
            self.assertEqual(conversation["channel"], "manual")
            self.assertTrue(str(conversation.get("external_chat_id") or "").startswith("manual-chat-"))

            match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            self.assertIsNotNone(match)
            self.assertEqual(match["status"], "needs_resume")

            session = db.get_pre_resume_session_by_conversation(conversation_id)
            self.assertIsNotNone(session)
            self.assertEqual(session["status"], "awaiting_reply")

            # Initial outbound for manual account is created without provider call.
            self.assertEqual(provider.send_calls, 0)

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="What is salary range?")
            self.assertEqual(reply["mode"], "pre_resume")
            self.assertIn(reply["intent"], {"salary", "default"})
            self.assertEqual(provider.send_calls, 0)

            messages = db.list_messages(conversation_id)
            latest_outbound = [m for m in messages if m.get("direction") == "outbound"][-1]
            self.assertEqual((latest_outbound.get("meta") or {}).get("type"), "pre_resume_auto_reply")
            self.assertEqual((latest_outbound.get("meta") or {}).get("delivery", {}).get("provider"), "manual")
            self.assertTrue((latest_outbound.get("meta") or {}).get("delivery", {}).get("sent"))

            done = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Here is my resume https://example.com/manual-candidate-cv.pdf",
            )
            self.assertEqual(done["mode"], "pre_resume")
            self.assertEqual((done.get("state") or {}).get("status"), "resume_received")

            updated_match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            self.assertIsNotNone(updated_match)
            self.assertEqual(updated_match["status"], "resume_received")


if __name__ == "__main__":
    unittest.main()
