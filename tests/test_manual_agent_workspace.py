import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional

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


class _StubLLMResponder:
    def generate_candidate_reply(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        fallback_reply: str,
        language: str = "en",
        state: Optional[Dict[str, Any]] = None,
        allow_fallback: bool = True,
    ) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized == "pre_resume":
            return "Got it. Send your CV when ready and share the key details in one message"
        if normalized == "faq":
            if state and state.get("terminal_reason"):
                return "Got it. I will close this chat here for now"
            return "Got it. Share what range you are targeting and I will confirm"
        if normalized == "linkedin_followup":
            return "Quick nudge on this one. If you are still interested, send me a short update"
        if normalized == "linkedin_interview_invite":
            return f"Here is the link: {job.get('interview_entry_url')}\nSend me a short reply when you are done"
        if normalized == "linkedin_interview_followup":
            return f"Quick check in. Here is the link again: {job.get('interview_entry_url')}\nNeed help before you do it?"
        return fallback_reply if allow_fallback else ""


class ManualAgentWorkspaceTests(unittest.TestCase):
    def _build_workflow(
        self,
        db: Database,
        root: Path,
        provider: _ManualWorkspaceProvider,
        llm_responder: Optional[Any] = None,
    ) -> WorkflowService:
        matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
        return WorkflowService(
            db=db,
            sourcing_agent=SourcingAgent(provider),
            verification_agent=VerificationAgent(matching),
            outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
            faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
            pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
            llm_responder=llm_responder,
            contact_all_mode=True,
            require_resume_before_final_verify=True,
            stage_instructions={"pre_resume": "request cv and track status"},
        )

    def _seed_manual_conversation(self, llm_responder: Optional[Any] = None) -> tuple[Database, WorkflowService, int, int]:
        root = Path(__file__).resolve().parents[1]
        td = TemporaryDirectory()
        self.addCleanup(td.cleanup)
        db = Database(str(Path(td.name) / "manual_agent_workspace.sqlite3"))
        db.init_schema()
        provider = _ManualWorkspaceProvider()
        workflow = self._build_workflow(db=db, root=root, provider=provider, llm_responder=llm_responder)
        job_id = db.insert_job(
            title="Senior Backend Engineer",
            jd_text="Need Python, AWS and distributed systems.",
            location="Ukraine / Eastern Europe",
            preferred_languages=["en"],
            seniority="senior",
        )
        added = workflow.add_manual_test_account(
            job_id=job_id,
            full_name="Manual Candidate",
            language="en",
        )
        return db, workflow, int(added["conversation_id"]), int(added["candidate_id"])

    def test_manual_account_chat_flow_uses_manual_delivery(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "manual_agent_workspace.sqlite3"))
            db.init_schema()

            provider = _ManualWorkspaceProvider()
            workflow = self._build_workflow(db=db, root=root, provider=provider, llm_responder=_StubLLMResponder())

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
            self.assertEqual(updated_match["status"], "resume_received_pending_must_have")

    def test_referral_offer_flags_operator_attention_without_auto_reply(self) -> None:
        db, workflow, conversation_id, _candidate_id = self._seed_manual_conversation(llm_responder=_StubLLMResponder())

        result = workflow.process_inbound_message(
            conversation_id=conversation_id,
            text="I can recommend someone else who is looking right now",
        )

        self.assertEqual(result["mode"], "operator_attention")
        self.assertEqual(result["reply"], "")
        conversation = db.get_conversation(conversation_id)
        self.assertTrue(bool(conversation.get("operator_attention_required")))
        messages = db.list_messages(conversation_id)
        self.assertEqual([m.get("direction") for m in messages], ["outbound", "inbound"])

    def test_budget_or_part_time_mismatch_closes_chat_and_blocks_future_replies(self) -> None:
        db, workflow, conversation_id, candidate_id = self._seed_manual_conversation(llm_responder=_StubLLMResponder())
        conversation = db.get_conversation(conversation_id)
        self.assertIsNotNone(conversation)
        job_id = int(conversation["job_id"])

        closed = workflow.process_inbound_message(
            conversation_id=conversation_id,
            text="Sorry, for full time this is low budget for me. I can work only part time",
        )

        self.assertEqual(closed["mode"], "closed")
        self.assertIn(closed["terminal_reason"], {"budget_mismatch", "part_time_only"})
        self.assertTrue(str(closed["reply"] or "").strip())
        updated_conversation = db.get_conversation(conversation_id)
        self.assertEqual(str(updated_conversation.get("status") or ""), "closed")
        self.assertFalse(bool(updated_conversation.get("ai_enabled")))
        updated_match = db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
        self.assertEqual(str(updated_match.get("status") or ""), "rejected")

        messages_before = db.list_messages(conversation_id)
        followup = workflow.process_inbound_message(conversation_id=conversation_id, text="Thanks")
        messages_after = db.list_messages(conversation_id)

        self.assertEqual(followup["mode"], "closed")
        self.assertEqual(followup["reply"], "")
        self.assertEqual(len(messages_after), len(messages_before) + 1)
        self.assertEqual(str(messages_after[-1].get("direction") or ""), "inbound")

    def test_manual_operator_reply_clears_attention_flag_but_keeps_conversation_active(self) -> None:
        db, workflow, conversation_id, _candidate_id = self._seed_manual_conversation(llm_responder=_StubLLMResponder())
        db.update_conversation_control(conversation_id=conversation_id, operator_attention_required=True)

        result = workflow.send_manual_conversation_message(
            conversation_id=conversation_id,
            message="Jumping in here manually from our side",
        )

        self.assertTrue(bool((result.get("delivery") or {}).get("sent")))
        updated_conversation = db.get_conversation(conversation_id)
        self.assertFalse(bool(updated_conversation.get("operator_attention_required")))
        self.assertNotEqual(str(updated_conversation.get("status") or ""), "closed")
        messages = db.list_messages(conversation_id)
        last_message = messages[-1]
        self.assertEqual(str(last_message.get("direction") or ""), "outbound")
        self.assertEqual((last_message.get("meta") or {}).get("type"), "operator_manual_reply")

    def test_llm_unavailable_keeps_chat_silent_after_intro(self) -> None:
        db, workflow, conversation_id, _candidate_id = self._seed_manual_conversation(llm_responder=None)

        reply = workflow.process_inbound_message(conversation_id=conversation_id, text="What is salary range?")

        self.assertEqual(reply["mode"], "pre_resume")
        self.assertEqual(reply["reply"], "")
        messages = db.list_messages(conversation_id)
        self.assertEqual([m.get("direction") for m in messages], ["outbound", "inbound"])


if __name__ == "__main__":
    unittest.main()
