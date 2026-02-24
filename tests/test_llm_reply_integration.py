from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _LLMStubProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": True, "provider": "stub", "chat_id": "chat-llm"}


class _FakeLLMResponder:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

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
        state: Dict[str, Any] | None = None,
    ) -> str:
        self.calls.append(
            {
                "mode": mode,
                "instruction": instruction,
                "job_title": job.get("title"),
                "jd_text": job.get("jd_text"),
                "inbound_text": inbound_text,
                "history_len": len(history or []),
                "language": language,
                "state_status": (state or {}).get("status"),
            }
        )
        if mode == "linkedin_outreach":
            return "LLM OUTREACH:\nGreetings,\nPlease share your CV and salary expectations."
        if mode == "linkedin_followup":
            return "LLM FOLLOWUP:\nHey,\nA short reply would really help."
        if mode == "pre_resume":
            return "LLM PRE: Please share your updated CV to proceed."
        return "LLM FAQ: Thanks for your question. We can discuss details once you share expected range."


class _NoCtaLLMResponder(_FakeLLMResponder):
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
        state: Dict[str, Any] | None = None,
    ) -> str:
        self.calls.append({"mode": mode, "language": language, "state_status": (state or {}).get("status")})
        if mode == "pre_resume":
            return "Thanks for your message, we can discuss role details."
        return "{scope_summary}"


class LLMReplyIntegrationTests(unittest.TestCase):
    def test_faq_reply_uses_llm_output(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_faq.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _FakeLLMResponder()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                llm_responder=llm,
                contact_all_mode=False,
                require_resume_before_final_verify=False,
                stage_instructions={"faq": "Answer candidate questions using JD context"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-faq-1",
                "full_name": "LLM FAQ Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.8, "status": "verified", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="What salary range do you offer?")
            self.assertEqual(reply["mode"] if "mode" in reply else "faq", "faq")
            self.assertTrue(reply["reply"].startswith("LLM FAQ:"))
            self.assertGreaterEqual(len(llm.calls), 1)
            self.assertEqual(llm.calls[-1]["mode"], "faq")
            self.assertIn("Python", llm.calls[-1]["jd_text"])

    def test_pre_resume_reply_uses_llm_output(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_pre.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _FakeLLMResponder()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
                llm_responder=llm,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "Always request CV before final verification"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-pre-1",
                "full_name": "LLM Pre Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 3,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.3, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="Can you share timeline?")
            self.assertEqual(reply["mode"], "pre_resume")
            self.assertTrue(reply["reply"].startswith("LLM PRE:"))
            self.assertGreaterEqual(len(llm.calls), 1)
            self.assertEqual(llm.calls[-1]["mode"], "pre_resume")
            self.assertEqual(llm.calls[-1]["state_status"], "engaged_no_resume")

    def test_pre_resume_llm_enforces_resume_cta_when_missing(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_pre_guard.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _NoCtaLLMResponder()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
                llm_responder=llm,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "Request resume before final verification"},
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-guard-1",
                "full_name": "LLM Guard Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 4,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.3, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="Can you share timeline?")
            self.assertEqual(reply["mode"], "pre_resume")
            self.assertIn("cv", reply["reply"].lower())

    def test_faq_llm_with_placeholders_falls_back_to_template(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_faq_guard.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _NoCtaLLMResponder()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                llm_responder=llm,
                contact_all_mode=False,
                require_resume_before_final_verify=False,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-faq-guard-1",
                "full_name": "LLM FAQ Guard",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.8, "status": "verified", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="What salary range do you offer?")
            self.assertNotIn("{scope_summary}", reply["reply"])

    def test_initial_outreach_message_uses_llm_generation(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_outreach.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _FakeLLMResponder()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json")),
                llm_responder=llm,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-outreach-1",
                "full_name": "LLM Outreach Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.3, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])

            outbound_messages = [m for m in db.list_messages(conversation_id) if m.get("direction") == "outbound"]
            self.assertTrue(outbound_messages)
            self.assertTrue(str(outbound_messages[-1]["content"]).startswith("LLM OUTREACH:"))
            self.assertTrue(any(call.get("mode") == "linkedin_outreach" for call in llm.calls))

    def test_pre_resume_followup_message_uses_llm_generation(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_followup.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _FakeLLMResponder()
            pre_resume = PreResumeCommunicationService(templates_path=str(root / "config" / "outreach_templates.json"))
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(_LLMStubProvider()),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=pre_resume,
                llm_responder=llm,
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-followup-1",
                "full_name": "LLM Followup Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": [],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.3, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])
            session_id = str(outreach["items"][0]["pre_resume_session_id"] or "")

            prs = db.get_pre_resume_session_by_conversation(conversation_id)
            self.assertIsNotNone(prs)
            assert prs is not None
            state = prs.get("state_json") if isinstance(prs.get("state_json"), dict) else {}
            state = dict(state)
            state["status"] = "awaiting_reply"
            state["next_followup_at"] = "2000-01-01T00:00:00+00:00"
            state["followups_sent"] = 0
            pre_resume.seed_session(state)
            db.upsert_pre_resume_session(
                session_id=session_id,
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state=state,
                instruction="",
            )

            result = workflow.run_due_pre_resume_followups(job_id=job_id, limit=10)
            self.assertEqual(result["sent"], 1)
            followups = [m for m in db.list_messages(conversation_id) if (m.get("meta") or {}).get("type") == "pre_resume_followup"]
            self.assertTrue(followups)
            self.assertTrue(str(followups[-1]["content"]).startswith("LLM FOLLOWUP:"))
            self.assertTrue(any(call.get("mode") == "linkedin_followup" for call in llm.calls))


if __name__ == "__main__":
    unittest.main()
