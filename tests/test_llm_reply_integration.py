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
        self.extraction_calls: List[Dict[str, Any]] = []

    def generate_candidate_extraction(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        state: Dict[str, Any] | None = None,
        attachments: List[Dict[str, Any]] | None = None,
        previous_language: str = "",
        fallback_language: str = "en",
    ) -> Dict[str, Any]:
        self.extraction_calls.append(
            {
                "mode": mode,
                "instruction": instruction,
                "job_title": job.get("title"),
                "inbound_text": inbound_text,
                "language": previous_language or fallback_language,
                "state_status": (state or {}).get("status"),
                "attachments_len": len(attachments or []),
            }
        )
        lowered = str(inbound_text or "").lower()
        if mode == "faq":
            if "salario" in lowered or "hola" in lowered:
                return {
                    "language": "es",
                    "intent": "salary",
                    "sanitized_text": inbound_text,
                    "confidence": {"language": 0.95, "intent": 0.96},
                    "warnings": [],
                }
            if "timeline" in lowered or "process" in lowered:
                return {
                    "language": "en",
                    "intent": "timeline",
                    "sanitized_text": inbound_text,
                    "confidence": {"language": 0.95, "intent": 0.92},
                    "warnings": [],
                }
            return {
                "language": "en",
                "intent": "default",
                "sanitized_text": inbound_text,
                "confidence": {"language": 0.85, "intent": 0.8},
                "warnings": [],
            }
        if "aqui esta mi cv" in lowered:
            return {
                "language": "es",
                "intent": "resume_shared",
                "resume_shared": True,
                "resume_links": [],
                "sanitized_text": inbound_text,
                "confidence": {"language": 0.97, "intent": 0.98},
                "warnings": [],
            }
        if "poland" in lowered and "3900" in lowered:
            return {
                "language": "en",
                "intent": "resume_shared",
                "resume_shared": True,
                "resume_links": [],
                "salary_expectation_gross_monthly": 3900,
                "salary_expectation_currency": "USD",
                "must_have_answer": None,
                "location_confirmed": None,
                "work_authorization_confirmed": None,
                "sanitized_text": "I am in Poland and I can work remotely as B2B. My salary expectation is 3900$ gross.",
                "confidence": {"language": 0.98, "salary_expectation_gross_monthly": 0.95},
                "warnings": ["attachment_payload_ignored"],
            }
        if "timeline" in lowered:
            return {
                "language": "en",
                "intent": "timeline",
                "resume_shared": False,
                "resume_links": [],
                "sanitized_text": inbound_text,
                "confidence": {"language": 0.95, "intent": 0.92},
                "warnings": [],
            }
        return {
            "language": "en",
            "intent": "default",
            "resume_shared": False,
            "resume_links": [],
            "sanitized_text": inbound_text,
            "confidence": {"language": 0.8, "intent": 0.75},
            "warnings": [],
        }

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


class _DashyLLMResponder(_FakeLLMResponder):
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
        if mode == "linkedin_outreach":
            return "Greetings,\nLong-term role -- high impact\nShort 10-15 minute screening call"
        if mode == "linkedin_followup":
            return "Hey,\nQuick check-in -- can you reply?"
        return super().generate_candidate_reply(mode, instruction, job, candidate, inbound_text, history, fallback_reply, language, state)


class LLMReplyIntegrationTests(unittest.TestCase):
    def test_faq_reply_switches_llm_language_to_latest_candidate_message(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_faq_language.sqlite3"))
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
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-llm-faq-language-1",
                "full_name": "LLM FAQ Language Candidate",
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

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="Hola, cual es el salario?")
            self.assertEqual(reply["language"], "es")
            self.assertEqual(llm.calls[-1]["language"], "es")
            messages = db.list_messages(conversation_id)
            inbound_messages = [m for m in messages if m.get("direction") == "inbound"]
            self.assertTrue(inbound_messages)
            self.assertEqual(inbound_messages[-1]["candidate_language"], "es")

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

    def test_pre_resume_reply_switches_llm_language_to_latest_candidate_message(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_pre_language.sqlite3"))
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
                "linkedin_id": "ln-llm-pre-language-1",
                "full_name": "LLM Pre Language Candidate",
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

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="Aqui esta mi CV")
            self.assertEqual(reply["language"], "es")
            self.assertEqual(reply["state"]["language"], "es")
            self.assertEqual(llm.calls[-1]["language"], "es")
            messages = db.list_messages(conversation_id)
            inbound_messages = [m for m in messages if m.get("direction") == "inbound"]
            self.assertTrue(inbound_messages)
            self.assertEqual(inbound_messages[-1]["candidate_language"], "es")

    def test_pre_resume_extraction_ignores_attachment_noise_for_language_and_salary(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_pre_extraction_noise.sqlite3"))
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
                title="Manual QA",
                jd_text="Need manual testing, API testing, regression testing and Postman.",
                location="Ukraine, Eastern Europe",
                preferred_languages=["ru"],
                seniority="middle",
                salary_min=500,
                salary_max=1200,
                salary_currency="USD",
            )
            profile = {
                "linkedin_id": "ln-llm-pre-noise-1",
                "full_name": "Anastasiya Saladukha",
                "headline": "Manual / Automation QA Engineer",
                "location": "Poland",
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

            workflow.process_inbound_message(
                conversation_id=conversation_id,
                text=(
                    "Hello Andres,\nI'm looking for a position as Manual QA. It would be perfect if I can work like "
                    "Manual and Automation QA. Can you send me more details about the position?\nHave a good day!"
                ),
            )
            reply = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text=(
                    "I am in Poland and I can work remotely as B2B. My salary expectation is 3900$ gross.\n"
                    "attached file CV_Nastya_Saladukha_2026.pdf "
                    "att://7kVV9pJISPGbnB3-DGVZ7Q/aHR0cHM6Ly93d3cubGlua2VkaW4uY29tL2RlbW8="
                ),
            )

            self.assertEqual(reply["language"], "en")
            self.assertEqual(reply["state"]["language"], "en")
            self.assertEqual(reply["state"]["prescreen_status"], "cv_received_pending_answers")
            self.assertEqual(float(reply["state"]["salary_expectation_gross_monthly"] or 0.0), 3900.0)
            self.assertFalse(bool(reply.get("interview", {}).get("started")))
            self.assertIsNone(reply["state"].get("must_have_answer"))
            self.assertTrue(llm.extraction_calls)
            self.assertEqual(llm.extraction_calls[-1]["mode"], "pre_resume")

            messages = db.list_messages(conversation_id)
            inbound_messages = [m for m in messages if m.get("direction") == "inbound"]
            self.assertTrue(inbound_messages)
            self.assertEqual(inbound_messages[-1]["candidate_language"], "en")

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

    def test_linkedin_outreach_and_followup_strip_dash_punctuation(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "llm_dash_guard.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            llm = _DashyLLMResponder()
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
                "linkedin_id": "ln-dash-guard-1",
                "full_name": "Dash Guard Candidate",
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

            outbound_messages = [m for m in db.list_messages(conversation_id) if m.get("direction") == "outbound"]
            self.assertTrue(outbound_messages)
            outreach_text = str(outbound_messages[-1]["content"])
            self.assertNotIn("-", outreach_text)
            self.assertNotIn("--", outreach_text)
            self.assertIn("Long term role", outreach_text)
            self.assertIn("10 to 15 minute screening call", outreach_text)

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
            followup_text = str(followups[-1]["content"])
            self.assertNotIn("-", followup_text)
            self.assertNotIn("--", followup_text)
            self.assertIn("Quick check in", followup_text)


if __name__ == "__main__":
    unittest.main()
