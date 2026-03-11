import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.linkedin_provider import MockLinkedInProvider
from tener_ai.matching import MatchingEngine
from tener_ai.pre_resume_service import PreResumeCommunicationService
from tener_ai.workflow import WorkflowService


class _FollowupChatProvider:
    def __init__(self) -> None:
        self.sent_calls = 0

    def search_profiles(self, query: str, limit: int = 50):
        return []

    def enrich_profile(self, profile):
        return dict(profile)

    def send_message(self, candidate_profile, message):
        self.sent_calls += 1
        return {"provider": "stub", "sent": True, "chat_id": "chat-followup-1"}


class _FailingSecondFollowupProvider:
    def __init__(self) -> None:
        self.calls = 0

    def search_profiles(self, query: str, limit: int = 50):
        return []

    def enrich_profile(self, profile):
        return dict(profile)

    def send_message(self, candidate_profile, message):
        self.calls += 1
        if self.calls == 1:
            return {"provider": "stub", "sent": True, "chat_id": "chat-followup-ok"}
        return {"provider": "stub", "sent": False, "error": "delivery_failed"}


class WorkflowPreResumeIntegrationTests(unittest.TestCase):
    def test_prescreen_first_chat_flow_until_ready_for_screening_call(self) -> None:
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
                location="Berlin",
                preferred_languages=["en"],
                seniority="senior",
                salary_min=120000,
                salary_max=150000,
                salary_currency="USD",
                work_authorization_required=True,
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
            prescreen = db.get_candidate_prescreen(job_id=job_id, candidate_id=candidate_id)
            self.assertIsNotNone(prescreen)
            self.assertEqual(prescreen["status"], "incomplete")

            first_reply = workflow.process_inbound_message(conversation_id=conversation_id, text="I'm targeting 145k USD.")
            self.assertEqual(first_reply["mode"], "pre_resume")
            self.assertIn("What hands-on experience", first_reply["reply"])

            second_reply = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Here is my resume https://example.com/candidate-one-resume.pdf",
            )
            self.assertEqual(second_reply["mode"], "pre_resume")
            self.assertEqual(second_reply["state"]["prescreen_status"], "cv_received_pending_answers")

            third_reply = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="I have 6 years of Python and AWS experience, I am based in Berlin, and I have full work authorization.",
            )
            self.assertEqual(third_reply["mode"], "pre_resume")
            self.assertEqual(third_reply["state"]["prescreen_status"], "ready_for_screening_call")

            match_rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(match_rows), 1)
            self.assertEqual(match_rows[0]["status"], "resume_received")
            self.assertEqual(match_rows[0]["candidate_prescreen_status"], "ready_for_screening_call")
            self.assertEqual(match_rows[0]["candidate_prescreen_salary_expectation_min"], 145000.0)

            events = db.list_pre_resume_events(limit=20, session_id=session_id)
            event_types = {x["event_type"] for x in events}
            self.assertIn("session_started", event_types)
            self.assertIn("inbound_processed", event_types)

    def test_followup_binds_external_chat_id_from_delivery(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "workflow_pre_resume_followup_binding.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _FollowupChatProvider()
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
                "linkedin_id": "ln-followup-bind-1",
                "full_name": "Candidate Followup",
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
            conversation_id = int(outreach["items"][0]["conversation_id"])
            session_id = str(outreach["items"][0]["pre_resume_session_id"])

            session = db.get_pre_resume_session(session_id)
            state = dict(session.get("state_json") or {})
            state["status"] = "awaiting_reply"
            state["next_followup_at"] = "2001-01-01T00:00:00+00:00"
            db.upsert_pre_resume_session(
                session_id=session_id,
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state=state,
                instruction="",
            )
            pre_resume.seed_session(state)

            result = workflow.run_due_pre_resume_followups(job_id=job_id, limit=5)
            self.assertEqual(result["sent"], 1)

            conversation = db.get_conversation(conversation_id)
            self.assertEqual(conversation["external_chat_id"], "chat-followup-1")
            messages = db.list_messages(conversation_id)
            self.assertTrue(messages)
            last_meta = messages[-1].get("meta") if isinstance(messages[-1].get("meta"), dict) else {}
            self.assertEqual(last_meta.get("external_chat_id"), "chat-followup-1")

    def test_pre_resume_followup_keeps_latest_candidate_language(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "workflow_pre_resume_language.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _FollowupChatProvider()
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
            )

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python, AWS and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            profile = {
                "linkedin_id": "ln-followup-language-1",
                "full_name": "Candidate Language Switch",
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
            conversation_id = int(outreach["items"][0]["conversation_id"])
            session_id = str(outreach["items"][0]["pre_resume_session_id"])

            reply = workflow.process_inbound_message(conversation_id=conversation_id, text="Aqui esta mi CV")
            self.assertEqual(reply["language"], "es")

            session = db.get_pre_resume_session(session_id)
            state = dict(session.get("state_json") or {})
            state["status"] = "awaiting_reply"
            state["next_followup_at"] = "2001-01-01T00:00:00+00:00"
            db.upsert_pre_resume_session(
                session_id=session_id,
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state=state,
                instruction="",
            )
            pre_resume.seed_session(state)

            result = workflow.run_due_pre_resume_followups(job_id=job_id, limit=5)
            self.assertEqual(result["sent"], 1)
            messages = db.list_messages(conversation_id)
            last_outbound = messages[-1]
            self.assertEqual(last_outbound["candidate_language"], "es")
            self.assertIn("Seguimiento", last_outbound["content"])

    def test_followup_delivery_failure_does_not_promote_communication_to_dialogue(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "workflow_pre_resume_followup_failure.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _FailingSecondFollowupProvider()
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
                "linkedin_id": "ln-followup-delivery-fail-1",
                "full_name": "Candidate Followup Fail",
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
            conversation_id = int(outreach["items"][0]["conversation_id"])
            session_id = str(outreach["items"][0]["pre_resume_session_id"])

            session = db.get_pre_resume_session(session_id)
            state = dict(session.get("state_json") or {})
            state["status"] = "awaiting_reply"
            state["next_followup_at"] = "2001-01-01T00:00:00+00:00"
            db.upsert_pre_resume_session(
                session_id=session_id,
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state=state,
                instruction="",
            )
            pre_resume.seed_session(state)
            provider.calls = 1

            result = workflow.run_due_pre_resume_followups(job_id=job_id, limit=5)
            self.assertEqual(result["errors"], 1)
            self.assertEqual(result["sent"], 0)

            rows = db.list_candidates_for_job(job_id)
            self.assertEqual(len(rows), 1)
            communication = ((rows[0].get("agent_scorecard") or {}).get("communication") or {})
            self.assertEqual(str(communication.get("latest_stage") or ""), "outreach")


if __name__ == "__main__":
    unittest.main()
