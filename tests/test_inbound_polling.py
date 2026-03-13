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


class _PollingProvider:
    def __init__(self) -> None:
        self.messages_by_chat: Dict[str, List[Dict[str, Any]]] = {}

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "chat_id": "chat-poll-1"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": "stub", "connected": True}

    def fetch_chat_messages(self, chat_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        return list((self.messages_by_chat.get(chat_id) or [])[: max(1, limit)])


class InboundPollingTests(unittest.TestCase):
    def test_poll_provider_inbound_processes_message_once(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_polling.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
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
                "linkedin_id": "ln-poll-1",
                "unipile_profile_id": "ln-poll-1",
                "attendee_provider_id": "ln-poll-1",
                "full_name": "Polling Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-poll-1")

            provider.messages_by_chat["chat-poll-1"] = [
                {
                    "provider_message_id": "msg-1",
                    "sender_provider_id": "ln-poll-1",
                    "direction": "inbound",
                    "text": "Tell me more",
                    "created_at": "2026-02-24T15:54:00Z",
                }
            ]

            first = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(first["processed"], 1)
            self.assertEqual(first["duplicates"], 0)

            second = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(second["processed"], 0)
            self.assertGreaterEqual(second["duplicates"], 1)

            messages = db.list_messages(conversation_id=conversation_id)
            outbound_auto = [m for m in messages if m.get("direction") == "outbound" and (m.get("meta") or {}).get("type") == "pre_resume_auto_reply"]
            self.assertTrue(outbound_auto)

    def test_poll_provider_inbound_processes_attachment_only_message(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_polling_attachment.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
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
                "linkedin_id": "ln-poll-attachment-1",
                "unipile_profile_id": "ln-poll-attachment-1",
                "attendee_provider_id": "ln-poll-attachment-1",
                "full_name": "Attachment Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-poll-1")

            provider.messages_by_chat["chat-poll-1"] = [
                {
                    "provider_message_id": "msg-attachment-1",
                    "sender_provider_id": "ln-poll-attachment-1",
                    "direction": "inbound",
                    "text": "",
                    "created_at": "2026-02-24T15:56:00Z",
                    "raw": {
                        "attachments": [
                            {
                                "name": "cv_latest.pdf",
                                "url": "https://files.example.com/download/abc123",
                            }
                        ]
                    },
                }
            ]

            first = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(first["processed"], 1)

            row = db.list_candidates_for_job(job_id)[0]
            self.assertEqual(str(row.get("status")), "resume_received_pending_must_have")
            assets = db.list_resume_assets_for_candidate(candidate_id=candidate_id, job_id=job_id, limit=20)
            self.assertGreaterEqual(len(assets), 1)
            self.assertEqual(str(assets[0].get("processing_status")), "stored_unparsed")

    def test_poll_provider_inbound_for_paused_job_stores_message_without_auto_reply(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_polling_paused.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
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
                "linkedin_id": "ln-poll-paused-1",
                "unipile_profile_id": "ln-poll-paused-1",
                "attendee_provider_id": "ln-poll-paused-1",
                "full_name": "Paused Polling Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-poll-paused-1")
            db.pause_job(job_id=job_id, reason="ops")

            provider.messages_by_chat["chat-poll-paused-1"] = [
                {
                    "provider_message_id": "msg-paused-1",
                    "sender_provider_id": "ln-poll-paused-1",
                    "direction": "inbound",
                    "text": "Tell me more",
                    "created_at": "2026-02-24T15:58:00Z",
                }
            ]

            result = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(int(result.get("processed") or 0), 1)
            self.assertEqual(str((result.get("items") or [{}])[0].get("result_mode") or ""), "paused")

            messages = db.list_messages(conversation_id=conversation_id)
            inbound = [m for m in messages if m.get("direction") == "inbound"]
            auto_replies = [
                m for m in messages
                if m.get("direction") == "outbound" and (m.get("meta") or {}).get("type") == "pre_resume_auto_reply"
            ]
            self.assertEqual(len(inbound), 1)
            self.assertEqual(auto_replies, [])

    def test_poll_provider_inbound_processes_attachment_name_without_url(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_polling_attachment_no_url.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
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
                "linkedin_id": "ln-poll-attachment-2",
                "unipile_profile_id": "ln-poll-attachment-2",
                "attendee_provider_id": "ln-poll-attachment-2",
                "full_name": "Attachment Candidate 2",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-poll-1")

            provider.messages_by_chat["chat-poll-1"] = [
                {
                    "provider_message_id": "msg-attachment-2",
                    "sender_provider_id": "ln-poll-attachment-2",
                    "direction": "inbound",
                    "text": "",
                    "created_at": "2026-02-24T15:57:00Z",
                    "raw": {
                        "attachments": [
                            {
                                "name": "romeet_latest_cv.pdf",
                            }
                        ]
                    },
                }
            ]

            result = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(result["processed"], 1)

            row = db.list_candidates_for_job(job_id)[0]
            self.assertEqual(str(row.get("status")), "resume_received_pending_must_have")
            assets = db.list_resume_assets_for_candidate(candidate_id=candidate_id, job_id=job_id, limit=20)
            self.assertGreaterEqual(len(assets), 1)
            self.assertEqual(str(assets[0].get("processing_status")), "received_no_url")

    def test_poll_provider_inbound_downloads_unipile_attachment_reference(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_polling_unipile_attachment.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
                ),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and track status"},
                managed_unipile_api_key="managed-key",
                managed_unipile_base_url="https://api.unipile.com",
            )

            def _fake_download_provider_attachment_payload(**kwargs: Any) -> tuple[bytes, str | None]:
                self.assertEqual(str(kwargs.get("provider") or ""), "unipile_poll")
                self.assertEqual(str(kwargs.get("provider_message_id") or ""), "msg-attachment-3")
                self.assertEqual(str(kwargs.get("attachment_id") or ""), "att-3")
                return (b"legacy doc bytes", "application/msword")

            workflow._download_provider_attachment_payload = _fake_download_provider_attachment_payload  # type: ignore[method-assign]

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            profile = {
                "linkedin_id": "ln-poll-attachment-3",
                "unipile_profile_id": "ln-poll-attachment-3",
                "attendee_provider_id": "ln-poll-attachment-3",
                "full_name": "Attachment Candidate 3",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-poll-1")

            provider.messages_by_chat["chat-poll-1"] = [
                {
                    "provider_message_id": "msg-attachment-3",
                    "sender_provider_id": "ln-poll-attachment-3",
                    "direction": "inbound",
                    "text": "I'll send my resume now.",
                    "created_at": "2026-02-24T15:58:00Z",
                    "raw": {
                        "attachments": [
                            {
                                "id": "att-3",
                                "mimetype": "application/msword",
                                "url": "att://att-3",
                            }
                        ]
                    },
                }
            ]

            result = workflow.poll_provider_inbound_messages(job_id=job_id, limit=20, per_chat_limit=10)
            self.assertEqual(result["processed"], 1)

            row = db.list_candidates_for_job(job_id)[0]
            self.assertEqual(str(row.get("status")), "resume_received_pending_must_have")
            assets = db.list_resume_assets_for_candidate(candidate_id=candidate_id, job_id=job_id, limit=20)
            self.assertGreaterEqual(len(assets), 1)
            self.assertEqual(str(assets[0].get("remote_url") or ""), "att://att-3")
            self.assertEqual(str(assets[0].get("processing_status") or ""), "stored_unparsed")
            self.assertTrue(bool(str(assets[0].get("storage_path") or "").strip()))

    def test_backfill_resume_assets_for_existing_message_downloads_unipile_attachment_reference(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "inbound_resume_backfill.sqlite3"))
            db.init_schema()
            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _PollingProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),  # type: ignore[arg-type]
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                pre_resume_service=PreResumeCommunicationService(
                    templates_path=str(root / "config" / "outreach_templates.json")
                ),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
                stage_instructions={"pre_resume": "request cv and track status"},
                managed_unipile_api_key="managed-key",
                managed_unipile_base_url="https://api.unipile.com",
            )

            def _fake_download_provider_attachment_payload(**kwargs: Any) -> tuple[bytes, str | None]:
                self.assertEqual(str(kwargs.get("provider") or ""), "unipile_poll")
                self.assertEqual(str(kwargs.get("provider_message_id") or ""), "msg-backfill-1")
                self.assertEqual(str(kwargs.get("attachment_id") or ""), "att-backfill-1")
                return (b"resume bytes from backfill", "application/pdf")

            workflow._download_provider_attachment_payload = _fake_download_provider_attachment_payload  # type: ignore[method-assign]

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            profile = {
                "linkedin_id": "ln-backfill-1",
                "unipile_profile_id": "ln-backfill-1",
                "attendee_provider_id": "ln-backfill-1",
                "full_name": "Backfill Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": profile, "score": 0.6, "status": "needs_resume", "notes": {}}],
            )
            candidate_id = int(added["added"][0]["candidate_id"])
            outreach = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(outreach["total"], 1)
            conversation_id = int(outreach["items"][0]["conversation_id"])
            db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-backfill-1")

            db.add_message(
                conversation_id=conversation_id,
                direction="inbound",
                content="I'll send my resume now.",
                candidate_language="en",
                meta={
                    "type": "candidate_message",
                    "provider": "unipile_poll",
                    "provider_message_id": "msg-backfill-1",
                    "occurred_at": "2026-02-24T15:59:00Z",
                },
            )

            provider.messages_by_chat["chat-backfill-1"] = [
                {
                    "provider_message_id": "msg-backfill-1",
                    "sender_provider_id": "ln-backfill-1",
                    "direction": "inbound",
                    "text": "I'll send my resume now.",
                    "created_at": "2026-02-24T15:59:00Z",
                    "attachments": [
                        {
                            "id": "att-backfill-1",
                            "name": "Tatiana_Vladimirova_resume.pdf",
                            "mimetype": "application/pdf",
                            "url": "att://att-backfill-1",
                        }
                    ],
                    "raw": {
                        "attachments": [
                            {
                                "id": "att-backfill-1",
                                "name": "Tatiana_Vladimirova_resume.pdf",
                                "mimetype": "application/pdf",
                                "url": "att://att-backfill-1",
                            }
                        ]
                    },
                }
            ]

            result = workflow.backfill_resume_assets_for_conversation(conversation_id=conversation_id, per_chat_limit=10)
            self.assertEqual(int(result.get("conversation_id") or 0), conversation_id)
            self.assertEqual(int(result.get("processed") or 0), 1)
            self.assertEqual(int(result.get("scanned") or 0), 1)

            assets = db.list_resume_assets_for_candidate(candidate_id=candidate_id, job_id=job_id, limit=20)
            self.assertGreaterEqual(len(assets), 1)
            self.assertEqual(str(assets[0].get("remote_url") or ""), "att://att-backfill-1")
            self.assertEqual(str(assets[0].get("provider_message_id") or ""), "msg-backfill-1")
            self.assertEqual(str(assets[0].get("processing_status") or ""), "stored_unparsed")
            self.assertTrue(bool(str(assets[0].get("storage_path") or "").strip()))


if __name__ == "__main__":
    unittest.main()
