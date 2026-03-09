from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _NoopProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False, "provider": "stub", "error": "not_used"}


class _ManagedStubProvider:
    def __init__(self, *, account_ref: str, require_connection: bool = False) -> None:
        self.account_ref = account_ref
        self.require_connection = require_connection
        self.sent_messages: List[str] = []
        self.connect_messages: List[str] = []

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent_messages.append(message)
        if self.require_connection:
            return {
                "sent": False,
                "provider": "unipile",
                "error": "Unipile HTTP error 422: errors/no_connection_with_recipient recipient is not first degree connection",
            }
        return {"sent": True, "provider": "unipile", "chat_id": f"chat-{self.account_ref}"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        self.connect_messages.append(message or "")
        return {"sent": True, "provider": "unipile", "request_id": f"req-{self.account_ref}"}


class _ExplodingManagedStubProvider:
    def __init__(self, *, error_text: str) -> None:
        self.error_text = error_text

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        raise RuntimeError(self.error_text)

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        raise RuntimeError(self.error_text)


class _LimitExceededManagedStubProvider:
    def __init__(self, *, account_ref: str) -> None:
        self.account_ref = account_ref
        self.sent_messages: List[str] = []

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        self.sent_messages.append(message)
        return {
            "sent": False,
            "provider": "unipile",
            "error": 'Unipile HTTP error 422: {"status":422,"type":"errors/limit_exceeded","title":"Limit exceeded"}',
        }

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {
            "sent": False,
            "provider": "unipile",
            "error": 'Unipile HTTP error 422: {"status":422,"type":"errors/limit_exceeded","title":"Limit exceeded"}',
        }


class _CannotResendManagedStubProvider:
    def __init__(self, *, account_ref: str) -> None:
        self.account_ref = account_ref
        self.connect_messages: List[str] = []

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False, "provider": "unipile", "reason": "connect_first"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        self.connect_messages.append(message or "")
        return {
            "sent": False,
            "provider": "unipile",
            "reason": "connection_request_failed",
            "error": 'Unipile HTTP error 422: {"status":422,"type":"errors/cannot_resend_yet","title":"Cannot resend yet"}',
        }


class _InvalidIdentityManagedStubProvider:
    def __init__(self, *, account_ref: str) -> None:
        self.account_ref = account_ref
        self.connect_messages: List[str] = []

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False, "provider": "unipile", "reason": "connect_first"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        self.connect_messages.append(message or "")
        return {
            "sent": False,
            "provider": "unipile",
            "reason": "invalid_candidate_identity",
            "error": 'Unipile HTTP error 400: {"status":400,"type":"errors/invalid_parameters","title":"User ID does not match provider\'s expected format"}',
        }


class OutboundDispatchTests(unittest.TestCase):
    def _create_workflow(
        self,
        *,
        db: Database,
        managed_linkedin_dispatch_inline: bool,
        linkedin_outreach_policy: Dict[str, Any] | None = None,
    ) -> WorkflowService:
        root = Path(__file__).resolve().parents[1]
        matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
        return WorkflowService(
            db=db,
            sourcing_agent=SourcingAgent(_NoopProvider()),
            verification_agent=VerificationAgent(matching),
            outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
            faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
            contact_all_mode=False,
            require_resume_before_final_verify=False,
            managed_linkedin_enabled=True,
            managed_linkedin_dispatch_inline=managed_linkedin_dispatch_inline,
            managed_unipile_api_key="managed-key",
            linkedin_outreach_policy=linkedin_outreach_policy or {},
        )

    def _seed_job_and_candidate(self, *, db: Database, workflow: WorkflowService, suffix: str) -> tuple[int, int]:
        job_id = db.insert_job(
            title="Senior Backend Engineer",
            jd_text="Need Python and distributed systems.",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        profile = {
            "linkedin_id": f"ln-outbound-{suffix}",
            "full_name": f"Outbound Candidate {suffix}",
            "headline": "Backend Engineer",
            "location": "Remote",
            "languages": ["en"],
            "skills": ["python"],
            "years_experience": 6,
            "raw": {},
        }
        added = workflow.add_verified_candidates(
            job_id=job_id,
            verified_items=[{"profile": profile, "score": 0.82, "status": "verified", "notes": {}}],
        )
        candidate_id = int(added["added"][0]["candidate_id"])
        return job_id, candidate_id

    def test_queue_assignment_prefers_lower_workload_account(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="a")
            loaded_candidate = db.upsert_candidate(
                {
                    "linkedin_id": "ln-loaded-a",
                    "full_name": "Loaded Candidate A",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )
            loaded_conversation = db.create_conversation(job_id=job_id, candidate_id=loaded_candidate, channel="linkedin")
            db.set_conversation_linkedin_account(conversation_id=loaded_conversation, account_id=account_1)
            db.update_conversation_status(conversation_id=loaded_conversation, status="active")
            waiting_candidate = db.upsert_candidate(
                {
                    "linkedin_id": "ln-loaded-b",
                    "full_name": "Loaded Candidate B",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )
            waiting_conversation = db.create_conversation(job_id=job_id, candidate_id=waiting_candidate, channel="linkedin")
            db.set_conversation_linkedin_account(conversation_id=waiting_conversation, account_id=account_1)
            db.update_conversation_status(conversation_id=waiting_conversation, status="waiting_connection")

            providers = {
                "acc-1": _ManagedStubProvider(account_ref="acc-1"),
                "acc-2": _ManagedStubProvider(account_ref="acc-2"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            self.assertEqual(int(queued["items"][0].get("linkedin_account_id") or 0), account_2)
            queued_action = db.get_outbound_action(action_id)
            self.assertEqual(int(queued_action["account_id"]), account_2)

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["sent"], 0)
            self.assertEqual(dispatched["pending_connection"], 1)
            self.assertEqual(dispatched["deferred"], 0)

            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "completed")
            self.assertEqual(int(action["account_id"]), account_2)
            self.assertEqual(str((action.get("result_json") or {}).get("planned_action_kind") or ""), "connect_request")

            conversation_id = int(queued["conversation_ids"][0])
            conversation = db.get_conversation(conversation_id)
            self.assertEqual(int(conversation["linkedin_account_id"]), account_2)
            self.assertEqual(conversation["status"], "waiting_connection")

    def test_dispatch_uses_preassigned_account_owner(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_owner.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-owner-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-owner-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-owner-1": _ManagedStubProvider(account_ref="acc-owner-1"),
                "acc-owner-2": _ManagedStubProvider(account_ref="acc-owner-2"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="owner")
            loaded_candidate = db.upsert_candidate(
                {
                    "linkedin_id": "ln-owner-load",
                    "full_name": "Owner Load Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )
            loaded_conversation = db.create_conversation(job_id=job_id, candidate_id=loaded_candidate, channel="linkedin")
            db.set_conversation_linkedin_account(conversation_id=loaded_conversation, account_id=account_1)
            db.update_conversation_status(conversation_id=loaded_conversation, status="active")

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            queued_action = db.get_outbound_action(action_id)
            self.assertEqual(int(queued_action["account_id"]), account_2)

            db.update_conversation_status(conversation_id=loaded_conversation, status="closed")

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["pending_connection"], 1)
            dispatched_action = db.get_outbound_action(action_id)
            self.assertEqual(int(dispatched_action["account_id"]), account_2)
            self.assertEqual(len(providers["acc-owner-1"].connect_messages), 0)
            self.assertEqual(len(providers["acc-owner-2"].connect_messages), 1)

    def test_connect_request_cannot_resend_yet_releases_action_with_cooldown(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_cooldown.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-cooldown",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            provider = _CannotResendManagedStubProvider(account_ref="acc-cooldown")
            workflow._build_managed_provider = lambda account_id: provider  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="cooldown")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            self.assertEqual(int(queued["items"][0].get("linkedin_account_id") or 0), account_id)

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["deferred"], 1)
            self.assertEqual(dispatched["failed"], 0)
            self.assertEqual(str((dispatched["items"][0] or {}).get("delivery_status") or ""), "deferred")

            action = db.get_outbound_action(action_id)
            self.assertEqual(str((action or {}).get("status") or ""), "pending")
            self.assertIn("cannot_resend_yet", str((action or {}).get("last_error") or ""))
            not_before = datetime.fromisoformat(str((action or {}).get("not_before") or "").replace("Z", "+00:00"))
            self.assertGreater(not_before, datetime.now(timezone.utc))

    def test_invalid_candidate_identity_stalls_recovery_for_conversation(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_invalid_identity.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-invalid",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            provider = _InvalidIdentityManagedStubProvider(account_ref="acc-invalid")
            workflow._build_managed_provider = lambda account_id: provider  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="invalid")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            self.assertEqual(int(queued["items"][0].get("linkedin_account_id") or 0), account_id)
            conversation_id = int(queued["conversation_ids"][0])
            db.upsert_pre_resume_session(
                session_id=f"pre-invalid-{conversation_id}",
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state={"status": "awaiting_reply", "language": "en"},
                instruction="",
            )

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["failed"], 1)
            self.assertEqual(dispatched["deferred"], 0)

            action = db.get_outbound_action(action_id)
            self.assertEqual(str((action or {}).get("status") or ""), "failed")

            session = db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)
            self.assertIsNotNone(session)
            self.assertEqual(str((session or {}).get("status") or ""), "stalled")
            self.assertIn("invalid_candidate_identity", str((session or {}).get("last_error") or ""))

            recovery_rows = db.list_unassigned_outreach_conversations(limit=10, job_id=job_id)
            self.assertEqual(recovery_rows, [])

    def test_recovery_backfill_assigns_owner_from_lower_workload_account(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_recovery.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-recovery-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-recovery-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-recovery-1": _ManagedStubProvider(account_ref="acc-recovery-1"),
                "acc-recovery-2": _ManagedStubProvider(account_ref="acc-recovery-2"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id = db.insert_job(
                title="Senior Backend Engineer",
                jd_text="Need Python and distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            load_candidate = db.upsert_candidate(
                {
                    "linkedin_id": "ln-recovery-load",
                    "full_name": "Recovery Load Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )
            load_conversation = db.create_conversation(job_id=job_id, candidate_id=load_candidate, channel="linkedin")
            db.set_conversation_linkedin_account(conversation_id=load_conversation, account_id=account_1)
            db.update_conversation_status(conversation_id=load_conversation, status="waiting_connection")

            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-recovery-target",
                    "full_name": "Recovery Target Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )
            conversation_id = db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            db.update_conversation_status(conversation_id=conversation_id, status="active")
            db.upsert_pre_resume_session(
                session_id="pre-recovery-target",
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state={"status": "awaiting_reply", "language": "en"},
                instruction="",
            )
            db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content="Following up on your application",
                candidate_language="en",
                meta={"delivery_status": "queued"},
            )

            result = workflow.backfill_outreach_for_unassigned_conversations(job_id=job_id, limit=10)
            self.assertEqual(int(result["queued"]), 1)
            self.assertEqual(int(result["dispatched"]["pending_connection"]), 1)
            self.assertEqual(int(result["items"][0].get("linkedin_account_id") or 0), account_2)
            action_id = int(result["items"][0]["action_id"])
            action = db.get_outbound_action(action_id)
            self.assertEqual(int(action["account_id"]), account_2)
            self.assertEqual(str((action.get("result_json") or {}).get("planned_action_kind") or ""), "connect_request")

    def test_dispatch_records_account_funnel_events_across_connect_and_reply(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_funnel.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-funnel-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-funnel-1": _ManagedStubProvider(account_ref="acc-funnel-1"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="funnel")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            conversation_id = int(queued["conversation_ids"][0])

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["pending_connection"], 1)

            processed = workflow.process_connection_event(sender_provider_id="ln-outbound-funnel")
            self.assertTrue(processed["processed"])
            workflow.process_inbound_message(conversation_id=conversation_id, text="Tell me more")

            funnel = db.summarize_outreach_account_funnel(account_ids=[account_id], recent_limit=5).get(account_id) or {}
            self.assertEqual(int(funnel.get("connects_planned") or 0), 1)
            self.assertEqual(int(funnel.get("connects_sent") or 0), 1)
            self.assertEqual(int(funnel.get("connects_accepted") or 0), 1)
            self.assertEqual(int(funnel.get("messages_sent") or 0), 1)
            self.assertEqual(int(funnel.get("replies_received") or 0), 1)
            recent = funnel.get("recent_candidates") or []
            self.assertEqual(len(recent), 1)
            self.assertEqual(str(recent[0].get("stage_label") or ""), "Replied")

    def test_dispatch_excludes_provider_limited_account_for_remaining_batch(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_provider_limit.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-limit-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-limit-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_3 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-limit-3",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-limit-1": _LimitExceededManagedStubProvider(account_ref="acc-limit-1"),
                "acc-limit-2": _ManagedStubProvider(account_ref="acc-limit-2"),
                "acc-limit-3": _ManagedStubProvider(account_ref="acc-limit-3"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id, candidate_1 = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="limit-1")
            added = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[
                    {
                        "profile": {
                            "linkedin_id": "ln-outbound-limit-2",
                            "full_name": "Outbound Candidate limit-2",
                            "headline": "Backend Engineer",
                            "location": "Remote",
                            "languages": ["en"],
                            "skills": ["python"],
                            "years_experience": 6,
                            "raw": {},
                        },
                        "score": 0.81,
                        "status": "verified",
                        "notes": {},
                    },
                    {
                        "profile": {
                            "linkedin_id": "ln-outbound-limit-3",
                            "full_name": "Outbound Candidate limit-3",
                            "headline": "Backend Engineer",
                            "location": "Remote",
                            "languages": ["en"],
                            "skills": ["python"],
                            "years_experience": 6,
                            "raw": {},
                        },
                        "score": 0.8,
                        "status": "verified",
                        "notes": {},
                    },
                ],
            )
            candidate_2 = int(added["added"][0]["candidate_id"])
            candidate_3 = int(added["added"][1]["candidate_id"])

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_1, candidate_2, candidate_3])
            for item in queued["items"]:
                action_id = int(item.get("action_id") or 0)
                action = db.get_outbound_action(action_id)
                payload = dict((action or {}).get("payload_json") or {})
                payload["delivery_mode"] = "message_first"
                payload["planned_action_kind"] = "message"
                with db.transaction() as conn:
                    conn.execute(
                        "UPDATE outbound_actions SET payload_json = ? WHERE id = ?",
                        (json.dumps(payload), action_id),
                    )
            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)

            self.assertEqual(dispatched["processed"], 3)
            self.assertEqual(dispatched["failed"], 1)
            self.assertEqual(dispatched["sent"], 2)
            items_by_candidate = {
                int(item.get("candidate_id") or 0): item
                for item in (dispatched.get("items") or [])
            }
            self.assertEqual(int(items_by_candidate[candidate_1].get("linkedin_account_id") or 0), account_1)
            self.assertEqual(str(items_by_candidate[candidate_1].get("delivery_status") or ""), "failed")
            self.assertEqual(int(items_by_candidate[candidate_2].get("linkedin_account_id") or 0), account_2)
            self.assertEqual(int(items_by_candidate[candidate_3].get("linkedin_account_id") or 0), account_3)

            self.assertEqual(len(providers["acc-limit-1"].sent_messages), 1)
            self.assertEqual(len(providers["acc-limit-2"].sent_messages), 1)
            self.assertEqual(len(providers["acc-limit-3"].sent_messages), 1)

    def test_dispatch_defers_when_no_connected_accounts(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_defer.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="b")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["sent"], 0)
            self.assertEqual(dispatched["deferred"], 1)
            self.assertEqual(dispatched["failed"], 0)

            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "pending")
            self.assertEqual(action["last_error"], "no_connected_accounts")

    def test_manual_mode_blocks_when_no_assigned_accounts(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_manual_blocked.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-global",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="m0")
            updated = db.update_job_linkedin_routing_mode(job_id=job_id, routing_mode="manual")
            self.assertTrue(updated)

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            dispatched = workflow.dispatch_outbound_actions(limit=5, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["deferred"], 1)
            self.assertEqual(dispatched["sent"], 0)
            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "pending")
            self.assertEqual(action["last_error"], "manual_no_assigned_accounts")

    def test_manual_mode_uses_only_assigned_accounts(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_manual_assigned.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-a",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-b",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-a": _ManagedStubProvider(account_ref="acc-a"),
                "acc-b": _ManagedStubProvider(account_ref="acc-b"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="m1")
            self.assertTrue(db.update_job_linkedin_routing_mode(job_id=job_id, routing_mode="manual"))
            assigned_ids = db.replace_job_linkedin_account_assignments(job_id=job_id, account_ids=[account_1])
            self.assertEqual(assigned_ids, [account_1])

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            dispatched = workflow.dispatch_outbound_actions(limit=5, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["sent"], 0)
            self.assertEqual(dispatched["pending_connection"], 1)
            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "completed")
            self.assertEqual(int(action["account_id"]), account_1)
            self.assertEqual(len(providers["acc-a"].sent_messages), 0)
            self.assertEqual(len(providers["acc-a"].connect_messages), 1)
            self.assertEqual(len(providers["acc-b"].sent_messages), 0)

    def test_dispatch_respects_connect_weekly_cap(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_connect_cap.sqlite3"))
            db.init_schema()
            policy = {
                "connect_invites": {"weekly_cap_per_account": 1},
                "outbound_messages": {"daily_new_threads_per_account": {"max": 30}},
                "warmup": {
                    "invite_ramp": [
                        {"days": "3-7", "invites_per_day": {"min": 2, "max": 3}},
                        {"days": "8-21", "daily_increment": {"min": 1, "max": 2}},
                    ]
                },
            }
            workflow = self._create_workflow(
                db=db,
                managed_linkedin_dispatch_inline=False,
                linkedin_outreach_policy=policy,
            )
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-cap",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            provider = _ManagedStubProvider(account_ref="acc-cap", require_connection=True)
            workflow._build_managed_provider = lambda account_id: provider  # type: ignore[method-assign]

            job_id, candidate_1 = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="c1")
            candidate_2_profile = {
                "linkedin_id": "ln-outbound-c2",
                "full_name": "Outbound Candidate c2",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 6,
                "raw": {},
            }
            added_2 = workflow.add_verified_candidates(
                job_id=job_id,
                verified_items=[{"profile": candidate_2_profile, "score": 0.81, "status": "verified", "notes": {}}],
            )
            candidate_2 = int(added_2["added"][0]["candidate_id"])

            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_1, candidate_2])
            action_ids = [int(item["action_id"]) for item in queued["items"]]

            dispatched = workflow.dispatch_outbound_actions(limit=20, job_id=job_id)
            self.assertEqual(dispatched["processed"], 2)
            self.assertEqual(dispatched["pending_connection"], 1)
            self.assertEqual(dispatched["deferred"], 1)
            self.assertEqual(dispatched["sent"], 0)

            first = db.get_outbound_action(action_ids[0])
            second = db.get_outbound_action(action_ids[1])
            statuses = {first["status"], second["status"]}
            self.assertEqual(statuses, {"completed", "pending"})
            deferred_action = first if first["status"] == "pending" else second
            self.assertEqual(deferred_action["last_error"], "connect_budget_reached")

            day_counter = db.get_linkedin_account_daily_counter(account_id=account_id, day_utc=workflow._utc_day_key())
            week_counter = db.get_linkedin_account_weekly_counter(
                account_id=account_id,
                week_start_utc=workflow._utc_week_start_key(),
            )
            self.assertEqual(int(day_counter["connect_sent"]), 1)
            self.assertEqual(int(week_counter["connect_sent"]), 1)

    def test_dispatch_handles_single_action_exception_without_crashing_batch(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_dispatch_exception.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)
            db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-explode",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="x1")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])
            workflow._dispatch_single_outbound_action = (  # type: ignore[method-assign]
                lambda row, selection_state=None: (_ for _ in ()).throw(RuntimeError("bad parameter or other API misuse"))
            )

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["failed"], 1)
            self.assertEqual(dispatched["sent"], 0)
            self.assertEqual(dispatched["pending_connection"], 0)

            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "failed")
            self.assertIn("bad parameter", str(action.get("last_error") or ""))

    def test_rebalance_round_robins_connect_capacity_across_auto_jobs(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_rebalance_round_robin.sqlite3"))
            db.init_schema()
            policy = {
                "connect_invites": {"weekly_cap_per_account": 1},
                "outbound_messages": {"daily_new_threads_per_account": {"max": 30}},
            }
            workflow = self._create_workflow(
                db=db,
                managed_linkedin_dispatch_inline=False,
                linkedin_outreach_policy=policy,
            )
            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-rr-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-rr-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            providers = {
                "acc-rr-1": _ManagedStubProvider(account_ref="acc-rr-1", require_connection=True),
                "acc-rr-2": _ManagedStubProvider(account_ref="acc-rr-2", require_connection=True),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_a = db.insert_job(
                title="Job A",
                jd_text="Need Python.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                linkedin_routing_mode="auto",
            )
            job_b = db.insert_job(
                title="Job B",
                jd_text="Need Python.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                linkedin_routing_mode="auto",
            )

            def _add(job_id: int, suffix: str, score: float) -> int:
                added = workflow.add_verified_candidates(
                    job_id=job_id,
                    verified_items=[
                        {
                            "profile": {
                                "linkedin_id": f"ln-rr-{suffix}",
                                "full_name": f"RR Candidate {suffix}",
                                "headline": "Backend Engineer",
                                "location": "Remote",
                                "languages": ["en"],
                                "skills": ["python"],
                                "years_experience": 6,
                                "raw": {},
                            },
                            "score": score,
                            "status": "verified",
                            "notes": {},
                        }
                    ],
                )
                return int(added["added"][0]["candidate_id"])

            job_a_first = _add(job_a, "a1", 0.95)
            job_a_second = _add(job_a, "a2", 0.90)
            job_b_first = _add(job_b, "b1", 0.94)
            job_b_second = _add(job_b, "b2", 0.89)

            out = workflow.rebalance_outreach_capacity(
                job_limit=2,
                candidates_per_job=10,
                recovery_per_job=10,
                jobs_scan_limit=10,
            )

            self.assertEqual(out["status"], "ok")
            self.assertEqual(int(out["planner"]["connect_capacity_total"] or 0), 2)
            self.assertEqual(int(out["planner"]["connect_planned"] or 0), 2)
            self.assertEqual(int(out["totals"]["new_threads_queued"] or 0), 2)
            self.assertEqual(int(out["totals"]["pending_connection"] or 0), 2)

            job_a_first_match = db.get_candidate_match(job_a, job_a_first)
            job_a_second_match = db.get_candidate_match(job_a, job_a_second)
            job_b_first_match = db.get_candidate_match(job_b, job_b_first)
            job_b_second_match = db.get_candidate_match(job_b, job_b_second)
            self.assertEqual(str((job_a_first_match or {}).get("status") or ""), "outreach_pending_connection")
            self.assertEqual(str((job_b_first_match or {}).get("status") or ""), "outreach_pending_connection")
            self.assertEqual(str((job_a_second_match or {}).get("status") or ""), "verified")
            self.assertEqual(str((job_b_second_match or {}).get("status") or ""), "verified")
            self.assertEqual(len(providers["acc-rr-1"].connect_messages), 1)
            self.assertEqual(len(providers["acc-rr-2"].connect_messages), 1)

    def test_outreach_inline_dispatch_error_does_not_fail_entire_step(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_inline_dispatch_error.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=True)

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="x2")
            workflow.dispatch_outbound_actions = (  # type: ignore[method-assign]
                lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bad parameter or other API misuse"))
            )

            out = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            self.assertEqual(out["job_id"], job_id)
            self.assertEqual(out["total"], 1)
            self.assertEqual(out["sent"], 0)
            self.assertEqual(out["pending_connection"], 0)
            self.assertEqual(out["failed"], 0)
            self.assertIn("bad parameter", str(out.get("dispatch_error") or ""))
            self.assertEqual(str(out["items"][0].get("delivery_status") or ""), "queued")

    def test_rebalance_targets_latest_auto_job_and_highest_score_candidate(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outbound_rebalance.sqlite3"))
            db.init_schema()
            workflow = self._create_workflow(db=db, managed_linkedin_dispatch_inline=False)

            db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-rebalance",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            provider = _ManagedStubProvider(account_ref="acc-rebalance")
            workflow._build_managed_provider = lambda account_id: provider  # type: ignore[method-assign]

            old_job_id = db.insert_job(
                title="Older Auto Job",
                jd_text="Need Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                linkedin_routing_mode="auto",
            )
            manual_job_id = db.insert_job(
                title="Manual Job",
                jd_text="Need Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                linkedin_routing_mode="manual",
            )
            new_job_id = db.insert_job(
                title="Newest Auto Job",
                jd_text="Need Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                linkedin_routing_mode="auto",
            )

            def _add_candidate(job_id: int, suffix: str, score: float) -> int:
                added = workflow.add_verified_candidates(
                    job_id=job_id,
                    verified_items=[
                        {
                            "profile": {
                                "linkedin_id": f"ln-rebalance-{suffix}",
                                "full_name": f"Rebalance Candidate {suffix}",
                                "headline": "Backend Engineer",
                                "location": "Remote",
                                "languages": ["en"],
                                "skills": ["python"],
                                "years_experience": 6,
                                "raw": {},
                            },
                            "score": score,
                            "status": "verified",
                            "notes": {},
                        }
                    ],
                )
                return int(added["added"][0]["candidate_id"])

            old_candidate = _add_candidate(old_job_id, "old", 0.82)
            manual_candidate = _add_candidate(manual_job_id, "manual", 0.95)
            high_candidate = _add_candidate(new_job_id, "high", 0.91)
            low_candidate = _add_candidate(new_job_id, "low", 0.63)

            out = workflow.rebalance_outreach_capacity(
                job_limit=1,
                candidates_per_job=1,
                recovery_per_job=1,
                jobs_scan_limit=10,
            )

            self.assertEqual(out["status"], "ok")
            self.assertEqual(out["jobs_selected"], 1)
            self.assertEqual(int(out["jobs"][0]["job_id"]), new_job_id)
            self.assertEqual(int(out["totals"]["new_threads_queued"]), 2)
            self.assertEqual(int(out["totals"]["sent"]) or 0, 0)
            self.assertEqual(int(out["totals"]["pending_connection"]) or 0, 2)

            high_match = db.get_candidate_match(new_job_id, high_candidate)
            low_match = db.get_candidate_match(new_job_id, low_candidate)
            old_match = db.get_candidate_match(old_job_id, old_candidate)
            manual_match = db.get_candidate_match(manual_job_id, manual_candidate)
            self.assertEqual(str((high_match or {}).get("status") or ""), "outreach_pending_connection")
            self.assertEqual(str((low_match or {}).get("status") or ""), "outreach_pending_connection")
            self.assertEqual(str((old_match or {}).get("status") or ""), "verified")
            self.assertEqual(str((manual_match or {}).get("status") or ""), "verified")
            self.assertEqual(len(provider.sent_messages), 0)
            self.assertEqual(len(provider.connect_messages), 2)


if __name__ == "__main__":
    unittest.main()
