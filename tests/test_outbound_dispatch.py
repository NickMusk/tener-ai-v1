from __future__ import annotations

import unittest
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

    def test_dispatch_prefers_least_used_connected_account(self) -> None:
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
            db.increment_linkedin_account_counters(
                account_id=account_1,
                day_utc=workflow._utc_day_key(),
                week_start_utc=workflow._utc_week_start_key(),
                new_threads_delta=7,
            )

            providers = {
                "acc-1": _ManagedStubProvider(account_ref="acc-1"),
                "acc-2": _ManagedStubProvider(account_ref="acc-2"),
            }
            workflow._build_managed_provider = lambda account_id: providers[str(account_id)]  # type: ignore[method-assign]

            job_id, candidate_id = self._seed_job_and_candidate(db=db, workflow=workflow, suffix="a")
            queued = workflow.outreach_candidates(job_id=job_id, candidate_ids=[candidate_id])
            action_id = int(queued["items"][0]["action_id"])

            dispatched = workflow.dispatch_outbound_actions(limit=10, job_id=job_id)
            self.assertEqual(dispatched["processed"], 1)
            self.assertEqual(dispatched["sent"], 1)
            self.assertEqual(dispatched["pending_connection"], 0)
            self.assertEqual(dispatched["deferred"], 0)

            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "completed")
            self.assertEqual(int(action["account_id"]), account_2)

            conversation_id = int(queued["conversation_ids"][0])
            conversation = db.get_conversation(conversation_id)
            self.assertEqual(int(conversation["linkedin_account_id"]), account_2)
            self.assertEqual(conversation["status"], "active")

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
            self.assertEqual(dispatched["sent"], 1)
            action = db.get_outbound_action(action_id)
            self.assertEqual(action["status"], "completed")
            self.assertEqual(int(action["account_id"]), account_1)
            self.assertEqual(len(providers["acc-a"].sent_messages), 1)
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


if __name__ == "__main__":
    unittest.main()
