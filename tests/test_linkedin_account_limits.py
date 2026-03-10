from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.linkedin_limits import (
    effective_daily_connect_limit,
    effective_daily_message_limit,
    validate_account_limits_payload,
)
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _NoopProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False, "provider": "stub", "error": "not_used"}


def _build_workflow(db: Database) -> WorkflowService:
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
        managed_linkedin_dispatch_inline=False,
        managed_unipile_api_key="managed-key",
    )


class LinkedInAccountLimitsTests(unittest.TestCase):
    def test_validate_account_limits_payload_accepts_int_and_null(self) -> None:
        out = validate_account_limits_payload({"daily_message_limit": 12, "daily_connect_limit": None})
        self.assertTrue(out["has_daily_message_limit"])
        self.assertTrue(out["has_daily_connect_limit"])
        self.assertEqual(out["daily_message_limit"], 12)
        self.assertIsNone(out["daily_connect_limit"])

    def test_validate_account_limits_payload_rejects_out_of_range(self) -> None:
        with self.assertRaises(ValueError):
            validate_account_limits_payload({"daily_message_limit": 0})
        with self.assertRaises(ValueError):
            validate_account_limits_payload({"daily_connect_limit": 1000})

    def test_db_update_linkedin_account_limits_supports_partial_updates(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_limits_db.sqlite3"))
            db.init_schema()
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-limits-1",
                status="connected",
            )

            updated = db.update_linkedin_account_limits(
                account_id=account_id,
                has_daily_message_limit=True,
                daily_message_limit=18,
                has_daily_connect_limit=True,
                daily_connect_limit=7,
            )
            self.assertIsNotNone(updated)
            self.assertEqual(int(updated["daily_message_limit"]), 18)
            self.assertEqual(int(updated["daily_connect_limit"]), 7)

            updated = db.update_linkedin_account_limits(
                account_id=account_id,
                has_daily_message_limit=False,
                daily_message_limit=None,
                has_daily_connect_limit=True,
                daily_connect_limit=None,
            )
            self.assertIsNotNone(updated)
            self.assertEqual(int(updated["daily_message_limit"]), 18)
            self.assertIsNone(updated["daily_connect_limit"])

    def test_workflow_select_skips_account_when_custom_message_limit_reached(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_limits_select.sqlite3"))
            db.init_schema()
            workflow = _build_workflow(db)
            job_id = db.insert_job(
                title="Manual QA Engineer",
                jd_text="Need strong test design.",
                location="Remote",
                preferred_languages=["en"],
                seniority="middle",
            )
            account_1 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-msg-limit-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            account_2 = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-msg-limit-2",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            db.update_linkedin_account_limits(
                account_id=account_1,
                has_daily_message_limit=True,
                daily_message_limit=1,
                has_daily_connect_limit=False,
                daily_connect_limit=None,
            )
            db.increment_linkedin_account_counters(
                account_id=account_1,
                day_utc=workflow._utc_day_key(),
                week_start_utc=workflow._utc_week_start_key(),
                new_threads_delta=1,
            )
            db.increment_linkedin_account_counters(
                account_id=account_2,
                day_utc=workflow._utc_day_key(),
                week_start_utc=workflow._utc_week_start_key(),
                new_threads_delta=4,
            )

            selected, err = workflow._select_linkedin_account_for_new_thread(
                job_id=job_id,
                planned_action_kind="message",
            )
            self.assertIsNone(err)
            self.assertIsNotNone(selected)
            self.assertEqual(int(selected["id"]), account_2)

    def test_workflow_custom_connect_limit_blocks_after_daily_cap(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_limits_connect.sqlite3"))
            db.init_schema()
            workflow = _build_workflow(db)
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-connect-limit-1",
                status="connected",
                connected_at="2025-01-01T00:00:00+00:00",
            )
            db.update_linkedin_account_limits(
                account_id=account_id,
                has_daily_message_limit=False,
                daily_message_limit=None,
                has_daily_connect_limit=True,
                daily_connect_limit=1,
            )
            account = db.get_linkedin_account(account_id)
            self.assertEqual(effective_daily_message_limit(account, workflow.linkedin_outreach_policy), 15)
            self.assertEqual(effective_daily_connect_limit(account, workflow.linkedin_outreach_policy), 1)
            self.assertTrue(workflow._can_send_connect_request(account))

            db.increment_linkedin_account_counters(
                account_id=account_id,
                day_utc=workflow._utc_day_key(),
                week_start_utc=workflow._utc_week_start_key(),
                connect_delta=1,
            )
            account = db.get_linkedin_account(account_id)
            self.assertFalse(workflow._can_send_connect_request(account))

    def test_freshly_connected_account_gets_full_policy_connect_limit_without_warmup(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_limits_no_warmup.sqlite3"))
            db.init_schema()
            workflow = _build_workflow(db)
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-connect-limit-fresh",
                status="connected",
                connected_at="2026-03-09T14:19:36+00:00",
            )
            account = db.get_linkedin_account(account_id)
            self.assertEqual(effective_daily_connect_limit(account, workflow.linkedin_outreach_policy), 14)
            self.assertTrue(workflow._can_send_connect_request(account))


if __name__ == "__main__":
    unittest.main()
