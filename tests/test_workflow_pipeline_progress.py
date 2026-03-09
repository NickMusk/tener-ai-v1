from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Tuple

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database, utc_now_iso
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _AccountAwareProvider:
    def __init__(self) -> None:
        self.account_id: str | None = None

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        account_id = str(self.account_id or "")
        if account_id == "acc-stale":
            raise RuntimeError("Unipile HTTP error 404: Account not found")
        if account_id != "acc-live":
            raise RuntimeError(f"unexpected_account:{account_id}")
        return [
            {
                "linkedin_id": "candidate-live-1",
                "full_name": "Live Candidate",
                "headline": "Manual QA Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["qa", "manual testing", "api testing"],
                "years_experience": 4,
                "raw": {"query": query, "account_id": account_id},
            }
        ]

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": False, "reason": "stub_no_delivery"}


class _AlwaysFailSourceProvider:
    def __init__(self) -> None:
        self.account_id: str | None = None

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        raise RuntimeError("Unipile HTTP error 404: Account not found")

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": False, "reason": "stub_no_delivery"}


class WorkflowPipelineProgressTests(unittest.TestCase):
    def _build_workflow(self, work_dir: Path, provider: Any) -> Tuple[Database, WorkflowService]:
        root = Path(__file__).resolve().parents[1]
        db = Database(str(work_dir / "workflow_pipeline_progress.sqlite3"))
        db.init_schema()
        matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
        workflow = WorkflowService(
            db=db,
            sourcing_agent=SourcingAgent(provider, matching_engine=matching),
            verification_agent=VerificationAgent(matching),
            outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
            faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
        )
        return db, workflow

    @staticmethod
    def _insert_job(db: Database) -> int:
        return db.insert_job(
            title="Manual QA Engineer",
            jd_text="Need manual testing, API testing, regression testing, bug reporting.",
            location="Remote",
            preferred_languages=["en"],
            seniority="middle",
        )

    def test_source_candidates_skip_removed_provider_account_and_mark_it_removed(self) -> None:
        with TemporaryDirectory() as td:
            db, workflow = self._build_workflow(Path(td), _AccountAwareProvider())
            job_id = self._insert_job(db)
            live_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-live",
                status="connected",
                connected_at=utc_now_iso(),
            )
            stale_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-stale",
                status="connected",
                connected_at=utc_now_iso(),
            )

            out = workflow.source_candidates(job_id=job_id, limit=5)
            self.assertEqual(int(out.get("total") or 0), 1)

            stale_row = db.get_linkedin_account(stale_id)
            live_row = db.get_linkedin_account(live_id)
            self.assertEqual(str((stale_row or {}).get("status") or ""), "removed")
            self.assertEqual(str((live_row or {}).get("status") or ""), "connected")

    def test_execute_job_workflow_records_source_error_and_skipped_downstream_steps(self) -> None:
        with TemporaryDirectory() as td:
            db, workflow = self._build_workflow(Path(td), _AlwaysFailSourceProvider())
            job_id = self._insert_job(db)
            db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-stale",
                status="connected",
                connected_at=utc_now_iso(),
            )

            with self.assertRaises(RuntimeError):
                workflow.execute_job_workflow(job_id=job_id, limit=5)

            by_step = {row["step"]: row for row in db.list_job_step_progress(job_id=job_id)}
            self.assertEqual(str((by_step.get("source") or {}).get("status") or ""), "error")
            self.assertEqual(str((by_step.get("enrich") or {}).get("status") or ""), "skipped")
            self.assertEqual(str((by_step.get("verify") or {}).get("status") or ""), "skipped")
            self.assertEqual(str((by_step.get("add") or {}).get("status") or ""), "skipped")
            self.assertEqual(str((by_step.get("outreach") or {}).get("status") or ""), "skipped")
            self.assertEqual(str((by_step.get("workflow") or {}).get("status") or ""), "error")


if __name__ == "__main__":
    unittest.main()
