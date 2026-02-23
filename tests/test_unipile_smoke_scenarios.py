import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Tuple

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.linkedin_provider import build_linkedin_provider
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class UnipileSmokeScenariosTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]
        cls.run_smoke = os.environ.get("RUN_UNIPILE_SMOKE", "0") == "1"
        cls.required_env = ("UNIPILE_API_KEY", "UNIPILE_ACCOUNT_ID", "UNIPILE_BASE_URL")

        if not cls.run_smoke:
            return

        missing = [k for k in cls.required_env if not os.environ.get(k)]
        if missing:
            raise unittest.SkipTest(f"RUN_UNIPILE_SMOKE=1 but missing env vars: {', '.join(missing)}")

        scenarios_path = cls.root / "tests" / "scenarios" / "unipile_smoke_scenarios.json"
        with scenarios_path.open("r", encoding="utf-8") as f:
            cls.scenarios = json.load(f)["scenarios"]

    def setUp(self) -> None:
        if not self.run_smoke:
            self.skipTest("Set RUN_UNIPILE_SMOKE=1 to run live Unipile smoke tests")

    def _build_workflow(self, work_dir: Path) -> Tuple[Database, WorkflowService]:
        # Smoke tests should not create real outbound conversations.
        os.environ["UNIPILE_DRY_RUN"] = "true"
        provider = build_linkedin_provider(str(self.root / "data" / "mock_linkedin_profiles.json"))

        db = Database(str(work_dir / "unipile_smoke.sqlite3"))
        db.init_schema()
        matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))

        workflow = WorkflowService(
            db=db,
            sourcing_agent=SourcingAgent(provider),
            verification_agent=VerificationAgent(matching),
            outreach_agent=OutreachAgent(str(self.root / "config" / "outreach_templates.json"), matching),
            faq_agent=FAQAgent(str(self.root / "config" / "outreach_templates.json"), matching),
        )
        return db, workflow

    def _insert_job(self, db: Database, job: Dict[str, Any]) -> int:
        return db.insert_job(
            title=job["title"],
            jd_text=job["jd_text"],
            location=job.get("location"),
            preferred_languages=job.get("preferred_languages", []),
            seniority=job.get("seniority"),
        )

    def _assert_range(self, value: int, min_value: int, max_value: int, label: str) -> None:
        self.assertGreaterEqual(value, min_value, f"{label} below expected range")
        self.assertLessEqual(value, max_value, f"{label} above expected range")

    def test_source_and_verify_ranges(self) -> None:
        for scenario in self.scenarios:
            with self.subTest(scenario=scenario["id"]):
                with TemporaryDirectory() as td:
                    db, workflow = self._build_workflow(Path(td))
                    job_id = self._insert_job(db, scenario["job"])

                    source = workflow.source_candidates(job_id=job_id, limit=scenario["source_limit"])
                    expected = scenario["expected"]
                    self._assert_range(source["total"], expected["source_min"], expected["source_max"], "source total")

                    verify = workflow.verify_profiles(job_id=job_id, profiles=source["profiles"])
                    self._assert_range(verify["verified"], expected["verified_min"], expected["verified_max"], "verified total")
                    self.assertEqual(verify["total"], source["total"])
                    self.assertEqual(verify["verified"] + verify["rejected"], verify["total"])

                    for item in verify["items"]:
                        explanation = str((item.get("notes") or {}).get("human_explanation") or "").strip()
                        self.assertTrue(explanation, "missing human_explanation for verified/rejected item")

    def test_execute_workflow_dry_run_outreach(self) -> None:
        scenario = self.scenarios[0]
        with TemporaryDirectory() as td:
            db, workflow = self._build_workflow(Path(td))
            job_id = self._insert_job(db, scenario["job"])
            summary = workflow.execute_job_workflow(job_id=job_id, limit=scenario["source_limit"])
            self.assertGreaterEqual(summary.searched, 1)
            self.assertEqual(summary.outreached, len(summary.conversation_ids))


if __name__ == "__main__":
    unittest.main()
