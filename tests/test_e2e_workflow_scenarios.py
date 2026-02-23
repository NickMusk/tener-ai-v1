import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Tuple

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.linkedin_provider import MockLinkedInProvider
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class WorkflowE2EScenariosTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]
        scenarios_path = cls.root / "tests" / "scenarios" / "workflow_e2e_scenarios.json"
        with scenarios_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        cls.scenarios = data["scenarios"]

    def _build_workflow(self, work_dir: Path) -> Tuple[Database, WorkflowService]:
        db = Database(str(work_dir / "workflow_e2e.sqlite3"))
        db.init_schema()

        matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))
        provider = MockLinkedInProvider(str(self.root / "data" / "mock_linkedin_profiles.json"))

        sourcing = SourcingAgent(provider)
        verification = VerificationAgent(matching)
        outreach = OutreachAgent(str(self.root / "config" / "outreach_templates.json"), matching)
        faq = FAQAgent(str(self.root / "config" / "outreach_templates.json"), matching)

        workflow = WorkflowService(
            db=db,
            sourcing_agent=sourcing,
            verification_agent=verification,
            outreach_agent=outreach,
            faq_agent=faq,
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

    def _assert_in_range(self, value: int, min_value: int, max_value: int, label: str) -> None:
        self.assertGreaterEqual(value, min_value, f"{label} is below expected range")
        self.assertLessEqual(value, max_value, f"{label} is above expected range")

    def _assert_human_explanations(self, items: List[Dict[str, Any]]) -> None:
        self.assertGreater(len(items), 0, "verify step returned no items")
        for item in items:
            notes = item.get("notes") or {}
            text = str(notes.get("human_explanation") or "").strip()
            self.assertTrue(text, "human_explanation must be present for each candidate")
            self.assertIn("score", text.lower())

    def test_manual_steps_match_expected_ranges(self) -> None:
        for scenario in self.scenarios:
            with self.subTest(scenario=scenario["id"]):
                with TemporaryDirectory() as td:
                    db, workflow = self._build_workflow(Path(td))
                    job_id = self._insert_job(db, scenario["job"])
                    expected = scenario["expected"]

                    source = workflow.source_candidates(job_id=job_id, limit=scenario["source_limit"])
                    self._assert_in_range(source["total"], expected["source_min"], expected["source_max"], "source total")

                    verify = workflow.verify_profiles(job_id=job_id, profiles=source["profiles"])
                    self._assert_in_range(verify["verified"], expected["verified_min"], expected["verified_max"], "verified total")
                    self._assert_in_range(verify["rejected"], expected["rejected_min"], expected["rejected_max"], "rejected total")
                    self._assert_human_explanations(verify["items"])

                    verified_names = {
                        str(item.get("profile", {}).get("full_name"))
                        for item in verify["items"]
                        if item.get("status") == "verified"
                    }
                    self.assertTrue(
                        verified_names.intersection(set(expected["expected_verified_names"])),
                        "expected verified candidate is missing",
                    )

                    verified_items = [x for x in verify["items"] if x.get("status") == "verified"]
                    added = workflow.add_verified_candidates(job_id=job_id, verified_items=verified_items)
                    self.assertEqual(added["total"], verify["verified"])

                    outreach = workflow.outreach_candidates(
                        job_id=job_id,
                        candidate_ids=[x["candidate_id"] for x in added["added"]],
                    )
                    self.assertEqual(outreach["total"], added["total"])
                    self.assertEqual(outreach["sent"], 0)  # Mock provider intentionally does not deliver.
                    self.assertEqual(outreach["failed"], added["total"])

    def test_full_workflow_and_faq_reply_language(self) -> None:
        # Frontend ES scenario gives an ES-speaking verified candidate (Miguel Santos).
        scenario = next(x for x in self.scenarios if x["id"] == "frontend_typescript_spain")
        with TemporaryDirectory() as td:
            db, workflow = self._build_workflow(Path(td))
            job_id = self._insert_job(db, scenario["job"])

            summary = workflow.execute_job_workflow(job_id=job_id, limit=scenario["source_limit"])
            self._assert_in_range(summary.searched, 8, 10, "summary searched")
            self._assert_in_range(summary.verified, 1, 2, "summary verified")
            self.assertEqual(summary.outreached, len(summary.conversation_ids))
            self.assertGreaterEqual(len(summary.conversation_ids), 1)

            conversation_id = summary.conversation_ids[0]
            reply = workflow.process_inbound_message(
                conversation_id=conversation_id,
                text="Hola, cual es el salario y el proceso?",
            )
            self.assertEqual(reply["language"], "es")
            self.assertIn(reply["intent"], {"salary", "timeline", "default"})
            self.assertTrue(reply["reply"].strip())


if __name__ == "__main__":
    unittest.main()
