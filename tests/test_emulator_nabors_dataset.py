from __future__ import annotations

from collections import Counter
import unittest
from pathlib import Path

from tener_ai.emulator.store import EmulatorProjectStore


class EmulatorNaborsDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_nabors_project_loads_with_expected_shape(self) -> None:
        store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        health = store.health()
        self.assertEqual(health.get("status"), "ok")

        project = store.get_project("nabors-drilling-engineer-2024")
        self.assertIsNotNone(project)
        self.assertEqual(str(project.get("company") or ""), "Major IOC Operator (MENA Region)")
        self.assertEqual(str(project.get("role") or ""), "Drilling Engineer")
        self.assertEqual(len(project.get("candidates") or []), 105)
        self.assertEqual(len(project.get("events") or []), 335)

        reveal = project.get("reveal") or {}
        self.assertEqual(str(reveal.get("hiredCandidateId") or ""), "ahmed-al-hammadi")
        self.assertEqual(str(reveal.get("tenerTopPick") or ""), "rajesh-kumar-sharma")

        categories = []
        for candidate in project.get("candidates") or []:
            for signal in candidate.get("signals") or []:
                if signal.get("category"):
                    categories.append(str(signal.get("category")))
        for event in project.get("events") or []:
            if event.get("signalCategory"):
                categories.append(str(event.get("signalCategory")))
        self.assertGreater(len(categories), 0)
        resume_like = {
            "cv_consistency",
            "skills_match",
            "skills_depth",
            "domain_expertise",
            "portfolio_quality",
            "education_signal",
            "technical_skills_match",
            "safety_certifications",
            "rig_type_experience",
            "rotational_readiness",
            "incident_safety_record",
            "contract_tenure_pattern",
            "education_credentials",
            "linkedin_completeness",
        }
        resume_count = sum(1 for category in categories if category in resume_like)
        self.assertGreaterEqual(resume_count / len(categories), 0.6)
        count_by_category = Counter(categories)
        comm_interview = int(count_by_category.get("communication", 0)) + int(count_by_category.get("interview", 0))
        self.assertLess(comm_interview, resume_count)


if __name__ == "__main__":
    unittest.main()
