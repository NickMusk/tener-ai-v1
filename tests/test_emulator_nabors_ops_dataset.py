from __future__ import annotations

import unittest
from pathlib import Path

from tener_ai.emulator.store import EmulatorProjectStore


class EmulatorNaborsOpsDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_nabors_ops_manager_project_loads_with_112_candidates(self) -> None:
        store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        health = store.health()
        self.assertEqual(health.get("status"), "ok")

        project = store.get_project("nabors-ops-manager-uae-2024")
        self.assertIsNotNone(project)
        self.assertEqual(str(project.get("company") or ""), "Major Drilling Contractor (UAE)")
        self.assertEqual(str(project.get("role") or ""), "Operations Manager")
        self.assertEqual(len(project.get("candidates") or []), 112)
        self.assertEqual(len(project.get("events") or []), 343)

        reveal = project.get("reveal") or {}
        funnel = reveal.get("funnel") or {}
        self.assertEqual(int(funnel.get("sourced") or 0), 105)
        self.assertEqual(int(funnel.get("filtered") or 0), 72)
        self.assertEqual(int(funnel.get("outreach") or 0), 41)
        self.assertEqual(int(funnel.get("engaged") or 0), 20)
        self.assertEqual(int(funnel.get("shortlisted") or 0), 5)


if __name__ == "__main__":
    unittest.main()
