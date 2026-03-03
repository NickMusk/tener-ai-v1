from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
