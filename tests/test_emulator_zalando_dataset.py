from __future__ import annotations

import unittest
from pathlib import Path

from tener_ai.emulator.contracts import SIGNAL_CATEGORIES
from tener_ai.emulator.store import EmulatorProjectStore


class EmulatorZalandoDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_zalando_project_loads_with_expected_shape(self) -> None:
        store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        health = store.health()
        self.assertEqual(health.get("status"), "ok")

        project = store.get_project("zalando-applied-scientist-diversity-2025")
        self.assertIsNotNone(project)
        assert project is not None

        self.assertEqual(str(project.get("company") or ""), "Zalando")
        self.assertEqual(str(project.get("role") or ""), "Senior Applied Scientist")
        self.assertEqual(len(project.get("candidates") or []), 100)
        self.assertEqual(len(project.get("events") or []), 199)

        reveal = project.get("reveal") or {}
        funnel = reveal.get("funnel") or {}
        self.assertEqual(int(funnel.get("sourced") or 0), 100)
        self.assertEqual(int(funnel.get("filtered") or 0), 72)
        self.assertEqual(int(funnel.get("outreach") or 0), 30)
        self.assertEqual(int(funnel.get("engaged") or 0), 20)
        self.assertEqual(int(funnel.get("shortlisted") or 0), 5)

    def test_zalando_candidate_signals_are_objects_with_supported_categories(self) -> None:
        store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        project = store.get_project("zalando-applied-scientist-diversity-2025")
        self.assertIsNotNone(project)
        assert project is not None

        candidates = project.get("candidates") or []
        self.assertGreater(len(candidates), 0)
        for candidate in candidates:
            for signal in candidate.get("signals") or []:
                self.assertIsInstance(signal, dict)
                category = str(signal.get("category") or "")
                self.assertIn(category, SIGNAL_CATEGORIES)


if __name__ == "__main__":
    unittest.main()
