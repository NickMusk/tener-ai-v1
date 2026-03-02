from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_emulator_lifesciences_tests.sqlite3"))

from tener_ai.emulator.store import EmulatorProjectStore


class EmulatorLifeSciencesDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]
        cls.projects_dir = cls.root / "config" / "emulator" / "projects"
        cls.company_profiles_path = cls.root / "config" / "emulator" / "company_profiles.json"

    def _build_store(self) -> EmulatorProjectStore:
        return EmulatorProjectStore(
            projects_dir=self.projects_dir,
            company_profiles_path=self.company_profiles_path,
        )

    def test_lifesciences_project_is_available_and_consistent(self) -> None:
        store = self._build_store()
        project = store.get_project("biotech-blindspot-2024")
        self.assertIsNotNone(project)

        assert project is not None
        self.assertEqual(project.get("role"), "Senior Scientist, Antibody Engineering (mAb Discovery)")

        candidates = project.get("candidates") or []
        events = project.get("events") or []
        reveal = project.get("reveal") or {}
        funnel = reveal.get("funnel") or {}

        self.assertEqual(len(candidates), 200)
        self.assertGreaterEqual(len(events), 30)
        self.assertEqual(int(funnel.get("sourced") or 0), 200)
        self.assertEqual(reveal.get("hiredCandidateId"), "cand-011")
        self.assertEqual(reveal.get("tenerTopPick"), "cand-003")

    def test_lifesciences_events_contain_expected_signal_story(self) -> None:
        store = self._build_store()
        project = store.get_project("biotech-blindspot-2024")
        self.assertIsNotNone(project)
        assert project is not None

        events = project.get("events") or []
        signal_events = [event for event in events if str(event.get("type")) == "signal_detected"]

        self.assertTrue(any("PubMed" in str(event.get("detail") or "") or "publication" in str(event.get("title") or "").lower() for event in events))
        self.assertTrue(any(str(event.get("signalCategory")) == "digital_footprint" for event in signal_events))
        self.assertTrue(any(str(event.get("signalCategory")) == "career_trajectory" for event in signal_events))
        self.assertTrue(any(str(event.get("signalCategory")) == "interview" for event in events if event.get("type") in {"signal_detected", "interview_complete", "score_update"}))

    def test_first_100_lifesciences_candidates_have_real_names(self) -> None:
        store = self._build_store()
        project = store.get_project("biotech-blindspot-2024")
        self.assertIsNotNone(project)
        assert project is not None

        candidates = project.get("candidates") or []
        self.assertGreaterEqual(len(candidates), 100)

        first_hundred = candidates[:100]
        self.assertEqual(len(first_hundred), 100)
        for candidate in first_hundred:
            name = str(candidate.get("name") or "").strip()
            self.assertTrue(name, "candidate name cannot be empty")
            self.assertFalse(name.startswith("Candidate "), f"placeholder name found: {name}")
            self.assertIn(" ", name, f"name must include first and last name: {name}")

        all_names = [str(candidate.get("name") or "") for candidate in candidates]
        self.assertFalse(any(name.startswith("Candidate ") for name in all_names))

    def test_invalid_lifesciences_signal_category_degrades_store(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            projects_dir = tmp_path / "projects"
            projects_dir.mkdir(parents=True, exist_ok=True)

            bad_project = {
                "id": "bad-ls",
                "company": "Demo Biotech",
                "role": "Senior Scientist, Antibody Engineering",
                "year": "2024",
                "companyProfile": {"values": ["Rigor"]},
                "candidates": [
                    {
                        "id": "cand-1",
                        "name": "Dr. Demo",
                        "location": "Basel, CH",
                        "experience": "7 years",
                        "currentScore": 60,
                        "currentConfidence": 50,
                        "stage": "sourced",
                        "signals": [],
                    }
                ],
                "events": [
                    {
                        "id": "e-1",
                        "timestamp": "10:00:00",
                        "candidateId": "cand-1",
                        "type": "signal_detected",
                        "title": "Bad category",
                        "detail": "Unsupported biotech category",
                        "signalCategory": "publication_patent",
                        "sentiment": "neutral",
                    }
                ],
                "reveal": {
                    "hiredCandidateId": "cand-1",
                    "hiredOutcome": "Bad",
                    "tenerTopPick": "cand-1",
                    "tenerOutcome": "Good",
                },
            }
            (projects_dir / "bad_lifesciences.json").write_text(json.dumps(bad_project), encoding="utf-8")

            profiles_path = tmp_path / "profiles.json"
            profiles_path.write_text(
                json.dumps(
                    {
                        "profiles": [
                            {
                                "id": "demo",
                                "name": "Demo",
                                "domain": "demo.example",
                                "profile": {},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            store = EmulatorProjectStore(projects_dir=projects_dir, company_profiles_path=profiles_path)
            health = store.health()
            self.assertEqual(health.get("status"), "degraded")
            self.assertIn("unsupported value", str(health.get("load_error") or "").lower())


if __name__ == "__main__":
    unittest.main()
