from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_interview.scoring import InterviewScoringEngine


class InterviewScoringFormulaTests(unittest.TestCase):
    def test_uses_custom_weights_from_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            formula_path = Path(tmpdir) / "formula.json"
            formula_path.write_text(
                json.dumps(
                    {
                        "version": "weights-v2",
                        "weights": {
                            "technical": 0.2,
                            "soft_skills": 0.3,
                            "culture_fit": 0.5,
                        },
                        "recommendation_thresholds": {
                            "strong_yes": 90,
                            "yes": 70,
                            "mixed": 50,
                        },
                    }
                ),
                encoding="utf-8",
            )

            engine = InterviewScoringEngine(formula_path=str(formula_path))
            out = engine.normalize_provider_result(
                {
                    "scores": {
                        "technical": 100,
                        "soft_skills": 50,
                        "culture_fit": 50,
                    }
                }
            )
            self.assertEqual(out["total_score"], 60.0)
            self.assertEqual(out["pass_recommendation"], "mixed")
            self.assertEqual(out["normalized_json"]["formula_version"], "weights-v2")

    def test_strict_missing_dimensions_strategy(self) -> None:
        with TemporaryDirectory() as tmpdir:
            formula_path = Path(tmpdir) / "formula.json"
            formula_path.write_text(
                json.dumps(
                    {
                        "missing_dimensions_strategy": "strict",
                    }
                ),
                encoding="utf-8",
            )

            engine = InterviewScoringEngine(formula_path=str(formula_path))
            out = engine.normalize_provider_result(
                {
                    "scores": {
                        "technical": 92,
                        "soft_skills": None,
                        "culture_fit": None,
                    }
                }
            )
            self.assertIsNone(out["total_score"])
            self.assertEqual(out["score_confidence"], 0.5)
            self.assertEqual(out["pass_recommendation"], "mixed")


if __name__ == "__main__":
    unittest.main()
