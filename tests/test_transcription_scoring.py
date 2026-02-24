from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_interview.transcription_scoring import TranscriptionScoringEngine


class TranscriptionScoringEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.criteria_path = Path(self.tmp.name) / "criteria.json"
        self.criteria_path.write_text(
            json.dumps(
                {
                    "version": "test-v1",
                    "defaults": {
                        "weights": {"keyword": 0.6, "length": 0.25, "clarity": 0.15},
                        "rubric": {"min_words": 5, "ideal_words": 15, "max_words": 120},
                        "disallowed_patterns": [],
                        "filler_words": ["um", "uh"],
                    },
                    "question_rules": [
                        {
                            "id": "tech_rule",
                            "dimension": "technical",
                            "match": {"title_contains": ["technical challenge"]},
                            "required_keywords": ["problem", "solution", "result"],
                            "optional_keywords": ["performance", "trade-off"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.engine = TranscriptionScoringEngine(criteria_path=str(self.criteria_path))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_scores_questions_from_transcriptions(self) -> None:
        payload = {
            "status": "ok",
            "raw": {
                "questions": [
                    {
                        "id": "q1",
                        "title": "Describe a technical challenge",
                        "answer": {
                            "transcription": {
                                "text": (
                                    "I faced a production problem in a payment service. "
                                    "My solution was caching, backpressure and query tuning. "
                                    "The result was lower latency and fewer incidents."
                                )
                            }
                        },
                    }
                ]
            },
        }

        out = self.engine.score_provider_payload(payload)
        self.assertTrue(out["applied"])
        self.assertEqual(out["criteria_version"], "test-v1")
        self.assertEqual(out["coverage"]["scored_questions"], 1)
        self.assertIsNotNone(out["scores"]["technical"])
        self.assertEqual(len(out["question_scores"]), 1)
        self.assertGreaterEqual(float(out["question_scores"][0]["score"]), 80.0)

    def test_returns_not_applied_when_no_transcriptions(self) -> None:
        payload = {
            "status": "ok",
            "raw": {
                "questions": [
                    {
                        "id": "q1",
                        "title": "Describe a technical challenge",
                        "answer": {},
                    }
                ]
            },
        }
        out = self.engine.score_provider_payload(payload)
        self.assertFalse(out["applied"])
        self.assertEqual(out["reason"], "no_transcriptions")
        self.assertEqual(out["coverage"]["missing_transcriptions"], 1)


if __name__ == "__main__":
    unittest.main()
