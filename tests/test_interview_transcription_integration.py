from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

from tener_interview.db import InterviewDatabase
from tener_interview.scoring import InterviewScoringEngine
from tener_interview.service import InterviewService
from tener_interview.token_service import InterviewTokenService
from tener_interview.transcription_scoring import TranscriptionScoringEngine


class _TranscriptProvider:
    name = "hireflix"

    def __init__(self) -> None:
        self.invitation_id = "int_test_1"

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ = payload
        return {
            "invitation_id": self.invitation_id,
            "assessment_id": "pos_1",
            "candidate_id": "candidate@example.com",
            "interview_url": "https://app.hireflix.com/hash_1",
        }

    def get_interview_status(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        _ = invitation_id, assessment_id, candidate_id, force
        return {"status": "completed"}

    def get_interview_result(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        _ = invitation_id, assessment_id, candidate_id
        return {
            "status": "ok",
            "result_id": "res_1",
            "scores": {"technical": None, "soft_skills": None, "culture_fit": None},
            "raw": {
                "questions": [
                    {
                        "id": "q1",
                        "title": "Describe a technical challenge",
                        "answer": {
                            "transcription": {
                                "text": (
                                    "I had a production problem in data ingestion. "
                                    "My solution included retries, batching and queue tuning. "
                                    "The result was lower failures and better latency."
                                )
                            }
                        },
                    }
                ]
            },
        }


class InterviewTranscriptionIntegrationTests(unittest.TestCase):
    def test_refresh_uses_transcription_scoring(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db = InterviewDatabase(str(Path(tmpdir) / "interview.sqlite3"))
            db.init_schema()

            criteria_path = Path(tmpdir) / "criteria.json"
            criteria_path.write_text(
                json.dumps(
                    {
                        "version": "test-v1",
                        "defaults": {
                            "weights": {"keyword": 0.6, "length": 0.25, "clarity": 0.15},
                            "rubric": {"min_words": 5, "ideal_words": 15, "max_words": 120},
                        },
                        "question_rules": [
                            {
                                "dimension": "technical",
                                "match": {"title_contains": ["technical challenge"]},
                                "required_keywords": ["problem", "solution", "result"],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            service = InterviewService(
                db=db,
                provider=_TranscriptProvider(),
                token_service=InterviewTokenService(secret="secret"),
                scoring_engine=InterviewScoringEngine(),
                transcription_scoring_engine=TranscriptionScoringEngine(str(criteria_path)),
                default_ttl_hours=72,
                public_base_url="http://localhost:8090",
            )

            started = service.start_session(job_id=1, candidate_id=42, candidate_name="Jane")
            out = service.refresh_session(started["session_id"], force=True)

            self.assertEqual(out["status"], "scored")
            self.assertIsNotNone(out["result"]["total_score"])
            self.assertEqual(len(out["result"]["question_scores"]), 1)
            self.assertIsNotNone(out["result"]["question_scores"][0]["score"])

            card = service.get_session_scorecard(started["session_id"])
            assert card is not None
            self.assertIn("transcription_scoring", card["scorecard"])
            self.assertTrue(card["scorecard"]["transcription_scoring"]["applied"])


if __name__ == "__main__":
    unittest.main()
