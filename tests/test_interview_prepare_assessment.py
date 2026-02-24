from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

from tener_interview.db import InterviewDatabase
from tener_interview.question_generation import InterviewQuestionGenerator
from tener_interview.scoring import InterviewScoringEngine
from tener_interview.service import InterviewService
from tener_interview.token_service import InterviewTokenService


class _SourceCatalog:
    def get_job(self, job_id: int) -> Dict[str, Any]:
        return {
            "id": int(job_id),
            "title": "Backend Engineer",
            "jd_text": "Python and AWS",
        }


class _Provider:
    name = "hireflix"

    def __init__(self) -> None:
        self.created = 0

    def create_assessment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ = payload
        self.created += 1
        return {"assessment_id": "pos_1", "assessment_name": "BE interview"}

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ = payload
        return {
            "invitation_id": "inv_1",
            "assessment_id": "pos_1",
            "candidate_id": "candidate@example.com",
            "interview_url": "https://app.hireflix.com/hash",
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
        return {"status": "in_progress"}

    def get_interview_result(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        _ = invitation_id, assessment_id, candidate_id
        return {"status": "failed", "error_code": "N/A", "error_message": "not used"}


class InterviewPrepareAssessmentTests(unittest.TestCase):
    def test_prepare_assessment_creates_once_and_reuses_cache(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db = InterviewDatabase(str(Path(tmpdir) / "interview.sqlite3"))
            db.init_schema()

            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"
            guidelines_path.write_text(
                json.dumps({"defaults": {"question_count": 3}, "skill_dictionary": ["python", "aws"]}),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps({"company_name": "Tener", "values": ["communication", "ownership"]}),
                encoding="utf-8",
            )

            provider = _Provider()
            service = InterviewService(
                db=db,
                provider=provider,
                token_service=InterviewTokenService(secret="test"),
                scoring_engine=InterviewScoringEngine(),
                source_catalog=_SourceCatalog(),
                question_generator=InterviewQuestionGenerator(
                    guidelines_path=str(guidelines_path),
                    company_profile_path=str(profile_path),
                    company_name="Tener",
                ),
            )

            first = service.prepare_job_assessment(job_id=11)
            second = service.prepare_job_assessment(job_id=11)

            self.assertEqual(first["assessment_id"], "pos_1")
            self.assertTrue(first["created_now"])
            self.assertFalse(second["created_now"])
            self.assertEqual(provider.created, 1)


if __name__ == "__main__":
    unittest.main()
