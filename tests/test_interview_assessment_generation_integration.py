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


class _MutableSourceCatalog:
    def __init__(self) -> None:
        self.jd_text = "Python, AWS, SQL"

    def get_job(self, job_id: int) -> Dict[str, Any]:
        return {
            "id": int(job_id),
            "title": "Backend Engineer",
            "jd_text": self.jd_text,
        }


class _AssessmentAwareProvider:
    name = "hireflix"

    def __init__(self) -> None:
        self.assessment_count = 0
        self.invite_count = 0

    def create_assessment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ = payload
        self.assessment_count += 1
        return {
            "assessment_id": f"pos_{self.assessment_count}",
            "assessment_name": f"Assessment {self.assessment_count}",
        }

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.invite_count += 1
        position_id = str(payload.get("position_id") or "")
        if not position_id:
            raise ValueError("position_id is required")
        return {
            "invitation_id": f"inv_{self.invite_count}",
            "assessment_id": position_id,
            "candidate_id": str(payload.get("candidate_id") or ""),
            "interview_url": f"https://app.hireflix.com/{self.invite_count}",
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
        return {
            "status": "ok",
            "result_id": "r1",
            "scores": {
                "technical": 80.0,
                "soft_skills": 80.0,
                "culture_fit": 80.0,
            },
            "raw": {},
        }


class InterviewAssessmentGenerationIntegrationTests(unittest.TestCase):
    def test_assessment_is_reused_per_job_and_regenerated_when_jd_changes(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db = InterviewDatabase(str(Path(tmpdir) / "interview.sqlite3"))
            db.init_schema()

            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"
            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v1",
                        "defaults": {
                            "question_count": 3,
                            "time_to_answer": 120,
                            "time_to_think": 10,
                            "retakes": 1,
                        },
                        "skill_dictionary": ["python", "aws", "sql", "java", "kubernetes"],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps(
                    {
                        "mission": "Build better teams",
                        "values": ["communication", "ownership"],
                    }
                ),
                encoding="utf-8",
            )

            source = _MutableSourceCatalog()
            provider = _AssessmentAwareProvider()
            generator = InterviewQuestionGenerator(
                guidelines_path=str(guidelines_path),
                company_profile_path=str(profile_path),
                company_name="Tener",
            )

            service = InterviewService(
                db=db,
                provider=provider,
                token_service=InterviewTokenService(secret="secret"),
                scoring_engine=InterviewScoringEngine(),
                source_catalog=source,
                question_generator=generator,
                default_ttl_hours=72,
                public_base_url="http://localhost:8090",
            )

            first = service.start_session(job_id=1, candidate_id=101, candidate_name="C1")
            second = service.start_session(job_id=1, candidate_id=102, candidate_name="C2")

            self.assertEqual(provider.assessment_count, 1)
            self.assertEqual(first["provider"]["assessment_id"], "pos_1")
            self.assertEqual(second["provider"]["assessment_id"], "pos_1")

            source.jd_text = "Java and Kubernetes platform engineering"
            third = service.start_session(job_id=1, candidate_id=103, candidate_name="C3")

            self.assertEqual(provider.assessment_count, 2)
            self.assertEqual(third["provider"]["assessment_id"], "pos_2")

            saved = db.get_job_assessment(1)
            self.assertIsNotNone(saved)
            assert saved is not None
            self.assertEqual(saved["provider_assessment_id"], "pos_2")


if __name__ == "__main__":
    unittest.main()
