from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_interview.question_generation import InterviewQuestionGenerator


class InterviewQuestionGenerationTests(unittest.TestCase):
    def test_generation_mentions_company_and_extracts_skills(self) -> None:
        with TemporaryDirectory() as tmpdir:
            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"

            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v1",
                        "defaults": {
                            "question_count": 3,
                            "time_to_answer": 130,
                            "time_to_think": 10,
                            "retakes": 1,
                        },
                        "company_values": ["communication", "ownership"],
                        "skill_dictionary": ["python", "aws", "sql"],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps(
                    {
                        "mission": "Build reliable hiring systems",
                        "values": ["communication", "ownership"],
                    }
                ),
                encoding="utf-8",
            )

            generator = InterviewQuestionGenerator(
                guidelines_path=str(guidelines_path),
                company_profile_path=str(profile_path),
                company_name="Acme Labs",
            )

            out = generator.generate_for_job(
                {
                    "id": 101,
                    "title": "Senior Backend Engineer",
                    "jd_text": "Strong Python and AWS experience. SQL optimization is important.",
                }
            )

            self.assertIn("assessment_name", out)
            self.assertIn("Acme Labs", out["assessment_name"])
            self.assertEqual(len(out["questions"]), 3)
            for question in out["questions"]:
                self.assertIn("Acme Labs", question["title"])

            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            detected = meta.get("skills_detected") if isinstance(meta.get("skills_detected"), list) else []
            self.assertIn("python", detected)
            self.assertIn("aws", detected)


if __name__ == "__main__":
    unittest.main()
