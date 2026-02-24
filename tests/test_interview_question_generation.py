from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_interview.question_generation import InterviewQuestionGenerator


class InterviewQuestionGenerationTests(unittest.TestCase):
    def test_generation_mentions_company_selectively_and_extracts_skills(self) -> None:
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
                    "company": "Orbit AI",
                    "title": "Senior Backend Engineer",
                    "jd_text": "Strong Python and AWS experience. SQL optimization is important.",
                }
            )

            self.assertIn("assessment_name", out)
            self.assertIn("Orbit AI", out["assessment_name"])
            self.assertEqual(len(out["questions"]), 3)
            mention_count = 0
            for question in out["questions"]:
                if "Orbit AI" in str(question.get("title") or ""):
                    mention_count += 1
                self.assertIn("category", question)
            self.assertGreaterEqual(mention_count, 1)
            self.assertLess(mention_count, len(out["questions"]))

            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            detected = meta.get("skills_detected") if isinstance(meta.get("skills_detected"), list) else []
            self.assertIn("python", detected)
            self.assertIn("aws", detected)

    def test_generation_produces_ten_questions_with_three_categories(self) -> None:
        with TemporaryDirectory() as tmpdir:
            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"

            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v2",
                        "defaults": {
                            "question_count": 10,
                            "time_to_answer": 120,
                            "time_to_think": 12,
                            "retakes": 1,
                            "category_targets": {
                                "hard_skills": 0.4,
                                "soft_skills": 0.3,
                                "cultural_fit": 0.3,
                            },
                        },
                        "skill_dictionary": ["python", "aws", "sql"],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps(
                    {
                        "mission": "Build reliable hiring systems",
                        "values": ["communication", "ownership", "collaboration"],
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
                    "id": 202,
                    "title": "Senior Backend Engineer",
                    "jd_text": "Strong Python and AWS experience. SQL optimization is important.",
                }
            )

            self.assertEqual(len(out["questions"]), 10)
            categories = [str(q.get("category") or "") for q in out["questions"]]
            self.assertIn("hard_skills", categories)
            self.assertIn("soft_skills", categories)
            self.assertIn("cultural_fit", categories)

            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            counts = meta.get("categories") if isinstance(meta.get("categories"), dict) else {}
            self.assertEqual(sum(int(v) for v in counts.values()), 10)


if __name__ == "__main__":
    unittest.main()
