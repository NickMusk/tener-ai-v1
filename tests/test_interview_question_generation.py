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

    def test_generation_prefers_job_culture_profile_questions_when_available(self) -> None:
        with TemporaryDirectory() as tmpdir:
            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"

            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v3",
                        "defaults": {"question_count": 5},
                        "skill_dictionary": ["python", "aws", "sql"],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps(
                    {
                        "mission": "Default mission",
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
                    "id": 303,
                    "company": "Orbit AI",
                    "title": "Senior Backend Engineer",
                    "jd_text": "Strong Python and AWS experience. SQL optimization is important.",
                    "company_culture_profile": {
                        "culture_values": ["candor", "ownership", "high standards"],
                        "culture_interview_questions": [
                            "Tell us about a time you challenged a decision with evidence",
                            "How do you balance speed and quality under pressure",
                        ],
                        "summary_200_300_words": "Team operates with high ownership and strong peer feedback loops.",
                    },
                }
            )
            questions = out.get("questions") if isinstance(out.get("questions"), list) else []
            joined_titles = "\n".join(str((q or {}).get("title") or "") for q in questions if isinstance(q, dict))
            self.assertIn("challenged a decision", joined_titles.lower())
            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            self.assertEqual(str(meta.get("culture_profile_source") or ""), "job")

    def test_manual_qa_questions_are_jd_specific_and_not_backend_architecture_templates(self) -> None:
        with TemporaryDirectory() as tmpdir:
            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"

            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v4",
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
                        "skill_dictionary": [
                            "manual testing",
                            "api testing",
                            "postman",
                            "sql",
                            "selenium",
                            "python",
                            "qa",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps(
                    {
                        "mission": "Ship high quality hiring workflows quickly",
                        "values": ["ownership", "clarity", "collaboration"],
                    }
                ),
                encoding="utf-8",
            )

            generator = InterviewQuestionGenerator(
                guidelines_path=str(guidelines_path),
                company_profile_path=str(profile_path),
                company_name="Tener.ai",
            )

            out = generator.generate_for_job(
                {
                    "id": 404,
                    "company": "Tener.ai",
                    "title": "Manual QA Engineer",
                    "jd_text": (
                        "Role Overview: Manual QA Engineer for web platform, APIs, and AI-generated outputs. "
                        "Responsibilities: test end-to-end flows, validate API responses in Postman, verify SQL data consistency, "
                        "document reproducible bug reports, and collaborate with engineering on release decisions."
                    ),
                    "company_culture_profile": {
                        "culture_values": ["ownership", "high standards", "clear communication"],
                        "summary_200_300_words": "Fast-paced delivery with strict quality accountability and transparent communication.",
                        "culture_interview_questions": [
                            "Describe a time you pushed back on a release due to quality risk",
                        ],
                    },
                }
            )

            self.assertEqual(len(out["questions"]), 10)
            titles = [str((q or {}).get("title") or "") for q in out["questions"] if isinstance(q, dict)]
            joined = "\n".join(titles).lower()

            self.assertNotIn("design and scale", joined)
            self.assertNotIn("architecture improvement", joined)
            self.assertNotIn("distributed systems", joined)
            self.assertNotIn("we are looking for", joined)
            self.assertNotIn("the environment reads as", joined)

    def test_generation_prefers_normalized_job_requirements_over_noisy_jd_copy(self) -> None:
        with TemporaryDirectory() as tmpdir:
            guidelines_path = Path(tmpdir) / "guidelines.json"
            profile_path = Path(tmpdir) / "company_profile.json"

            guidelines_path.write_text(
                json.dumps(
                    {
                        "version": "test-v5",
                        "defaults": {"question_count": 6},
                        "skill_dictionary": ["sql", "ml", "postman", "selenium"],
                    }
                ),
                encoding="utf-8",
            )
            profile_path.write_text(
                json.dumps({"mission": "Build reliable hiring systems", "values": ["ownership", "clarity"]}),
                encoding="utf-8",
            )

            generator = InterviewQuestionGenerator(
                guidelines_path=str(guidelines_path),
                company_profile_path=str(profile_path),
                company_name="Tener",
            )

            out = generator.generate_for_job(
                {
                    "id": 505,
                    "company": "Tener",
                    "title": "Manual QA",
                    "jd_text": (
                        "Tener.ai is building an autonomous AI recruiter. "
                        "We are looking for a Manual QA Engineer. "
                        "Responsibilities: test web applications and APIs, verify AI-driven workflows, "
                        "test integrations, ensure stability before releases."
                    ),
                    "must_have_skills": [
                        "manual testing",
                        "api testing",
                        "regression testing",
                        "test case design",
                        "bug reporting",
                        "postman",
                    ],
                    "nice_to_have_skills": ["sql", "selenium", "automation testing"],
                    "questionable_skills": ["ml"],
                    "company_culture_profile": {
                        "culture_values": ["quality bar", "ownership", "remote-first"],
                        "culture_interview_questions": [
                            "Tell us about a time you challenged a decision with data and changed the outcome.",
                            "What kind of management style makes you underperform?",
                        ],
                    },
                }
            )

            questions = out.get("questions") if isinstance(out.get("questions"), list) else []
            titles = [str((q or {}).get("title") or "") for q in questions if isinstance(q, dict)]
            joined = "\n".join(
                f"{str((q or {}).get('title') or '')} {str((q or {}).get('description') or '')}"
                for q in questions
                if isinstance(q, dict)
            ).lower()

            self.assertIn("manual coverage", joined)
            self.assertIn("ui, api, and data consistency", joined)
            self.assertIn("challenged a decision", joined)
            self.assertNotIn("management style", joined)
            self.assertNotIn("underperform", joined)
            self.assertNotIn("we are looking for", joined)
            self.assertNotIn(" ml", joined)
            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            self.assertEqual(
                meta.get("skills_detected"),
                ["manual testing", "api testing", "regression testing", "test case design", "bug reporting", "postman"],
            )

            # Must contain QA-specific signal words, not generic backend-only framing.
            self.assertTrue(
                any(any(marker in t.lower() for marker in ("test", "qa", "bug", "release", "api")) for t in titles)
            )

            # Culture profile should influence generated set.
            self.assertIn("pushed back on a release", joined)

            meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
            self.assertEqual(str(meta.get("role_family") or ""), "qa")


if __name__ == "__main__":
    unittest.main()
