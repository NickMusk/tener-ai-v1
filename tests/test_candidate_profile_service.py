from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.candidate_profile import CandidateProfileService
from tener_ai.candidate_scoring import CandidateScoringPolicy
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine


class _LLMResponder:
    def __init__(self, text: str) -> None:
        self.text = text

    def generate_candidate_reply(self, **_: object) -> str:
        return self.text


class CandidateProfileServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "candidate_profile_service.sqlite3"))
        self.db.init_schema()
        self.matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))
        self.scoring = CandidateScoringPolicy(str(self.root / "config" / "candidate_scoring_formula.json"))
        self.service = CandidateProfileService(
            db=self.db,
            matching_engine=self.matching,
            scoring_policy=self.scoring,
            llm_responder=None,
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_build_candidate_profile_surfaces_weak_skill_evidence_without_marking_must_haves_missing(self) -> None:
        job_id = self.db.insert_job(
            title="Manual QA Engineer",
            company="Tener",
            jd_text="Requirements: manual testing, api testing, regression testing. Nice to have: sql.",
            location="Remote",
            preferred_languages=["en"],
            seniority="middle",
            must_have_skills=["manual testing", "api testing", "regression testing"],
            nice_to_have_skills=["sql"],
        )
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "weak-skill-evidence-1",
                "full_name": "Mykola B.",
                "headline": "Senior QA Engineer",
                "location": "Mexico",
                "languages": ["en"],
                "skills": ["manual testing", "api testing", "regression testing", "sql"],
                "years_experience": 6,
                "raw": {},
            },
            source="manual",
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.72,
            status="review",
            verification_notes={},
        )

        payload = self.service.build_candidate_profile(candidate_id=candidate_id, selected_job_id=job_id, include_explanation=False)
        jobs = payload.get("jobs") if isinstance(payload.get("jobs"), list) else []
        self.assertTrue(jobs)
        fit_breakdown = jobs[0].get("fit_breakdown") if isinstance(jobs[0].get("fit_breakdown"), dict) else {}
        must_have = fit_breakdown.get("must_have") if isinstance(fit_breakdown.get("must_have"), dict) else {}

        self.assertEqual(must_have.get("matched") or [], [])
        self.assertEqual(
            must_have.get("weak") or [],
            ["manual testing", "api testing", "regression testing"],
        )
        self.assertEqual(must_have.get("missing") or [], [])
        risk_types = [str(item.get("type") or "") for item in (fit_breakdown.get("risk_flags") or []) if isinstance(item, dict)]
        self.assertIn("weak_must_have_evidence", risk_types)

    def test_culture_summary_is_normalized_to_operator_facing_fallback_when_llm_addresses_candidate(self) -> None:
        service = CandidateProfileService(
            db=self.db,
            matching_engine=self.matching,
            scoring_policy=self.scoring,
            llm_responder=_LLMResponder(
                "Mykola, based on what we know so far, your strong communication skills make you a great fit."
            ),
        )

        analysis = service._build_culture_agent_analysis(
            job={"title": "Senior QA Engineer"},
            candidate={"full_name": "Mykola B."},
            company_culture_profile={},
            values=["clear communication"],
            resume_links=[],
            chat_signal_lines=["I care about quality and clear bug reports."],
            interview_score=None,
            interview_signals={},
            alignment=["strong communication fit for distributed startup environment"],
            concerns=[],
            predictive_signals=[],
        )

        summary = str(analysis.get("summary") or "")
        self.assertTrue(summary.startswith("After reviewing Mykola B.'s communication"))
        self.assertNotIn(" your ", f" {summary.lower()} ")
        self.assertEqual(str(analysis.get("source") or ""), "fallback")


if __name__ == "__main__":
    unittest.main()
