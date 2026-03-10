from __future__ import annotations

import unittest

from tener_ai.candidate_scoring import CandidateScoringPolicy


class CandidateScoringPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = CandidateScoringPolicy(path=None)

    @staticmethod
    def _scorecard(
        source: float | None,
        communication: float | None,
        interview: float | None,
        communication_status: str = "in_dialogue",
        communication_stage: str = "dialogue",
    ):
        return {
            "sourcing_vetting": {"latest_score": source, "latest_status": "qualified" if source is not None else "not_started"},
            "communication": {
                "latest_score": communication,
                "latest_status": communication_status,
                "latest_stage": communication_stage,
            },
            "interview_evaluation": {"latest_score": interview, "latest_status": "scored" if interview is not None else "not_started"},
        }

    def test_shortlist_when_all_stages_scored(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=90.0, communication=80.0, interview=85.0),
            current_status_key="cv_received",
            row={},
        )
        self.assertEqual(out["overall_status"], "shortlist")
        self.assertGreaterEqual(float(out["overall_score"]), 80.0)

    def test_blocked_when_candidate_not_interested(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=95.0, communication=90.0, interview=88.0, communication_status="not_interested"),
            current_status_key="not_interested",
            row={},
        )
        self.assertEqual(out["overall_status"], "blocked")
        self.assertEqual(float(out["overall_score"]), 0.0)

    def test_overall_is_na_without_all_three_scores(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=95.0, communication=95.0, interview=None),
            current_status_key="in_dialogue",
            row={},
        )
        self.assertIsNone(out["overall_score"])
        self.assertEqual(out["overall_status"], "review")
        self.assertIn("cap_without_cv", out["gates_applied"])

    def test_overall_is_na_without_interview_score_even_with_cv_status(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=100.0, communication=100.0, interview=None),
            current_status_key="cv_received",
            row={},
        )
        self.assertIsNone(out["overall_score"])
        self.assertEqual(out["overall_status"], "review")
        self.assertIn("cap_without_interview_score", out["gates_applied"])

    def test_all_three_scores_use_weighted_average_even_without_cv_status(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=90.0, communication=80.0, interview=70.0),
            current_status_key="in_dialogue",
            row={},
        )
        self.assertEqual(float(out["overall_score"]), 81.0)
        self.assertTrue(bool(out["has_all_scores"]))
        self.assertNotIn("cap_without_cv", out["gates_applied"])

    def test_communication_score_is_na_until_dialogue_stage(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(
                source=90.0,
                communication=88.0,
                interview=86.0,
                communication_stage="outreach",
            ),
            current_status_key="in_dialogue",
            row={},
        )
        inputs = out.get("inputs") if isinstance(out.get("inputs"), dict) else {}
        self.assertIsNone(inputs.get("communication"))
        self.assertIsNone(out["overall_score"])

    def test_salary_fit_affects_score(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=90.0, communication=85.0, interview=90.0),
            current_status_key="cv_received",
            row={
                "job_salary_max": 150000,
                "job_salary_currency": "USD",
                "candidate_prescreen_salary_expectation_min": 170000,
                "candidate_prescreen_salary_expectation_currency": "USD",
            },
        )
        self.assertEqual((out.get("salary_fit") or {}).get("status"), "slightly_above")
        self.assertIn("salary_fit:slightly_above", out.get("gates_applied") or [])
        self.assertLess(float(out.get("overall_score") or 0.0), 89.0)


if __name__ == "__main__":
    unittest.main()
