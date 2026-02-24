from __future__ import annotations

import unittest

from tener_ai.candidate_scoring import CandidateScoringPolicy


class CandidateScoringPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = CandidateScoringPolicy(path=None)

    @staticmethod
    def _scorecard(source: float | None, communication: float | None, interview: float | None, communication_status: str = "in_dialogue"):
        return {
            "sourcing_vetting": {"latest_score": source, "latest_status": "qualified" if source is not None else "not_started"},
            "communication": {"latest_score": communication, "latest_status": communication_status},
            "interview_evaluation": {"latest_score": interview, "latest_status": "scored" if interview is not None else "not_started"},
        }

    def test_shortlist_when_all_stages_scored(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=90.0, communication=80.0, interview=85.0),
            current_status_key="cv_received",
        )
        self.assertEqual(out["overall_status"], "shortlist")
        self.assertGreaterEqual(float(out["overall_score"]), 80.0)

    def test_blocked_when_candidate_not_interested(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=95.0, communication=90.0, interview=88.0, communication_status="not_interested"),
            current_status_key="not_interested",
        )
        self.assertEqual(out["overall_status"], "blocked")
        self.assertEqual(float(out["overall_score"]), 0.0)

    def test_cap_without_cv(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=95.0, communication=95.0, interview=95.0),
            current_status_key="in_dialogue",
        )
        self.assertLessEqual(float(out["overall_score"]), 70.0)
        self.assertIn("cap_without_cv", out["gates_applied"])

    def test_cap_without_interview_score(self) -> None:
        out = self.policy.compute_overall(
            scorecard=self._scorecard(source=100.0, communication=100.0, interview=None),
            current_status_key="cv_received",
        )
        self.assertLessEqual(float(out["overall_score"]), 80.0)
        self.assertIn("cap_without_interview_score", out["gates_applied"])


if __name__ == "__main__":
    unittest.main()
