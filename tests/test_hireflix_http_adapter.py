from __future__ import annotations

import unittest
from typing import Any, Dict, Optional

from tener_interview.providers import HireflixConfig
from tener_interview.providers.hireflix_http import HireflixHTTPAdapter


class _FakeAdapter(HireflixHTTPAdapter):
    def __init__(self, config: HireflixConfig, scripted_responses: list[Dict[str, Any]]) -> None:
        super().__init__(config)
        self.scripted = list(scripted_responses)
        self.calls: list[Dict[str, Any]] = []

    def _graphql(self, *, query: str, variables: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        self.calls.append({"query": query, "variables": variables})
        if not self.scripted:
            raise ValueError("no scripted response")
        return self.scripted.pop(0)


class HireflixHttpAdapterTests(unittest.TestCase):
    def test_synthetic_email_is_unique_per_invite(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[],
        )
        one = adapter._synthetic_email({"candidate_id": 42})
        two = adapter._synthetic_email({"candidate_id": 42})
        self.assertNotEqual(one, two)
        self.assertIn("hireflix-42-", one)
        self.assertTrue(one.endswith("@interview.local"))

    def test_create_assessment_with_position_save_mutation(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id=""),
            scripted_responses=[
                {
                    "data": {
                        "Position": {
                            "save": {
                                "id": "pos_new_1",
                                "name": "Acme Interview",
                            }
                        }
                    }
                }
            ],
        )

        out = adapter.create_assessment(
            {
                "assessment_name": "Acme Interview",
                "questions": [
                    {
                        "title": "At Acme, describe your project",
                        "description": "Context and measurable outcome",
                        "timeToAnswer": 120,
                        "timeToThink": 10,
                        "retakes": 1,
                    }
                ],
                "language": "en",
            }
        )

        self.assertEqual(out["assessment_id"], "pos_new_1")
        self.assertEqual(out["assessment_name"], "Acme Interview")
        self.assertIn("save(position", adapter.calls[0]["query"])

    def test_create_invitation_with_new_mutation(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[
                {
                    "data": {
                        "inviteCandidateToInterview": {
                            "__typename": "InterviewType",
                            "id": "int_1",
                            "hash": "hash_1",
                            "url": {"public": "https://app.hireflix.com/hash_1"},
                        }
                    }
                },
                {
                    "data": {
                        "interview": {
                            "id": "int_1",
                            "status": "pending",
                            "hash": "hash_1",
                            "url": {"public": "https://app.hireflix.com/hash_1"},
                            "candidate": {"email": "jane@example.com"},
                        }
                    }
                },
            ],
        )

        out = adapter.create_invitation(
            {
                "candidate_name": "Jane Doe",
                "candidate_email": "jane@example.com",
                "candidate_id": 42,
            }
        )

        self.assertEqual(out["invitation_id"], "int_1")
        self.assertEqual(out["assessment_id"], "pos_1")
        self.assertEqual(out["candidate_id"], "jane@example.com")
        self.assertEqual(out["interview_url"], "https://app.hireflix.com/hash_1")

    def test_create_invitation_falls_back_to_legacy_mutation(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1", allow_legacy_invite_fallback=True),
            scripted_responses=[
                {"data": {"inviteCandidateToInterview": {"__typename": "ExceededInvitesThisPeriodError"}}},
                {
                    "data": {
                        "Position": {
                            "invite": {
                                "id": "int_legacy",
                                "hash": "hash_l",
                                "url": {"public": "https://app.hireflix.com/hash_l"},
                            }
                        }
                    }
                },
                {
                    "data": {
                        "interview": {
                            "id": "int_legacy",
                            "status": "pending",
                            "hash": "hash_l",
                            "url": {"public": "https://app.hireflix.com/hash_l"},
                        }
                    }
                },
            ],
        )

        out = adapter.create_invitation(
            {
                "candidate_name": "Fallback",
                "candidate_email": "fallback@example.com",
            }
        )

        self.assertEqual(out["invitation_id"], "int_legacy")
        self.assertIn("hash_l", out["interview_url"])

    def test_create_invitation_without_legacy_fallback_raises_new_error(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1", allow_legacy_invite_fallback=False),
            scripted_responses=[
                {"data": {"inviteCandidateToInterview": {"__typename": "ExceededInvitesThisPeriodError"}}},
            ],
        )

        with self.assertRaises(ValueError) as ctx:
            adapter.create_invitation(
                {
                    "candidate_name": "Fallback",
                    "candidate_email": "fallback@example.com",
                }
            )
        self.assertIn("inviteCandidateToInterview failed", str(ctx.exception))

    def test_status_mapping(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[
                {
                    "data": {
                        "interview": {
                            "id": "int_1",
                            "status": "pending",
                            "answered": False,
                            "hash": "h1",
                        }
                    }
                }
            ],
        )
        out = adapter.get_interview_status("int_1")
        self.assertEqual(out["status"], "invited")

        adapter2 = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[
                {
                    "data": {
                        "interview": {
                            "id": "int_2",
                            "status": "completed",
                            "answered": True,
                            "hash": "h2",
                        }
                    }
                }
            ],
        )
        out2 = adapter2.get_interview_status("int_2")
        self.assertEqual(out2["status"], "completed")

    def test_result_uses_global_score_for_dimensions(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[
                {
                    "data": {
                        "interview": {
                            "id": "int_1",
                            "status": "completed",
                            "score": {"value": 82},
                            "questions": [
                                {"id": "q1", "title": "Python", "answer": {"id": "a1"}},
                                {"id": "q2", "title": "Communication", "answer": {"id": "a2"}},
                            ],
                        }
                    }
                }
            ],
        )

        out = adapter.get_interview_result("int_1")
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["scores"]["technical"], 82.0)
        self.assertEqual(out["scores"]["soft_skills"], 82.0)

    def test_result_fallback_from_completion_ratio(self) -> None:
        adapter = _FakeAdapter(
            HireflixConfig(api_key="k", position_id="pos_1"),
            scripted_responses=[
                {
                    "data": {
                        "interview": {
                            "id": "int_1",
                            "status": "completed",
                            "score": {"value": None},
                            "questions": [
                                {"id": "q1", "title": "Question 1", "answer": {"id": "a1"}},
                                {"id": "q2", "title": "Question 2", "answer": {}},
                            ],
                        }
                    }
                }
            ],
        )

        out = adapter.get_interview_result("int_1")
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["scores"]["technical"], 50.0)


if __name__ == "__main__":
    unittest.main()
