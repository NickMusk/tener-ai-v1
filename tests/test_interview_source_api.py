from __future__ import annotations

import unittest
from typing import Any, Dict

from tener_interview.source_api import SourceAPIClient


class _FakeSourceAPI(SourceAPIClient):
    def __init__(self, scripted: Dict[str, Dict[str, Any]]) -> None:
        super().__init__(base_url="https://example.test", timeout_seconds=5)
        self.scripted = scripted

    def _get_json(self, path: str) -> Dict[str, Any]:
        payload = self.scripted.get(path)
        if payload is None:
            self._last_error = "not found"
            return {}
        self._last_error = ""
        return payload


class SourceAPIClientTests(unittest.TestCase):
    def test_list_jobs(self) -> None:
        client = _FakeSourceAPI(
            {
                "/api/jobs?limit=200": {
                    "items": [
                        {"id": 2, "title": "ML Engineer"},
                        {"id": 1, "title": "Data Engineer"},
                    ]
                }
            }
        )
        out = client.list_jobs(limit=200)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["id"], 2)
        self.assertEqual(out[0]["title"], "ML Engineer")
        self.assertTrue(client.status()["available"])

    def test_list_candidates_mapping(self) -> None:
        client = _FakeSourceAPI(
            {
                "/api/jobs/5/candidates?limit=300": {
                    "items": [
                        {
                            "candidate_id": 42,
                            "score": 0.95,
                            "status": "verified",
                            "full_name": "Jane Doe",
                            "headline": "Backend",
                            "location": "Berlin",
                            "languages": ["en"],
                            "skills": ["python"],
                            "years_experience": 7,
                            "linkedin_id": "ln_42",
                        }
                    ]
                }
            }
        )
        out = client.list_candidates_for_job(job_id=5, limit=300)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["job_id"], 5)
        self.assertEqual(out[0]["candidate_id"], 42)
        self.assertEqual(out[0]["candidate_name"], "Jane Doe")
        self.assertEqual(out[0]["match_status"], "verified")

    def test_error_returns_empty(self) -> None:
        client = _FakeSourceAPI(scripted={})
        out = client.list_jobs(limit=10)
        self.assertEqual(out, [])
        status = client.status()
        self.assertFalse(status["available"])
        self.assertIn("not found", str(status["last_error"]))


if __name__ == "__main__":
    unittest.main()
