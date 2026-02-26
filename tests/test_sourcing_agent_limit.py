from __future__ import annotations

import re
import unittest
from typing import Any, Dict, List

from tener_ai.agents import SourcingAgent


class _DuplicateHeavyProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 1), 100))
        shared_count = min(25, safe_limit)
        out: List[Dict[str, Any]] = []
        for idx in range(shared_count):
            out.append(
                {
                    "linkedin_id": f"shared-{idx}",
                    "full_name": f"Shared Candidate {idx}",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 5,
                    "raw": {"query": query},
                }
            )

        if safe_limit > 25:
            slug = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-") or "q"
            for idx in range(safe_limit - 25):
                out.append(
                    {
                        "linkedin_id": f"{slug}-extra-{idx}",
                        "full_name": f"{slug} Extra {idx}",
                        "headline": "Senior Backend Engineer",
                        "location": "Remote",
                        "languages": ["en"],
                        "skills": ["python", "go"],
                        "years_experience": 6,
                        "raw": {"query": query},
                    }
                )
        return out

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False}


class SourcingAgentLimitTests(unittest.TestCase):
    def test_find_candidates_can_reach_high_limit_with_duplicate_heavy_provider(self) -> None:
        agent = SourcingAgent(_DuplicateHeavyProvider())
        job = {
            "title": "Senior Backend Engineer",
            "location": "Germany",
            "jd_text": (
                "Need Python, Go, AWS, Docker, PostgreSQL, distributed systems, CI/CD, "
                "Kubernetes, microservices, observability, async processing, cloud infra."
            ),
        }

        out = agent.find_candidates(job=job, limit=80)
        self.assertEqual(len(out), 80)
        ids = [str(item.get("linkedin_id") or "") for item in out]
        self.assertEqual(len(set(ids)), 80)


if __name__ == "__main__":
    unittest.main()

