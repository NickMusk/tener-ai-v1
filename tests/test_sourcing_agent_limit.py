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


class _FlakyProvider:
    def __init__(self) -> None:
        self.calls = 0

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("transient unipile error")
        return [
            {
                "linkedin_id": f"candidate-{self.calls}",
                "full_name": f"Candidate {self.calls}",
                "headline": "Manual QA Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["qa"],
                "years_experience": 3,
                "raw": {"query": query},
            }
        ]

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False}


class _AlwaysFailProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        raise RuntimeError("provider unavailable")

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"sent": False}


class _MatchingEngineStub:
    def build_core_profile(self, job: Dict[str, Any], max_skills: int = 6) -> Dict[str, Any]:
        return {
            "title": str(job.get("title") or ""),
            "target_seniority": str(job.get("seniority") or "middle"),
            "core_skills": ["manual testing", "api testing", "regression"],
            "location": job.get("location"),
            "preferred_languages": job.get("preferred_languages") or [],
        }


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

    def test_find_candidates_continues_after_single_query_failure(self) -> None:
        agent = SourcingAgent(_FlakyProvider())
        job = {
            "title": "Manual QA Engineer",
            "location": "Remote",
            "jd_text": "Manual testing, API checks, regression, bug reports.",
        }

        out = agent.find_candidates(job=job, limit=3)
        self.assertGreaterEqual(len(out), 1)

    def test_find_candidates_raises_when_all_queries_fail(self) -> None:
        agent = SourcingAgent(_AlwaysFailProvider())
        job = {
            "title": "Manual QA Engineer",
            "location": "Remote",
            "jd_text": "Manual testing, API checks, regression, bug reports.",
        }

        with self.assertRaises(RuntimeError):
            agent.find_candidates(job=job, limit=3)

    def test_build_search_preview_uses_title_plus_structured_filters(self) -> None:
        agent = SourcingAgent(_DuplicateHeavyProvider(), matching_engine=_MatchingEngineStub())
        job = {
            "title": "Manual QA Engineer",
            "company": "Tener.ai",
            "location": "Remote",
            "seniority": "junior",
            "preferred_languages": ["en", "ru"],
            "jd_text": (
                "About Tener.ai: our AI recruiting platform transforms delivery cycles. "
                "Need manual testing, api testing, regression and bug triage."
            ),
        }

        preview = agent.build_search_preview(job)
        self.assertEqual(preview.get("primary_query"), "Manual QA Engineer")
        self.assertEqual((preview.get("filters") or {}).get("location"), "Remote")
        self.assertEqual((preview.get("filters") or {}).get("profile_language"), ["en", "ru"])
        self.assertEqual((preview.get("filters") or {}).get("skills"), ["manual testing", "api testing", "regression"])
        fallback_queries = preview.get("fallback_queries") or []
        self.assertGreaterEqual(len(fallback_queries), 1)
        self.assertLessEqual(len(fallback_queries), 4)
        self.assertEqual(fallback_queries[0], "Manual QA Engineer")
        self.assertFalse(any("about tener.ai" in str(item).lower() for item in fallback_queries))


if __name__ == "__main__":
    unittest.main()
