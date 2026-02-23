from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, parse, request


class LinkedInProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        raise NotImplementedError


class MockLinkedInProvider(LinkedInProvider):
    def __init__(self, dataset_path: str) -> None:
        self.dataset_path = dataset_path
        self._profiles = self._load_profiles()

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        q = (query or "").lower()
        scored = []
        for profile in self._profiles:
            text = " ".join(
                [
                    profile.get("headline", ""),
                    " ".join(profile.get("skills", [])),
                    profile.get("location", ""),
                ]
            ).lower()
            score = self._score(q, text)
            scored.append((score, profile))

        ranked = [p for score, p in sorted(scored, key=lambda x: x[0], reverse=True) if score > 0]
        if not ranked:
            ranked = self._profiles
        return ranked[: max(1, min(limit, 200))]

    def _load_profiles(self) -> List[Dict[str, Any]]:
        path = Path(self.dataset_path)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _score(query: str, text: str) -> float:
        if not query.strip():
            return 1.0
        tokens = [token for token in query.split() if len(token) > 2]
        if not tokens:
            return 1.0
        matched = sum(1 for token in tokens if token in text)
        return matched / len(tokens)


class UnipileLinkedInProvider(LinkedInProvider):
    def __init__(self, api_key: str, base_url: str = "https://api.unipile.com") -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        # NOTE: Endpoint path may vary by account plan and API version.
        # This is intentionally isolated in provider layer for quick updates.
        params = parse.urlencode({"q": query, "limit": max(1, min(limit, 100))})
        url = f"{self.base_url}/v1/linkedin/search?{params}"
        req = request.Request(url, headers={"Authorization": f"Bearer {self.api_key}"})

        try:
            with request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
                results = payload.get("results", [])
                return [self._normalize_profile(item) for item in results]
        except error.HTTPError as exc:
            raise RuntimeError(f"Unipile HTTP error: {exc.code}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Unipile network error: {exc.reason}") from exc

    @staticmethod
    def _normalize_profile(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "linkedin_id": item.get("id") or item.get("linkedin_id"),
            "full_name": item.get("full_name") or item.get("name") or "Unknown",
            "headline": item.get("headline") or "",
            "location": item.get("location") or "",
            "languages": item.get("languages") or ["en"],
            "skills": item.get("skills") or [],
            "years_experience": item.get("years_experience") or 0,
        }


def build_linkedin_provider(mock_dataset_path: str) -> LinkedInProvider:
    api_key = os.environ.get("UNIPILE_API_KEY")
    base_url = os.environ.get("UNIPILE_BASE_URL", "https://api.unipile.com")

    if api_key:
        return UnipileLinkedInProvider(api_key=api_key, base_url=base_url)
    return MockLinkedInProvider(dataset_path=mock_dataset_path)
