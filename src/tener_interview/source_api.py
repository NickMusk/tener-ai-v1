from __future__ import annotations

import json
from typing import Any, Dict, List
from urllib import error, request


class SourceAPIClient:
    """Read jobs/candidates from external Tener API."""

    def __init__(self, base_url: str, timeout_seconds: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds))
        self._last_error: str = ""

    def status(self) -> Dict[str, Any]:
        return {
            "type": "api",
            "base_url": self.base_url,
            "available": self._last_error == "",
            "last_error": self._last_error or None,
        }

    def list_jobs(self, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        payload = self._get_json(f"/api/jobs?limit={safe_limit}")
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        return [x for x in items if isinstance(x, dict)]

    def list_candidates_for_job(self, job_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 2000))
        payload = self._get_json(f"/api/jobs/{int(job_id)}/candidates?limit={safe_limit}")
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        out: List[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "job_id": int(job_id),
                    "candidate_id": item.get("candidate_id"),
                    "match_score": item.get("score"),
                    "match_status": item.get("status"),
                    "candidate_name": item.get("full_name"),
                    "headline": item.get("headline"),
                    "location": item.get("location"),
                    "languages": item.get("languages"),
                    "skills": item.get("skills"),
                    "years_experience": item.get("years_experience"),
                    "linkedin_id": item.get("linkedin_id"),
                }
            )
        return out

    def _get_json(self, path: str) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        req = request.Request(
            url=url,
            method="GET",
            headers={"Accept": "application/json"},
        )

        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            body_raw = exc.read().decode("utf-8") if exc.fp else ""
            self._last_error = f"HTTP {exc.code}: {body_raw or exc.reason or 'http error'}"
            return {}
        except error.URLError as exc:
            self._last_error = f"Network error: {exc.reason}"
            return {}
        except Exception as exc:
            self._last_error = str(exc)
            return {}

        if not raw:
            self._last_error = "Empty response body"
            return {}

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            self._last_error = "Non-JSON response"
            return {}

        if not isinstance(parsed, dict):
            self._last_error = "Invalid JSON payload"
            return {}

        self._last_error = ""
        return parsed

