from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib import error, parse, request


class InterviewAPIClient:
    """HTTP client for the isolated interview module."""

    def __init__(self, base_url: str, timeout_seconds: int = 20) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds or 20))

    def available(self) -> bool:
        return bool(self.base_url)

    def start_session(
        self,
        job_id: int,
        candidate_id: int,
        candidate_name: str,
        conversation_id: int,
        language: str = "en",
        candidate_email: Optional[str] = None,
        ttl_hours: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "job_id": int(job_id),
            "candidate_id": int(candidate_id),
            "candidate_name": str(candidate_name or "").strip() or None,
            "conversation_id": int(conversation_id),
            "language": str(language or "en").strip().lower() or "en",
        }
        if candidate_email:
            payload["candidate_email"] = str(candidate_email).strip()
        if ttl_hours is not None:
            payload["ttl_hours"] = int(ttl_hours)
        return self._request_json("POST", "/api/interviews/sessions/start", payload)

    def get_session(self, session_id: str) -> Dict[str, Any]:
        sid = parse.quote(str(session_id or "").strip(), safe="")
        return self._request_json("GET", f"/api/interviews/sessions/{sid}", None)

    def refresh_session(self, session_id: str, force: bool = False) -> Dict[str, Any]:
        sid = parse.quote(str(session_id or "").strip(), safe="")
        return self._request_json(
            "POST",
            f"/api/interviews/sessions/{sid}/refresh",
            {"force": bool(force)},
        )

    def list_sessions(
        self,
        job_id: int | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"limit": max(1, min(int(limit or 100), 500))}
        if job_id is not None:
            query["job_id"] = int(job_id)
        if status:
            query["status"] = str(status).strip()
        qs = parse.urlencode(query)
        return self._request_json("GET", f"/api/interviews/sessions?{qs}", None)

    def get_scorecard(self, session_id: str) -> Dict[str, Any]:
        sid = parse.quote(str(session_id or "").strip(), safe="")
        return self._request_json("GET", f"/api/interviews/sessions/{sid}/scorecard", None)

    def _request_json(self, method: str, path: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not self.base_url:
            raise RuntimeError("Interview API base URL is not configured")
        if not path.startswith("/"):
            path = f"/{path}"
        url = f"{self.base_url}{path}"
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url=url, data=body, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            err = self._read_error_body(exc)
            raise RuntimeError(f"Interview API HTTP {exc.code}: {err}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Interview API network error: {exc.reason}") from exc

        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Interview API returned non-JSON response") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("Interview API returned invalid payload")
        return parsed

    @staticmethod
    def _read_error_body(exc: error.HTTPError) -> str:
        try:
            raw = exc.read().decode("utf-8")
        except Exception:
            raw = ""
        return raw[:400] if raw else str(exc.reason or "request failed")
