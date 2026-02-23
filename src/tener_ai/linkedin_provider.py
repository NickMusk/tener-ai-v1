from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, parse, request


class LinkedInProvider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
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

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {
            "provider": "mock",
            "sent": False,
            "reason": "mock_provider_no_external_delivery",
            "candidate": candidate_profile.get("linkedin_id"),
            "preview": message[:120],
        }

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
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.unipile.com",
        account_id: Optional[str] = None,
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.timeout_seconds = max(5, timeout_seconds)

        self.search_path = os.environ.get("UNIPILE_LINKEDIN_SEARCH_PATH", "/api/v1/users/search")
        self.chat_create_path = os.environ.get("UNIPILE_CHAT_CREATE_PATH", "/api/v1/chats")
        self.api_type = (os.environ.get("UNIPILE_LINKEDIN_API_TYPE") or "").strip()
        self.force_inmail = (os.environ.get("UNIPILE_LINKEDIN_INMAIL") or "").strip().lower() in {"1", "true", "yes"}
        self.dry_run = (os.environ.get("UNIPILE_DRY_RUN") or "").strip().lower() in {"1", "true", "yes"}

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        if not self.account_id:
            raise RuntimeError("UNIPILE_ACCOUNT_ID is required for Unipile search")

        limit = max(1, min(limit, 100))
        last_error = "unipile_search_unknown_error"
        for path in self._candidate_search_paths():
            endpoint = self._with_account_id(self._build_url(path), self.account_id)
            for method, payload, query_params in self._search_attempts(query=query, limit=limit):
                try:
                    if method == "GET":
                        url = f"{endpoint}{'&' if '?' in endpoint else '?'}{parse.urlencode(query_params)}"
                        response = self._request_json("GET", url)
                    else:
                        body = dict(payload or {})
                        if self.api_type:
                            body["api"] = self.api_type
                        response = self._request_json("POST", endpoint, body)

                    items = self._extract_results(response)
                    if not items:
                        continue

                    # /users/search may sometimes resolve as /users/{id} and return "search" placeholder.
                    if self._looks_like_search_placeholder(items):
                        continue

                    return [self._normalize_profile(item) for item in items]
                except RuntimeError as exc:
                    last_error = str(exc)
                    continue

        raise RuntimeError(f"Unipile search failed: {last_error}")

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        if self.dry_run:
            return {
                "provider": "unipile",
                "sent": False,
                "reason": "dry_run_enabled",
                "candidate": candidate_profile.get("linkedin_id"),
                "preview": message[:120],
            }

        if not self.account_id:
            return {"provider": "unipile", "sent": False, "reason": "missing_account_id"}

        attendee_id = self._extract_attendee_id(candidate_profile)
        if not attendee_id:
            return {"provider": "unipile", "sent": False, "reason": "missing_attendee_provider_id"}

        endpoint = self._build_url(self.chat_create_path)
        fields = {
            "account_id": self.account_id,
            "text": message,
            "attendees_ids": attendee_id,
        }
        if self.api_type:
            fields["linkedin[api]"] = self.api_type
        if self.force_inmail:
            fields["linkedin[inmail]"] = "true"

        response = self._request_multipart("POST", endpoint, fields)
        chat_id = (
            response.get("id")
            or response.get("chat_id")
            or (response.get("chat") or {}).get("id")
            or (response.get("data") or {}).get("id")
        )
        return {
            "provider": "unipile",
            "sent": True,
            "attendee_provider_id": attendee_id,
            "chat_id": chat_id,
        }

    def _extract_attendee_id(self, candidate_profile: Dict[str, Any]) -> Optional[str]:
        candidates = [
            candidate_profile.get("attendee_provider_id"),
            candidate_profile.get("unipile_profile_id"),
            candidate_profile.get("provider_id"),
            candidate_profile.get("linkedin_id"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _request_json(self, method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = None
        headers = self._headers_json()
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url=url, data=data, method=method, headers=headers)
        return self._execute_json(req)

    def _request_multipart(self, method: str, url: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        boundary = f"----TENER-{uuid.uuid4().hex}"
        body = self._encode_multipart(fields, boundary)
        headers = self._headers_json()
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        req = request.Request(url=url, data=body, method=method, headers=headers)
        return self._execute_json(req)

    def _execute_json(self, req: request.Request) -> Dict[str, Any]:
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except error.HTTPError as exc:
            body = self._safe_read_error(exc)
            raise RuntimeError(f"Unipile HTTP error {exc.code}: {body}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Unipile network error: {exc.reason}") from exc

    def _headers_json(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "X-API-KEY": self.api_key,
        }

    def _extract_results(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]

        for key in ("results", "items", "data", "profiles"):
            bucket = payload.get(key)
            if isinstance(bucket, list):
                return [x for x in bucket if isinstance(x, dict)]

        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("results", "items", "profiles"):
                bucket = data.get(key)
                if isinstance(bucket, list):
                    return [x for x in bucket if isinstance(x, dict)]

        if payload.get("object") == "UserProfile":
            return [payload]

        # Some envelope payloads may include provider_id metadata and an items list.
        # Treat as a single profile only when profile-like fields are present.
        if payload.get("provider_id") and any(
            payload.get(k) for k in ("full_name", "name", "headline", "first_name", "last_name", "skills")
        ):
            return [payload]

        return []

    def _candidate_search_paths(self) -> List[str]:
        candidates = [
            self.search_path,
            "/api/v1/linkedin/search",
            "/api/v1/search",
            "/api/v1/users",
        ]
        out: List[str] = []
        seen = set()
        for path in candidates:
            p = (path or "").strip()
            if not p:
                continue
            if not p.startswith("/"):
                p = f"/{p}"
            if p in seen:
                continue
            seen.add(p)
            out.append(p)
        return out

    def _search_attempts(self, query: str, limit: int) -> List[tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]]:
        return [
            ("POST", {"query": query, "limit": limit}, {}),
            ("POST", {"keywords": query, "limit": limit}, {}),
            ("POST", {"text": query, "limit": limit}, {}),
            ("GET", None, {"q": query, "limit": limit}),
            ("GET", None, {"query": query, "limit": limit}),
            ("GET", None, {"keywords": query, "limit": limit}),
            ("GET", None, {"text": query, "limit": limit}),
        ]

    @staticmethod
    def _looks_like_search_placeholder(items: List[Dict[str, Any]]) -> bool:
        if len(items) != 1:
            return False
        item = items[0]
        public_identifier = (item.get("public_identifier") or "").strip().lower()
        obj = (item.get("object") or "").strip()
        return obj == "UserProfile" and public_identifier == "search"

    def _normalize_profile(self, item: Dict[str, Any]) -> Dict[str, Any]:
        first_name = (item.get("first_name") or "").strip()
        last_name = (item.get("last_name") or "").strip()
        full_name = item.get("full_name") or item.get("name") or f"{first_name} {last_name}".strip() or "Unknown"

        profile_id = item.get("attendee_provider_id") or item.get("provider_id") or item.get("id") or item.get("linkedin_id")
        location = item.get("location") or item.get("geo") or ""
        languages = item.get("languages")
        if not isinstance(languages, list):
            languages = [item.get("language")] if item.get("language") else ["en"]

        skills = item.get("skills")
        if not isinstance(skills, list):
            skills = []

        years_experience = item.get("years_experience") or item.get("experience_years") or 0
        try:
            years_experience = int(years_experience)
        except (TypeError, ValueError):
            years_experience = 0

        normalized = {
            "linkedin_id": profile_id,
            "unipile_profile_id": profile_id,
            "attendee_provider_id": item.get("attendee_provider_id") or profile_id,
            "full_name": full_name,
            "headline": item.get("headline") or item.get("title") or "",
            "location": location,
            "languages": [str(x).lower() for x in languages if x],
            "skills": [str(x).lower() for x in skills if x],
            "years_experience": years_experience,
            "raw": item,
        }
        return normalized

    @staticmethod
    def _safe_read_error(exc: error.HTTPError) -> str:
        try:
            raw = exc.read().decode("utf-8")
            if raw:
                return raw[:400]
        except Exception:
            pass
        return "no_error_body"

    @staticmethod
    def _encode_multipart(fields: Dict[str, Any], boundary: str) -> bytes:
        lines: List[bytes] = []
        for key, value in fields.items():
            lines.append(f"--{boundary}".encode("utf-8"))
            lines.append(f'Content-Disposition: form-data; name="{key}"'.encode("utf-8"))
            lines.append(b"")
            lines.append(str(value).encode("utf-8"))
        lines.append(f"--{boundary}--".encode("utf-8"))
        lines.append(b"")
        return b"\r\n".join(lines)

    @staticmethod
    def _with_account_id(url: str, account_id: str) -> str:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}{parse.urlencode({'account_id': account_id})}"

    def _build_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        if not path_or_url.startswith("/"):
            path_or_url = f"/{path_or_url}"
        return f"{self.base_url}{path_or_url}"


def build_linkedin_provider(mock_dataset_path: str) -> LinkedInProvider:
    api_key = os.environ.get("UNIPILE_API_KEY")
    base_url = os.environ.get("UNIPILE_BASE_URL", "https://api.unipile.com")
    account_id = os.environ.get("UNIPILE_ACCOUNT_ID")
    timeout_seconds_raw = os.environ.get("UNIPILE_TIMEOUT_SECONDS", "30")

    try:
        timeout_seconds = int(timeout_seconds_raw)
    except ValueError:
        timeout_seconds = 30

    if api_key:
        return UnipileLinkedInProvider(
            api_key=api_key,
            base_url=base_url,
            account_id=account_id,
            timeout_seconds=timeout_seconds,
        )
    return MockLinkedInProvider(dataset_path=mock_dataset_path)
