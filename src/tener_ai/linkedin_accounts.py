from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib import error, parse, request
from uuid import uuid4

from .db import Database


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class LinkedInAccountService:
    def __init__(
        self,
        db: Database,
        *,
        provider: str = "unipile",
        api_key: str = "",
        base_url: str = "https://api.unipile.com",
        timeout_seconds: int = 20,
        state_secret: str = "",
        state_ttl_seconds: int = 900,
        connect_url_template: str = "",
        callback_url: str = "",
        accounts_path: str = "/api/v1/accounts",
        hosted_connect_path: str = "/api/v1/hosted/accounts/linkedin",
        disconnect_path_template: str = "/api/v1/accounts/{account_id}",
    ) -> None:
        self.db = db
        self.provider = str(provider or "unipile").strip().lower() or "unipile"
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "https://api.unipile.com").strip().rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds or 20))
        self.state_secret = str(state_secret or "").strip() or self._build_default_state_secret()
        self.state_ttl_seconds = max(60, int(state_ttl_seconds or 900))
        self.connect_url_template = str(connect_url_template or "").strip()
        self.callback_url = str(callback_url or "").strip()
        self.accounts_path = str(accounts_path or "/api/v1/accounts").strip() or "/api/v1/accounts"
        self.hosted_connect_path = (
            str(hosted_connect_path or "/api/v1/hosted/accounts/linkedin").strip() or "/api/v1/hosted/accounts/linkedin"
        )
        self.disconnect_path_template = (
            str(disconnect_path_template or "/api/v1/accounts/{account_id}").strip() or "/api/v1/accounts/{account_id}"
        )

    def start_connect(self, *, callback_url: str, label: str = "") -> Dict[str, Any]:
        session_id = f"lnk-{uuid4().hex}"
        nonce = uuid4().hex
        expires_at = datetime.now(UTC) + timedelta(seconds=self.state_ttl_seconds)
        state_payload = {
            "sid": session_id,
            "nonce": nonce,
            "exp": int(expires_at.timestamp()),
            "provider": self.provider,
        }
        state = self._sign_state(state_payload)
        connect_url = self._build_connect_url(state=state, callback_url=callback_url, label=label)
        self.db.create_linkedin_onboarding_session(
            session_id=session_id,
            provider=self.provider,
            state_nonce=nonce,
            state_expires_at=expires_at.isoformat(),
            redirect_uri=callback_url,
            connect_url=connect_url,
            metadata={"label": label},
        )
        return {
            "session_id": session_id,
            "provider": self.provider,
            "state_expires_at": expires_at.isoformat(),
            "connect_url": connect_url,
            "callback_url": callback_url,
        }

    def complete_connect_callback(self, *, query: Dict[str, List[str]]) -> Dict[str, Any]:
        raw_state = self._first_query_value(query, "state")
        if not raw_state:
            return {"status": "error", "reason": "missing_state"}

        try:
            payload = self._verify_state(raw_state)
        except ValueError as exc:
            return {"status": "error", "reason": "invalid_state", "details": str(exc)}

        session_id = str(payload.get("sid") or "").strip()
        nonce = str(payload.get("nonce") or "").strip()
        if not session_id or not nonce:
            return {"status": "error", "reason": "invalid_state_payload"}

        session = self.db.get_linkedin_onboarding_session(session_id)
        if not session:
            return {"status": "error", "reason": "session_not_found"}
        if str(session.get("state_nonce") or "") != nonce:
            return {"status": "error", "reason": "state_nonce_mismatch"}

        state_expires_at = self._parse_iso(str(session.get("state_expires_at") or ""))
        if state_expires_at and datetime.now(UTC) > state_expires_at:
            self.db.update_linkedin_onboarding_session_status(
                session_id=session_id,
                status="expired",
                error="state_expired",
            )
            return {"status": "error", "reason": "state_expired"}

        current_status = str(session.get("status") or "").strip().lower()
        if current_status in {"completed", "connected"}:
            provider_account_id = str(session.get("provider_account_id") or "").strip()
            account = (
                self.db.get_linkedin_account_by_provider_account_id(provider_account_id)
                if provider_account_id
                else None
            )
            return {
                "status": "already_completed",
                "session_id": session_id,
                "provider_account_id": provider_account_id or None,
                "account": account,
            }

        callback_error = self._first_query_value(query, "error") or self._first_query_value(query, "status")
        if callback_error and callback_error.lower() in {"error", "failed", "denied", "cancelled", "canceled"}:
            error_description = self._first_query_value(query, "error_description")
            error_reason = error_description or callback_error
            self.db.update_linkedin_onboarding_session_status(
                session_id=session_id,
                status="failed",
                error=error_reason[:400],
                metadata={"callback_query": self._flatten_query(query)},
            )
            return {"status": "error", "reason": "provider_auth_failed", "details": error_reason}

        provider_account_id = (
            self._first_query_value(query, "account_id")
            or self._first_query_value(query, "accountId")
            or self._first_query_value(query, "provider_account_id")
            or self._first_query_value(query, "unipile_account_id")
            or self._first_query_value(query, "id")
        )
        if not provider_account_id:
            self.db.update_linkedin_onboarding_session_status(
                session_id=session_id,
                status="failed",
                error="missing_account_id_in_callback",
                metadata={"callback_query": self._flatten_query(query)},
            )
            return {"status": "error", "reason": "missing_account_id_in_callback"}

        label = str((session.get("metadata") or {}).get("label") or "").strip() if isinstance(session.get("metadata"), dict) else ""
        account_id = self.db.upsert_linkedin_account(
            provider=self.provider,
            provider_account_id=provider_account_id,
            status="connected",
            label=label or None,
            metadata={"source": "oauth_callback", "callback_query": self._flatten_query(query)},
            connected_at=utc_now_iso(),
            last_synced_at=utc_now_iso(),
        )
        self.db.update_linkedin_onboarding_session_status(
            session_id=session_id,
            status="completed",
            provider_account_id=provider_account_id,
            error=None,
            metadata={"callback_query": self._flatten_query(query)},
        )
        account = self.db.get_linkedin_account(account_id)
        return {
            "status": "connected",
            "session_id": session_id,
            "provider_account_id": provider_account_id,
            "account": account,
        }

    def list_accounts(self, *, status: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
        normalized_status = str(status or "").strip().lower() or None
        items = self.db.list_linkedin_accounts(limit=limit, status=normalized_status)
        return {"provider": self.provider, "items": items}

    def sync_accounts(self, *, account_id: Optional[int] = None) -> Dict[str, Any]:
        if not self.api_key:
            return {"status": "error", "reason": "unipile_api_key_missing", "updated": 0, "items": []}

        remote_items = self._fetch_remote_accounts()
        normalized = [self._normalize_remote_account(x) for x in remote_items]
        normalized = [x for x in normalized if x.get("provider_account_id")]
        if account_id is not None:
            local = self.db.get_linkedin_account(account_id)
            if not local:
                return {"status": "error", "reason": "account_not_found", "updated": 0, "items": []}
            target_provider_id = str(local.get("provider_account_id") or "").strip()
            normalized = [x for x in normalized if str(x.get("provider_account_id") or "").strip() == target_provider_id]

        updated = 0
        items: List[Dict[str, Any]] = []
        for entry in normalized:
            row_id = self.db.upsert_linkedin_account(
                provider=self.provider,
                provider_account_id=str(entry["provider_account_id"]),
                status=str(entry.get("status") or "connected"),
                label=(str(entry.get("label") or "").strip() or None),
                provider_user_id=(str(entry.get("provider_user_id") or "").strip() or None),
                metadata={"remote": entry.get("raw") or {}, "source": "sync"},
                last_synced_at=utc_now_iso(),
            )
            updated += 1
            row = self.db.get_linkedin_account(row_id)
            if row:
                items.append(row)
        return {"status": "ok", "provider": self.provider, "updated": updated, "items": items}

    def disconnect_account(self, *, account_id: int, remote_disable: bool = False) -> Dict[str, Any]:
        row = self.db.get_linkedin_account(account_id)
        if not row:
            return {"status": "error", "reason": "account_not_found"}

        provider_account_id = str(row.get("provider_account_id") or "").strip()
        remote_out: Dict[str, Any] = {"attempted": False}
        if remote_disable and provider_account_id and self.api_key:
            remote_out = self._disable_remote_account(provider_account_id)

        self.db.update_linkedin_account_status(
            account_id=account_id,
            status="disconnected",
            metadata={"disconnected_at": utc_now_iso(), "remote": remote_out},
        )
        account = self.db.get_linkedin_account(account_id)
        return {
            "status": "ok",
            "account": account,
            "remote": remote_out,
        }

    def _build_connect_url(self, *, state: str, callback_url: str, label: str) -> str:
        template = self.connect_url_template
        if template:
            encoded_state = parse.quote(state, safe="")
            encoded_callback = parse.quote(callback_url, safe="")
            out = template.replace("{state}", encoded_state).replace("{redirect_uri}", encoded_callback).replace(
                "{callback_url}", encoded_callback
            )
            if "{label}" in out:
                out = out.replace("{label}", parse.quote(label or "", safe=""))
            return out

        return self._create_hosted_connect_url(state=state, callback_url=callback_url, label=label)

    def _create_hosted_connect_url(self, *, state: str, callback_url: str, label: str) -> str:
        endpoint = self._build_url(self.hosted_connect_path)
        attempts = [
            {
                "provider": "LINKEDIN",
                "type": "create",
                "state": state,
                "redirect_url": callback_url,
            },
            {
                "provider": "linkedin",
                "state": state,
                "success_redirect_url": callback_url,
                "failure_redirect_url": callback_url,
            },
            {
                "provider": "linkedin",
                "state": state,
                "redirect_uri": callback_url,
            },
        ]
        if label:
            for payload in attempts:
                payload["name"] = label

        last_error = "hosted_connect_url_not_available"
        for payload in attempts:
            try:
                out = self._request_json("POST", endpoint, payload)
            except RuntimeError as exc:
                last_error = str(exc)
                continue
            for candidate in (
                out.get("url"),
                out.get("link"),
                out.get("auth_url"),
                out.get("hosted_url"),
                (out.get("data") or {}).get("url") if isinstance(out.get("data"), dict) else None,
                (out.get("data") or {}).get("link") if isinstance(out.get("data"), dict) else None,
            ):
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        raise RuntimeError(last_error)

    def _fetch_remote_accounts(self) -> List[Dict[str, Any]]:
        endpoint = self._build_url(self.accounts_path)
        out = self._request_json("GET", endpoint, None)
        if isinstance(out, list):
            return [x for x in out if isinstance(x, dict)]
        if isinstance(out.get("items"), list):
            return [x for x in out["items"] if isinstance(x, dict)]
        data = out.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        return []

    def _normalize_remote_account(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider_account_id = (
            payload.get("id")
            or payload.get("account_id")
            or payload.get("accountId")
            or payload.get("provider_account_id")
        )
        label = payload.get("name") or payload.get("label") or payload.get("title")
        provider_user_id = payload.get("user_id") or payload.get("provider_user_id")
        status_raw = str(payload.get("status") or payload.get("state") or "").strip().lower()
        status = "connected"
        if status_raw in {"disconnected", "revoked", "disabled"}:
            status = "disconnected"
        elif status_raw in {"pending", "connecting", "authorizing"}:
            status = "pending"
        elif status_raw in {"error", "failed"}:
            status = "error"
        return {
            "provider_account_id": str(provider_account_id or "").strip(),
            "label": str(label or "").strip(),
            "provider_user_id": str(provider_user_id or "").strip(),
            "status": status,
            "raw": payload,
        }

    def _disable_remote_account(self, provider_account_id: str) -> Dict[str, Any]:
        endpoint = self._build_url(
            self.disconnect_path_template.replace("{account_id}", parse.quote(provider_account_id, safe=""))
        )
        try:
            out = self._request_json("DELETE", endpoint, None)
            return {"attempted": True, "status": "ok", "response": out}
        except RuntimeError as exc:
            return {"attempted": True, "status": "error", "error": str(exc)}

    def _request_json(self, method: str, url: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        data = None
        headers = self._headers_json()
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(url=url, data=data, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return {}
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                if isinstance(parsed, list):
                    return {"items": parsed}
                return {}
        except error.HTTPError as exc:
            body = self._safe_read_error(exc)
            raise RuntimeError(f"Unipile HTTP error {exc.code}: {body}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Unipile network error: {exc.reason}") from exc

    def _headers_json(self) -> Dict[str, str]:
        if not self.api_key:
            return {"Accept": "application/json"}
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "X-API-KEY": self.api_key,
        }

    def _build_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        path = path_or_url if path_or_url.startswith("/") else f"/{path_or_url}"
        return f"{self.base_url}{path}"

    @staticmethod
    def _safe_read_error(exc: error.HTTPError) -> str:
        try:
            raw = exc.read().decode("utf-8")
        except Exception:
            raw = ""
        return raw[:400] if raw else str(exc.reason or "request failed")

    @staticmethod
    def _first_query_value(query: Dict[str, List[str]], key: str) -> str:
        values = query.get(key)
        if not values:
            return ""
        for item in values:
            text = str(item or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _flatten_query(query: Dict[str, List[str]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, values in query.items():
            if not values:
                out[key] = ""
            elif len(values) == 1:
                out[key] = values[0]
            else:
                out[key] = values
        return out

    @staticmethod
    def _parse_iso(raw: str) -> Optional[datetime]:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def _build_default_state_secret(self) -> str:
        seed = os.environ.get("UNIPILE_API_KEY", "").strip() or "dev-linkedin-state-secret"
        return hashlib.sha256(f"{seed}|{self.provider}".encode("utf-8")).hexdigest()

    def _sign_state(self, payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        sig = hmac.new(self.state_secret.encode("utf-8"), raw, hashlib.sha256).digest()
        return f"{self._b64(raw)}.{self._b64(sig)}"

    def _verify_state(self, token: str) -> Dict[str, Any]:
        text = str(token or "").strip()
        if "." not in text:
            raise ValueError("state_format_invalid")
        payload_part, sig_part = text.split(".", 1)
        raw = self._b64_decode(payload_part)
        actual_sig = self._b64_decode(sig_part)
        expected_sig = hmac.new(self.state_secret.encode("utf-8"), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(actual_sig, expected_sig):
            raise ValueError("state_signature_invalid")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("state_payload_invalid_json") from exc
        if not isinstance(payload, dict):
            raise ValueError("state_payload_invalid")
        exp_raw = payload.get("exp")
        try:
            exp = int(exp_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("state_exp_invalid") from exc
        if datetime.now(UTC).timestamp() > exp:
            raise ValueError("state_expired")
        return payload

    @staticmethod
    def _b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    @staticmethod
    def _b64_decode(raw: str) -> bytes:
        text = str(raw or "").strip()
        if not text:
            return b""
        padding = "=" * ((4 - (len(text) % 4)) % 4)
        return base64.urlsafe_b64decode(f"{text}{padding}")
