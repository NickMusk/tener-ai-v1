from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

UTC = timezone.utc


class InvalidTokenError(ValueError):
    pass


class InterviewTokenService:
    def __init__(self, secret: str) -> None:
        if not secret:
            raise ValueError("token secret cannot be empty")
        self._secret = secret.encode("utf-8")

    def generate(self, payload: Dict[str, Any]) -> str:
        payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        encoded = base64.urlsafe_b64encode(payload_bytes).decode("ascii").rstrip("=")
        signature = hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest()
        sig_encoded = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
        return f"{encoded}.{sig_encoded}"

    def parse_and_validate(self, token: str, now: Optional[datetime] = None) -> Dict[str, Any]:
        try:
            encoded, sig_encoded = token.split(".", 1)
        except ValueError as exc:
            raise InvalidTokenError("invalid token format") from exc

        expected_sig = hmac.new(self._secret, encoded.encode("ascii"), hashlib.sha256).digest()
        provided_sig = self._b64decode(sig_encoded)
        if not hmac.compare_digest(expected_sig, provided_sig):
            raise InvalidTokenError("invalid token signature")

        payload_raw = self._b64decode(encoded)
        try:
            payload = json.loads(payload_raw.decode("utf-8"))
        except Exception as exc:
            raise InvalidTokenError("invalid token payload") from exc

        exp = payload.get("exp")
        if exp is None:
            raise InvalidTokenError("token exp is missing")

        ref_now = now or datetime.now(UTC)
        if int(exp) < int(ref_now.timestamp()):
            raise InvalidTokenError("token expired")

        return payload

    @staticmethod
    def token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _b64decode(s: str) -> bytes:
        pad = "=" * ((4 - len(s) % 4) % 4)
        return base64.urlsafe_b64decode((s + pad).encode("ascii"))
