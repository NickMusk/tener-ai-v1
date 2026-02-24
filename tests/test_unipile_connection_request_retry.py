from __future__ import annotations

import os
import unittest
from typing import Any, Dict, Optional
from unittest.mock import patch
from urllib import parse

from tener_ai.linkedin_provider import UnipileLinkedInProvider


class _StubInviteProvider(UnipileLinkedInProvider):
    def __init__(self, success_path: Optional[str] = None, users_invite_error: Optional[str] = None) -> None:
        super().__init__(api_key="k", base_url="https://api.unipile.com", account_id="acc")
        self.success_path = success_path
        self.users_invite_error = users_invite_error

    def _request_json(self, method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        path = parse.urlparse(url).path
        if self.success_path and path == self.success_path:
            return {"id": "req-1"}
        if path == "/api/v1/users/invite" and self.users_invite_error:
            raise RuntimeError(self.users_invite_error)
        raise RuntimeError(f"Unipile HTTP error 404: cannot post {path}")


class UnipileConnectionRequestRetryTests(unittest.TestCase):
    def test_users_invite_path_is_prioritized_even_if_env_is_legacy_path(self) -> None:
        with patch.dict(os.environ, {"UNIPILE_CONNECT_CREATE_PATH": "/api/v1/linkedin/invite"}, clear=False):
            provider = UnipileLinkedInProvider(api_key="k", base_url="https://api.unipile.com", account_id="acc")
            paths = provider._candidate_connect_paths()

        self.assertGreaterEqual(len(paths), 2)
        self.assertEqual(paths[0], "/api/v1/users/invite")
        self.assertIn("/api/v1/linkedin/invite", paths)

    def test_connection_request_succeeds_on_users_invite_when_legacy_path_is_configured(self) -> None:
        with patch.dict(os.environ, {"UNIPILE_CONNECT_CREATE_PATH": "/api/v1/linkedin/invite"}, clear=False):
            provider = _StubInviteProvider(success_path="/api/v1/users/invite")

        out = provider.send_connection_request(
            candidate_profile={"linkedin_id": "ACoAATest"},
            message="Connect?",
        )

        self.assertTrue(out["sent"])
        self.assertEqual(out.get("path"), "/api/v1/users/invite")

    def test_connection_request_reports_non_404_error_when_following_fallbacks(self) -> None:
        with patch.dict(os.environ, {"UNIPILE_CONNECT_CREATE_PATH": "/api/v1/linkedin/invite"}, clear=False):
            provider = _StubInviteProvider(
                success_path=None,
                users_invite_error=(
                    "Unipile HTTP error 422: {\"status\":422,\"type\":\"errors/no_connection_with_recipient\"}"
                ),
            )

        out = provider.send_connection_request(candidate_profile={"linkedin_id": "ACoAATest"}, message="Connect?")

        self.assertFalse(out["sent"])
        self.assertEqual(out["reason"], "connection_request_failed")
        self.assertIn("HTTP error 422", out.get("error") or "")
        attempts = out.get("attempts") or []
        self.assertTrue(attempts)
        self.assertIn("path", attempts[0])


if __name__ == "__main__":
    unittest.main()
