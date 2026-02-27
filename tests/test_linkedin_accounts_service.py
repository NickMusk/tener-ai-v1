from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlparse

from tener_ai.db import Database
from tener_ai.linkedin_accounts import LinkedInAccountService


class _StubSyncService(LinkedInAccountService):
    def __init__(self, db: Database) -> None:
        super().__init__(
            db=db,
            api_key="k",
            connect_url_template="https://unipile.test/connect?state={state}&redirect_uri={redirect_uri}",
            state_secret="secret",
        )

    def _fetch_remote_accounts(self):  # type: ignore[override]
        return [
            {
                "id": "acc_sync_1",
                "name": "Recruiter 01",
                "status": "connected",
                "user_id": "usr_1",
            }
        ]


class _StubHostedConnectFallbackService(LinkedInAccountService):
    def __init__(self, db: Database) -> None:
        super().__init__(
            db=db,
            api_key="unipile-key",
            state_secret="test-secret",
            hosted_connect_path="/api/v1/hosted/accounts/linkedin",
        )
        self.calls = []

    def _request_json(self, method, url, payload, *, auth_mode="default"):  # type: ignore[override]
        self.calls.append((method, url, payload, auth_mode))
        if auth_mode != "api_key_only":
            raise RuntimeError(
                "Unipile HTTP error 401: "
                '{"status":401,"type":"errors/invalid_credentials","detail":"This authentication link doesn\'t exist or is no longer active."}'
            )
        return {"url": "https://hosted.unipile.test/connect/abc"}


class LinkedInAccountsServiceTests(unittest.TestCase):
    def test_start_connect_and_callback_connects_account(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts.sqlite3"))
            db.init_schema()
            service = LinkedInAccountService(
                db=db,
                connect_url_template="https://unipile.test/connect?state={state}&redirect_uri={redirect_uri}",
                state_secret="test-secret",
            )
            started = service.start_connect(
                callback_url="https://tener.test/api/linkedin/accounts/connect/callback",
                label="US Recruiter 01",
            )
            self.assertTrue(str(started.get("session_id") or "").startswith("lnk-"))
            connect_url = str(started.get("connect_url") or "")
            parsed = urlparse(connect_url)
            state = (parse_qs(parsed.query).get("state") or [""])[0]
            out = service.complete_connect_callback(query={"state": [state], "account_id": ["acc_123"]})
            self.assertEqual(out.get("status"), "connected")

            rows = db.list_linkedin_accounts(limit=20)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["provider_account_id"], "acc_123")
            self.assertEqual(rows[0]["status"], "connected")

            session = db.get_linkedin_onboarding_session(str(started["session_id"]))
            self.assertIsNotNone(session)
            self.assertEqual(str((session or {}).get("status")), "completed")

    def test_callback_with_invalid_state_returns_error(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts_invalid.sqlite3"))
            db.init_schema()
            service = LinkedInAccountService(
                db=db,
                connect_url_template="https://unipile.test/connect?state={state}&redirect_uri={redirect_uri}",
                state_secret="test-secret",
            )
            out = service.complete_connect_callback(query={"state": ["invalid-token"]})
            self.assertEqual(out.get("status"), "error")
            self.assertEqual(out.get("reason"), "invalid_state")

    def test_sync_accounts_upserts_remote_accounts(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts_sync.sqlite3"))
            db.init_schema()
            service = _StubSyncService(db)
            out = service.sync_accounts()
            self.assertEqual(out.get("status"), "ok")
            self.assertEqual(int(out.get("updated") or 0), 1)
            rows = db.list_linkedin_accounts(limit=20)
            self.assertEqual(len(rows), 1)
            self.assertEqual(str(rows[0].get("provider_account_id")), "acc_sync_1")
            self.assertEqual(str(rows[0].get("label")), "Recruiter 01")

    def test_disconnect_marks_account_disconnected(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts_disconnect.sqlite3"))
            db.init_schema()
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc_disc_1",
                status="connected",
                label="Recruiter",
            )
            service = LinkedInAccountService(
                db=db,
                connect_url_template="https://unipile.test/connect?state={state}&redirect_uri={redirect_uri}",
                state_secret="test-secret",
            )
            out = service.disconnect_account(account_id=account_id, remote_disable=False)
            self.assertEqual(out.get("status"), "ok")
            row = db.get_linkedin_account(account_id)
            self.assertIsNotNone(row)
            self.assertEqual(str((row or {}).get("status")), "disconnected")

    def test_start_connect_hosted_fallback_uses_api_key_only_mode(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts_hosted_fallback.sqlite3"))
            db.init_schema()
            service = _StubHostedConnectFallbackService(db)
            out = service.start_connect(
                callback_url="https://tener.test/api/linkedin/accounts/connect/callback",
                label="Recruiter 2",
            )
            self.assertEqual(out.get("provider"), "unipile")
            self.assertEqual(out.get("connect_url"), "https://hosted.unipile.test/connect/abc")
            self.assertGreaterEqual(len(service.calls), 1)
            first_call = service.calls[0]
            self.assertEqual(first_call[3], "api_key_only")

    def test_headers_modes(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "linkedin_accounts_headers.sqlite3"))
            db.init_schema()
            service = LinkedInAccountService(
                db=db,
                api_key="k-test",
                connect_url_template="https://unipile.test/connect?state={state}&redirect_uri={redirect_uri}",
                state_secret="test-secret",
            )
            default_headers = service._headers_json()
            self.assertEqual(default_headers.get("Authorization"), "Bearer k-test")
            self.assertEqual(default_headers.get("X-API-KEY"), "k-test")

            api_key_only_headers = service._headers_json(auth_mode="api_key_only")
            self.assertIsNone(api_key_only_headers.get("Authorization"))
            self.assertEqual(api_key_only_headers.get("X-API-KEY"), "k-test")

            bearer_only_headers = service._headers_json(auth_mode="bearer_only")
            self.assertEqual(bearer_only_headers.get("Authorization"), "Bearer k-test")
            self.assertIsNone(bearer_only_headers.get("X-API-KEY"))


if __name__ == "__main__":
    unittest.main()
