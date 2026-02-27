from __future__ import annotations

import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any, Dict, Optional, Tuple
from urllib import error, request

# Prevent import-time default service bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_auth_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.auth import AuthRepository, AuthService
from tener_ai.db import Database


class AuthApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.db = Database(str(tmp / "auth_api.sqlite3"))
        self.db.init_schema()
        self.db.insert_job(
            title="Auth Test Backend Engineer",
            jd_text="Python, PostgreSQL, distributed systems",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )

        auth_repo = AuthRepository(
            backend="sqlite",
            sqlite_path=str(tmp / "auth.sqlite3"),
        )
        auth_repo.init_schema()
        org_id = auth_repo.create_organization(name="Tener QA")
        user_id = auth_repo.create_user(email="qa-admin@tener.local", full_name="QA Admin")
        auth_repo.upsert_membership(org_id=org_id, user_id=user_id, role="admin", is_active=True)
        self.read_token = auth_repo.create_api_key(
            org_id=org_id,
            user_id=user_id,
            name="Read Key",
            scopes=["api:read"],
        )["token"]
        self.write_token = auth_repo.create_api_key(
            org_id=org_id,
            user_id=user_id,
            name="Write Key",
            scopes=["api:write"],
        )["token"]

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "auth": AuthService(enabled=True, repository=auth_repo, legacy_admin_token=""),
        }

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services
        self._tmp.cleanup()

    def _request(self, method: str, path: str, token: Optional[str] = None) -> Tuple[int, Dict[str, Any]]:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = request.Request(url=f"{self.base_url}{path}", method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8")
        body: Dict[str, Any]
        if raw:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                body = {"raw_text": raw}
        else:
            body = {}
        return status, body

    def test_public_health_stays_open(self) -> None:
        status, payload = self._request("GET", "/health", token=None)
        self.assertEqual(status, 200)
        self.assertEqual(payload.get("status"), "ok")

    def test_protected_endpoint_requires_bearer(self) -> None:
        status, payload = self._request("GET", "/api/jobs", token=None)
        self.assertEqual(status, 401)
        self.assertEqual(payload.get("error"), "auth_required")

    def test_scope_is_enforced_for_get(self) -> None:
        status, payload = self._request("GET", "/api/jobs", token=self.write_token)
        self.assertEqual(status, 403)
        self.assertEqual(payload.get("error"), "scope_forbidden")

        status_ok, payload_ok = self._request("GET", "/api/jobs", token=self.read_token)
        self.assertEqual(status_ok, 200)
        self.assertIsInstance(payload_ok.get("items"), list)
        self.assertGreaterEqual(len(payload_ok.get("items") or []), 1)


if __name__ == "__main__":
    unittest.main()

