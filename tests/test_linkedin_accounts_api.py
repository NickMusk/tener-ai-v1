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

# Prevent import-time bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_linkedin_accounts_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database
from tener_ai.linkedin_accounts import LinkedInAccountService
from tener_ai.outreach_policy import LinkedInOutreachPolicy


class _StubSyncAccountsService(LinkedInAccountService):
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
                "id": "acc_sync_api_1",
                "name": "Imported Recruiter 01",
                "status": "connected",
                "user_id": "usr_sync_1",
            },
            {
                "id": "acc_sync_api_2",
                "name": "Imported Recruiter 02",
                "status": "connected",
                "user_id": "usr_sync_2",
            },
        ]


class LinkedInAccountsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "linkedin_accounts_api.sqlite3"))
        self.db.init_schema()
        self.linkedin_service = _StubSyncAccountsService(self.db)
        self.outreach_policy = LinkedInOutreachPolicy(path=None)

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "db": self.db,
            "linkedin_accounts": self.linkedin_service,
            "outreach_policy": self.outreach_policy,
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

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
        data = None
        headers: Dict[str, str] = {}
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"{self.base_url}{path}", method=method, data=data, headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8")
        body = json.loads(raw) if raw else {}
        return status, body

    def test_sync_all_imports_accounts_and_exposes_them_in_list_endpoint(self) -> None:
        status, sync_payload = self._request("POST", "/api/linkedin/accounts/sync-all", {})
        self.assertEqual(status, 200)
        self.assertEqual(str(sync_payload.get("status") or ""), "ok")
        self.assertEqual(int(sync_payload.get("updated") or 0), 2)

        rows = self.db.list_linkedin_accounts(limit=20)
        self.assertEqual(len(rows), 2)

        status, list_payload = self._request("GET", "/api/linkedin/accounts?limit=20")
        self.assertEqual(status, 200)
        items = list_payload.get("items") or []
        self.assertEqual(len(items), 2)
        first = items[0]
        self.assertIn("daily_message_limit", first)
        self.assertIn("daily_connect_limit", first)


if __name__ == "__main__":
    unittest.main()
