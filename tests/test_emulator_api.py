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
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_emulator_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.emulator.store import EmulatorProjectStore


class EmulatorStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_load_real_config_projects_and_profiles(self) -> None:
        store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        health = store.health()
        self.assertEqual(health["status"], "ok")
        self.assertGreaterEqual(int(health["project_count"]), 1)
        self.assertGreaterEqual(int(health["company_profile_count"]), 1)

        projects = store.list_projects()
        self.assertGreaterEqual(len(projects), 1)
        first_id = str(projects[0]["id"])
        project = store.get_project(first_id)
        self.assertIsNotNone(project)
        self.assertIsInstance(project.get("events"), list)
        self.assertIsInstance(project.get("candidates"), list)

        profile = store.get_company_profile("https://stripe.com")
        self.assertIsNotNone(profile)
        self.assertEqual(str(profile.get("domain")), "stripe.com")

    def test_invalid_project_marks_store_as_degraded(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            projects_dir = tmp_path / "projects"
            projects_dir.mkdir(parents=True, exist_ok=True)
            (projects_dir / "bad.json").write_text('{"id":"x"}', encoding="utf-8")

            profiles_path = tmp_path / "profiles.json"
            profiles_path.write_text(
                json.dumps(
                    {
                        "profiles": [
                            {
                                "id": "ok",
                                "name": "Ok Inc",
                                "domain": "ok.inc",
                                "summary": "ok",
                                "profile": {"values": ["one"]},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            store = EmulatorProjectStore(projects_dir=projects_dir, company_profiles_path=profiles_path)
            health = store.health()
            self.assertEqual(health["status"], "degraded")
            self.assertIn("bad.json", str(health.get("load_error") or ""))
            self.assertEqual(store.list_projects(), [])


class EmulatorApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        self._previous_services = api_main.SERVICES
        self.store = EmulatorProjectStore(
            projects_dir=self.root / "config" / "emulator" / "projects",
            company_profiles_path=self.root / "config" / "emulator" / "company_profiles.json",
        )
        api_main.SERVICES = {"emulator_store": self.store}

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api_main.TenerRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        api_main.SERVICES = self._previous_services

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any], str]:
        data = None
        headers: Dict[str, str] = {}
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")

        req = request.Request(url=f"{self.base_url}{path}", method=method, headers=headers, data=data)
        try:
            with request.urlopen(req, timeout=20) as resp:
                status = int(resp.status)
                text = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            status = int(exc.code)
            text = exc.read().decode("utf-8")

        if text:
            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                body = {}
        else:
            body = {}

        return status, body, text

    def test_dashboard_emulator_page_serves(self) -> None:
        status, _body, text = self._request("GET", "/dashboard/emulator")
        self.assertEqual(status, 200)
        self.assertIn("Emulator Mode", text)

    def test_emulator_status_and_list_endpoints(self) -> None:
        status, payload, _ = self._request("GET", "/api/emulator")
        self.assertEqual(status, 200)
        self.assertEqual(payload.get("status"), "ok")
        self.assertGreaterEqual(int(payload.get("project_count") or 0), 1)

        status, payload, _ = self._request("GET", "/api/emulator/projects")
        self.assertEqual(status, 200)
        items = payload.get("items") or []
        self.assertGreaterEqual(len(items), 1)
        first_id = str(items[0].get("id") or "")
        self.assertTrue(first_id)

        status, project, _ = self._request("GET", f"/api/emulator/projects/{first_id}")
        self.assertEqual(status, 200)
        self.assertEqual(project.get("id"), first_id)
        self.assertIsInstance(project.get("events"), list)

    def test_company_profile_lookup_and_reload(self) -> None:
        status, payload, _ = self._request("GET", "/api/emulator/company-profiles")
        self.assertEqual(status, 200)
        items = payload.get("items") or []
        self.assertGreaterEqual(len(items), 1)

        status, profile, _ = self._request("GET", "/api/emulator/company-profiles/stripe.com")
        self.assertEqual(status, 200)
        self.assertEqual(profile.get("domain"), "stripe.com")

        status, payload, _ = self._request("POST", "/api/emulator/reload", payload={})
        self.assertEqual(status, 200)
        self.assertEqual(payload.get("status"), "ok")

    def test_missing_project_returns_not_found(self) -> None:
        status, payload, _ = self._request("GET", "/api/emulator/projects/unknown-project")
        self.assertEqual(status, 404)
        self.assertIn("not found", str(payload.get("error") or "").lower())


if __name__ == "__main__":
    unittest.main()
