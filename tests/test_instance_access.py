from __future__ import annotations

import json
import os
import threading
import unittest
from dataclasses import replace
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Dict, Optional, Tuple
from unittest.mock import patch
from urllib import error, request

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_instance_access_bootstrap.sqlite3"))
os.environ.setdefault("TENER_INTERVIEW_DB_PATH", str(Path(gettempdir()) / "tener_instance_interview_bootstrap.sqlite3"))
os.environ.setdefault("TENER_INTERVIEW_SOURCE_DB_PATH", str(Path(gettempdir()) / "tener_instance_source_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_interview import http_api as interview_api
from tener_shared import InstanceConfig


class _InterviewStub:
    def get_entry_landing(self, token: str) -> Dict[str, Any]:
        raise LookupError(token)


class MainInstanceAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {
            "instance_config": InstanceConfig.from_payload(
                {
                    "instance_id": "easysofthire-test",
                    "branding": {
                        "brand_name": "EasySoftHire",
                        "main_dashboard_title": "EasySoftHire Pipeline Control Center",
                        "main_dashboard_logo_text": "EasySoftHire",
                        "main_dashboard_subnote_html": "Pipeline Control Center<br>EasySoftHire operations + live diagnostics",
                        "main_default_job_company": "EasySoftHire",
                        "landing_title": "EasySoftHire Hiring Operations",
                        "landing_headline": "EasySoftHire dedicated hiring operations",
                        "landing_body": "Private hiring operations for EasySoftHire.",
                    },
                    "access": {
                        "require_private_bearer_token": True,
                        "public_dashboard": False,
                        "public_candidate_profiles": False,
                        "allow_demo_routes": False,
                    },
                    "features": {
                        "use_generic_public_landing": True,
                    },
                }
            )
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

    def _request(self, path: str, *, token: Optional[str] = None) -> Tuple[int, str]:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = request.Request(url=f"{self.base_url}{path}", method="GET", headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                return int(resp.status), resp.read().decode("utf-8")
        except error.HTTPError as exc:
            return int(exc.code), exc.read().decode("utf-8")

    def test_public_landing_remains_open_and_branded(self) -> None:
        status, body = self._request("/")
        self.assertEqual(status, 200)
        self.assertIn("EasySoftHire dedicated hiring operations", body)

    def test_private_dashboard_requires_bearer_token(self) -> None:
        with patch.dict(os.environ, {"TENER_ADMIN_API_TOKEN": "main-secret"}, clear=False):
            status, body = self._request("/dashboard")
            self.assertEqual(status, 401)
            self.assertIn("private bearer token required", body)

            ok_status, ok_body = self._request("/dashboard", token="main-secret")
            self.assertEqual(ok_status, 200)
            self.assertIn("EasySoftHire Pipeline Control Center", ok_body)
            self.assertIn("EasySoftHire operations + live diagnostics", ok_body)

    def test_public_mode_bypasses_admin_token_for_dashboard(self) -> None:
        api_main.SERVICES["instance_config"] = InstanceConfig.from_payload(
            {
                "instance_id": "easysofthire-test",
                "branding": {
                    "brand_name": "EasySoftHire",
                    "main_dashboard_title": "EasySoftHire Pipeline Control Center",
                    "main_dashboard_logo_text": "EasySoftHire",
                },
                "access": {
                    "require_private_bearer_token": False,
                    "public_dashboard": True,
                    "public_candidate_profiles": True,
                    "allow_demo_routes": False,
                },
            }
        )
        with patch.dict(os.environ, {"TENER_ADMIN_API_TOKEN": "main-secret"}, clear=False):
            status, body = self._request("/dashboard")
        self.assertEqual(status, 200)
        self.assertIn("EasySoftHire Pipeline Control Center", body)


class InterviewInstanceAccessTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_services = interview_api.SERVICES
        config = replace(
            interview_api.SERVICES["config"],
            company_name="EasySoftHire",
            public_base_url="https://tener-easysoftgroup-interview.onrender.com",
            admin_token="interview-secret",
            require_private_bearer_token=True,
            public_interview_dashboard=False,
            public_interview_api_index=True,
            strict_provider_mode=True,
            dashboard_title="EasySoftHire Interview Admin",
            dashboard_heading="EasySoftHire Interview Admin",
            dashboard_subcopy="Select a role, generate interview links, and track scored results for EasySoftHire.",
            candidate_title="EasySoftHire Interview",
            interview_system_name="EasySoftHire",
            interview_header_note="Candidate Interview Entry",
        )
        interview_api.SERVICES = {
            "config": config,
            "provider_name": "hireflix",
            "provider_error": "",
            "source_db": interview_api.SERVICES["source_db"],
            "db": interview_api.SERVICES["db"],
            "interview": _InterviewStub(),
        }
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), interview_api.InterviewRequestHandler)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        interview_api.SERVICES = self._previous_services

    def _request(self, path: str, *, token: Optional[str] = None) -> Tuple[int, str]:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = request.Request(url=f"{self.base_url}{path}", method="GET", headers=headers)
        try:
            with request.urlopen(req, timeout=20) as resp:
                return int(resp.status), resp.read().decode("utf-8")
        except error.HTTPError as exc:
            return int(exc.code), exc.read().decode("utf-8")

    def test_public_entry_route_stays_open_without_token(self) -> None:
        status, body = self._request("/api/interviews/entry/test-token")
        self.assertEqual(status, 404)
        self.assertIn("session not found", body)

    def test_private_dashboard_requires_token_and_is_branded(self) -> None:
        status, body = self._request("/dashboard")
        self.assertEqual(status, 401)
        payload = json.loads(body)
        self.assertEqual(payload.get("code"), "ADMIN_AUTH_REQUIRED")

        ok_status, ok_body = self._request("/dashboard", token="interview-secret")
        self.assertEqual(ok_status, 200)
        self.assertIn("EasySoftHire Interview Admin", ok_body)
        self.assertIn("track scored results for EasySoftHire", ok_body)

    def test_public_mode_bypasses_token_for_interview_dashboard(self) -> None:
        interview_api.SERVICES["config"] = replace(
            interview_api.SERVICES["config"],
            admin_token="interview-secret",
            require_private_bearer_token=False,
            public_interview_dashboard=True,
        )
        status, body = self._request("/dashboard")
        self.assertEqual(status, 200)
        self.assertIn("EasySoftHire Interview Admin", body)


if __name__ == "__main__":
    unittest.main()
