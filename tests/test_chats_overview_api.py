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

os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_chats_overview_api_bootstrap.sqlite3"))

from tener_ai import main as api_main
from tener_ai.db import Database


class ChatsOverviewApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "chats_overview_api.sqlite3"))
        self.db.init_schema()

        self._previous_services = api_main.SERVICES
        api_main.SERVICES = {"db": self.db}

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

    def test_chats_overview_filters_started_replied_and_outbound_only_dialogues(self) -> None:
        job_id = self.db.insert_job(
            title="Backend Engineer",
            jd_text="Need Python and PostgreSQL.",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )

        started_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "started-ln-1",
                "full_name": "Replied Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            },
            source="linkedin",
        )
        queued_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "queued-ln-1",
                "full_name": "Queued Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 3,
                "raw": {},
            },
            source="linkedin",
        )
        outbound_only_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "outbound-only-ln-1",
                "full_name": "Outbound Only Candidate",
                "headline": "Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 4,
                "raw": {},
            },
            source="linkedin",
        )

        replied_conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=started_candidate_id, channel="linkedin")
        outbound_only_conversation_id = self.db.create_conversation(
            job_id=job_id,
            candidate_id=outbound_only_candidate_id,
            channel="linkedin",
        )
        queued_conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=queued_candidate_id, channel="linkedin")
        self.db.update_conversation_status(conversation_id=replied_conversation_id, status="active")
        self.db.update_conversation_status(conversation_id=outbound_only_conversation_id, status="active")
        self.db.update_conversation_status(conversation_id=queued_conversation_id, status="queued")
        self.db.add_message(
            conversation_id=replied_conversation_id,
            direction="outbound",
            content="Initial outreach",
            candidate_language="en",
            meta={"delivery_status": "sent"},
        )
        self.db.add_message(
            conversation_id=replied_conversation_id,
            direction="inbound",
            content="Thanks, I am interested.",
            candidate_language="en",
            meta={},
        )
        self.db.add_message(
            conversation_id=outbound_only_conversation_id,
            direction="outbound",
            content="Following up on the role.",
            candidate_language="en",
            meta={"delivery_status": "sent"},
        )

        status_all, payload_all = self._request("GET", f"/api/chats/overview?job_id={job_id}&limit=20")
        self.assertEqual(status_all, 200)
        all_ids = {int(item.get("conversation_id") or 0) for item in (payload_all.get("items") or [])}
        self.assertIn(replied_conversation_id, all_ids)
        self.assertIn(outbound_only_conversation_id, all_ids)
        self.assertIn(queued_conversation_id, all_ids)

        status_started, payload_started = self._request(
            "GET",
            f"/api/chats/overview?job_id={job_id}&limit=20&started_only=1",
        )
        self.assertEqual(status_started, 200)
        started_ids = {int(item.get("conversation_id") or 0) for item in (payload_started.get("items") or [])}
        self.assertEqual(started_ids, {replied_conversation_id, outbound_only_conversation_id})

        status_replied, payload_replied = self._request(
            "GET",
            f"/api/chats/overview?job_id={job_id}&limit=20&dialogue_bucket=candidate_replied",
        )
        self.assertEqual(status_replied, 200)
        replied_items = payload_replied.get("items") or []
        self.assertEqual(len(replied_items), 1)
        self.assertEqual(int(replied_items[0].get("conversation_id") or 0), replied_conversation_id)
        self.assertEqual(str(replied_items[0].get("candidate_name") or ""), "Replied Candidate")

        status_outbound_only, payload_outbound_only = self._request(
            "GET",
            f"/api/chats/overview?job_id={job_id}&limit=20&dialogue_bucket=outbound_only",
        )
        self.assertEqual(status_outbound_only, 200)
        outbound_only_items = payload_outbound_only.get("items") or []
        self.assertEqual(len(outbound_only_items), 1)
        self.assertEqual(int(outbound_only_items[0].get("conversation_id") or 0), outbound_only_conversation_id)
        self.assertEqual(str(outbound_only_items[0].get("candidate_name") or ""), "Outbound Only Candidate")


if __name__ == "__main__":
    unittest.main()
