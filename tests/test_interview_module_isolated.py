from __future__ import annotations

import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

from tener_interview.db import InterviewDatabase
from tener_interview.providers import HireflixMockAdapter
from tener_interview.scoring import InterviewScoringEngine
from tener_interview.service import InterviewService
from tener_interview.token_service import InterviewTokenService

UTC = timezone.utc


class InterviewModuleIsolatedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        db_path = str(Path(self.tmp.name) / "interview.sqlite3")
        self.db = InterviewDatabase(db_path=db_path)
        self.db.init_schema()

        self.provider = HireflixMockAdapter()
        self.token_service = InterviewTokenService(secret="unit-test-secret")
        self.scoring = InterviewScoringEngine()
        self.service = InterviewService(
            db=self.db,
            provider=self.provider,
            token_service=self.token_service,
            scoring_engine=self.scoring,
            default_ttl_hours=72,
            public_base_url="http://localhost:8090",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_start_session_and_get_view(self) -> None:
        started = self.service.start_session(
            job_id=1,
            candidate_id=101,
            candidate_name="Jane Doe",
            conversation_id=7,
            language="en",
        )

        self.assertEqual(started["status"], "invited")
        self.assertIn("session_id", started)
        self.assertIn("entry_url", started)

        view = self.service.get_session_view(started["session_id"])
        self.assertIsNotNone(view)
        assert view is not None
        self.assertEqual(view["job_id"], 1)
        self.assertEqual(view["candidate_id"], 101)
        self.assertEqual(view["status"], "invited")

    def test_token_resolution_and_refresh_to_scored(self) -> None:
        started = self.service.start_session(job_id=2, candidate_id=102, candidate_name="Alex")
        token = Path(urlparse(started["entry_url"]).path).name

        resolved = self.service.resolve_entry_token(token)
        self.assertEqual(resolved["status"], "in_progress")
        self.assertIn("provider_url", resolved)

        first = self.service.refresh_session(started["session_id"])
        self.assertEqual(first["status"], "in_progress")

        second = self.service.refresh_session(started["session_id"])
        self.assertEqual(second["status"], "scored")
        self.assertIsNotNone(second["result"]["total_score"])

        view = self.service.get_session_view(started["session_id"])
        assert view is not None
        self.assertEqual(view["status"], "scored")
        self.assertIsNotNone(view["summary"]["total_score"])

    def test_step_endpoint_equivalent_and_leaderboard(self) -> None:
        first_step = self.service.run_interview_step(job_id=3, candidate_ids=[1, 2], mode="start_or_refresh")
        self.assertEqual(first_step["started"], 2)

        second_step = self.service.run_interview_step(job_id=3, candidate_ids=[1, 2], mode="start_or_refresh")
        self.assertGreaterEqual(second_step["in_progress"] + second_step["scored"], 2)

        third_step = self.service.run_interview_step(job_id=3, candidate_ids=[1, 2], mode="start_or_refresh")
        self.assertGreaterEqual(third_step["scored"], 2)

        leaderboard = self.service.get_leaderboard(job_id=3, limit=10)
        self.assertEqual(leaderboard["job_id"], 3)
        self.assertEqual(len(leaderboard["items"]), 2)
        self.assertGreaterEqual(leaderboard["items"][0]["total_score"], leaderboard["items"][1]["total_score"])

    def test_idempotency_storage_contract(self) -> None:
        payload = {"job_id": 1, "candidate_id": 111}
        payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        response = {"session_id": "iv_1", "status": "invited"}

        self.db.put_idempotency_record(
            route="/api/interviews/sessions/start",
            key="idem-1",
            payload_hash=payload_hash,
            status_code=201,
            response=response,
        )

        saved = self.db.get_idempotency_record(route="/api/interviews/sessions/start", key="idem-1")
        self.assertIsNotNone(saved)
        assert saved is not None
        self.assertEqual(saved["payload_hash"], payload_hash)
        self.assertEqual(saved["status_code"], 201)
        self.assertEqual(saved["response_json"]["session_id"], "iv_1")

    def test_expired_token_is_rejected(self) -> None:
        started = self.service.start_session(job_id=4, candidate_id=104)
        token = Path(urlparse(started["entry_url"]).path).name

        session = self.db.get_session(started["session_id"])
        assert session is not None
        past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        self.db.update_session(started["session_id"], {"entry_token_expires_at": past, "updated_at": past})

        with self.assertRaises(ValueError):
            self.service.resolve_entry_token(token)


if __name__ == "__main__":
    unittest.main()
