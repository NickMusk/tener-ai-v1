from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.db import Database
from tener_ai.db_dual import DualWriteDatabase


class _FakeMirror:
    def __init__(self, fail_ops: set[str] | None = None) -> None:
        self.fail_ops = set(fail_ops or set())
        self.calls: List[str] = []

    def _call(self, op: str) -> None:
        self.calls.append(op)
        if op in self.fail_ops:
            raise RuntimeError(f"forced mirror failure: {op}")

    def upsert_job(self, row: Dict[str, Any]) -> None:
        self._call("upsert_job")

    def upsert_job_culture_profile(self, row: Dict[str, Any]) -> None:
        self._call("upsert_job_culture_profile")

    def upsert_candidate(self, row: Dict[str, Any]) -> None:
        self._call("upsert_candidate")

    def upsert_candidate_match(self, row: Dict[str, Any]) -> None:
        self._call("upsert_candidate_match")

    def upsert_conversation(self, row: Dict[str, Any]) -> None:
        self._call("upsert_conversation")

    def upsert_message(self, row: Dict[str, Any]) -> None:
        self._call("upsert_message")

    def insert_operation_log(self, row: Dict[str, Any]) -> None:
        self._call("insert_operation_log")

    def upsert_pre_resume_session(self, row: Dict[str, Any]) -> None:
        self._call("upsert_pre_resume_session")

    def upsert_pre_resume_event(self, row: Dict[str, Any]) -> None:
        self._call("upsert_pre_resume_event")

    def insert_webhook_event(self, *, event_key: str, source: str, payload: Dict[str, Any] | None = None) -> None:
        self._call("insert_webhook_event")

    def upsert_job_step_progress(self, row: Dict[str, Any]) -> None:
        self._call("upsert_job_step_progress")

    def upsert_candidate_agent_assessment(self, row: Dict[str, Any]) -> None:
        self._call("upsert_candidate_agent_assessment")


class DualWriteDatabaseTests(unittest.TestCase):
    def test_mirrors_core_write_paths(self) -> None:
        with TemporaryDirectory() as td:
            primary = Database(str(Path(td) / "dual.sqlite3"))
            primary.init_schema()
            mirror = _FakeMirror()
            db = DualWriteDatabase(primary=primary, mirror=mirror, strict=False)

            job_id = db.insert_job(
                title="Dual Write Backend Engineer",
                jd_text="Python, PostgreSQL",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
                company="Tener",
                company_website="https://tener.ai",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "dual-write-candidate-1",
                    "full_name": "Dual Candidate",
                    "headline": "Backend Engineer",
                    "location": "Warsaw",
                    "languages": ["en", "pl"],
                    "skills": ["python", "postgresql"],
                    "years_experience": 7,
                },
                source="linkedin",
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.88,
                status="verified",
                verification_notes={"matched": ["python"]},
            )
            conversation_id = db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content="Hi!",
                candidate_language="en",
                meta={"type": "initial"},
            )
            db.upsert_job_step_progress(job_id=job_id, step="source", status="success", output={"total": 12})
            db.upsert_pre_resume_session(
                session_id="session-dual-1",
                conversation_id=conversation_id,
                job_id=job_id,
                candidate_id=candidate_id,
                state={"status": "awaiting_reply", "resume_links": []},
                instruction="collect resume",
            )
            db.insert_pre_resume_event(
                session_id="session-dual-1",
                conversation_id=conversation_id,
                event_type="inbound_processed",
                intent="resume_requested",
                inbound_text="hello",
                outbound_text="please share cv",
                state_status="awaiting_reply",
                details={"source": "test"},
            )
            db.record_webhook_event("event-1", "unipile", payload={"x": 1})
            db.upsert_candidate_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="communication",
                agent_name="Casey AI",
                stage_key="dialogue",
                score=77,
                status="in_dialogue",
                reason="Clear responses",
                details={"turns": 3},
            )
            db.log_operation(
                operation="dual.write.test",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"case": "core"},
            )

            expected_ops = {
                "upsert_job",
                "upsert_candidate",
                "upsert_candidate_match",
                "upsert_conversation",
                "upsert_message",
                "upsert_job_step_progress",
                "upsert_pre_resume_session",
                "upsert_pre_resume_event",
                "insert_webhook_event",
                "upsert_candidate_agent_assessment",
                "insert_operation_log",
            }
            self.assertTrue(expected_ops.issubset(set(mirror.calls)))
            status = db.dual_write_status
            self.assertEqual(int(status.get("mirror_errors") or 0), 0)
            self.assertGreater(int(status.get("mirror_success") or 0), 0)

    def test_mirror_failure_does_not_break_primary_when_not_strict(self) -> None:
        with TemporaryDirectory() as td:
            primary = Database(str(Path(td) / "dual_fallback.sqlite3"))
            primary.init_schema()
            mirror = _FakeMirror(fail_ops={"upsert_job"})
            db = DualWriteDatabase(primary=primary, mirror=mirror, strict=False)

            job_id = db.insert_job(
                title="Fallback Role",
                jd_text="Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="mid",
            )
            job = db.get_job(job_id)
            self.assertIsNotNone(job)

            status = db.dual_write_status
            self.assertEqual(int(status.get("mirror_errors") or 0), 1)
            self.assertIn("insert_job", str(status.get("last_error") or ""))

    def test_set_strict_mode_updates_runtime_behavior(self) -> None:
        with TemporaryDirectory() as td:
            primary = Database(str(Path(td) / "dual_strict.sqlite3"))
            primary.init_schema()
            mirror = _FakeMirror(fail_ops={"upsert_job"})
            db = DualWriteDatabase(primary=primary, mirror=mirror, strict=False)

            self.assertFalse(db.strict_mode)
            db.set_strict_mode(True)
            self.assertTrue(db.strict_mode)
            with self.assertRaises(RuntimeError):
                db.insert_job(
                    title="Strict Role",
                    jd_text="Python",
                    location="Remote",
                    preferred_languages=["en"],
                    seniority="mid",
                )


if __name__ == "__main__":
    unittest.main()
