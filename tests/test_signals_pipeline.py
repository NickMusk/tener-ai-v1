from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import AGENT_DEFAULT_NAMES, Database
from tener_ai.signals import JobSignalsLiveViewService, MonitoringService, SignalIngestionService


class SignalsPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.db = Database(str(Path(self.tmp.name) / "signals.sqlite3"))
        self.db.init_schema()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _seed_job_with_candidates(self) -> int:
        job_id = self.db.insert_job(
            title="Live signals backend role",
            jd_text="Need Python, AWS, communication and ownership.",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )

        top_candidate = self.db.upsert_candidate(
            {
                "linkedin_id": "signals-top-1",
                "full_name": "Top Baseline Candidate",
                "headline": "Backend Engineer",
                "location": "Warsaw",
                "languages": ["en"],
                "skills": ["python", "aws"],
                "years_experience": 8,
                "raw": {},
            }
        )
        challenger = self.db.upsert_candidate(
            {
                "linkedin_id": "signals-challenger-1",
                "full_name": "Challenger Candidate",
                "headline": "Senior Engineer",
                "location": "Kyiv",
                "languages": ["en"],
                "skills": ["python", "aws", "postgresql"],
                "years_experience": 7,
                "raw": {},
            }
        )

        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=top_candidate,
            score=0.90,
            status="verified",
            verification_notes={},
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=challenger,
            score=0.74,
            status="verified",
            verification_notes={},
        )

        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=top_candidate,
            agent_key="communication",
            agent_name=AGENT_DEFAULT_NAMES["communication"],
            stage_key="dialogue",
            score=41,
            status="warning",
            reason="Weak response quality",
            details={"signal": "vague examples"},
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=challenger,
            agent_key="communication",
            agent_name=AGENT_DEFAULT_NAMES["communication"],
            stage_key="dialogue",
            score=86,
            status="qualified",
            reason="Strong communication and ownership",
            details={"signal": "specific outcomes"},
        )

        conv_top = self.db.get_or_create_conversation(job_id=job_id, candidate_id=top_candidate, channel="linkedin")
        conv_challenger = self.db.get_or_create_conversation(job_id=job_id, candidate_id=challenger, channel="linkedin")

        session_top = f"pre-{conv_top}"
        session_challenger = f"pre-{conv_challenger}"
        self.db.upsert_pre_resume_session(
            session_id=session_top,
            conversation_id=conv_top,
            job_id=job_id,
            candidate_id=top_candidate,
            state={"session_id": session_top, "status": "not_interested"},
            instruction="",
        )
        self.db.upsert_pre_resume_session(
            session_id=session_challenger,
            conversation_id=conv_challenger,
            job_id=job_id,
            candidate_id=challenger,
            state={"session_id": session_challenger, "status": "resume_received"},
            instruction="",
        )
        self.db.insert_pre_resume_event(
            session_id=session_top,
            conversation_id=conv_top,
            event_type="inbound_processed",
            intent="not_interested",
            inbound_text="not interested now",
            outbound_text="ack",
            state_status="not_interested",
            details={"reason": "explicit_opt_out"},
        )
        self.db.insert_pre_resume_event(
            session_id=session_challenger,
            conversation_id=conv_challenger,
            event_type="inbound_processed",
            intent="resume_shared",
            inbound_text="here is my CV",
            outbound_text="thanks",
            state_status="resume_received",
            details={"reason": "resume_detected"},
        )

        self.db.log_operation(
            operation="agent.pre_resume.followup",
            status="error",
            entity_type="candidate",
            entity_id=str(top_candidate),
            details={"job_id": job_id, "error": "delivery_failed"},
        )
        self.db.log_operation(
            operation="agent.pre_resume.followup",
            status="ok",
            entity_type="candidate",
            entity_id=str(challenger),
            details={"job_id": job_id, "result": "sent"},
        )
        return job_id

    def test_ingestion_creates_persisted_signals(self) -> None:
        job_id = self._seed_job_with_candidates()
        ingestion = SignalIngestionService(self.db).ingest_job(job_id=job_id, limit_candidates=20)
        self.assertEqual(ingestion.get("status"), "ok")
        self.assertGreater(int(ingestion.get("signals_upserted") or 0), 0)

        rows = self.db.list_job_signals(job_id=job_id, limit=1000)
        self.assertGreater(len(rows), 0)
        source_types = {str(row.get("source_type") or "") for row in rows}
        self.assertIn("assessment", source_types)
        self.assertIn("pre_resume_event", source_types)
        self.assertIn("operation_log", source_types)
        self.assertIn("match_snapshot", source_types)
        for row in rows:
            meta = row.get("signal_meta") if isinstance(row.get("signal_meta"), dict) else {}
            self.assertIn("signal_role", meta)
            self.assertIn("score_weight", meta)
            self.assertIn("signal_rules_version", meta)

    def test_live_view_reflects_rank_shifts_after_ingestion(self) -> None:
        job_id = self._seed_job_with_candidates()
        SignalIngestionService(self.db).ingest_job(job_id=job_id, limit_candidates=20)
        view = JobSignalsLiveViewService(self.db).build_job_view(job_id=job_id, limit_candidates=20, limit_signals=1000)

        ranking = view.get("ranking") if isinstance(view.get("ranking"), list) else []
        self.assertGreaterEqual(len(ranking), 2)
        self.assertEqual(str(ranking[0].get("candidate_name") or ""), "Challenger Candidate")
        deltas = {int(item.get("candidate_id") or 0): int(item.get("rank_delta") or 0) for item in ranking}
        self.assertTrue(any(value != 0 for value in deltas.values()))

    def test_monitoring_warns_when_jobs_have_no_signals(self) -> None:
        job_id = self.db.insert_job(
            title="Signals monitoring job",
            jd_text="Backend engineer",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        for idx in range(6):
            candidate_id = self.db.upsert_candidate(
                {
                    "linkedin_id": f"signals-monitor-{idx}",
                    "full_name": f"Signals Monitor {idx}",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 4,
                    "raw": {},
                }
            )
            self.db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.5,
                status="verified",
                verification_notes={},
            )

        report = MonitoringService(self.db).build_status(limit_jobs=10)
        self.assertEqual(report.get("status"), "warning")
        alerts = report.get("alerts") if isinstance(report.get("alerts"), list) else []
        self.assertTrue(alerts)
        reasons = {str(item.get("reason") or "") for item in alerts}
        self.assertIn("signals_missing", reasons)

    def test_administrative_signal_does_not_change_live_score(self) -> None:
        job_id = self.db.insert_job(
            title="Signals admin-only job",
            jd_text="Backend engineer",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "signals-admin-only",
                "full_name": "Admin Only Candidate",
                "headline": "Backend Engineer",
                "location": "Warsaw",
                "languages": ["en"],
                "skills": ["python"],
                "years_experience": 5,
                "raw": {},
            }
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.80,
            status="verified",
            verification_notes={},
        )
        self.db.upsert_candidate_signal(
            job_id=job_id,
            candidate_id=candidate_id,
            source_type="operation_log",
            source_id="manual-admin-signal",
            signal_type="agent.outreach.send",
            signal_category="agent",
            title="manual admin signal",
            impact_score=2.0,
            confidence=0.9,
            signal_meta={"status": "ok"},
            observed_at="2026-02-28T12:00:00+00:00",
        )
        view = JobSignalsLiveViewService(self.db).build_job_view(job_id=job_id, limit_candidates=10, limit_signals=100)
        ranking = view.get("ranking") if isinstance(view.get("ranking"), list) else []
        self.assertEqual(len(ranking), 1)
        row = ranking[0]
        self.assertEqual(float(row.get("base_score") or 0.0), float(row.get("live_score") or 0.0))
        self.assertEqual(int(row.get("signal_count") or 0), 0)
        self.assertEqual(int(row.get("signal_count_total") or 0), 1)

    def test_evaluative_signal_changes_live_score(self) -> None:
        job_id = self.db.insert_job(
            title="Signals evaluative job",
            jd_text="Backend engineer",
            location="Remote",
            preferred_languages=["en"],
            seniority="senior",
        )
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "signals-evaluative",
                "full_name": "Evaluative Candidate",
                "headline": "Backend Engineer",
                "location": "Kyiv",
                "languages": ["en"],
                "skills": ["python", "aws"],
                "years_experience": 6,
                "raw": {},
            }
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.75,
            status="verified",
            verification_notes={},
        )
        self.db.upsert_candidate_signal(
            job_id=job_id,
            candidate_id=candidate_id,
            source_type="assessment",
            source_id="manual-evaluative-signal",
            signal_type="vetting",
            signal_category="sourcing_vetting",
            title="manual evaluative signal",
            impact_score=1.5,
            confidence=0.9,
            signal_meta={"agent_key": "sourcing_vetting", "stage_key": "vetting"},
            observed_at="2026-02-28T12:00:00+00:00",
        )
        view = JobSignalsLiveViewService(self.db).build_job_view(job_id=job_id, limit_candidates=10, limit_signals=100)
        ranking = view.get("ranking") if isinstance(view.get("ranking"), list) else []
        self.assertEqual(len(ranking), 1)
        row = ranking[0]
        self.assertGreater(float(row.get("live_score") or 0.0), float(row.get("base_score") or 0.0))
        self.assertEqual(int(row.get("signal_count") or 0), 1)


if __name__ == "__main__":
    unittest.main()
