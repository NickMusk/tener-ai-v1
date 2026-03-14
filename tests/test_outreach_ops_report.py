from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai import main as api_main
from tener_ai.db import Database, utc_now_iso


class OutreachOpsReportTests(unittest.TestCase):
    def test_report_includes_recent_replies_and_recent_outbound_logs(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "outreach_ops_report.sqlite3"))
            db.init_schema()

            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-report-1",
                status="connected",
                label="QA Sender",
                connected_at=utc_now_iso(),
            )
            job_id = db.insert_job(
                title="Manual QA Engineer",
                jd_text="Need QA experience.",
                location="Remote",
                preferred_languages=["en"],
                seniority="middle",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ops-report-ln-1",
                    "full_name": "Report Candidate",
                    "headline": "QA",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["qa"],
                    "years_experience": 4,
                    "raw": {},
                },
                source="linkedin",
            )
            conversation_id = db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
            db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content="Quick follow-up from our side",
                candidate_language="en",
                meta={"type": "pre_resume_followup", "auto": True},
            )
            db.add_message(
                conversation_id=conversation_id,
                direction="inbound",
                content="Got it, will reply later today",
                candidate_language="en",
                meta={"type": "candidate_message"},
            )

            report = api_main.TenerRequestHandler._build_outreach_ops_report(
                db=db,
                job_id=job_id,
                logs_limit=200,
                chats_limit=200,
            )

            summary = report.get("summary") or {}
            self.assertEqual(int(summary.get("replies_24h") or 0), 1)
            recent_replies = report.get("recent_replies") or []
            self.assertEqual(len(recent_replies), 1)
            self.assertEqual(str(recent_replies[0].get("candidate_name") or ""), "Report Candidate")
            self.assertEqual(str(recent_replies[0].get("text") or ""), "Got it, will reply later today")
            recent_outbound = report.get("recent_outbound") or []
            self.assertEqual(len(recent_outbound), 1)
            self.assertEqual(str(recent_outbound[0].get("linkedin_account_label") or ""), "QA Sender")
            self.assertEqual(str(recent_outbound[0].get("text") or ""), "Quick follow-up from our side")


if __name__ == "__main__":
    unittest.main()
