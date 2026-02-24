from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _StaticChatProvider:
    def __init__(self) -> None:
        self.chat_id = "chat-shared-42"

    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return [
            {
                "linkedin_id": "ln-shared-chat-1",
                "full_name": "Shared Chat Candidate",
                "headline": "Senior Backend Engineer",
                "location": "Remote",
                "languages": ["en"],
                "skills": ["python", "aws", "docker", "postgresql"],
                "years_experience": 8,
                "raw": {"public_identifier": "ln-shared-chat-1"},
            }
        ]

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {
            "sent": True,
            "provider": "stub",
            "chat_id": self.chat_id,
        }


class ExternalChatIdRebindTests(unittest.TestCase):
    def test_workflow_rebinds_shared_chat_id_for_same_candidate_across_jobs(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "external_chat_id_rebind.sqlite3"))
            db.init_schema()

            matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
            provider = _StaticChatProvider()
            workflow = WorkflowService(
                db=db,
                sourcing_agent=SourcingAgent(provider),
                verification_agent=VerificationAgent(matching),
                outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                contact_all_mode=True,
                require_resume_before_final_verify=True,
            )

            job_1 = db.insert_job(
                title="Job One",
                jd_text="Senior Backend Engineer. Python, AWS, distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            job_2 = db.insert_job(
                title="Job Two",
                jd_text="Senior Backend Engineer. Python, AWS, distributed systems.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            summary_1 = workflow.execute_job_workflow(job_id=job_1, limit=5)
            summary_2 = workflow.execute_job_workflow(job_id=job_2, limit=5)
            self.assertEqual(summary_1.outreach_sent, 1)
            self.assertEqual(summary_2.outreach_sent, 1)

            candidate = db.get_candidate_by_linkedin_id("ln-shared-chat-1")
            self.assertIsNotNone(candidate)
            candidate_id = int(candidate["id"])

            rows = db._conn.execute(
                """
                SELECT id, job_id, external_chat_id
                FROM conversations
                WHERE candidate_id = ?
                ORDER BY id ASC
                """,
                (candidate_id,),
            ).fetchall()
            self.assertEqual(len(rows), 2)

            chat_holders = [r for r in rows if str(r["external_chat_id"] or "") == provider.chat_id]
            self.assertEqual(len(chat_holders), 1)
            self.assertEqual(int(chat_holders[0]["job_id"]), job_2)
            self.assertEqual(str(rows[0]["external_chat_id"] or ""), "")


if __name__ == "__main__":
    unittest.main()
