from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

from tener_ai.agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.workflow import WorkflowService


class _Provider:
    def search_profiles(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        return []

    def enrich_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        return dict(profile)

    def send_message(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "chat_id": "chat-1"}

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        return {"provider": "stub", "sent": True, "request_id": "req-1"}

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        return {"provider": "stub", "connected": True}


class LinkedInSenderIdentityTests(unittest.TestCase):
    def test_followup_uses_linkedin_account_label_and_not_casey_default(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as td:
            previous = os.environ.get("TENER_LINKEDIN_RECRUITER_NAME")
            os.environ["TENER_LINKEDIN_RECRUITER_NAME"] = ""
            try:
                db = Database(str(Path(td) / "linkedin_sender_identity.sqlite3"))
                db.init_schema()

                matching = MatchingEngine(str(root / "config" / "matching_rules.json"))
                workflow = WorkflowService(
                    db=db,
                    sourcing_agent=SourcingAgent(_Provider()),  # type: ignore[arg-type]
                    verification_agent=VerificationAgent(matching),
                    outreach_agent=OutreachAgent(str(root / "config" / "outreach_templates.json"), matching),
                    faq_agent=FAQAgent(str(root / "config" / "outreach_templates.json"), matching),
                )

                account_id = db.upsert_linkedin_account(
                    provider="unipile",
                    provider_account_id="acc-1",
                    status="connected",
                    label="Nick Nagatkin",
                )
                job_id = db.insert_job(
                    title="Senior Backend Engineer",
                    jd_text="Need Python and AWS.",
                    location="Remote",
                    preferred_languages=["en"],
                    seniority="senior",
                )
                candidate_id = db.upsert_candidate(
                    {
                        "linkedin_id": "candidate-1",
                        "full_name": "Arpit",
                        "headline": "Backend Engineer",
                        "location": "Remote",
                        "languages": ["en"],
                        "skills": ["python"],
                        "years_experience": 5,
                        "raw": {},
                    }
                )
                conversation_id = db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
                db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)

                msg = workflow._compose_linkedin_followup_message(
                    job=db.get_job(job_id) or {},
                    candidate=db.get_candidate(candidate_id) or {},
                    language="en",
                    history=[],
                    state=None,
                    fallback_message="",
                    conversation=db.get_conversation(conversation_id),
                )

                self.assertIn("Nick Nagatkin", msg)
                self.assertNotIn("Casey", msg)
                self.assertEqual(workflow._linkedin_recruiter_name(conversation=None), "")
            finally:
                if previous is None:
                    os.environ.pop("TENER_LINKEDIN_RECRUITER_NAME", None)
                else:
                    os.environ["TENER_LINKEDIN_RECRUITER_NAME"] = previous


if __name__ == "__main__":
    unittest.main()
