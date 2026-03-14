from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.candidate_profile import CandidateProfileService
from tener_ai.candidate_scoring import CandidateScoringPolicy
from tener_ai.db import Database
from tener_ai.matching import MatchingEngine
from tener_ai.agents import VerificationAgent
from tener_ai.workflow import WorkflowService
from tener_ai.message_extraction import parse_resume_links


class _DummyAgent:
    pass


class _FakeSourcingAgent:
    def __init__(self, messages):
        self._messages = list(messages)

    def fetch_chat_messages(self, chat_id: str, limit: int = 20):
        return self._messages[:limit]


class _FakeManagedProvider:
    def __init__(self, enriched_profile):
        self.enriched_profile = dict(enriched_profile)

    def enrich_profile(self, profile):
        merged = dict(profile)
        merged.update(self.enriched_profile)
        return merged


class _RepairWorkflow(WorkflowService):
    def __init__(self, *, managed_profile, **kwargs):
        super().__init__(**kwargs)
        self._managed_profile = dict(managed_profile)

    def _build_managed_provider(self, account_id: str):
        return _FakeManagedProvider(self._managed_profile)

    def _download_resume_payload(self, remote_url: str):
        return b"%PDF-1.4 fake resume payload", "application/pdf"


class CandidateRepairWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp_path = Path(self._tmp.name)
        self.db = Database(str(tmp_path / "candidate_repair.sqlite3"))
        self.db.init_schema()
        self.matching = MatchingEngine(str(self.root / "config" / "matching_rules.json"))
        self.scoring = CandidateScoringPolicy(str(self.root / "config" / "candidate_scoring_formula.json"))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_parse_resume_links_rejects_linkedin_profiles(self) -> None:
        links = parse_resume_links("Here is the CV https://www.linkedin.com/in/someone")
        self.assertEqual(links, [])
        valid = parse_resume_links("Here is the CV https://drive.google.com/file/d/abc/resume.pdf")
        self.assertEqual(valid, ["https://drive.google.com/file/d/abc/resume.pdf"])

    def test_reenrich_candidate_updates_skills_and_backfills_valid_resume_link(self) -> None:
        job_id = self.db.insert_job(
            title="Manual QA Engineer",
            company="Tener",
            jd_text="Requirements: manual testing, api testing. Nice to have: sql.",
            location="Remote",
            preferred_languages=["en"],
            seniority="middle",
            must_have_skills=["manual testing", "api testing"],
            nice_to_have_skills=["sql"],
        )
        candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": "repair-candidate-1",
                "provider_id": "repair-candidate-1",
                "unipile_profile_id": "repair-candidate-1",
                "attendee_provider_id": "repair-candidate-1",
                "full_name": "Mykola B.",
                "headline": "Senior QA Engineer",
                "location": "Mexico",
                "languages": ["en"],
                "skills": ["qa"],
                "years_experience": 0,
            },
            source="linkedin",
        )
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.12,
            status="needs_resume",
            verification_notes={},
        )
        account_id = self.db.upsert_linkedin_account(
            provider="unipile",
            provider_account_id="acc-live",
            status="connected",
            label="QA Account",
        )
        conversation_id = self.db.create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
        self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
        self.db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id="chat-1")

        workflow = _RepairWorkflow(
            db=self.db,
            sourcing_agent=_FakeSourcingAgent(
                [
                    {
                        "direction": "inbound",
                        "provider_message_id": "msg-1",
                        "created_at": "2026-03-14T02:30:17+00:00",
                        "text": "Here is the CV https://drive.google.com/file/d/abc/resume.pdf",
                        "attachments": [],
                    }
                ]
            ),
            verification_agent=VerificationAgent(self.matching),
            outreach_agent=_DummyAgent(),
            faq_agent=_DummyAgent(),
            managed_profile={
                "skills": ["manual testing", "api testing", "sql"],
                "years_experience": 5,
                "headline": "Senior QA Engineer | Manual Testing | API Testing",
            },
            managed_unipile_api_key="test-key",
        )

        result = workflow.re_enrich_candidate(
            candidate_id=candidate_id,
            job_id=job_id,
            account_id=account_id,
            backfill_resume=True,
        )

        updated_candidate = self.db.get_candidate(candidate_id) or {}
        self.assertEqual(updated_candidate.get("skills") or [], ["manual testing", "api testing", "sql"])
        self.assertEqual(int(updated_candidate.get("years_experience") or 0), 5)
        self.assertEqual(int((result.get("resume_backfill") or {}).get("processed") or 0), 1)

        assets = self.db.list_resume_assets_for_candidate(candidate_id=candidate_id, job_id=job_id, limit=20)
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].get("remote_url"), "https://drive.google.com/file/d/abc/resume.pdf")
        self.assertTrue(str(assets[0].get("storage_path") or "").strip())

        profile_service = CandidateProfileService(
            db=self.db,
            matching_engine=self.matching,
            scoring_policy=self.scoring,
            llm_responder=None,
        )
        payload = profile_service.build_candidate_profile(
            candidate_id=candidate_id,
            selected_job_id=job_id,
            include_explanation=False,
        )
        jobs = payload.get("jobs") if isinstance(payload.get("jobs"), list) else []
        fit_breakdown = jobs[0].get("fit_breakdown") if jobs and isinstance(jobs[0].get("fit_breakdown"), dict) else {}
        must_have = fit_breakdown.get("must_have") if isinstance(fit_breakdown.get("must_have"), dict) else {}
        self.assertEqual(must_have.get("missing") or [], [])
        self.assertEqual(
            must_have.get("weak") or [],
            ["manual testing", "api testing"],
        )


if __name__ == "__main__":
    unittest.main()
