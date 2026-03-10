from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import Database


class CandidateLinkedinPublicUrlTests(unittest.TestCase):
    def test_upsert_extracts_public_url_and_keeps_existing_value(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "db.sqlite3"))
            db.init_schema()

            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-public-url-1",
                    "full_name": "LinkedIn Public Url Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {"public_identifier": "ln-public-url-1"},
                },
                source="linkedin",
            )
            row = db.get_candidate(candidate_id)
            self.assertIsNotNone(row)
            self.assertEqual(
                str((row or {}).get("linkedin_public_url") or ""),
                "https://www.linkedin.com/in/ln-public-url-1",
            )

            db.upsert_candidate(
                {
                    "linkedin_id": "ln-public-url-1",
                    "full_name": "LinkedIn Public Url Candidate Updated",
                    "headline": "Staff Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python", "aws"],
                    "years_experience": 7,
                    "raw": {},
                },
                source="linkedin",
            )
            row_after = db.get_candidate(candidate_id)
            self.assertEqual(
                str((row_after or {}).get("linkedin_public_url") or ""),
                "https://www.linkedin.com/in/ln-public-url-1",
            )

    def test_list_candidates_for_job_returns_public_url(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "db.sqlite3"))
            db.init_schema()

            job_id = db.insert_job(
                title="Backend Engineer",
                jd_text="Need Python and AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-public-url-2",
                    "linkedin_public_url": "https://www.linkedin.com/in/ln-public-url-2",
                    "full_name": "List Candidate Url",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python", "aws"],
                    "years_experience": 5,
                    "raw": {},
                },
                source="linkedin",
            )
            db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=0.9,
                status="verified",
                verification_notes={},
            )

            rows = db.list_candidates_for_job(job_id=job_id)
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                str(rows[0].get("linkedin_public_url") or ""),
                "https://www.linkedin.com/in/ln-public-url-2",
            )

    def test_upsert_persists_provider_identity_and_lookup_matches_provider_id(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "db.sqlite3"))
            db.init_schema()

            candidate_id = db.upsert_candidate(
                {
                    "linkedin_id": "ln-provider-1",
                    "provider_id": "provider-1",
                    "unipile_profile_id": "provider-1",
                    "attendee_provider_id": "provider-1",
                    "full_name": "Provider Identity Candidate",
                    "headline": "Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python"],
                    "years_experience": 6,
                    "raw": {},
                },
                source="linkedin",
            )

            row = db.get_candidate(candidate_id)
            self.assertEqual(str((row or {}).get("provider_id") or ""), "provider-1")
            self.assertEqual(str((row or {}).get("unipile_profile_id") or ""), "provider-1")
            self.assertEqual(str((row or {}).get("attendee_provider_id") or ""), "provider-1")

            db.upsert_candidate(
                {
                    "linkedin_id": "ln-provider-1",
                    "full_name": "Provider Identity Candidate Updated",
                    "headline": "Staff Backend Engineer",
                    "location": "Remote",
                    "languages": ["en"],
                    "skills": ["python", "aws"],
                    "years_experience": 7,
                    "raw": {},
                },
                source="linkedin",
            )
            updated = db.get_candidate(candidate_id)
            self.assertEqual(str((updated or {}).get("attendee_provider_id") or ""), "provider-1")

            by_provider = db.get_candidate_by_linkedin_id("provider-1")
            self.assertIsNotNone(by_provider)
            self.assertEqual(int((by_provider or {}).get("id") or 0), candidate_id)


if __name__ == "__main__":
    unittest.main()
