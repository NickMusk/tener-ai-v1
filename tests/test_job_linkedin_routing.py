from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import Database


class JobLinkedinRoutingDbTests(unittest.TestCase):
    def test_job_defaults_to_auto_routing(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "job_routing_default.sqlite3"))
            db.init_schema()
            job_id = db.insert_job(
                title="Backend Engineer",
                jd_text="Need Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            job = db.get_job(job_id) or {}
            self.assertEqual(str(job.get("linkedin_routing_mode") or ""), "auto")
            self.assertEqual(db.list_job_linkedin_account_ids(job_id), [])

    def test_replace_assignments_filters_missing_account_ids(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "job_routing_assign.sqlite3"))
            db.init_schema()
            job_id = db.insert_job(
                title="Backend Engineer",
                jd_text="Need Python",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-1",
                status="connected",
            )

            assigned = db.replace_job_linkedin_account_assignments(
                job_id=job_id,
                account_ids=[-1, 0, account_id, account_id + 9999],
            )
            self.assertEqual(assigned, [account_id])
            self.assertEqual(db.list_job_linkedin_account_ids(job_id), [account_id])

            updated = db.update_job_linkedin_routing_mode(job_id=job_id, routing_mode="manual")
            self.assertTrue(updated)
            job = db.get_job(job_id) or {}
            self.assertEqual(str(job.get("linkedin_routing_mode") or ""), "manual")


if __name__ == "__main__":
    unittest.main()
