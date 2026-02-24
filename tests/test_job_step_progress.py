import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import Database


class JobStepProgressTests(unittest.TestCase):
    def test_upsert_and_list_progress(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "job_step_progress.sqlite3"))
            db.init_schema()
            job_id = db.insert_job(
                title="Backend Engineer",
                jd_text="Python, AWS",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )

            db.upsert_job_step_progress(job_id=job_id, step="source", status="success", output={"total": 7})
            db.upsert_job_step_progress(job_id=job_id, step="verify", status="success", output={"verified": 3})
            db.upsert_job_step_progress(job_id=job_id, step="source", status="error", output={"error": "timeout"})

            rows = db.list_job_step_progress(job_id=job_id)
            by_step = {row["step"]: row for row in rows}

            self.assertIn("source", by_step)
            self.assertIn("verify", by_step)
            self.assertEqual(by_step["source"]["status"], "error")
            self.assertEqual((by_step["source"]["output_json"] or {}).get("error"), "timeout")
            self.assertEqual((by_step["verify"]["output_json"] or {}).get("verified"), 3)


if __name__ == "__main__":
    unittest.main()
