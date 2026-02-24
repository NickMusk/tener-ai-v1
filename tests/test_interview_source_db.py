from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_interview.source_db import SourceReadDatabase


class SourceReadDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "source.sqlite3"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_list_jobs_and_candidates(self) -> None:
        conn = sqlite3.connect(str(self.db_path))
        conn.executescript(
            """
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                jd_text TEXT NOT NULL,
                location TEXT,
                preferred_languages TEXT,
                seniority TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                linkedin_id TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                headline TEXT,
                location TEXT,
                languages TEXT,
                skills TEXT,
                years_experience INTEGER,
                source TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE candidate_job_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                candidate_id INTEGER NOT NULL,
                score REAL NOT NULL,
                status TEXT NOT NULL,
                verification_notes TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO jobs (id, title, jd_text, location, preferred_languages, seniority, created_at) VALUES (1, ?, ?, ?, ?, ?, ?)",
            ("ML Engineer", "JD text", "Remote", '["en"]', "senior", "2026-02-24T00:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO candidates
            (id, linkedin_id, full_name, headline, location, languages, skills, years_experience, source, created_at)
            VALUES (7, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ln_7",
                "Jane Doe",
                "Backend Engineer",
                "Berlin",
                '["en"]',
                '["python"]',
                6,
                "linkedin",
                "2026-02-24T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO candidate_job_matches
            (job_id, candidate_id, score, status, verification_notes, created_at)
            VALUES (1, 7, 0.91, 'verified', '{}', '2026-02-24T00:00:00+00:00')
            """
        )
        conn.commit()
        conn.close()

        source = SourceReadDatabase(str(self.db_path))
        jobs = source.list_jobs(limit=20)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], 1)
        self.assertEqual(jobs[0]["title"], "ML Engineer")
        self.assertIn("company", jobs[0])
        self.assertIsNone(jobs[0]["company"])

        candidates = source.list_candidates_for_job(job_id=1, limit=20)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["candidate_id"], 7)
        self.assertEqual(candidates[0]["candidate_name"], "Jane Doe")

        status = source.status()
        self.assertTrue(status["available"])
        self.assertIsNone(status["last_error"])

    def test_missing_schema_returns_empty_and_sets_error(self) -> None:
        source = SourceReadDatabase(str(self.db_path))
        jobs = source.list_jobs(limit=10)
        self.assertEqual(jobs, [])
        status = source.status()
        self.assertFalse(status["available"])
        self.assertIn("no such table", str(status["last_error"]).lower())


if __name__ == "__main__":
    unittest.main()
