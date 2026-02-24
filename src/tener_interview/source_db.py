from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional


class SourceReadDatabase:
    """Read-only accessor for jobs/candidates from the main Tener DB."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._last_error: str = ""

    def status(self) -> Dict[str, Any]:
        return {
            "path": self.db_path,
            "available": self._last_error == "",
            "last_error": self._last_error or None,
        }

    def list_jobs(self, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        try:
            rows = self._conn.execute(
                """
                SELECT id, title, jd_text, location, preferred_languages, seniority, created_at
                FROM jobs
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            self._last_error = ""
            return [self._row_to_dict(r) for r in rows]
        except sqlite3.Error as exc:
            self._last_error = str(exc)
            return []

    def list_candidates_for_job(self, job_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 2000))
        try:
            rows = self._conn.execute(
                """
                SELECT
                    m.job_id,
                    m.candidate_id,
                    m.score AS match_score,
                    m.status AS match_status,
                    c.full_name AS candidate_name,
                    c.headline,
                    c.location,
                    c.languages,
                    c.skills,
                    c.years_experience,
                    c.linkedin_id
                FROM candidate_job_matches m
                JOIN candidates c ON c.id = m.candidate_id
                WHERE m.job_id = ?
                ORDER BY m.score DESC, c.id ASC
                LIMIT ?
                """,
                (int(job_id), safe_limit),
            ).fetchall()
            self._last_error = ""
            return [self._row_to_dict(r) for r in rows]
        except sqlite3.Error as exc:
            self._last_error = str(exc)
            return []

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

