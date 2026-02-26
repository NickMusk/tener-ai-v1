from __future__ import annotations

import json
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
            select_columns, join_clause = self._job_select_columns(job_alias="j")
            rows = self._conn.execute(
                f"""
                SELECT {select_columns}
                FROM jobs j
                {join_clause}
                ORDER BY j.id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            self._last_error = ""
            return [self._row_to_dict(r) for r in rows]
        except sqlite3.Error as exc:
            self._last_error = str(exc)
            return []

    def get_job(self, job_id: int) -> Dict[str, Any]:
        try:
            select_columns, join_clause = self._job_select_columns(job_alias="j")
            row = self._conn.execute(
                f"""
                SELECT {select_columns}
                FROM jobs j
                {join_clause}
                WHERE j.id = ?
                LIMIT 1
                """,
                (int(job_id),),
            ).fetchone()
            self._last_error = ""
            return self._row_to_dict(row) if row else {}
        except sqlite3.Error as exc:
            self._last_error = str(exc)
            return {}

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
        item = dict(row)
        for key in ("preferred_languages", "company_culture_profile"):
            raw = item.get(key)
            if not raw:
                continue
            if isinstance(raw, (dict, list)):
                continue
            try:
                item[key] = json.loads(raw)
            except Exception:
                pass
        return item

    def _job_select_columns(self, job_alias: str = "jobs") -> tuple[str, str]:
        columns = set(self._table_columns("jobs"))
        company_expr = f"{job_alias}.company" if "company" in columns else "NULL AS company"
        website_expr = f"{job_alias}.company_website" if "company_website" in columns else "NULL AS company_website"
        has_culture_table = self._table_exists("job_culture_profiles")
        join_clause = (
            f"LEFT JOIN job_culture_profiles cp ON cp.job_id = {job_alias}.id"
            if has_culture_table
            else ""
        )
        culture_profile_expr = "cp.profile_json AS company_culture_profile" if has_culture_table else "NULL AS company_culture_profile"
        culture_status_expr = "cp.status AS company_culture_profile_status" if has_culture_table else "NULL AS company_culture_profile_status"
        culture_generated_expr = (
            "cp.generated_at AS company_culture_profile_generated_at"
            if has_culture_table
            else "NULL AS company_culture_profile_generated_at"
        )
        select_columns = (
            f"{job_alias}.id, {job_alias}.title, {company_expr}, {website_expr}, "
            f"{job_alias}.jd_text, {job_alias}.location, {job_alias}.preferred_languages, "
            f"{job_alias}.seniority, {job_alias}.created_at, "
            f"{culture_profile_expr}, {culture_status_expr}, {culture_generated_expr}"
        )
        return select_columns, join_clause

    def _table_columns(self, table_name: str) -> List[str]:
        try:
            rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except sqlite3.Error:
            return []
        return [str(row["name"]) for row in rows]

    def _table_exists(self, table_name: str) -> bool:
        try:
            row = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
                (str(table_name),),
            ).fetchone()
        except sqlite3.Error:
            return False
        return row is not None
