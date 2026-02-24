from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class InterviewDatabase:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

    @contextmanager
    def transaction(self) -> Iterable[sqlite3.Connection]:
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def init_schema(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS interview_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE NOT NULL,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            candidate_name TEXT,
            conversation_id INTEGER,
            provider TEXT NOT NULL,
            provider_assessment_id TEXT,
            provider_invitation_id TEXT,
            provider_candidate_id TEXT,
            status TEXT NOT NULL,
            language TEXT,
            entry_token_hash TEXT UNIQUE NOT NULL,
            entry_token_expires_at TEXT NOT NULL,
            provider_interview_url TEXT,
            started_at TEXT,
            completed_at TEXT,
            scored_at TEXT,
            last_sync_at TEXT,
            last_error_code TEXT,
            last_error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_interview_sessions_job_status
            ON interview_sessions(job_id, status);
        CREATE INDEX IF NOT EXISTS idx_interview_sessions_candidate
            ON interview_sessions(candidate_id);
        CREATE INDEX IF NOT EXISTS idx_interview_sessions_provider
            ON interview_sessions(provider, provider_invitation_id);

        CREATE TABLE IF NOT EXISTS interview_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            provider_result_id TEXT,
            result_version INTEGER NOT NULL DEFAULT 1,
            technical_score REAL,
            soft_skills_score REAL,
            culture_fit_score REAL,
            total_score REAL,
            score_confidence REAL,
            pass_recommendation TEXT,
            normalized_json TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(session_id, result_version)
        );

        CREATE TABLE IF NOT EXISTS interview_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            payload TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS candidate_interview_summary (
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            candidate_name TEXT,
            session_id TEXT NOT NULL,
            interview_status TEXT NOT NULL,
            technical_score REAL,
            soft_skills_score REAL,
            culture_fit_score REAL,
            total_score REAL,
            score_confidence REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(job_id, candidate_id)
        );

        CREATE TABLE IF NOT EXISTS idempotency_keys (
            route TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            response_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(route, idempotency_key)
        );

        CREATE TABLE IF NOT EXISTS job_step_progress (
            job_id INTEGER NOT NULL,
            step TEXT NOT NULL,
            status TEXT NOT NULL,
            output_json TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(job_id, step)
        );
        """
        with self.transaction() as conn:
            conn.executescript(schema)

    def insert_session(self, item: Dict[str, Any]) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO interview_sessions (
                    session_id, job_id, candidate_id, candidate_name, conversation_id,
                    provider, provider_assessment_id, provider_invitation_id, provider_candidate_id,
                    status, language, entry_token_hash, entry_token_expires_at,
                    provider_interview_url, started_at, completed_at, scored_at,
                    last_sync_at, last_error_code, last_error_message,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["session_id"],
                    item["job_id"],
                    item["candidate_id"],
                    item.get("candidate_name"),
                    item.get("conversation_id"),
                    item["provider"],
                    item.get("provider_assessment_id"),
                    item.get("provider_invitation_id"),
                    item.get("provider_candidate_id"),
                    item["status"],
                    item.get("language"),
                    item["entry_token_hash"],
                    item["entry_token_expires_at"],
                    item.get("provider_interview_url"),
                    item.get("started_at"),
                    item.get("completed_at"),
                    item.get("scored_at"),
                    item.get("last_sync_at"),
                    item.get("last_error_code"),
                    item.get("last_error_message"),
                    item["created_at"],
                    item["updated_at"],
                ),
            )

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM interview_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_session_by_token_hash(self, token_hash: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM interview_sessions WHERE entry_token_hash = ?",
            (token_hash,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_sessions(
        self,
        limit: int = 100,
        status: Optional[str] = None,
        job_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 1000))
        params: List[Any] = []
        where: List[str] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if job_id is not None:
            where.append("job_id = ?")
            params.append(job_id)

        where_sql = ""
        if where:
            where_sql = "WHERE " + " AND ".join(where)

        rows = self._conn.execute(
            f"""
            SELECT *
            FROM interview_sessions
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params, safe_limit),
        ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_latest_session_for_candidate(self, job_id: int, candidate_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM interview_sessions
            WHERE job_id = ? AND candidate_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_id, candidate_id),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def update_session(self, session_id: str, fields: Dict[str, Any]) -> None:
        if not fields:
            return
        keys = sorted(fields.keys())
        set_sql = ", ".join([f"{k} = ?" for k in keys])
        values = [fields[k] for k in keys]
        with self.transaction() as conn:
            conn.execute(
                f"UPDATE interview_sessions SET {set_sql} WHERE session_id = ?",
                (*values, session_id),
            )

    def insert_event(
        self,
        session_id: str,
        event_type: str,
        source: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO interview_events (session_id, event_type, source, payload, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, event_type, source, json.dumps(payload or {}), utc_now_iso()),
            )
            return int(cur.lastrowid)

    def insert_result(
        self,
        session_id: str,
        provider_result_id: Optional[str],
        scores: Dict[str, Any],
        normalized: Dict[str, Any],
        raw_payload: Dict[str, Any],
    ) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(result_version), 0) AS max_v FROM interview_results WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        max_v = int(row["max_v"]) if row and row["max_v"] is not None else 0
        version = max_v + 1

        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO interview_results (
                    session_id, provider_result_id, result_version,
                    technical_score, soft_skills_score, culture_fit_score, total_score,
                    score_confidence, pass_recommendation, normalized_json, raw_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    provider_result_id,
                    version,
                    scores.get("technical_score"),
                    scores.get("soft_skills_score"),
                    scores.get("culture_fit_score"),
                    scores.get("total_score"),
                    scores.get("score_confidence"),
                    scores.get("pass_recommendation"),
                    json.dumps(normalized),
                    json.dumps(raw_payload),
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def get_latest_result(self, session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM interview_results
            WHERE session_id = ?
            ORDER BY result_version DESC
            LIMIT 1
            """,
            (session_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def upsert_candidate_summary(
        self,
        job_id: int,
        candidate_id: int,
        candidate_name: Optional[str],
        session_id: str,
        interview_status: str,
        technical_score: Optional[float],
        soft_skills_score: Optional[float],
        culture_fit_score: Optional[float],
        total_score: Optional[float],
        score_confidence: Optional[float],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO candidate_interview_summary (
                    job_id, candidate_id, candidate_name, session_id, interview_status,
                    technical_score, soft_skills_score, culture_fit_score,
                    total_score, score_confidence, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, candidate_id)
                DO UPDATE SET
                    candidate_name = excluded.candidate_name,
                    session_id = excluded.session_id,
                    interview_status = excluded.interview_status,
                    technical_score = excluded.technical_score,
                    soft_skills_score = excluded.soft_skills_score,
                    culture_fit_score = excluded.culture_fit_score,
                    total_score = excluded.total_score,
                    score_confidence = excluded.score_confidence,
                    updated_at = excluded.updated_at
                """,
                (
                    job_id,
                    candidate_id,
                    candidate_name,
                    session_id,
                    interview_status,
                    technical_score,
                    soft_skills_score,
                    culture_fit_score,
                    total_score,
                    score_confidence,
                    utc_now_iso(),
                ),
            )

    def list_leaderboard(self, job_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 500))
        rows = self._conn.execute(
            """
            SELECT
                job_id,
                candidate_id,
                candidate_name,
                session_id,
                interview_status,
                technical_score,
                soft_skills_score,
                culture_fit_score,
                total_score,
                score_confidence,
                updated_at
            FROM candidate_interview_summary
            WHERE job_id = ?
            ORDER BY
                CASE
                    WHEN technical_score IS NOT NULL AND soft_skills_score IS NOT NULL THEN 1
                    WHEN technical_score IS NOT NULL OR soft_skills_score IS NOT NULL THEN 2
                    ELSE 3
                END ASC,
                COALESCE(total_score, -1.0) DESC,
                candidate_id ASC
            LIMIT ?
            """,
            (job_id, safe_limit),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_idempotency_record(self, route: str, key: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT route, idempotency_key, payload_hash, status_code, response_json, created_at
            FROM idempotency_keys
            WHERE route = ? AND idempotency_key = ?
            """,
            (route, key),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def put_idempotency_record(
        self,
        route: str,
        key: str,
        payload_hash: str,
        status_code: int,
        response: Dict[str, Any],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO idempotency_keys (
                    route, idempotency_key, payload_hash, status_code, response_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (route, key, payload_hash, status_code, json.dumps(response), utc_now_iso()),
            )

    def upsert_job_step_progress(
        self,
        job_id: int,
        status: str,
        output: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO job_step_progress (job_id, step, status, output_json, updated_at)
                VALUES (?, 'interview', ?, ?, ?)
                ON CONFLICT(job_id, step)
                DO UPDATE SET
                    status = excluded.status,
                    output_json = excluded.output_json,
                    updated_at = excluded.updated_at
                """,
                (job_id, status, json.dumps(output or {}), utc_now_iso()),
            )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        for field in ("payload", "normalized_json", "raw_payload", "response_json", "output_json"):
            if field in item and item[field]:
                try:
                    item[field] = json.loads(item[field])
                except json.JSONDecodeError:
                    pass
        return item
