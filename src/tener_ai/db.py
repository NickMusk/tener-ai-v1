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


class Database:
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
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            jd_text TEXT NOT NULL,
            location TEXT,
            preferred_languages TEXT,
            seniority TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS candidates (
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

        CREATE TABLE IF NOT EXISTS candidate_job_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            score REAL NOT NULL,
            status TEXT NOT NULL,
            verification_notes TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(job_id, candidate_id),
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(candidate_id) REFERENCES candidates(id)
        );

        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            status TEXT NOT NULL,
            last_message_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(candidate_id) REFERENCES candidates(id)
        );

        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            direction TEXT NOT NULL,
            candidate_language TEXT,
            content TEXT NOT NULL,
            meta TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(conversation_id) REFERENCES conversations(id)
        );

        CREATE TABLE IF NOT EXISTS operation_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            status TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL
        );
        """
        with self.transaction() as conn:
            conn.executescript(schema)

    def insert_job(
        self,
        title: str,
        jd_text: str,
        location: Optional[str],
        preferred_languages: List[str],
        seniority: Optional[str],
    ) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO jobs (title, jd_text, location, preferred_languages, seniority, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    jd_text,
                    location,
                    json.dumps(preferred_languages),
                    seniority,
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def upsert_candidate(self, profile: Dict[str, Any], source: str = "linkedin") -> int:
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM candidates WHERE linkedin_id = ?",
                (profile["linkedin_id"],),
            ).fetchone()
            if existing:
                candidate_id = int(existing["id"])
                conn.execute(
                    """
                    UPDATE candidates
                    SET full_name = ?, headline = ?, location = ?, languages = ?, skills = ?, years_experience = ?, source = ?
                    WHERE id = ?
                    """,
                    (
                        profile.get("full_name"),
                        profile.get("headline"),
                        profile.get("location"),
                        json.dumps(profile.get("languages", [])),
                        json.dumps(profile.get("skills", [])),
                        profile.get("years_experience"),
                        source,
                        candidate_id,
                    ),
                )
                return candidate_id

            cur = conn.execute(
                """
                INSERT INTO candidates
                (linkedin_id, full_name, headline, location, languages, skills, years_experience, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile.get("linkedin_id"),
                    profile.get("full_name"),
                    profile.get("headline"),
                    profile.get("location"),
                    json.dumps(profile.get("languages", [])),
                    json.dumps(profile.get("skills", [])),
                    profile.get("years_experience"),
                    source,
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def get_candidate(self, candidate_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM candidates WHERE id = ?", (candidate_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def create_candidate_match(
        self,
        job_id: int,
        candidate_id: int,
        score: float,
        status: str,
        verification_notes: Dict[str, Any],
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO candidate_job_matches
                (id, job_id, candidate_id, score, status, verification_notes, created_at)
                VALUES (
                    (SELECT id FROM candidate_job_matches WHERE job_id = ? AND candidate_id = ?),
                    ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    job_id,
                    candidate_id,
                    job_id,
                    candidate_id,
                    score,
                    status,
                    json.dumps(verification_notes),
                    utc_now_iso(),
                ),
            )

    def list_candidates_for_job(self, job_id: int) -> List[Dict[str, Any]]:
        query = """
        SELECT
            m.id AS match_id,
            m.score,
            m.status,
            m.verification_notes,
            c.id AS candidate_id,
            c.linkedin_id,
            c.full_name,
            c.headline,
            c.location,
            c.languages,
            c.skills,
            c.years_experience
        FROM candidate_job_matches m
        JOIN candidates c ON c.id = m.candidate_id
        WHERE m.job_id = ?
        ORDER BY m.score DESC
        """
        rows = self._conn.execute(query, (job_id,)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def create_conversation(self, job_id: int, candidate_id: int, channel: str = "linkedin") -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO conversations (job_id, candidate_id, channel, status, last_message_at, created_at)
                VALUES (?, ?, ?, 'active', ?, ?)
                """,
                (job_id, candidate_id, channel, utc_now_iso(), utc_now_iso()),
            )
            return int(cur.lastrowid)

    def get_or_create_conversation(self, job_id: int, candidate_id: int, channel: str = "linkedin") -> int:
        row = self._conn.execute(
            "SELECT id FROM conversations WHERE job_id = ? AND candidate_id = ? AND channel = ? ORDER BY id DESC LIMIT 1",
            (job_id, candidate_id, channel),
        ).fetchone()
        if row:
            return int(row["id"])
        return self.create_conversation(job_id=job_id, candidate_id=candidate_id, channel=channel)

    def add_message(
        self,
        conversation_id: int,
        direction: str,
        content: str,
        candidate_language: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO messages (conversation_id, direction, candidate_language, content, meta, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    direction,
                    candidate_language,
                    content,
                    json.dumps(meta or {}),
                    utc_now_iso(),
                ),
            )
            conn.execute(
                "UPDATE conversations SET last_message_at = ? WHERE id = ?",
                (utc_now_iso(), conversation_id),
            )
            return int(cur.lastrowid)

    def get_conversation(self, conversation_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM conversations WHERE id = ?", (conversation_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def list_messages(self, conversation_id: int) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY id ASC",
            (conversation_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def log_operation(
        self,
        operation: str,
        status: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO operation_logs (operation, entity_type, entity_id, status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    operation,
                    entity_type,
                    entity_id,
                    status,
                    json.dumps(details or {}),
                    utc_now_iso(),
                ),
            )

    def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM operation_logs ORDER BY id DESC LIMIT ?",
            (max(1, min(limit, 1000)),),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        for field in ("preferred_languages", "languages", "skills", "verification_notes", "meta", "details"):
            if field in item and item[field]:
                try:
                    item[field] = json.loads(item[field])
                except json.JSONDecodeError:
                    pass
        return item
