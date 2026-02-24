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
            external_chat_id TEXT UNIQUE,
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

        CREATE TABLE IF NOT EXISTS pre_resume_sessions (
            session_id TEXT PRIMARY KEY,
            conversation_id INTEGER UNIQUE NOT NULL,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            language TEXT,
            last_intent TEXT,
            followups_sent INTEGER NOT NULL DEFAULT 0,
            turns INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            resume_links TEXT,
            next_followup_at TEXT,
            state_json TEXT NOT NULL,
            instruction TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pre_resume_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            conversation_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            intent TEXT,
            inbound_text TEXT,
            outbound_text TEXT,
            state_status TEXT,
            details TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            payload TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_step_progress (
            job_id INTEGER NOT NULL,
            step TEXT NOT NULL,
            status TEXT NOT NULL,
            output_json TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(job_id, step),
            FOREIGN KEY(job_id) REFERENCES jobs(id)
        );
        """
        with self.transaction() as conn:
            conn.executescript(schema)
        self._migrate_schema()

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

    def update_job_jd_text(self, job_id: int, jd_text: str) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                SET jd_text = ?
                WHERE id = ?
                """,
                (jd_text, job_id),
            )
            return cur.rowcount > 0

    def list_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?",
            (max(1, min(limit, 1000)),),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

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

    def get_candidate_match(self, job_id: int, candidate_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM candidate_job_matches
            WHERE job_id = ? AND candidate_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_id, candidate_id),
        ).fetchone()
        return self._row_to_dict(row) if row else None

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

    def set_conversation_external_chat_id(self, conversation_id: int, external_chat_id: str) -> None:
        if not external_chat_id:
            return
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE conversations
                SET external_chat_id = ?
                WHERE id = ?
                """,
                (external_chat_id, conversation_id),
            )

    def get_conversation_by_external_chat_id(self, external_chat_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE external_chat_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (external_chat_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_candidate_by_linkedin_id(self, linkedin_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM candidates WHERE linkedin_id = ?",
            (linkedin_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_latest_conversation_for_candidate(self, candidate_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM conversations
            WHERE candidate_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (candidate_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

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

    def update_conversation_status(self, conversation_id: int, status: str) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE conversations
                SET status = ?
                WHERE id = ?
                """,
                (status, conversation_id),
            )
            return cur.rowcount > 0

    def list_conversations_overview(self, limit: int = 200, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        where = ""
        args: List[Any] = []
        if job_id is not None:
            where = "WHERE conv.job_id = ?"
            args.append(job_id)
        query = f"""
        SELECT
            conv.id AS conversation_id,
            conv.job_id,
            conv.candidate_id,
            conv.channel,
            conv.status AS conversation_status,
            conv.external_chat_id,
            conv.last_message_at,
            j.title AS job_title,
            c.full_name AS candidate_name,
            c.linkedin_id AS candidate_linkedin_id,
            c.source AS candidate_source,
            c.location AS candidate_location,
            prs.session_id AS pre_resume_session_id,
            prs.status AS pre_resume_status,
            prs.next_followup_at AS pre_resume_next_followup_at,
            (
                SELECT m.content
                FROM messages m
                WHERE m.conversation_id = conv.id
                ORDER BY m.id DESC
                LIMIT 1
            ) AS last_message
        FROM conversations conv
        LEFT JOIN jobs j ON j.id = conv.job_id
        LEFT JOIN candidates c ON c.id = conv.candidate_id
        LEFT JOIN pre_resume_sessions prs ON prs.conversation_id = conv.id
        {where}
        ORDER BY conv.last_message_at DESC, conv.id DESC
        LIMIT ?
        """
        args.append(safe_limit)
        rows = self._conn.execute(query, tuple(args)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def list_conversations_by_status(
        self,
        status: str,
        limit: int = 200,
        job_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        args: List[Any] = [status]
        where = "WHERE conv.status = ?"
        if job_id is not None:
            where += " AND conv.job_id = ?"
            args.append(job_id)
        query = f"""
        SELECT
            conv.id AS conversation_id,
            conv.job_id,
            conv.candidate_id,
            conv.channel,
            conv.status AS conversation_status,
            conv.external_chat_id,
            conv.last_message_at,
            j.title AS job_title,
            c.full_name AS candidate_name,
            c.linkedin_id AS candidate_linkedin_id,
            c.source AS candidate_source
        FROM conversations conv
        LEFT JOIN jobs j ON j.id = conv.job_id
        LEFT JOIN candidates c ON c.id = conv.candidate_id
        {where}
        ORDER BY conv.last_message_at DESC, conv.id DESC
        LIMIT ?
        """
        args.append(safe_limit)
        rows = self._conn.execute(query, tuple(args)).fetchall()
        return [self._row_to_dict(r) for r in rows]

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

    def upsert_pre_resume_session(
        self,
        session_id: str,
        conversation_id: int,
        job_id: int,
        candidate_id: int,
        state: Dict[str, Any],
        instruction: str = "",
    ) -> None:
        status = str(state.get("status") or "awaiting_reply")
        language = state.get("language")
        last_intent = state.get("last_intent")
        followups_sent = int(state.get("followups_sent") or 0)
        turns = int(state.get("turns") or 0)
        last_error = state.get("last_error")
        resume_links = json.dumps(state.get("resume_links") or [])
        next_followup_at = state.get("next_followup_at")
        state_json = json.dumps(state)
        created_at = state.get("created_at") or utc_now_iso()
        updated_at = state.get("updated_at") or utc_now_iso()

        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pre_resume_sessions
                (
                    session_id, conversation_id, job_id, candidate_id, status, language,
                    last_intent, followups_sent, turns, last_error, resume_links,
                    next_followup_at, state_json, instruction, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    conversation_id,
                    job_id,
                    candidate_id,
                    status,
                    language,
                    last_intent,
                    followups_sent,
                    turns,
                    last_error,
                    resume_links,
                    next_followup_at,
                    state_json,
                    instruction,
                    created_at,
                    updated_at,
                ),
            )

    def get_pre_resume_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM pre_resume_sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_pre_resume_session_by_conversation(self, conversation_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM pre_resume_sessions WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_pre_resume_sessions(
        self,
        limit: int = 100,
        status: Optional[str] = None,
        job_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 1000))
        query = """
        SELECT
            prs.*,
            c.full_name AS candidate_name,
            j.title AS job_title
        FROM pre_resume_sessions prs
        LEFT JOIN candidates c ON c.id = prs.candidate_id
        LEFT JOIN jobs j ON j.id = prs.job_id
        """
        args: List[Any] = []
        where: List[str] = []
        if status:
            where.append("prs.status = ?")
            args.append(status)
        if job_id is not None:
            where.append("prs.job_id = ?")
            args.append(job_id)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY prs.updated_at DESC LIMIT ?"
        args.append(safe_limit)

        rows = self._conn.execute(query, tuple(args)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def insert_pre_resume_event(
        self,
        session_id: str,
        conversation_id: int,
        event_type: str,
        intent: Optional[str],
        inbound_text: Optional[str],
        outbound_text: Optional[str],
        state_status: Optional[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO pre_resume_events
                (session_id, conversation_id, event_type, intent, inbound_text, outbound_text, state_status, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    conversation_id,
                    event_type,
                    intent,
                    inbound_text,
                    outbound_text,
                    state_status,
                    json.dumps(details or {}),
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def list_pre_resume_events(self, limit: int = 200, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        if session_id:
            rows = self._conn.execute(
                """
                SELECT * FROM pre_resume_events
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, safe_limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM pre_resume_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def record_webhook_event(self, event_key: str, source: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO webhook_events (event_key, source, payload, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (event_key, source, json.dumps(payload or {}), utc_now_iso()),
            )
            return cur.rowcount > 0

    def upsert_job_step_progress(
        self,
        job_id: int,
        step: str,
        status: str,
        output: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO job_step_progress (job_id, step, status, output_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(job_id, step)
                DO UPDATE SET
                    status = excluded.status,
                    output_json = excluded.output_json,
                    updated_at = excluded.updated_at
                """,
                (
                    job_id,
                    step,
                    status,
                    json.dumps(output or {}),
                    utc_now_iso(),
                ),
            )

    def list_job_step_progress(self, job_id: int) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT job_id, step, status, output_json, updated_at
            FROM job_step_progress
            WHERE job_id = ?
            ORDER BY updated_at DESC
            """,
            (job_id,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def update_candidate_match_status(
        self,
        job_id: int,
        candidate_id: int,
        status: str,
        extra_notes: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT verification_notes
                FROM candidate_job_matches
                WHERE job_id = ? AND candidate_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (job_id, candidate_id),
            ).fetchone()
            merged_notes: Dict[str, Any] = {}
            if row and row["verification_notes"]:
                try:
                    merged_notes = json.loads(row["verification_notes"])
                except json.JSONDecodeError:
                    merged_notes = {}
            if extra_notes:
                merged_notes.update(extra_notes)

            conn.execute(
                """
                UPDATE candidate_job_matches
                SET status = ?, verification_notes = ?
                WHERE job_id = ? AND candidate_id = ?
                """,
                (status, json.dumps(merged_notes), job_id, candidate_id),
            )

    def _migrate_schema(self) -> None:
        columns = self._table_columns("conversations")
        if "external_chat_id" not in columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE conversations ADD COLUMN external_chat_id TEXT")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_external_chat_id ON conversations(external_chat_id)")

    def _table_columns(self, table_name: str) -> List[str]:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [str(row["name"]) for row in rows]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        for field in (
            "preferred_languages",
            "languages",
            "skills",
            "verification_notes",
            "meta",
            "details",
            "resume_links",
            "state_json",
            "output_json",
        ):
            if field in item and item[field]:
                try:
                    item[field] = json.loads(item[field])
                except json.JSONDecodeError:
                    pass
        return item
