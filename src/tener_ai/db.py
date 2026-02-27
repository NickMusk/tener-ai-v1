from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


UTC = timezone.utc
AGENT_DEFAULT_NAMES = {
    "sourcing_vetting": "Reed AI (Talent Scout)",
    "communication": "Casey AI (Hiring Coordinator)",
    "interview_evaluation": "Jordan AI (Lead Interviewer)",
    "culture_analyst": "Harper AI (Culture Analyst)",
    "job_architect": "Spencer AI (Job Architect)",
}


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
            company TEXT,
            company_website TEXT,
            jd_text TEXT NOT NULL,
            location TEXT,
            preferred_languages TEXT,
            seniority TEXT,
            linkedin_routing_mode TEXT NOT NULL DEFAULT 'auto',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_culture_profiles (
            job_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            company_name TEXT,
            company_website TEXT,
            profile_json TEXT,
            sources_json TEXT,
            warnings_json TEXT,
            search_queries_json TEXT,
            error TEXT,
            generated_at TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id)
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
            linkedin_account_id INTEGER,
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

        CREATE TABLE IF NOT EXISTS candidate_agent_assessments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            agent_key TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            stage_key TEXT NOT NULL,
            score REAL,
            status TEXT NOT NULL,
            reason TEXT,
            instruction TEXT,
            details TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(job_id, candidate_id, agent_key, stage_key),
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(candidate_id) REFERENCES candidates(id)
        );
        CREATE INDEX IF NOT EXISTS idx_candidate_agent_assessments_job_candidate
            ON candidate_agent_assessments(job_id, candidate_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS candidate_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            conversation_id INTEGER,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            signal_category TEXT,
            title TEXT NOT NULL,
            detail TEXT,
            impact_score REAL,
            confidence REAL,
            signal_meta TEXT,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(candidate_id) REFERENCES candidates(id)
        );
        CREATE INDEX IF NOT EXISTS idx_candidate_signals_job_observed
            ON candidate_signals(job_id, observed_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_candidate_signals_candidate_observed
            ON candidate_signals(candidate_id, observed_at DESC, id DESC);

        CREATE TABLE IF NOT EXISTS linkedin_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            provider_account_id TEXT NOT NULL UNIQUE,
            provider_user_id TEXT,
            label TEXT,
            status TEXT NOT NULL,
            metadata TEXT,
            connected_at TEXT,
            last_synced_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS linkedin_onboarding_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            provider TEXT NOT NULL,
            status TEXT NOT NULL,
            state_nonce TEXT NOT NULL,
            state_expires_at TEXT NOT NULL,
            redirect_uri TEXT,
            connect_url TEXT,
            provider_account_id TEXT,
            error TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_linkedin_accounts_status
            ON linkedin_accounts(status);
        CREATE INDEX IF NOT EXISTS idx_linkedin_onboarding_sessions_status
            ON linkedin_onboarding_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_linkedin_onboarding_sessions_provider_account
            ON linkedin_onboarding_sessions(provider_account_id);

        CREATE TABLE IF NOT EXISTS outbound_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            candidate_id INTEGER NOT NULL,
            conversation_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            not_before TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            account_id INTEGER,
            payload_json TEXT NOT NULL,
            result_json TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_outbound_actions_status_due
            ON outbound_actions(status, not_before, priority DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_outbound_actions_job
            ON outbound_actions(job_id, status, id DESC);

        CREATE TABLE IF NOT EXISTS linkedin_account_daily_counters (
            account_id INTEGER NOT NULL,
            day_utc TEXT NOT NULL,
            connect_sent INTEGER NOT NULL DEFAULT 0,
            new_threads_sent INTEGER NOT NULL DEFAULT 0,
            replies_sent INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, day_utc)
        );

        CREATE TABLE IF NOT EXISTS linkedin_account_weekly_counters (
            account_id INTEGER NOT NULL,
            week_start_utc TEXT NOT NULL,
            connect_sent INTEGER NOT NULL DEFAULT 0,
            new_threads_sent INTEGER NOT NULL DEFAULT 0,
            replies_sent INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(account_id, week_start_utc)
        );

        CREATE TABLE IF NOT EXISTS job_linkedin_account_assignments (
            job_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(job_id, account_id),
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(account_id) REFERENCES linkedin_accounts(id)
        );
        CREATE INDEX IF NOT EXISTS idx_job_linkedin_account_assignments_job
            ON job_linkedin_account_assignments(job_id, account_id);
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
        company: Optional[str] = None,
        company_website: Optional[str] = None,
        linkedin_routing_mode: str = "auto",
    ) -> int:
        routing_mode = self._normalize_linkedin_routing_mode(linkedin_routing_mode)
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO jobs (title, company, company_website, jd_text, location, preferred_languages, seniority, linkedin_routing_mode, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    company,
                    company_website,
                    jd_text,
                    location,
                    json.dumps(preferred_languages),
                    seniority,
                    routing_mode,
                    utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return None
        item = self._row_to_dict(row)
        profile = self.get_job_culture_profile(job_id=int(job_id))
        self._attach_job_culture_profile(item=item, profile=profile)
        return item

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
        items = [self._row_to_dict(r) for r in rows]
        job_ids = [int(item.get("id") or 0) for item in items if int(item.get("id") or 0) > 0]
        profiles = self.list_job_culture_profiles(job_ids=job_ids)
        for item in items:
            job_id = int(item.get("id") or 0)
            self._attach_job_culture_profile(item=item, profile=profiles.get(job_id))
        return items

    def upsert_job_culture_profile(
        self,
        *,
        job_id: int,
        status: str,
        company_name: Optional[str],
        company_website: Optional[str],
        profile: Optional[Dict[str, Any]],
        sources: Optional[List[Dict[str, Any]]],
        warnings: Optional[List[str]],
        search_queries: Optional[List[str]],
        error: Optional[str],
        generated_at: Optional[str] = None,
    ) -> None:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO job_culture_profiles (
                    job_id, status, company_name, company_website, profile_json, sources_json, warnings_json, search_queries_json, error, generated_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    status = excluded.status,
                    company_name = excluded.company_name,
                    company_website = excluded.company_website,
                    profile_json = excluded.profile_json,
                    sources_json = excluded.sources_json,
                    warnings_json = excluded.warnings_json,
                    search_queries_json = excluded.search_queries_json,
                    error = excluded.error,
                    generated_at = excluded.generated_at,
                    updated_at = excluded.updated_at
                """,
                (
                    int(job_id),
                    str(status or "unknown").strip().lower() or "unknown",
                    company_name,
                    company_website,
                    json.dumps(profile or {}),
                    json.dumps(sources or []),
                    json.dumps(warnings or []),
                    json.dumps(search_queries or []),
                    (str(error or "").strip() or None),
                    generated_at,
                    now,
                ),
            )

    def get_job_culture_profile(self, job_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM job_culture_profiles
            WHERE job_id = ?
            LIMIT 1
            """,
            (int(job_id),),
        ).fetchone()
        if not row:
            return None
        return self._row_to_dict(row)

    def list_job_culture_profiles(self, job_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        normalized: List[int] = []
        seen: set[int] = set()
        for raw in job_ids or []:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        if not normalized:
            return {}
        placeholders = ",".join(["?"] * len(normalized))
        rows = self._conn.execute(
            f"""
            SELECT *
            FROM job_culture_profiles
            WHERE job_id IN ({placeholders})
            """,
            tuple(normalized),
        ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            parsed = self._row_to_dict(row)
            key = int(parsed.get("job_id") or 0)
            if key > 0:
                out[key] = parsed
        return out

    def update_job_linkedin_routing_mode(self, job_id: int, routing_mode: str) -> bool:
        normalized = self._normalize_linkedin_routing_mode(routing_mode)
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE jobs
                SET linkedin_routing_mode = ?
                WHERE id = ?
                """,
                (normalized, int(job_id)),
            )
            return cur.rowcount > 0

    def replace_job_linkedin_account_assignments(self, job_id: int, account_ids: List[int]) -> List[int]:
        unique_ids: List[int] = []
        seen: set[int] = set()
        for raw in account_ids or []:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            unique_ids.append(value)

        existing_ids: List[int] = []
        if unique_ids:
            placeholders = ",".join(["?"] * len(unique_ids))
            rows = self._conn.execute(
                f"SELECT id FROM linkedin_accounts WHERE id IN ({placeholders})",
                tuple(unique_ids),
            ).fetchall()
            existing_ids = sorted(int(r["id"]) for r in rows)

        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM job_linkedin_account_assignments WHERE job_id = ?",
                (int(job_id),),
            )
            for account_id in existing_ids:
                conn.execute(
                    """
                    INSERT INTO job_linkedin_account_assignments (job_id, account_id, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (int(job_id), int(account_id), now),
                )
        return existing_ids

    def list_job_linkedin_account_ids(self, job_id: int) -> List[int]:
        rows = self._conn.execute(
            """
            SELECT account_id
            FROM job_linkedin_account_assignments
            WHERE job_id = ?
            ORDER BY account_id ASC
            """,
            (int(job_id),),
        ).fetchall()
        return [int(r["account_id"]) for r in rows]

    def list_job_linkedin_accounts(self, job_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
        args: List[Any] = [int(job_id)]
        where = ""
        if status:
            where = "AND a.status = ?"
            args.append(str(status))
        rows = self._conn.execute(
            f"""
            SELECT a.*
            FROM job_linkedin_account_assignments ja
            JOIN linkedin_accounts a ON a.id = ja.account_id
            WHERE ja.job_id = ?
            {where}
            ORDER BY a.updated_at DESC, a.id DESC
            """,
            tuple(args),
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
            c.years_experience,
            conv.id AS conversation_id,
            conv.status AS conversation_status,
            conv.external_chat_id,
            conv.last_message_at,
            prs.session_id AS pre_resume_session_id,
            prs.status AS pre_resume_status,
            prs.next_followup_at AS pre_resume_next_followup_at,
            (
                SELECT msg.direction
                FROM messages msg
                WHERE msg.conversation_id = conv.id
                ORDER BY msg.id DESC
                LIMIT 1
            ) AS last_message_direction,
            (
                SELECT msg.created_at
                FROM messages msg
                WHERE msg.conversation_id = conv.id
                ORDER BY msg.id DESC
                LIMIT 1
            ) AS last_message_created_at
        FROM candidate_job_matches m
        JOIN candidates c ON c.id = m.candidate_id
        LEFT JOIN conversations conv ON conv.id = (
            SELECT c2.id
            FROM conversations c2
            WHERE c2.job_id = m.job_id
              AND c2.candidate_id = m.candidate_id
            ORDER BY c2.id DESC
            LIMIT 1
        )
        LEFT JOIN pre_resume_sessions prs ON prs.conversation_id = conv.id
        WHERE m.job_id = ?
        ORDER BY m.score DESC
        """
        rows = self._conn.execute(query, (job_id,)).fetchall()
        items = [self._row_to_dict(r) for r in rows]
        assessments_by_candidate = self._list_candidate_assessments_grouped(job_id=job_id)
        for item in items:
            key, label = self._derive_candidate_current_status(item)
            item["current_status_key"] = key
            item["current_status_label"] = label
            candidate_id = int(item.get("candidate_id") or 0)
            candidate_assessments = list(assessments_by_candidate.get(candidate_id, []))
            item["agent_assessments"] = candidate_assessments
            item["agent_scorecard"] = self._build_agent_scorecard(
                assessments=candidate_assessments,
                candidate_row=item,
            )
        return items

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

    def list_candidate_matches(self, candidate_id: int) -> List[Dict[str, Any]]:
        query = """
        SELECT
            m.id AS match_id,
            m.job_id,
            m.candidate_id,
            m.score,
            m.status,
            m.verification_notes,
            m.created_at AS match_created_at,
            j.title AS job_title,
            j.company AS job_company,
            j.company_website AS job_company_website,
            j.jd_text AS job_jd_text,
            j.location AS job_location,
            j.preferred_languages AS job_preferred_languages,
            j.seniority AS job_seniority,
            cp.profile_json AS job_company_culture_profile,
            conv.id AS conversation_id,
            conv.status AS conversation_status,
            conv.external_chat_id,
            conv.linkedin_account_id,
            conv.last_message_at,
            prs.session_id AS pre_resume_session_id,
            prs.status AS pre_resume_status,
            prs.next_followup_at AS pre_resume_next_followup_at,
            prs.resume_links AS pre_resume_resume_links,
            prs.state_json AS pre_resume_state_json,
            (
                SELECT msg.direction
                FROM messages msg
                WHERE msg.conversation_id = conv.id
                ORDER BY msg.id DESC
                LIMIT 1
            ) AS last_message_direction,
            (
                SELECT msg.content
                FROM messages msg
                WHERE msg.conversation_id = conv.id
                ORDER BY msg.id DESC
                LIMIT 1
            ) AS last_message_content,
            (
                SELECT msg.created_at
                FROM messages msg
                WHERE msg.conversation_id = conv.id
                ORDER BY msg.id DESC
                LIMIT 1
            ) AS last_message_created_at
        FROM candidate_job_matches m
        JOIN jobs j ON j.id = m.job_id
        LEFT JOIN job_culture_profiles cp ON cp.job_id = m.job_id
        LEFT JOIN conversations conv ON conv.id = (
            SELECT c2.id
            FROM conversations c2
            WHERE c2.job_id = m.job_id
              AND c2.candidate_id = m.candidate_id
            ORDER BY c2.id DESC
            LIMIT 1
        )
        LEFT JOIN pre_resume_sessions prs ON prs.conversation_id = conv.id
        WHERE m.candidate_id = ?
        ORDER BY m.created_at DESC, m.id DESC
        """
        rows = self._conn.execute(query, (int(candidate_id),)).fetchall()
        items = [self._row_to_dict(r) for r in rows]
        assessments = self.list_candidate_assessments(candidate_id=candidate_id)
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for assessment in assessments:
            job_id = int(assessment.get("job_id") or 0)
            grouped.setdefault(job_id, []).append(assessment)
        for item in items:
            job_id = int(item.get("job_id") or 0)
            candidate_assessments = list(grouped.get(job_id, []))
            item["agent_assessments"] = candidate_assessments
            item["agent_scorecard"] = self._build_agent_scorecard(
                assessments=candidate_assessments,
                candidate_row=item,
            )
            key, label = self._derive_candidate_current_status(item)
            item["current_status_key"] = key
            item["current_status_label"] = label
        return items

    def list_candidate_assessments(self, candidate_id: int, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
        base_query = """
            SELECT
                id,
                job_id,
                candidate_id,
                agent_key,
                agent_name,
                stage_key,
                score,
                status,
                reason,
                instruction,
                details,
                updated_at
            FROM candidate_agent_assessments
        """
        args: List[Any] = [int(candidate_id)]
        where = "WHERE candidate_id = ?"
        if job_id is not None:
            where += " AND job_id = ?"
            args.append(int(job_id))
        query = f"{base_query} {where} ORDER BY updated_at DESC, id DESC"
        rows = self._conn.execute(query, tuple(args)).fetchall()
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

    def set_conversation_external_chat_id(self, conversation_id: int, external_chat_id: str) -> Dict[str, Any]:
        external_chat_id = str(external_chat_id or "").strip()
        if not external_chat_id:
            return {"status": "skipped_empty"}

        with self.transaction() as conn:
            target = conn.execute(
                """
                SELECT id, candidate_id
                FROM conversations
                WHERE id = ?
                """,
                (conversation_id,),
            ).fetchone()
            if not target:
                return {
                    "status": "conversation_not_found",
                    "conversation_id": conversation_id,
                    "external_chat_id": external_chat_id,
                }

            existing = conn.execute(
                """
                SELECT id, candidate_id
                FROM conversations
                WHERE external_chat_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (external_chat_id,),
            ).fetchone()

            if existing and int(existing["id"]) != int(target["id"]):
                # The same candidate can be contacted for multiple jobs but share one real chat thread.
                # Rebind chat id to the latest conversation, keep workflow alive.
                if int(existing["candidate_id"]) == int(target["candidate_id"]):
                    conn.execute(
                        """
                        UPDATE conversations
                        SET external_chat_id = NULL
                        WHERE id = ?
                        """,
                        (int(existing["id"]),),
                    )
                    conn.execute(
                        """
                        UPDATE conversations
                        SET external_chat_id = ?
                        WHERE id = ?
                        """,
                        (external_chat_id, conversation_id),
                    )
                    return {
                        "status": "rebound_same_candidate",
                        "external_chat_id": external_chat_id,
                        "from_conversation_id": int(existing["id"]),
                        "to_conversation_id": conversation_id,
                    }

                return {
                    "status": "conflict_other_candidate",
                    "external_chat_id": external_chat_id,
                    "target_conversation_id": conversation_id,
                    "target_candidate_id": int(target["candidate_id"]),
                    "existing_conversation_id": int(existing["id"]),
                    "existing_candidate_id": int(existing["candidate_id"]),
                }

            conn.execute(
                """
                UPDATE conversations
                SET external_chat_id = ?
                WHERE id = ?
                """,
                (external_chat_id, conversation_id),
            )
            return {
                "status": "set",
                "conversation_id": conversation_id,
                "external_chat_id": external_chat_id,
            }

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

    def set_conversation_linkedin_account(self, conversation_id: int, account_id: Optional[int]) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE conversations
                SET linkedin_account_id = ?
                WHERE id = ?
                """,
                (account_id, conversation_id),
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

    def list_conversations_for_candidate(self, candidate_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        rows = self._conn.execute(
            """
            SELECT
                conv.id AS conversation_id,
                conv.job_id,
                conv.candidate_id,
                conv.channel,
                conv.status AS conversation_status,
                conv.external_chat_id,
                conv.linkedin_account_id,
                conv.last_message_at,
                conv.created_at,
                j.title AS job_title,
                prs.session_id AS pre_resume_session_id,
                prs.status AS pre_resume_status,
                prs.next_followup_at AS pre_resume_next_followup_at,
                (
                    SELECT m.content
                    FROM messages m
                    WHERE m.conversation_id = conv.id
                    ORDER BY m.id DESC
                    LIMIT 1
                ) AS last_message,
                (
                    SELECT m.direction
                    FROM messages m
                    WHERE m.conversation_id = conv.id
                    ORDER BY m.id DESC
                    LIMIT 1
                ) AS last_message_direction
            FROM conversations conv
            LEFT JOIN jobs j ON j.id = conv.job_id
            LEFT JOIN pre_resume_sessions prs ON prs.conversation_id = conv.id
            WHERE conv.candidate_id = ?
            ORDER BY conv.last_message_at DESC, conv.id DESC
            LIMIT ?
            """,
            (int(candidate_id), safe_limit),
        ).fetchall()
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

    def list_logs_for_candidate(self, candidate_id: int, limit: int = 300) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 300), 2000))
        rows = self._conn.execute(
            """
            SELECT *
            FROM operation_logs
            WHERE
                (entity_type = 'candidate' AND entity_id = ?)
                OR (
                    entity_type = 'conversation'
                    AND entity_id IN (
                        SELECT CAST(id AS TEXT)
                        FROM conversations
                        WHERE candidate_id = ?
                    )
                )
            ORDER BY id DESC
            LIMIT ?
            """,
            (str(int(candidate_id)), int(candidate_id), safe_limit),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def create_outbound_action(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        action_type: str,
        payload: Dict[str, Any],
        priority: int = 0,
        not_before: Optional[str] = None,
    ) -> int:
        now = utc_now_iso()
        due = str(not_before or now)
        with self.transaction() as conn:
            cur = conn.execute(
                """
                INSERT INTO outbound_actions (
                    job_id, candidate_id, conversation_id, action_type, status, priority,
                    not_before, attempts, account_id, payload_json, result_json, last_error,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'pending', ?, ?, 0, NULL, ?, NULL, NULL, ?, ?)
                """,
                (
                    job_id,
                    candidate_id,
                    conversation_id,
                    action_type,
                    int(priority),
                    due,
                    json.dumps(payload or {}),
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def list_pending_outbound_actions(
        self,
        *,
        limit: int = 100,
        job_id: Optional[int] = None,
        action_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 2000))
        args: List[Any] = [utc_now_iso()]
        where_parts = ["status = 'pending'", "not_before <= ?"]
        if job_id is not None:
            where_parts.append("job_id = ?")
            args.append(int(job_id))
        if action_ids:
            valid_ids = [int(x) for x in action_ids if int(x) > 0]
            if valid_ids:
                placeholders = ",".join(["?"] * len(valid_ids))
                where_parts.append(f"id IN ({placeholders})")
                args.extend(valid_ids)
        query = f"""
        SELECT *
        FROM outbound_actions
        WHERE {' AND '.join(where_parts)}
        ORDER BY priority DESC, id ASC
        LIMIT ?
        """
        args.append(safe_limit)
        rows = self._conn.execute(query, tuple(args)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def claim_outbound_action(self, action_id: int) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE outbound_actions
                SET
                    status = 'running',
                    attempts = attempts + 1,
                    updated_at = ?
                WHERE id = ? AND status = 'pending'
                """,
                (utc_now_iso(), int(action_id)),
            )
            return cur.rowcount > 0

    def complete_outbound_action(
        self,
        *,
        action_id: int,
        status: str,
        account_id: Optional[int] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> bool:
        normalized = str(status or "").strip().lower() or "completed"
        if normalized not in {"completed", "failed", "pending"}:
            normalized = "completed"
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE outbound_actions
                SET
                    status = ?,
                    account_id = ?,
                    result_json = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    normalized,
                    account_id,
                    json.dumps(result or {}),
                    (str(error or "")[:400] or None),
                    utc_now_iso(),
                    int(action_id),
                ),
            )
            return cur.rowcount > 0

    def release_outbound_action(
        self,
        *,
        action_id: int,
        not_before: str,
        error: Optional[str] = None,
    ) -> bool:
        with self.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE outbound_actions
                SET
                    status = 'pending',
                    not_before = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    str(not_before),
                    (str(error or "")[:400] or None),
                    utc_now_iso(),
                    int(action_id),
                ),
            )
            return cur.rowcount > 0

    def get_outbound_action(self, action_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM outbound_actions WHERE id = ?",
            (int(action_id),),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_linkedin_account_daily_counter(self, account_id: int, day_utc: str) -> Dict[str, Any]:
        row = self._conn.execute(
            """
            SELECT *
            FROM linkedin_account_daily_counters
            WHERE account_id = ? AND day_utc = ?
            """,
            (int(account_id), str(day_utc)),
        ).fetchone()
        if not row:
            return {
                "account_id": int(account_id),
                "day_utc": str(day_utc),
                "connect_sent": 0,
                "new_threads_sent": 0,
                "replies_sent": 0,
            }
        return self._row_to_dict(row)

    def get_linkedin_account_weekly_counter(self, account_id: int, week_start_utc: str) -> Dict[str, Any]:
        row = self._conn.execute(
            """
            SELECT *
            FROM linkedin_account_weekly_counters
            WHERE account_id = ? AND week_start_utc = ?
            """,
            (int(account_id), str(week_start_utc)),
        ).fetchone()
        if not row:
            return {
                "account_id": int(account_id),
                "week_start_utc": str(week_start_utc),
                "connect_sent": 0,
                "new_threads_sent": 0,
                "replies_sent": 0,
            }
        return self._row_to_dict(row)

    def increment_linkedin_account_counters(
        self,
        *,
        account_id: int,
        day_utc: str,
        week_start_utc: str,
        connect_delta: int = 0,
        new_threads_delta: int = 0,
        replies_delta: int = 0,
    ) -> None:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO linkedin_account_daily_counters (
                    account_id, day_utc, connect_sent, new_threads_sent, replies_sent, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, day_utc)
                DO UPDATE SET
                    connect_sent = connect_sent + excluded.connect_sent,
                    new_threads_sent = new_threads_sent + excluded.new_threads_sent,
                    replies_sent = replies_sent + excluded.replies_sent,
                    updated_at = excluded.updated_at
                """,
                (
                    int(account_id),
                    str(day_utc),
                    int(connect_delta),
                    int(new_threads_delta),
                    int(replies_delta),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO linkedin_account_weekly_counters (
                    account_id, week_start_utc, connect_sent, new_threads_sent, replies_sent, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, week_start_utc)
                DO UPDATE SET
                    connect_sent = connect_sent + excluded.connect_sent,
                    new_threads_sent = new_threads_sent + excluded.new_threads_sent,
                    replies_sent = replies_sent + excluded.replies_sent,
                    updated_at = excluded.updated_at
                """,
                (
                    int(account_id),
                    str(week_start_utc),
                    int(connect_delta),
                    int(new_threads_delta),
                    int(replies_delta),
                    now,
                ),
            )

    def create_linkedin_onboarding_session(
        self,
        session_id: str,
        provider: str,
        state_nonce: str,
        state_expires_at: str,
        redirect_uri: str,
        connect_url: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = utc_now_iso()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO linkedin_onboarding_sessions (
                    session_id, provider, status, state_nonce, state_expires_at, redirect_uri,
                    connect_url, provider_account_id, error, metadata, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
                """,
                (
                    session_id,
                    provider,
                    "pending",
                    state_nonce,
                    state_expires_at,
                    redirect_uri,
                    connect_url,
                    json.dumps(metadata or {}),
                    now,
                    now,
                ),
            )

    def get_linkedin_onboarding_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM linkedin_onboarding_sessions WHERE session_id = ? ORDER BY id DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def update_linkedin_onboarding_session_status(
        self,
        session_id: str,
        status: str,
        provider_account_id: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        with self.transaction() as conn:
            existing_row = conn.execute(
                """
                SELECT metadata
                FROM linkedin_onboarding_sessions
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
            merged_metadata: Dict[str, Any] = {}
            if existing_row and existing_row["metadata"]:
                try:
                    merged_metadata = json.loads(existing_row["metadata"])
                except json.JSONDecodeError:
                    merged_metadata = {}
            if metadata:
                merged_metadata.update(metadata)
            cur = conn.execute(
                """
                UPDATE linkedin_onboarding_sessions
                SET
                    status = ?,
                    provider_account_id = ?,
                    error = ?,
                    metadata = ?,
                    updated_at = ?
                WHERE session_id = ?
                """,
                (
                    status,
                    provider_account_id,
                    error,
                    json.dumps(merged_metadata),
                    utc_now_iso(),
                    session_id,
                ),
            )
            return cur.rowcount > 0

    def upsert_linkedin_account(
        self,
        provider: str,
        provider_account_id: str,
        status: str,
        *,
        label: Optional[str] = None,
        provider_user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        connected_at: Optional[str] = None,
        last_synced_at: Optional[str] = None,
    ) -> int:
        now = utc_now_iso()
        with self.transaction() as conn:
            existing = conn.execute(
                """
                SELECT id, metadata, connected_at
                FROM linkedin_accounts
                WHERE provider_account_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (provider_account_id,),
            ).fetchone()
            merged_metadata: Dict[str, Any] = {}
            if existing and existing["metadata"]:
                try:
                    merged_metadata = json.loads(existing["metadata"])
                except json.JSONDecodeError:
                    merged_metadata = {}
            if metadata:
                merged_metadata.update(metadata)
            normalized_connected_at = connected_at
            if not normalized_connected_at:
                if status == "connected":
                    normalized_connected_at = (existing["connected_at"] if existing else None) or now
                elif existing:
                    normalized_connected_at = existing["connected_at"]
            normalized_last_synced_at = last_synced_at or now

            if existing:
                account_id = int(existing["id"])
                conn.execute(
                    """
                    UPDATE linkedin_accounts
                    SET
                        provider = ?,
                        provider_user_id = COALESCE(?, provider_user_id),
                        label = COALESCE(?, label),
                        status = ?,
                        metadata = ?,
                        connected_at = ?,
                        last_synced_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        provider,
                        provider_user_id,
                        label,
                        status,
                        json.dumps(merged_metadata),
                        normalized_connected_at,
                        normalized_last_synced_at,
                        now,
                        account_id,
                    ),
                )
                return account_id

            cur = conn.execute(
                """
                INSERT INTO linkedin_accounts (
                    provider,
                    provider_account_id,
                    provider_user_id,
                    label,
                    status,
                    metadata,
                    connected_at,
                    last_synced_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider,
                    provider_account_id,
                    provider_user_id,
                    label,
                    status,
                    json.dumps(merged_metadata),
                    normalized_connected_at,
                    normalized_last_synced_at,
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def get_linkedin_account(self, account_id: int) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM linkedin_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_linkedin_account_by_provider_account_id(self, provider_account_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            """
            SELECT *
            FROM linkedin_accounts
            WHERE provider_account_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (provider_account_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def list_linkedin_accounts(self, limit: int = 200, status: Optional[str] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        if status:
            rows = self._conn.execute(
                """
                SELECT *
                FROM linkedin_accounts
                WHERE status = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (status, safe_limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT *
                FROM linkedin_accounts
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def update_linkedin_account_status(
        self,
        account_id: int,
        status: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT metadata FROM linkedin_accounts WHERE id = ?",
                (account_id,),
            ).fetchone()
            if not existing:
                return False
            merged_metadata: Dict[str, Any] = {}
            if existing["metadata"]:
                try:
                    merged_metadata = json.loads(existing["metadata"])
                except json.JSONDecodeError:
                    merged_metadata = {}
            if metadata:
                merged_metadata.update(metadata)
            cur = conn.execute(
                """
                UPDATE linkedin_accounts
                SET
                    status = ?,
                    metadata = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    json.dumps(merged_metadata),
                    utc_now_iso(),
                    account_id,
                ),
            )
            return cur.rowcount > 0

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

    def list_pre_resume_sessions_for_candidate(self, candidate_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM pre_resume_sessions
            WHERE candidate_id = ?
            ORDER BY updated_at DESC, session_id DESC
            LIMIT ?
            """,
            (int(candidate_id), max(1, min(int(limit or 200), 2000))),
        ).fetchall()
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

    def list_pre_resume_events_for_candidate(
        self,
        candidate_id: int,
        *,
        job_id: Optional[int] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 300), 2000))
        if job_id is None:
            rows = self._conn.execute(
                """
                SELECT
                    e.*,
                    s.job_id,
                    s.candidate_id
                FROM pre_resume_events e
                JOIN pre_resume_sessions s ON s.session_id = e.session_id
                WHERE s.candidate_id = ?
                ORDER BY e.id DESC
                LIMIT ?
                """,
                (int(candidate_id), safe_limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT
                    e.*,
                    s.job_id,
                    s.candidate_id
                FROM pre_resume_events e
                JOIN pre_resume_sessions s ON s.session_id = e.session_id
                WHERE s.candidate_id = ? AND s.job_id = ?
                ORDER BY e.id DESC
                LIMIT ?
                """,
                (int(candidate_id), int(job_id), safe_limit),
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

    def upsert_candidate_agent_assessment(
        self,
        job_id: int,
        candidate_id: int,
        agent_key: str,
        agent_name: str,
        stage_key: str,
        score: Optional[float],
        status: str,
        reason: Optional[str] = None,
        instruction: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_score = None if score is None else float(score)
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO candidate_agent_assessments (
                    job_id, candidate_id, agent_key, agent_name, stage_key, score, status,
                    reason, instruction, details, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, candidate_id, agent_key, stage_key)
                DO UPDATE SET
                    agent_name = excluded.agent_name,
                    score = excluded.score,
                    status = excluded.status,
                    reason = excluded.reason,
                    instruction = excluded.instruction,
                    details = excluded.details,
                    updated_at = excluded.updated_at
                """,
                (
                    job_id,
                    candidate_id,
                    agent_key,
                    agent_name,
                    stage_key,
                    normalized_score,
                    status,
                    reason,
                    instruction,
                    json.dumps(details or {}),
                    utc_now_iso(),
                ),
            )

    def upsert_candidate_signal(
        self,
        *,
        job_id: int,
        candidate_id: int,
        source_type: str,
        source_id: str,
        signal_type: str,
        signal_category: Optional[str],
        title: str,
        detail: Optional[str] = None,
        impact_score: Optional[float] = None,
        confidence: Optional[float] = None,
        conversation_id: Optional[int] = None,
        observed_at: Optional[str] = None,
        signal_meta: Optional[Dict[str, Any]] = None,
        signal_key: Optional[str] = None,
    ) -> int:
        now = utc_now_iso()
        normalized_observed = str(observed_at or now)
        resolved_key = str(
            signal_key
            or f"{int(job_id)}:{int(candidate_id)}:{str(source_type).strip().lower()}:{str(source_id).strip().lower()}"
        ).strip()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO candidate_signals (
                    signal_key,
                    job_id,
                    candidate_id,
                    conversation_id,
                    source_type,
                    source_id,
                    signal_type,
                    signal_category,
                    title,
                    detail,
                    impact_score,
                    confidence,
                    signal_meta,
                    observed_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_key)
                DO UPDATE SET
                    conversation_id = excluded.conversation_id,
                    signal_type = excluded.signal_type,
                    signal_category = excluded.signal_category,
                    title = excluded.title,
                    detail = excluded.detail,
                    impact_score = excluded.impact_score,
                    confidence = excluded.confidence,
                    signal_meta = excluded.signal_meta,
                    observed_at = excluded.observed_at,
                    updated_at = excluded.updated_at
                """,
                (
                    resolved_key,
                    int(job_id),
                    int(candidate_id),
                    int(conversation_id) if conversation_id is not None else None,
                    str(source_type or "").strip().lower() or "unknown",
                    str(source_id or "").strip() or "unknown",
                    str(signal_type or "").strip().lower() or "unknown",
                    str(signal_category or "").strip().lower() or None,
                    str(title or "").strip() or "Signal",
                    str(detail or "").strip() or None,
                    None if impact_score is None else float(impact_score),
                    None if confidence is None else float(confidence),
                    json.dumps(signal_meta or {}),
                    normalized_observed,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT id FROM candidate_signals WHERE signal_key = ? LIMIT 1",
                (resolved_key,),
            ).fetchone()
            return int(row["id"] if row else 0)

    def list_candidate_signals(
        self,
        *,
        candidate_id: int,
        job_id: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 5000))
        if job_id is None:
            rows = self._conn.execute(
                """
                SELECT *
                FROM candidate_signals
                WHERE candidate_id = ?
                ORDER BY observed_at DESC, id DESC
                LIMIT ?
                """,
                (int(candidate_id), safe_limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT *
                FROM candidate_signals
                WHERE candidate_id = ? AND job_id = ?
                ORDER BY observed_at DESC, id DESC
                LIMIT ?
                """,
                (int(candidate_id), int(job_id), safe_limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def list_job_signals(
        self,
        *,
        job_id: int,
        limit: int = 2000,
        candidate_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 2000), 10000))
        if candidate_id is None:
            rows = self._conn.execute(
                """
                SELECT
                    s.*,
                    c.full_name AS candidate_name
                FROM candidate_signals s
                LEFT JOIN candidates c ON c.id = s.candidate_id
                WHERE s.job_id = ?
                ORDER BY s.observed_at DESC, s.id DESC
                LIMIT ?
                """,
                (int(job_id), safe_limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT
                    s.*,
                    c.full_name AS candidate_name
                FROM candidate_signals s
                LEFT JOIN candidates c ON c.id = s.candidate_id
                WHERE s.job_id = ? AND s.candidate_id = ?
                ORDER BY s.observed_at DESC, s.id DESC
                LIMIT ?
                """,
                (int(job_id), int(candidate_id), safe_limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def _list_candidate_assessments_grouped(self, job_id: int) -> Dict[int, List[Dict[str, Any]]]:
        rows = self._conn.execute(
            """
            SELECT
                id,
                job_id,
                candidate_id,
                agent_key,
                agent_name,
                stage_key,
                score,
                status,
                reason,
                instruction,
                details,
                updated_at
            FROM candidate_agent_assessments
            WHERE job_id = ?
            ORDER BY updated_at DESC, id DESC
            """,
            (job_id,),
        ).fetchall()
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            item = self._row_to_dict(row)
            candidate_id = int(item.get("candidate_id") or 0)
            grouped.setdefault(candidate_id, []).append(item)
        return grouped

    @staticmethod
    def _build_agent_scorecard(
        assessments: List[Dict[str, Any]],
        candidate_row: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        scorecard: Dict[str, Dict[str, Any]] = {
            "sourcing_vetting": {
                "agent_key": "sourcing_vetting",
                "agent_name": AGENT_DEFAULT_NAMES["sourcing_vetting"],
                "latest_stage": None,
                "latest_score": None,
                "latest_status": "not_started",
                "stages": [],
            },
            "communication": {
                "agent_key": "communication",
                "agent_name": AGENT_DEFAULT_NAMES["communication"],
                "latest_stage": None,
                "latest_score": None,
                "latest_status": "not_started",
                "stages": [],
            },
            "interview_evaluation": {
                "agent_key": "interview_evaluation",
                "agent_name": AGENT_DEFAULT_NAMES["interview_evaluation"],
                "latest_stage": None,
                "latest_score": None,
                "latest_status": "not_started",
                "stages": [],
            },
        }

        for item in assessments:
            agent_key = str(item.get("agent_key") or "").strip().lower()
            if not agent_key:
                continue
            bucket = scorecard.setdefault(
                agent_key,
                {
                    "agent_key": agent_key,
                    "agent_name": AGENT_DEFAULT_NAMES.get(agent_key, agent_key.replace("_", " ").title()),
                    "latest_stage": None,
                    "latest_score": None,
                    "latest_status": "not_started",
                    "stages": [],
                },
            )
            agent_name = str(item.get("agent_name") or "").strip()
            if agent_name:
                bucket["agent_name"] = agent_name
            stage = {
                "stage_key": item.get("stage_key"),
                "score": item.get("score"),
                "status": item.get("status"),
                "reason": item.get("reason"),
                "updated_at": item.get("updated_at"),
            }
            bucket["stages"].append(stage)
            if bucket.get("latest_stage") is None:
                bucket["latest_stage"] = item.get("stage_key")
                bucket["latest_score"] = item.get("score")
                bucket["latest_status"] = item.get("status") or "unknown"

        interview = scorecard.get("interview_evaluation")
        if interview:
            notes = candidate_row.get("verification_notes") if isinstance(candidate_row.get("verification_notes"), dict) else {}
            interview_score = None
            for key in ("interview_total_score", "interview_score", "final_interview_score"):
                raw = notes.get(key) if isinstance(notes, dict) else None
                if raw is None:
                    continue
                try:
                    interview_score = float(raw)
                except (TypeError, ValueError):
                    interview_score = None
                if interview_score is not None:
                    break
            if interview_score is not None:
                notes_status = str((notes or {}).get("interview_status") or "").strip().lower()
                normalized_notes_status = "scored" if notes_status == "scored" else ""
                if interview.get("latest_stage") is None:
                    interview["latest_stage"] = "interview_results"
                    interview["latest_score"] = interview_score
                    interview["latest_status"] = normalized_notes_status or "scored"
                    interview["stages"] = [
                        {
                            "stage_key": "interview_results",
                            "score": interview_score,
                            "status": normalized_notes_status or "scored",
                            "reason": "Loaded from candidate verification notes.",
                            "updated_at": candidate_row.get("last_message_created_at") or candidate_row.get("created_at"),
                        }
                    ]
                elif interview.get("latest_score") is None:
                    interview["latest_score"] = interview_score
                    if normalized_notes_status:
                        interview["latest_status"] = normalized_notes_status
                    stages = interview.get("stages") if isinstance(interview.get("stages"), list) else []
                    if stages:
                        first = stages[0]
                        if isinstance(first, dict) and first.get("score") is None:
                            first["score"] = interview_score
                            if normalized_notes_status:
                                first["status"] = normalized_notes_status
                            reason = str(first.get("reason") or "").strip()
                            suffix = "Score loaded from candidate verification notes."
                            first["reason"] = f"{reason} {suffix}".strip() if reason else suffix

        communication = scorecard.get("communication")
        if isinstance(communication, dict):
            communication_stage = str(communication.get("latest_stage") or "").strip().lower()
            if communication_stage != "dialogue":
                communication["latest_score"] = None

        interview = scorecard.get("interview_evaluation")
        if isinstance(interview, dict):
            interview_stage = str(interview.get("latest_stage") or "").strip().lower()
            interview_status = str(interview.get("latest_status") or "").strip().lower()
            if interview_stage != "interview_results" or interview_status != "scored":
                interview["latest_score"] = None

        return scorecard

    def build_agent_scorecard(
        self,
        *,
        assessments: List[Dict[str, Any]],
        candidate_row: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        return self._build_agent_scorecard(assessments=assessments, candidate_row=candidate_row)

    def derive_candidate_current_status(self, item: Dict[str, Any]) -> tuple[str, str]:
        return self._derive_candidate_current_status(item)

    @staticmethod
    def _attach_job_culture_profile(item: Dict[str, Any], profile: Optional[Dict[str, Any]]) -> None:
        if not isinstance(item, dict):
            return
        if not isinstance(profile, dict):
            item.setdefault("company_culture_profile_status", "not_generated")
            item.setdefault("company_culture_profile", None)
            item.setdefault("company_culture_profile_warnings", [])
            item.setdefault("company_culture_profile_error", None)
            item.setdefault("company_culture_profile_generated_at", None)
            return
        item["company_culture_profile_status"] = str(profile.get("status") or "unknown")
        raw_profile = profile.get("profile_json")
        item["company_culture_profile"] = raw_profile if isinstance(raw_profile, dict) else None
        warnings = profile.get("warnings_json")
        item["company_culture_profile_warnings"] = warnings if isinstance(warnings, list) else []
        item["company_culture_profile_error"] = profile.get("error")
        item["company_culture_profile_generated_at"] = profile.get("generated_at")

    def _migrate_schema(self) -> None:
        job_columns = self._table_columns("jobs")
        if "company" not in job_columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE jobs ADD COLUMN company TEXT")
        if "company_website" not in job_columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE jobs ADD COLUMN company_website TEXT")
        if "linkedin_routing_mode" not in job_columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE jobs ADD COLUMN linkedin_routing_mode TEXT")
                conn.execute("UPDATE jobs SET linkedin_routing_mode = 'auto' WHERE linkedin_routing_mode IS NULL OR TRIM(linkedin_routing_mode) = ''")

        with self.transaction() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_culture_profiles (
                    job_id INTEGER PRIMARY KEY,
                    status TEXT NOT NULL,
                    company_name TEXT,
                    company_website TEXT,
                    profile_json TEXT,
                    sources_json TEXT,
                    warnings_json TEXT,
                    search_queries_json TEXT,
                    error TEXT,
                    generated_at TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS candidate_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_key TEXT NOT NULL UNIQUE,
                    job_id INTEGER NOT NULL,
                    candidate_id INTEGER NOT NULL,
                    conversation_id INTEGER,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    signal_category TEXT,
                    title TEXT NOT NULL,
                    detail TEXT,
                    impact_score REAL,
                    confidence REAL,
                    signal_meta TEXT,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_signals_job_observed ON candidate_signals(job_id, observed_at DESC, id DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_candidate_signals_candidate_observed ON candidate_signals(candidate_id, observed_at DESC, id DESC)"
            )

        columns = self._table_columns("conversations")
        if "external_chat_id" not in columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE conversations ADD COLUMN external_chat_id TEXT")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_external_chat_id ON conversations(external_chat_id)")
        if "linkedin_account_id" not in columns:
            with self.transaction() as conn:
                conn.execute("ALTER TABLE conversations ADD COLUMN linkedin_account_id INTEGER")

    def _table_columns(self, table_name: str) -> List[str]:
        rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [str(row["name"]) for row in rows]

    @staticmethod
    def _normalize_linkedin_routing_mode(mode: str | None) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized in {"auto", "manual"}:
            return normalized
        return "auto"

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        for field in (
            "preferred_languages",
            "languages",
            "skills",
            "verification_notes",
            "meta",
            "metadata",
            "details",
            "resume_links",
            "state_json",
            "output_json",
            "payload_json",
            "result_json",
            "profile_json",
            "sources_json",
            "warnings_json",
            "search_queries_json",
            "company_culture_profile",
            "job_company_culture_profile",
            "signal_meta",
        ):
            if field in item and item[field]:
                try:
                    item[field] = json.loads(item[field])
                except json.JSONDecodeError:
                    pass
        return item

    @staticmethod
    def _derive_candidate_current_status(item: Dict[str, Any]) -> tuple[str, str]:
        match_status = str(item.get("status") or "").strip().lower()
        conversation_status = str(item.get("conversation_status") or "").strip().lower()
        pre_resume_status = str(item.get("pre_resume_status") or "").strip().lower()
        last_message_direction = str(item.get("last_message_direction") or "").strip().lower()
        verification_notes = item.get("verification_notes") if isinstance(item.get("verification_notes"), dict) else {}
        interview_status = str((verification_notes or {}).get("interview_status") or "").strip().lower()

        interview_map = {
            "created": ("interview_invited", "Interview Invited"),
            "invited": ("interview_invited", "Interview Invited"),
            "in_progress": ("interview_in_progress", "Interview In Progress"),
            "completed": ("interview_completed", "Interview Completed"),
            "scored": ("interview_scored", "Interview Scored"),
            "failed": ("interview_failed", "Interview Failed"),
            "expired": ("interview_failed", "Interview Failed"),
            "canceled": ("interview_failed", "Interview Failed"),
        }
        if interview_status in interview_map:
            return interview_map[interview_status]
        if match_status in {"interview_invited", "interview_in_progress", "interview_completed", "interview_scored", "interview_failed"}:
            return match_status, match_status.replace("_", " ").title()

        if pre_resume_status == "resume_received" or match_status == "resume_received":
            return "cv_received", "CV Received"
        if pre_resume_status == "not_interested":
            return "not_interested", "Not Interested"
        if pre_resume_status == "unreachable":
            return "unreachable", "Unreachable"
        if pre_resume_status == "stalled":
            return "stalled", "Stalled"
        if pre_resume_status in {"engaged_no_resume", "will_send_later"}:
            return "in_dialogue", "In Dialogue"
        if match_status == "rejected":
            return "rejected", "Rejected"
        if conversation_status == "waiting_connection" or match_status == "outreach_pending_connection":
            return "outreach_pending_connection", "Outreach Pending Connection"
        if conversation_status == "active" and last_message_direction == "inbound":
            return "in_dialogue", "In Dialogue"
        if (
            match_status in {"outreach_sent"}
            or conversation_status == "active"
            or pre_resume_status in {"awaiting_reply"}
        ):
            return "outreached", "Outreached"
        if match_status in {"verified", "needs_resume"}:
            return "added", "Added"
        if match_status:
            return match_status, match_status.replace("_", " ").title()
        return "unknown", "Unknown"
