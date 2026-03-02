from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional

from .db import AGENT_DEFAULT_NAMES, Database, utc_now_iso
from .db_read_pg import PostgresReadDatabase


class PostgresRuntimeDatabase(PostgresReadDatabase):
    """Primary runtime database backed by Postgres (read + write)."""

    def _json(self, value: Any) -> Any:
        return self._psycopg.types.json.Json(value)

    @contextmanager
    def transaction(self) -> Iterable[Any]:
        with self._connect() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _normalize_linkedin_routing_mode(mode: Optional[str]) -> str:
        normalized = str(mode or "").strip().lower()
        if normalized in {"auto", "manual"}:
            return normalized
        return "auto"

    @staticmethod
    def _attach_job_culture_profile(item: Dict[str, Any], profile: Optional[Dict[str, Any]]) -> None:
        Database._attach_job_culture_profile(item=item, profile=profile)

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs (
                        title, company, company_website, jd_text, location,
                        preferred_languages, seniority, linkedin_routing_mode, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        title,
                        company,
                        company_website,
                        jd_text,
                        location,
                        self._json(preferred_languages or []),
                        seniority,
                        routing_mode,
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def update_job_jd_text(self, job_id: int, jd_text: str) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET jd_text = %s
                    WHERE id = %s
                    """,
                    (jd_text, int(job_id)),
                )
                return int(cur.rowcount or 0) > 0

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_culture_profiles (
                        job_id, status, company_name, company_website,
                        profile_json, sources_json, warnings_json, search_queries_json,
                        error, generated_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(job_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        company_name = EXCLUDED.company_name,
                        company_website = EXCLUDED.company_website,
                        profile_json = EXCLUDED.profile_json,
                        sources_json = EXCLUDED.sources_json,
                        warnings_json = EXCLUDED.warnings_json,
                        search_queries_json = EXCLUDED.search_queries_json,
                        error = EXCLUDED.error,
                        generated_at = EXCLUDED.generated_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        int(job_id),
                        str(status or "unknown").strip().lower() or "unknown",
                        company_name,
                        company_website,
                        self._json(profile or {}),
                        self._json(sources or []),
                        self._json(warnings or []),
                        self._json(search_queries or []),
                        (str(error or "").strip() or None),
                        generated_at,
                        now,
                    ),
                )

    def get_job_culture_profile(self, job_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM job_culture_profiles
                    WHERE job_id = %s
                    LIMIT 1
                    """,
                    (int(job_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

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
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM job_culture_profiles
                    WHERE job_id = ANY(%s)
                    """,
                    (normalized,),
                )
                rows = cur.fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            parsed = self._row_to_dict(dict(row))
            key = int(parsed.get("job_id") or 0)
            if key > 0:
                out[key] = parsed
        return out

    def update_job_linkedin_routing_mode(self, job_id: int, routing_mode: str) -> bool:
        normalized = self._normalize_linkedin_routing_mode(routing_mode)
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET linkedin_routing_mode = %s
                    WHERE id = %s
                    """,
                    (normalized, int(job_id)),
                )
                return int(cur.rowcount or 0) > 0

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
            with self._connect() as conn:
                with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                    cur.execute(
                        "SELECT id FROM linkedin_accounts WHERE id = ANY(%s)",
                        (unique_ids,),
                    )
                    rows = cur.fetchall()
            existing_ids = sorted(int(r["id"]) for r in rows)

        now = utc_now_iso()
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM job_linkedin_account_assignments WHERE job_id = %s",
                    (int(job_id),),
                )
                for account_id in existing_ids:
                    cur.execute(
                        """
                        INSERT INTO job_linkedin_account_assignments (job_id, account_id, created_at)
                        VALUES (%s, %s, %s)
                        """,
                        (int(job_id), int(account_id), now),
                    )
        return existing_ids

    def list_job_linkedin_account_ids(self, job_id: int) -> List[int]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT account_id
                    FROM job_linkedin_account_assignments
                    WHERE job_id = %s
                    ORDER BY account_id ASC
                    """,
                    (int(job_id),),
                )
                rows = cur.fetchall()
        return [int(r["account_id"]) for r in rows]

    def list_job_linkedin_accounts(self, job_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
        args: List[Any] = [int(job_id)]
        where = ""
        if status:
            where = "AND a.status = %s"
            args.append(str(status))
        query = f"""
            SELECT a.*
            FROM job_linkedin_account_assignments ja
            JOIN linkedin_accounts a ON a.id = ja.account_id
            WHERE ja.job_id = %s
            {where}
            ORDER BY a.updated_at DESC, a.id DESC
        """
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def upsert_candidate(self, profile: Dict[str, Any], source: str = "linkedin") -> int:
        linkedin_id = str(profile.get("linkedin_id") or "").strip()
        if not linkedin_id:
            raise ValueError("linkedin_id is required")
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidates (
                        linkedin_id, full_name, headline, location, languages,
                        skills, years_experience, source, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(linkedin_id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        headline = EXCLUDED.headline,
                        location = EXCLUDED.location,
                        languages = EXCLUDED.languages,
                        skills = EXCLUDED.skills,
                        years_experience = EXCLUDED.years_experience,
                        source = EXCLUDED.source
                    RETURNING id
                    """,
                    (
                        linkedin_id,
                        profile.get("full_name"),
                        profile.get("headline"),
                        profile.get("location"),
                        self._json(profile.get("languages", [])),
                        self._json(profile.get("skills", [])),
                        profile.get("years_experience"),
                        source,
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def get_candidate(self, candidate_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM candidates WHERE id = %s", (int(candidate_id),))
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def get_candidate_by_linkedin_id(self, linkedin_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM candidates WHERE linkedin_id = %s", (str(linkedin_id),))
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def create_candidate_match(
        self,
        job_id: int,
        candidate_id: int,
        score: float,
        status: str,
        verification_notes: Dict[str, Any],
    ) -> None:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_job_matches (
                        job_id, candidate_id, score, status, verification_notes, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(job_id, candidate_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        status = EXCLUDED.status,
                        verification_notes = EXCLUDED.verification_notes,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        int(job_id),
                        int(candidate_id),
                        float(score),
                        status,
                        self._json(verification_notes or {}),
                        utc_now_iso(),
                    ),
                )

    def get_candidate_match(self, job_id: int, candidate_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM candidate_job_matches
                    WHERE job_id = %s AND candidate_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(job_id), int(candidate_id)),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

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
        WHERE m.candidate_id = %s
        ORDER BY m.created_at DESC, m.id DESC
        """
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, (int(candidate_id),))
                rows = cur.fetchall()
        items = [self._row_to_dict(dict(r)) for r in rows]
        assessments = self.list_candidate_assessments(candidate_id=candidate_id)
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for assessment in assessments:
            job_key = int(assessment.get("job_id") or 0)
            grouped.setdefault(job_key, []).append(assessment)
        for item in items:
            job_key = int(item.get("job_id") or 0)
            candidate_assessments = list(grouped.get(job_key, []))
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
        args: List[Any] = [int(candidate_id)]
        where = "WHERE candidate_id = %s"
        if job_id is not None:
            where += " AND job_id = %s"
            args.append(int(job_id))
        query = f"""
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
            {where}
            ORDER BY updated_at DESC, id DESC
        """
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_candidate_assessments_for_job(self, job_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
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
                    WHERE job_id = %s
                    ORDER BY updated_at DESC, id DESC
                    """,
                    (int(job_id),),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def _list_candidate_assessments_grouped(self, job_id: int) -> Dict[int, List[Dict[str, Any]]]:
        rows = self.list_candidate_assessments_for_job(int(job_id))
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for item in rows:
            candidate_key = int(item.get("candidate_id") or 0)
            grouped.setdefault(candidate_key, []).append(item)
        return grouped

    def create_conversation(self, job_id: int, candidate_id: int, channel: str = "linkedin") -> int:
        now = utc_now_iso()
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversations (job_id, candidate_id, channel, status, last_message_at, created_at)
                    VALUES (%s, %s, %s, 'active', %s, %s)
                    RETURNING id
                    """,
                    (int(job_id), int(candidate_id), channel, now, now),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def get_or_create_conversation(self, job_id: int, candidate_id: int, channel: str = "linkedin") -> int:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT id FROM conversations
                    WHERE job_id = %s AND candidate_id = %s AND channel = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(job_id), int(candidate_id), channel),
                )
                row = cur.fetchone()
        if row:
            return int(row["id"])
        return self.create_conversation(job_id=job_id, candidate_id=candidate_id, channel=channel)

    def set_conversation_external_chat_id(self, conversation_id: int, external_chat_id: str) -> Dict[str, Any]:
        external_chat_id = str(external_chat_id or "").strip()
        if not external_chat_id:
            return {"status": "skipped_empty"}

        with self.transaction() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT id, candidate_id
                    FROM conversations
                    WHERE id = %s
                    """,
                    (int(conversation_id),),
                )
                target = cur.fetchone()
                if not target:
                    return {
                        "status": "conversation_not_found",
                        "conversation_id": int(conversation_id),
                        "external_chat_id": external_chat_id,
                    }

                cur.execute(
                    """
                    SELECT id, candidate_id
                    FROM conversations
                    WHERE external_chat_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (external_chat_id,),
                )
                existing = cur.fetchone()

                if existing and int(existing["id"]) != int(target["id"]):
                    if int(existing["candidate_id"]) == int(target["candidate_id"]):
                        cur.execute(
                            """
                            UPDATE conversations
                            SET external_chat_id = NULL
                            WHERE id = %s
                            """,
                            (int(existing["id"]),),
                        )
                        cur.execute(
                            """
                            UPDATE conversations
                            SET external_chat_id = %s
                            WHERE id = %s
                            """,
                            (external_chat_id, int(conversation_id)),
                        )
                        return {
                            "status": "rebound_same_candidate",
                            "external_chat_id": external_chat_id,
                            "from_conversation_id": int(existing["id"]),
                            "to_conversation_id": int(conversation_id),
                        }
                    return {
                        "status": "conflict_other_candidate",
                        "external_chat_id": external_chat_id,
                        "target_conversation_id": int(conversation_id),
                        "target_candidate_id": int(target["candidate_id"]),
                        "existing_conversation_id": int(existing["id"]),
                        "existing_candidate_id": int(existing["candidate_id"]),
                    }

                cur.execute(
                    """
                    UPDATE conversations
                    SET external_chat_id = %s
                    WHERE id = %s
                    """,
                    (external_chat_id, int(conversation_id)),
                )
                return {
                    "status": "set",
                    "conversation_id": int(conversation_id),
                    "external_chat_id": external_chat_id,
                }

    def get_conversation_by_external_chat_id(self, external_chat_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM conversations
                    WHERE external_chat_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(external_chat_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def get_latest_conversation_for_candidate(self, candidate_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM conversations
                    WHERE candidate_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(candidate_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def add_message(
        self,
        conversation_id: int,
        direction: str,
        content: str,
        candidate_language: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
    ) -> int:
        now = utc_now_iso()
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO messages (conversation_id, direction, candidate_language, content, meta, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        int(conversation_id),
                        direction,
                        candidate_language,
                        content,
                        self._json(meta or {}),
                        now,
                    ),
                )
                row = cur.fetchone()
                message_id = int(row[0] if row else 0)
                cur.execute(
                    "UPDATE conversations SET last_message_at = %s WHERE id = %s",
                    (now, int(conversation_id)),
                )
                return message_id

    def get_conversation(self, conversation_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM conversations WHERE id = %s", (int(conversation_id),))
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def update_conversation_status(self, conversation_id: int, status: str) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET status = %s
                    WHERE id = %s
                    """,
                    (status, int(conversation_id)),
                )
                return int(cur.rowcount or 0) > 0

    def set_conversation_linkedin_account(self, conversation_id: int, account_id: Optional[int]) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE conversations
                    SET linkedin_account_id = %s
                    WHERE id = %s
                    """,
                    (account_id, int(conversation_id)),
                )
                return int(cur.rowcount or 0) > 0

    def list_conversations_for_candidate(self, candidate_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
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
                    WHERE conv.candidate_id = %s
                    ORDER BY conv.last_message_at DESC, conv.id DESC
                    LIMIT %s
                    """,
                    (int(candidate_id), safe_limit),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_conversations_by_status(
        self,
        status: str,
        limit: int = 200,
        job_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        args: List[Any] = [status]
        where = "WHERE conv.status = %s"
        if job_id is not None:
            where += " AND conv.job_id = %s"
            args.append(int(job_id))
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
        LIMIT %s
        """
        args.append(safe_limit)
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_messages(self, conversation_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT * FROM messages WHERE conversation_id = %s ORDER BY id ASC",
                    (int(conversation_id),),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def log_operation(
        self,
        operation: str,
        status: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO operation_logs (operation, entity_type, entity_id, status, details, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation,
                        entity_type,
                        entity_id,
                        status,
                        self._json(details or {}),
                        utc_now_iso(),
                    ),
                )

    def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return super().list_logs(limit=limit)

    def list_logs_for_candidate(self, candidate_id: int, limit: int = 300) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 300), 2000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM operation_logs
                    WHERE
                        (entity_type = 'candidate' AND entity_id = %s)
                        OR (
                            entity_type = 'conversation'
                            AND entity_id IN (
                                SELECT id::text
                                FROM conversations
                                WHERE candidate_id = %s
                            )
                        )
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (str(int(candidate_id)), int(candidate_id), safe_limit),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO outbound_actions (
                        job_id, candidate_id, conversation_id, action_type, status, priority,
                        not_before, attempts, account_id, payload_json, result_json, last_error,
                        created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, 'pending', %s, %s, 0, NULL, %s, NULL, NULL, %s, %s)
                    RETURNING id
                    """,
                    (
                        int(job_id),
                        int(candidate_id),
                        int(conversation_id),
                        action_type,
                        int(priority),
                        due,
                        self._json(payload or {}),
                        now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def list_pending_outbound_actions(
        self,
        *,
        limit: int = 100,
        job_id: Optional[int] = None,
        action_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 2000))
        args: List[Any] = [utc_now_iso()]
        where_parts = ["status = 'pending'", "not_before <= %s"]
        if job_id is not None:
            where_parts.append("job_id = %s")
            args.append(int(job_id))
        if action_ids:
            valid_ids = [int(x) for x in action_ids if int(x) > 0]
            if valid_ids:
                where_parts.append("id = ANY(%s)")
                args.append(valid_ids)
        query = f"""
            SELECT *
            FROM outbound_actions
            WHERE {' AND '.join(where_parts)}
            ORDER BY priority DESC, id ASC
            LIMIT %s
        """
        args.append(safe_limit)
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def claim_outbound_action(self, action_id: int) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE outbound_actions
                    SET
                        status = 'running',
                        attempts = attempts + 1,
                        updated_at = %s
                    WHERE id = %s AND status = 'pending'
                    """,
                    (utc_now_iso(), int(action_id)),
                )
                return int(cur.rowcount or 0) > 0

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE outbound_actions
                    SET
                        status = %s,
                        account_id = %s,
                        result_json = %s,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        normalized,
                        account_id,
                        self._json(result or {}),
                        (str(error or "")[:400] or None),
                        utc_now_iso(),
                        int(action_id),
                    ),
                )
                return int(cur.rowcount or 0) > 0

    def release_outbound_action(
        self,
        *,
        action_id: int,
        not_before: str,
        error: Optional[str] = None,
    ) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE outbound_actions
                    SET
                        status = 'pending',
                        not_before = %s,
                        last_error = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        str(not_before),
                        (str(error or "")[:400] or None),
                        utc_now_iso(),
                        int(action_id),
                    ),
                )
                return int(cur.rowcount or 0) > 0

    def get_outbound_action(self, action_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT * FROM outbound_actions WHERE id = %s",
                    (int(action_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def get_linkedin_account_daily_counter(self, account_id: int, day_utc: str) -> Dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_account_daily_counters
                    WHERE account_id = %s AND day_utc = %s
                    """,
                    (int(account_id), str(day_utc)),
                )
                row = cur.fetchone()
        if not row:
            return {
                "account_id": int(account_id),
                "day_utc": str(day_utc),
                "connect_sent": 0,
                "new_threads_sent": 0,
                "replies_sent": 0,
            }
        return self._row_to_dict(dict(row))

    def get_linkedin_account_weekly_counter(self, account_id: int, week_start_utc: str) -> Dict[str, Any]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_account_weekly_counters
                    WHERE account_id = %s AND week_start_utc = %s
                    """,
                    (int(account_id), str(week_start_utc)),
                )
                row = cur.fetchone()
        if not row:
            return {
                "account_id": int(account_id),
                "week_start_utc": str(week_start_utc),
                "connect_sent": 0,
                "new_threads_sent": 0,
                "replies_sent": 0,
            }
        return self._row_to_dict(dict(row))

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO linkedin_account_daily_counters (
                        account_id, day_utc, connect_sent, new_threads_sent, replies_sent, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(account_id, day_utc)
                    DO UPDATE SET
                        connect_sent = linkedin_account_daily_counters.connect_sent + EXCLUDED.connect_sent,
                        new_threads_sent = linkedin_account_daily_counters.new_threads_sent + EXCLUDED.new_threads_sent,
                        replies_sent = linkedin_account_daily_counters.replies_sent + EXCLUDED.replies_sent,
                        updated_at = EXCLUDED.updated_at
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
                cur.execute(
                    """
                    INSERT INTO linkedin_account_weekly_counters (
                        account_id, week_start_utc, connect_sent, new_threads_sent, replies_sent, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(account_id, week_start_utc)
                    DO UPDATE SET
                        connect_sent = linkedin_account_weekly_counters.connect_sent + EXCLUDED.connect_sent,
                        new_threads_sent = linkedin_account_weekly_counters.new_threads_sent + EXCLUDED.new_threads_sent,
                        replies_sent = linkedin_account_weekly_counters.replies_sent + EXCLUDED.replies_sent,
                        updated_at = EXCLUDED.updated_at
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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO linkedin_onboarding_sessions (
                        session_id, provider, status, state_nonce, state_expires_at, redirect_uri,
                        connect_url, provider_account_id, error, metadata, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL, %s, %s, %s)
                    """,
                    (
                        session_id,
                        provider,
                        "pending",
                        state_nonce,
                        state_expires_at,
                        redirect_uri,
                        connect_url,
                        self._json(metadata or {}),
                        now,
                        now,
                    ),
                )

    def get_linkedin_onboarding_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_onboarding_sessions
                    WHERE session_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (session_id,),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def update_linkedin_onboarding_session_status(
        self,
        session_id: str,
        status: str,
        provider_account_id: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        with self.transaction() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT metadata
                    FROM linkedin_onboarding_sessions
                    WHERE session_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (session_id,),
                )
                existing_row = cur.fetchone()
                merged_metadata: Dict[str, Any] = {}
                if existing_row and isinstance(existing_row.get("metadata"), dict):
                    merged_metadata.update(existing_row.get("metadata") or {})
                if metadata:
                    merged_metadata.update(metadata)
                cur.execute(
                    """
                    UPDATE linkedin_onboarding_sessions
                    SET
                        status = %s,
                        provider_account_id = %s,
                        error = %s,
                        metadata = %s,
                        updated_at = %s
                    WHERE session_id = %s
                    """,
                    (
                        status,
                        provider_account_id,
                        error,
                        self._json(merged_metadata),
                        utc_now_iso(),
                        session_id,
                    ),
                )
                return int(cur.rowcount or 0) > 0

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
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT id, metadata, connected_at
                    FROM linkedin_accounts
                    WHERE provider_account_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (provider_account_id,),
                )
                existing = cur.fetchone()
                merged_metadata: Dict[str, Any] = {}
                if existing and isinstance(existing.get("metadata"), dict):
                    merged_metadata.update(existing.get("metadata") or {})
                if metadata:
                    merged_metadata.update(metadata)
                normalized_connected_at = connected_at
                if not normalized_connected_at:
                    if status == "connected":
                        normalized_connected_at = (existing.get("connected_at") if existing else None) or now
                    elif existing:
                        normalized_connected_at = existing.get("connected_at")
                normalized_last_synced_at = last_synced_at or now

                if existing:
                    account_id = int(existing.get("id") or 0)
                    cur.execute(
                        """
                        UPDATE linkedin_accounts
                        SET
                            provider = %s,
                            provider_user_id = COALESCE(%s, provider_user_id),
                            label = COALESCE(%s, label),
                            status = %s,
                            metadata = %s,
                            connected_at = %s,
                            last_synced_at = %s,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            provider,
                            provider_user_id,
                            label,
                            status,
                            self._json(merged_metadata),
                            normalized_connected_at,
                            normalized_last_synced_at,
                            now,
                            account_id,
                        ),
                    )
                    return account_id

                cur.execute(
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        provider,
                        provider_account_id,
                        provider_user_id,
                        label,
                        status,
                        self._json(merged_metadata),
                        normalized_connected_at,
                        normalized_last_synced_at,
                        now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row.get("id") if isinstance(row, dict) else (row[0] if row else 0))

    def get_linkedin_account(self, account_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM linkedin_accounts WHERE id = %s", (int(account_id),))
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def get_linkedin_account_by_provider_account_id(self, provider_account_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM linkedin_accounts
                    WHERE provider_account_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (provider_account_id,),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def list_linkedin_accounts(self, limit: int = 200, status: Optional[str] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if status:
                    cur.execute(
                        """
                        SELECT *
                        FROM linkedin_accounts
                        WHERE status = %s
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (status, safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM linkedin_accounts
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                        """,
                        (safe_limit,),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def update_linkedin_account_status(
        self,
        account_id: int,
        status: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        with self.transaction() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT metadata FROM linkedin_accounts WHERE id = %s",
                    (int(account_id),),
                )
                existing = cur.fetchone()
                if not existing:
                    return False
                merged_metadata: Dict[str, Any] = {}
                if isinstance(existing.get("metadata"), dict):
                    merged_metadata.update(existing.get("metadata") or {})
                if metadata:
                    merged_metadata.update(metadata)
                cur.execute(
                    """
                    UPDATE linkedin_accounts
                    SET
                        status = %s,
                        metadata = %s,
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (
                        status,
                        self._json(merged_metadata),
                        utc_now_iso(),
                        int(account_id),
                    ),
                )
                return int(cur.rowcount or 0) > 0

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
        resume_links = state.get("resume_links") or []
        next_followup_at = state.get("next_followup_at")
        created_at = state.get("created_at") or utc_now_iso()
        updated_at = state.get("updated_at") or utc_now_iso()

        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pre_resume_sessions (
                        session_id, conversation_id, job_id, candidate_id, status, language,
                        last_intent, followups_sent, turns, last_error, resume_links,
                        next_followup_at, state_json, instruction, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(session_id) DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        job_id = EXCLUDED.job_id,
                        candidate_id = EXCLUDED.candidate_id,
                        status = EXCLUDED.status,
                        language = EXCLUDED.language,
                        last_intent = EXCLUDED.last_intent,
                        followups_sent = EXCLUDED.followups_sent,
                        turns = EXCLUDED.turns,
                        last_error = EXCLUDED.last_error,
                        resume_links = EXCLUDED.resume_links,
                        next_followup_at = EXCLUDED.next_followup_at,
                        state_json = EXCLUDED.state_json,
                        instruction = EXCLUDED.instruction,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        session_id,
                        int(conversation_id),
                        int(job_id),
                        int(candidate_id),
                        status,
                        language,
                        last_intent,
                        followups_sent,
                        turns,
                        last_error,
                        self._json(resume_links),
                        next_followup_at,
                        self._json(state),
                        instruction,
                        created_at,
                        updated_at,
                    ),
                )

    def get_pre_resume_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute("SELECT * FROM pre_resume_sessions WHERE session_id = %s", (session_id,))
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def get_pre_resume_session_by_conversation(self, conversation_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT * FROM pre_resume_sessions WHERE conversation_id = %s",
                    (int(conversation_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

    def list_pre_resume_sessions(
        self,
        limit: int = 100,
        status: Optional[str] = None,
        job_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 1000))
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
            where.append("prs.status = %s")
            args.append(status)
        if job_id is not None:
            where.append("prs.job_id = %s")
            args.append(int(job_id))
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY prs.updated_at DESC LIMIT %s"
        args.append(safe_limit)

        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_pre_resume_sessions_for_candidate(self, candidate_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM pre_resume_sessions
                    WHERE candidate_id = %s
                    ORDER BY updated_at DESC, session_id DESC
                    LIMIT %s
                    """,
                    (int(candidate_id), max(1, min(int(limit or 200), 2000))),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pre_resume_events
                        (session_id, conversation_id, event_type, intent, inbound_text, outbound_text, state_status, details, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        session_id,
                        int(conversation_id),
                        event_type,
                        intent,
                        inbound_text,
                        outbound_text,
                        state_status,
                        self._json(details or {}),
                        utc_now_iso(),
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def list_pre_resume_events(self, limit: int = 200, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if session_id:
                    cur.execute(
                        """
                        SELECT * FROM pre_resume_events
                        WHERE session_id = %s
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        (session_id, safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT * FROM pre_resume_events
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        (safe_limit,),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_pre_resume_events_for_candidate(
        self,
        candidate_id: int,
        *,
        job_id: Optional[int] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 300), 2000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if job_id is None:
                    cur.execute(
                        """
                        SELECT
                            e.*,
                            s.job_id,
                            s.candidate_id
                        FROM pre_resume_events e
                        JOIN pre_resume_sessions s ON s.session_id = e.session_id
                        WHERE s.candidate_id = %s
                        ORDER BY e.id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            e.*,
                            s.job_id,
                            s.candidate_id
                        FROM pre_resume_events e
                        JOIN pre_resume_sessions s ON s.session_id = e.session_id
                        WHERE s.candidate_id = %s AND s.job_id = %s
                        ORDER BY e.id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), int(job_id), safe_limit),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def record_webhook_event(self, event_key: str, source: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO webhook_events (event_key, source, payload, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(event_key) DO NOTHING
                    """,
                    (event_key, source, self._json(payload or {}), utc_now_iso()),
                )
                return int(cur.rowcount or 0) > 0

    def upsert_job_step_progress(
        self,
        job_id: int,
        step: str,
        status: str,
        output: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_step_progress (job_id, step, status, output_json, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(job_id, step)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        output_json = EXCLUDED.output_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        int(job_id),
                        step,
                        status,
                        self._json(output or {}),
                        utc_now_iso(),
                    ),
                )

    def list_job_step_progress(self, job_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT job_id, step, status, output_json, updated_at
                    FROM job_step_progress
                    WHERE job_id = %s
                    ORDER BY updated_at DESC
                    """,
                    (int(job_id),),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def update_candidate_match_status(
        self,
        job_id: int,
        candidate_id: int,
        status: str,
        extra_notes: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.transaction() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT verification_notes
                    FROM candidate_job_matches
                    WHERE job_id = %s AND candidate_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(job_id), int(candidate_id)),
                )
                row = cur.fetchone()
                merged_notes: Dict[str, Any] = {}
                if row and isinstance(row.get("verification_notes"), dict):
                    merged_notes.update(row.get("verification_notes") or {})
                if extra_notes:
                    merged_notes.update(extra_notes)
                cur.execute(
                    """
                    UPDATE candidate_job_matches
                    SET status = %s, verification_notes = %s
                    WHERE job_id = %s AND candidate_id = %s
                    """,
                    (status, self._json(merged_notes), int(job_id), int(candidate_id)),
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
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_agent_assessments (
                        job_id, candidate_id, agent_key, agent_name, stage_key, score, status,
                        reason, instruction, details, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(job_id, candidate_id, agent_key, stage_key)
                    DO UPDATE SET
                        agent_name = EXCLUDED.agent_name,
                        score = EXCLUDED.score,
                        status = EXCLUDED.status,
                        reason = EXCLUDED.reason,
                        instruction = EXCLUDED.instruction,
                        details = EXCLUDED.details,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        int(job_id),
                        int(candidate_id),
                        agent_key,
                        agent_name,
                        stage_key,
                        normalized_score,
                        status,
                        reason,
                        instruction,
                        self._json(details or {}),
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
            with conn.cursor() as cur:
                cur.execute(
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(signal_key)
                    DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        signal_type = EXCLUDED.signal_type,
                        signal_category = EXCLUDED.signal_category,
                        title = EXCLUDED.title,
                        detail = EXCLUDED.detail,
                        impact_score = EXCLUDED.impact_score,
                        confidence = EXCLUDED.confidence,
                        signal_meta = EXCLUDED.signal_meta,
                        observed_at = EXCLUDED.observed_at,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
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
                        self._json(signal_meta or {}),
                        normalized_observed,
                        now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def upsert_resume_asset(
        self,
        *,
        job_id: int,
        candidate_id: int,
        source_type: str,
        source_id: str,
        processing_status: str,
        conversation_id: Optional[int] = None,
        provider: Optional[str] = None,
        provider_message_id: Optional[str] = None,
        file_name: Optional[str] = None,
        mime_type: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        remote_url: Optional[str] = None,
        storage_path: Optional[str] = None,
        content_sha256: Optional[str] = None,
        processing_error: Optional[str] = None,
        extracted_text: Optional[str] = None,
        parsed_json: Optional[Dict[str, Any]] = None,
        observed_at: Optional[str] = None,
        asset_key: Optional[str] = None,
    ) -> int:
        now = utc_now_iso()
        normalized_observed = str(observed_at or now)
        resolved_key = str(
            asset_key
            or f"{int(job_id)}:{int(candidate_id)}:{str(source_type or '').strip().lower()}:{str(source_id or '').strip().lower()}"
        ).strip()
        with self.transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO resume_assets (
                        asset_key,
                        job_id,
                        candidate_id,
                        conversation_id,
                        source_type,
                        source_id,
                        provider,
                        provider_message_id,
                        file_name,
                        mime_type,
                        file_size_bytes,
                        remote_url,
                        storage_path,
                        content_sha256,
                        processing_status,
                        processing_error,
                        extracted_text,
                        parsed_json,
                        observed_at,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(asset_key)
                    DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        provider = EXCLUDED.provider,
                        provider_message_id = EXCLUDED.provider_message_id,
                        file_name = EXCLUDED.file_name,
                        mime_type = EXCLUDED.mime_type,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        remote_url = EXCLUDED.remote_url,
                        storage_path = EXCLUDED.storage_path,
                        content_sha256 = EXCLUDED.content_sha256,
                        processing_status = EXCLUDED.processing_status,
                        processing_error = EXCLUDED.processing_error,
                        extracted_text = EXCLUDED.extracted_text,
                        parsed_json = EXCLUDED.parsed_json,
                        observed_at = EXCLUDED.observed_at,
                        updated_at = EXCLUDED.updated_at
                    RETURNING id
                    """,
                    (
                        resolved_key,
                        int(job_id),
                        int(candidate_id),
                        int(conversation_id) if conversation_id is not None else None,
                        str(source_type or "").strip().lower() or "unknown",
                        str(source_id or "").strip() or "unknown",
                        str(provider or "").strip().lower() or None,
                        str(provider_message_id or "").strip() or None,
                        str(file_name or "").strip() or None,
                        str(mime_type or "").strip().lower() or None,
                        int(file_size_bytes) if file_size_bytes is not None else None,
                        str(remote_url or "").strip() or None,
                        str(storage_path or "").strip() or None,
                        str(content_sha256 or "").strip().lower() or None,
                        str(processing_status or "").strip().lower() or "pending",
                        str(processing_error or "").strip() or None,
                        str(extracted_text or "").strip() or None,
                        self._json(parsed_json or {}),
                        normalized_observed,
                        now,
                        now,
                    ),
                )
                row = cur.fetchone()
                return int(row[0] if row else 0)

    def list_resume_assets_for_candidate(
        self,
        *,
        candidate_id: int,
        job_id: Optional[int] = None,
        limit: int = 300,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 300), 5000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if job_id is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM resume_assets
                        WHERE candidate_id = %s
                        ORDER BY observed_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM resume_assets
                        WHERE candidate_id = %s AND job_id = %s
                        ORDER BY observed_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), int(job_id), safe_limit),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_resume_assets_for_job(
        self,
        *,
        job_id: int,
        candidate_id: Optional[int] = None,
        limit: int = 3000,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 3000), 10000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if candidate_id is None:
                    cur.execute(
                        """
                        SELECT
                            a.*,
                            c.full_name AS candidate_name
                        FROM resume_assets a
                        LEFT JOIN candidates c ON c.id = a.candidate_id
                        WHERE a.job_id = %s
                        ORDER BY a.observed_at DESC, a.id DESC
                        LIMIT %s
                        """,
                        (int(job_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            a.*,
                            c.full_name AS candidate_name
                        FROM resume_assets a
                        LEFT JOIN candidates c ON c.id = a.candidate_id
                        WHERE a.job_id = %s AND a.candidate_id = %s
                        ORDER BY a.observed_at DESC, a.id DESC
                        LIMIT %s
                        """,
                        (int(job_id), int(candidate_id), safe_limit),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_candidate_signals(
        self,
        *,
        candidate_id: int,
        job_id: Optional[int] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 5000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if job_id is None:
                    cur.execute(
                        """
                        SELECT *
                        FROM candidate_signals
                        WHERE candidate_id = %s
                        ORDER BY observed_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT *
                        FROM candidate_signals
                        WHERE candidate_id = %s AND job_id = %s
                        ORDER BY observed_at DESC, id DESC
                        LIMIT %s
                        """,
                        (int(candidate_id), int(job_id), safe_limit),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

    def list_job_signals(
        self,
        *,
        job_id: int,
        limit: int = 2000,
        candidate_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 2000), 10000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                if candidate_id is None:
                    cur.execute(
                        """
                        SELECT
                            s.*,
                            c.full_name AS candidate_name
                        FROM candidate_signals s
                        LEFT JOIN candidates c ON c.id = s.candidate_id
                        WHERE s.job_id = %s
                        ORDER BY s.observed_at DESC, s.id DESC
                        LIMIT %s
                        """,
                        (int(job_id), safe_limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            s.*,
                            c.full_name AS candidate_name
                        FROM candidate_signals s
                        LEFT JOIN candidates c ON c.id = s.candidate_id
                        WHERE s.job_id = %s AND s.candidate_id = %s
                        ORDER BY s.observed_at DESC, s.id DESC
                        LIMIT %s
                        """,
                        (int(job_id), int(candidate_id), safe_limit),
                    )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(r)) for r in rows]

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

    @staticmethod
    def _derive_candidate_current_status(item: Dict[str, Any]) -> tuple[str, str]:
        return Database._derive_candidate_current_status(item)
