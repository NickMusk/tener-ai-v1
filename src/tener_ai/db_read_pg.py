from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .db import Database


def _require_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
    except Exception as exc:
        raise RuntimeError("psycopg is required for postgres read database") from exc
    return psycopg


class PostgresReadDatabase:
    """Read-only adapter for selected API endpoints backed by Postgres."""

    def __init__(self, dsn: str) -> None:
        self.dsn = str(dsn or "").strip()
        if not self.dsn:
            raise ValueError("postgres dsn is required")
        self._psycopg = _require_psycopg()

    def _connect(self) -> Any:
        return self._psycopg.connect(self.dsn)

    def list_jobs(self, limit: int = 100) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 1000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        j.*,
                        cp.profile_json AS company_culture_profile,
                        cp.status AS company_culture_profile_status,
                        cp.generated_at AS company_culture_profile_generated_at
                    FROM jobs j
                    LEFT JOIN job_culture_profiles cp ON cp.job_id = j.id
                    ORDER BY j.id DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(row)) for row in rows]

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        j.*,
                        cp.profile_json AS company_culture_profile,
                        cp.status AS company_culture_profile_status,
                        cp.generated_at AS company_culture_profile_generated_at
                    FROM jobs j
                    LEFT JOIN job_culture_profiles cp ON cp.job_id = j.id
                    WHERE j.id = %s
                    LIMIT 1
                    """,
                    (int(job_id),),
                )
                row = cur.fetchone()
        return self._row_to_dict(dict(row)) if row else None

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
        return [self._row_to_dict(dict(row)) for row in rows]

    def list_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 100), 1000))
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM operation_logs
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                rows = cur.fetchall()
        return [self._row_to_dict(dict(row)) for row in rows]

    def list_candidates_for_job(self, job_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
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
                    WHERE m.job_id = %s
                    ORDER BY m.score DESC
                    """,
                    (int(job_id),),
                )
                rows = cur.fetchall()

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
                assessments_rows = cur.fetchall()

        items = [self._row_to_dict(dict(row)) for row in rows]
        assessments_by_candidate: Dict[int, List[Dict[str, Any]]] = {}
        for row in assessments_rows:
            item = self._row_to_dict(dict(row))
            candidate_id = int(item.get("candidate_id") or 0)
            assessments_by_candidate.setdefault(candidate_id, []).append(item)

        for item in items:
            key, label = Database._derive_candidate_current_status(item)  # type: ignore[attr-defined]
            item["current_status_key"] = key
            item["current_status_label"] = label
            candidate_id = int(item.get("candidate_id") or 0)
            candidate_assessments = list(assessments_by_candidate.get(candidate_id, []))
            item["agent_assessments"] = candidate_assessments
            item["agent_scorecard"] = Database._build_agent_scorecard(  # type: ignore[attr-defined]
                assessments=candidate_assessments,
                candidate_row=item,
            )
        return items

    def list_conversations_overview(self, limit: int = 200, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        where = ""
        args: List[Any] = []
        if job_id is not None:
            where = "WHERE conv.job_id = %s"
            args.append(int(job_id))
        args.append(safe_limit)
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
        LIMIT %s
        """
        with self._connect() as conn:
            with conn.cursor(row_factory=self._psycopg.rows.dict_row) as cur:
                cur.execute(query, tuple(args))
                rows = cur.fetchall()
        return [self._row_to_dict(dict(row)) for row in rows]

    @staticmethod
    def _row_to_dict(item: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, value in item.items():
            if isinstance(value, datetime):
                out[key] = value.isoformat()
            else:
                out[key] = value
        return out
