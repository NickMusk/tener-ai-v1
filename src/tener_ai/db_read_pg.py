from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

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
        self._pool = self._build_pool()

    def _build_pool(self) -> Any:
        try:
            from psycopg_pool import ConnectionPool  # type: ignore
        except Exception:
            return None

        min_size_raw = os.environ.get("TENER_DB_POOL_MIN", "1")
        max_size_raw = os.environ.get("TENER_DB_POOL_MAX", "10")
        timeout_raw = os.environ.get("TENER_DB_POOL_TIMEOUT_SECONDS", "15")
        try:
            min_size = max(1, int(min_size_raw))
        except ValueError:
            min_size = 1
        try:
            max_size = max(min_size, int(max_size_raw))
        except ValueError:
            max_size = max(min_size, 10)
        try:
            timeout_seconds = max(1.0, float(timeout_raw))
        except ValueError:
            timeout_seconds = 15.0

        try:
            return ConnectionPool(
                conninfo=self.dsn,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout_seconds,
                open=True,
            )
        except Exception:
            return None

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        if self._pool is not None:
            with self._pool.connection() as conn:
                yield conn
            return
        with self._psycopg.connect(self.dsn) as conn:
            yield conn

    def list_jobs(self, limit: int = 100, include_archived: bool = False) -> List[Dict[str, Any]]:
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
        items = [self._row_to_dict(dict(row)) for row in rows]
        for item in items:
            Database._decorate_job_item(item)
        if include_archived:
            return items
        return [item for item in items if not bool(item.get("is_archived"))]

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
        if not row:
            return None
        item = self._row_to_dict(dict(row))
        Database._decorate_job_item(item)
        return item

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
                        c.provider_id,
                        c.unipile_profile_id,
                        c.attendee_provider_id,
                        c.linkedin_public_url,
                        c.full_name,
                        c.headline,
                        c.location,
                        c.languages,
                        c.skills,
                        c.years_experience,
                        j.salary_min AS job_salary_min,
                        j.salary_max AS job_salary_max,
                        j.salary_currency AS job_salary_currency,
                        j.work_authorization_required AS job_work_authorization_required,
                        conv.id AS conversation_id,
                        conv.status AS conversation_status,
                        conv.external_chat_id,
                        conv.last_message_at,
                        prs.session_id AS pre_resume_session_id,
                        prs.status AS pre_resume_status,
                        prs.last_error AS pre_resume_last_error,
                        prs.next_followup_at AS pre_resume_next_followup_at,
                        cps.status AS candidate_prescreen_status,
                        cps.must_have_answers_json AS candidate_prescreen_must_have_answers,
                        cps.salary_expectation_min AS candidate_prescreen_salary_expectation_min,
                        cps.salary_expectation_max AS candidate_prescreen_salary_expectation_max,
                        cps.salary_expectation_currency AS candidate_prescreen_salary_expectation_currency,
                        cps.location_confirmed AS candidate_prescreen_location_confirmed,
                        cps.work_authorization_confirmed AS candidate_prescreen_work_authorization_confirmed,
                        cps.cv_received AS candidate_prescreen_cv_received,
                        cps.summary AS candidate_prescreen_summary,
                        cps.notes AS candidate_prescreen_notes,
                        cps.updated_at AS candidate_prescreen_updated_at,
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
                    JOIN jobs j ON j.id = m.job_id
                    LEFT JOIN conversations conv ON conv.id = (
                        SELECT c2.id
                        FROM conversations c2
                        WHERE c2.job_id = m.job_id
                          AND c2.candidate_id = m.candidate_id
                        ORDER BY c2.id DESC
                        LIMIT 1
                    )
                    LEFT JOIN pre_resume_sessions prs ON prs.conversation_id = conv.id
                    LEFT JOIN candidate_prescreens cps ON cps.job_id = m.job_id AND cps.candidate_id = m.candidate_id
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

    def list_outreach_ats_candidates(
        self,
        *,
        job_id: Optional[int] = None,
        limit: Optional[int] = 500,
    ) -> List[Dict[str, Any]]:
        safe_limit = None if limit is None else max(1, min(int(limit or 500), 10000))
        account_labels = {
            int(item.get("id") or 0): str(item.get("label") or "").strip() or f"Account {int(item.get('id') or 0)}"
            for item in self.list_linkedin_accounts(limit=500)
            if int(item.get("id") or 0) > 0
        }

        rows: List[Dict[str, Any]] = []
        job_refs: List[Dict[str, Any]] = []
        if job_id is not None:
            job = self.get_job(int(job_id))
            if job and not bool(job.get("is_archived")):
                job_refs = [job]
        else:
            job_refs = self.list_jobs(limit=300)

        for job in job_refs:
            row_job_id = int(job.get("id") or 0)
            if row_job_id <= 0:
                continue
            for row in self.list_candidates_for_job(row_job_id):
                if not isinstance(row, dict):
                    continue
                current_status_key = str(row.get("current_status_key") or "").strip().lower()
                if current_status_key in {"unknown"}:
                    continue
                enriched = dict(row)
                enriched["job_id"] = row_job_id
                enriched["job_title"] = str(job.get("title") or "").strip() or "-"
                enriched.update(Database._derive_candidate_ats_stage(enriched))
                assigned_account_id = int(
                    enriched.get("pending_action_account_id")
                    or enriched.get("linkedin_account_id")
                    or 0
                ) or None
                enriched["assigned_account_id"] = assigned_account_id
                enriched["assigned_account_label"] = (
                    account_labels.get(int(assigned_account_id or 0)) if assigned_account_id else None
                )
                enriched["last_activity_at"] = (
                    enriched.get("pending_action_not_before")
                    or enriched.get("last_message_created_at")
                    or enriched.get("last_message_at")
                    or enriched.get("match_created_at")
                )
                rows.append(enriched)

        rows.sort(
            key=lambda item: (
                int(item.get("ats_stage_rank") or 999),
                str(item.get("last_activity_at") or ""),
                -float(item.get("score") or 0.0),
                int(item.get("candidate_id") or 0),
            ),
            reverse=False,
        )
        return rows if safe_limit is None else rows[:safe_limit]

    def list_conversations_overview(
        self,
        limit: int = 200,
        job_id: Optional[int] = None,
        started_only: bool = False,
        dialogue_bucket: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 2000))
        where_parts = ["j.archived_at IS NULL"]
        args: List[Any] = []
        normalized_bucket = str(dialogue_bucket or "").strip().lower()
        if normalized_bucket in {"all_started", "candidate_replied", "outbound_only"}:
            started_only = True
        if job_id is not None:
            where_parts.append("conv.job_id = %s")
            args.append(int(job_id))
        if started_only:
            where_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM messages msg_started
                    WHERE msg_started.conversation_id = conv.id
                      AND msg_started.direction = %s
                )
                """.strip()
            )
            args.append("outbound")
        if normalized_bucket == "candidate_replied":
            where_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM messages msg_inbound
                    WHERE msg_inbound.conversation_id = conv.id
                      AND msg_inbound.direction = %s
                )
                """.strip()
            )
            args.append("inbound")
        elif normalized_bucket == "outbound_only":
            where_parts.append(
                """
                NOT EXISTS (
                    SELECT 1
                    FROM messages msg_inbound
                    WHERE msg_inbound.conversation_id = conv.id
                      AND msg_inbound.direction = %s
                )
                """.strip()
            )
            args.append("inbound")
        where = f"WHERE {' AND '.join(where_parts)}"
        args.append(safe_limit)
        query = f"""
        SELECT
            conv.id AS conversation_id,
            conv.job_id,
            conv.candidate_id,
            conv.channel,
            conv.status AS conversation_status,
            conv.external_chat_id,
            conv.linkedin_account_id,
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
