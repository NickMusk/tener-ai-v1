from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class PostgresMirrorWriter:
    """Best-effort writer for mirroring SQLite runtime data into Postgres."""

    def __init__(self, dsn: str) -> None:
        self.dsn = str(dsn or "").strip()
        if not self.dsn:
            raise ValueError("postgres dsn is required")
        self._lock = threading.Lock()
        self._require_psycopg()

    @staticmethod
    def _require_psycopg() -> Any:
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for postgres dual-write") from exc
        return psycopg

    def _json(self, value: Any) -> Any:
        psycopg = self._require_psycopg()
        return psycopg.types.json.Json(value)

    @contextmanager
    def _transaction(self) -> Any:
        psycopg = self._require_psycopg()
        with self._lock:
            with psycopg.connect(self.dsn) as conn:
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

    def upsert_job(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs (
                        id, title, company, company_website, jd_text, location,
                        preferred_languages, seniority, linkedin_routing_mode, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        title = EXCLUDED.title,
                        company = EXCLUDED.company,
                        company_website = EXCLUDED.company_website,
                        jd_text = EXCLUDED.jd_text,
                        location = EXCLUDED.location,
                        preferred_languages = EXCLUDED.preferred_languages,
                        seniority = EXCLUDED.seniority,
                        linkedin_routing_mode = EXCLUDED.linkedin_routing_mode
                    """,
                    (
                        int(row.get("id") or 0),
                        row.get("title"),
                        row.get("company"),
                        row.get("company_website"),
                        row.get("jd_text"),
                        row.get("location"),
                        self._json(row.get("preferred_languages") or []),
                        row.get("seniority"),
                        row.get("linkedin_routing_mode") or "auto",
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def upsert_job_culture_profile(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_culture_profiles (
                        job_id, status, company_name, company_website, profile_json,
                        sources_json, warnings_json, search_queries_json, error,
                        generated_at, updated_at
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
                        int(row.get("job_id") or 0),
                        row.get("status") or "unknown",
                        row.get("company_name"),
                        row.get("company_website"),
                        self._json(row.get("profile_json") or {}),
                        self._json(row.get("sources_json") or []),
                        self._json(row.get("warnings_json") or []),
                        self._json(row.get("search_queries_json") or []),
                        row.get("error"),
                        row.get("generated_at"),
                        row.get("updated_at") or utc_now_iso(),
                    ),
                )

    def upsert_candidate(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidates (
                        id, linkedin_id, full_name, headline, location,
                        languages, skills, years_experience, source, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(linkedin_id) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        headline = EXCLUDED.headline,
                        location = EXCLUDED.location,
                        languages = EXCLUDED.languages,
                        skills = EXCLUDED.skills,
                        years_experience = EXCLUDED.years_experience,
                        source = EXCLUDED.source
                    """,
                    (
                        int(row.get("id") or 0),
                        row.get("linkedin_id"),
                        row.get("full_name"),
                        row.get("headline"),
                        row.get("location"),
                        self._json(row.get("languages") or []),
                        self._json(row.get("skills") or []),
                        row.get("years_experience"),
                        row.get("source"),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def upsert_candidate_match(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
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
                        int(row.get("job_id") or 0),
                        int(row.get("candidate_id") or 0),
                        float(row.get("score") or 0.0),
                        row.get("status") or "unknown",
                        self._json(row.get("verification_notes") or {}),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def upsert_candidate_agent_assessment(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_agent_assessments (
                        job_id, candidate_id, agent_key, agent_name, stage_key,
                        score, status, reason, instruction, details, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(job_id, candidate_id, agent_key, stage_key) DO UPDATE SET
                        agent_name = EXCLUDED.agent_name,
                        score = EXCLUDED.score,
                        status = EXCLUDED.status,
                        reason = EXCLUDED.reason,
                        instruction = EXCLUDED.instruction,
                        details = EXCLUDED.details,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        int(row.get("job_id") or 0),
                        int(row.get("candidate_id") or 0),
                        row.get("agent_key"),
                        row.get("agent_name"),
                        row.get("stage_key"),
                        row.get("score"),
                        row.get("status") or "unknown",
                        row.get("reason"),
                        row.get("instruction"),
                        self._json(row.get("details") or {}),
                        row.get("updated_at") or utc_now_iso(),
                    ),
                )

    def upsert_conversation(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversations (
                        id, job_id, candidate_id, channel, status, external_chat_id,
                        linkedin_account_id, last_message_at, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        status = EXCLUDED.status,
                        external_chat_id = EXCLUDED.external_chat_id,
                        linkedin_account_id = EXCLUDED.linkedin_account_id,
                        last_message_at = EXCLUDED.last_message_at
                    """,
                    (
                        int(row.get("id") or 0),
                        int(row.get("job_id") or 0),
                        int(row.get("candidate_id") or 0),
                        row.get("channel") or "linkedin",
                        row.get("status") or "active",
                        row.get("external_chat_id"),
                        row.get("linkedin_account_id"),
                        row.get("last_message_at"),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def upsert_message(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO messages (
                        id, conversation_id, direction, candidate_language,
                        content, meta, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        direction = EXCLUDED.direction,
                        candidate_language = EXCLUDED.candidate_language,
                        content = EXCLUDED.content,
                        meta = EXCLUDED.meta
                    """,
                    (
                        int(row.get("id") or 0),
                        int(row.get("conversation_id") or 0),
                        row.get("direction"),
                        row.get("candidate_language"),
                        row.get("content"),
                        self._json(row.get("meta") or {}),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def insert_operation_log(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO operation_logs (
                        operation, entity_type, entity_id, status, details, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row.get("operation"),
                        row.get("entity_type"),
                        row.get("entity_id"),
                        row.get("status") or "ok",
                        self._json(row.get("details") or {}),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def upsert_pre_resume_session(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
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
                        row.get("session_id"),
                        int(row.get("conversation_id") or 0),
                        int(row.get("job_id") or 0),
                        int(row.get("candidate_id") or 0),
                        row.get("status") or "awaiting_reply",
                        row.get("language"),
                        row.get("last_intent"),
                        int(row.get("followups_sent") or 0),
                        int(row.get("turns") or 0),
                        row.get("last_error"),
                        self._json(row.get("resume_links") or []),
                        row.get("next_followup_at"),
                        self._json(row.get("state_json") or {}),
                        row.get("instruction") or "",
                        row.get("created_at") or utc_now_iso(),
                        row.get("updated_at") or utc_now_iso(),
                    ),
                )

    def upsert_pre_resume_event(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pre_resume_events (
                        id, session_id, conversation_id, event_type, intent,
                        inbound_text, outbound_text, state_status, details, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        session_id = EXCLUDED.session_id,
                        conversation_id = EXCLUDED.conversation_id,
                        event_type = EXCLUDED.event_type,
                        intent = EXCLUDED.intent,
                        inbound_text = EXCLUDED.inbound_text,
                        outbound_text = EXCLUDED.outbound_text,
                        state_status = EXCLUDED.state_status,
                        details = EXCLUDED.details,
                        created_at = EXCLUDED.created_at
                    """,
                    (
                        int(row.get("id") or 0),
                        row.get("session_id"),
                        int(row.get("conversation_id") or 0),
                        row.get("event_type"),
                        row.get("intent"),
                        row.get("inbound_text"),
                        row.get("outbound_text"),
                        row.get("state_status"),
                        self._json(row.get("details") or {}),
                        row.get("created_at") or utc_now_iso(),
                    ),
                )

    def insert_webhook_event(self, *, event_key: str, source: str, payload: Optional[Dict[str, Any]] = None) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO webhook_events (event_key, source, payload, created_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(event_key) DO NOTHING
                    """,
                    (
                        str(event_key or "").strip(),
                        str(source or "").strip() or "unknown",
                        self._json(payload or {}),
                        utc_now_iso(),
                    ),
                )

    def upsert_job_step_progress(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_step_progress (job_id, step, status, output_json, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(job_id, step) DO UPDATE SET
                        status = EXCLUDED.status,
                        output_json = EXCLUDED.output_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        int(row.get("job_id") or 0),
                        row.get("step"),
                        row.get("status"),
                        self._json(row.get("output_json") or {}),
                        row.get("updated_at") or utc_now_iso(),
                    ),
                )

    def upsert_candidate_signal(self, row: Dict[str, Any]) -> None:
        with self._transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO candidate_signals (
                        id, signal_key, job_id, candidate_id, conversation_id, source_type, source_id,
                        signal_type, signal_category, title, detail, impact_score, confidence,
                        signal_meta, observed_at, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(signal_key) DO UPDATE SET
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
                    """,
                    (
                        int(row.get("id") or 0),
                        row.get("signal_key"),
                        int(row.get("job_id") or 0),
                        int(row.get("candidate_id") or 0),
                        row.get("conversation_id"),
                        row.get("source_type") or "unknown",
                        row.get("source_id") or "unknown",
                        row.get("signal_type") or "unknown",
                        row.get("signal_category"),
                        row.get("title") or "Signal",
                        row.get("detail"),
                        row.get("impact_score"),
                        row.get("confidence"),
                        self._json(row.get("signal_meta") or {}),
                        row.get("observed_at") or utc_now_iso(),
                        row.get("created_at") or utc_now_iso(),
                        row.get("updated_at") or utc_now_iso(),
                    ),
                )


class DualWriteDatabase:
    """Proxy that writes to SQLite primary and mirrors selected writes to Postgres."""

    def __init__(self, *, primary: Any, mirror: PostgresMirrorWriter, strict: bool = False) -> None:
        self._primary = primary
        self._mirror = mirror
        self._strict = bool(strict)
        self._status_lock = threading.Lock()
        self._mirror_errors = 0
        self._mirror_success = 0
        self._last_error: Optional[str] = None

    @property
    def dual_write_status(self) -> Dict[str, Any]:
        with self._status_lock:
            return {
                "enabled": True,
                "strict": self._strict,
                "mirror_errors": self._mirror_errors,
                "mirror_success": self._mirror_success,
                "last_error": self._last_error,
            }

    @property
    def strict_mode(self) -> bool:
        with self._status_lock:
            return bool(self._strict)

    def set_strict_mode(self, strict: bool) -> Dict[str, Any]:
        with self._status_lock:
            self._strict = bool(strict)
            return {
                "enabled": True,
                "strict": self._strict,
                "mirror_errors": self._mirror_errors,
                "mirror_success": self._mirror_success,
                "last_error": self._last_error,
            }

    def _mirror_call(self, op: str, fn: Callable[[], None]) -> None:
        try:
            fn()
            with self._status_lock:
                self._mirror_success += 1
        except Exception as exc:
            with self._status_lock:
                self._mirror_errors += 1
                self._last_error = f"{op}: {exc}"
            if self._strict:
                raise

    def __getattr__(self, name: str) -> Any:
        return getattr(self._primary, name)

    def insert_job(self, *args: Any, **kwargs: Any) -> int:
        job_id = int(self._primary.insert_job(*args, **kwargs))
        row = self._primary.get_job(job_id)
        if isinstance(row, dict):
            self._mirror_call("insert_job", lambda: self._mirror.upsert_job(row))
        return job_id

    def update_job_jd_text(self, *args: Any, **kwargs: Any) -> bool:
        updated = bool(self._primary.update_job_jd_text(*args, **kwargs))
        if updated:
            job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
            row = self._primary.get_job(job_id)
            if isinstance(row, dict):
                self._mirror_call("update_job_jd_text", lambda: self._mirror.upsert_job(row))
        return updated

    def upsert_job_culture_profile(self, *args: Any, **kwargs: Any) -> None:
        self._primary.upsert_job_culture_profile(*args, **kwargs)
        job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
        row = self._primary.get_job_culture_profile(job_id)
        if isinstance(row, dict):
            self._mirror_call("upsert_job_culture_profile", lambda: self._mirror.upsert_job_culture_profile(row))

    def upsert_candidate(self, *args: Any, **kwargs: Any) -> int:
        candidate_id = int(self._primary.upsert_candidate(*args, **kwargs))
        row = self._primary.get_candidate(candidate_id)
        if isinstance(row, dict):
            self._mirror_call("upsert_candidate", lambda: self._mirror.upsert_candidate(row))
        return candidate_id

    def create_candidate_match(self, *args: Any, **kwargs: Any) -> None:
        self._primary.create_candidate_match(*args, **kwargs)
        job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
        candidate_id = int(kwargs.get("candidate_id") if "candidate_id" in kwargs else args[1])
        row = self._primary.get_candidate_match(job_id, candidate_id)
        if isinstance(row, dict):
            self._mirror_call("create_candidate_match", lambda: self._mirror.upsert_candidate_match(row))

    def update_candidate_match_status(self, *args: Any, **kwargs: Any) -> bool:
        updated = bool(self._primary.update_candidate_match_status(*args, **kwargs))
        if updated:
            job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
            candidate_id = int(kwargs.get("candidate_id") if "candidate_id" in kwargs else args[1])
            row = self._primary.get_candidate_match(job_id, candidate_id)
            if isinstance(row, dict):
                self._mirror_call("update_candidate_match_status", lambda: self._mirror.upsert_candidate_match(row))
        return updated

    def create_conversation(self, *args: Any, **kwargs: Any) -> int:
        conversation_id = int(self._primary.create_conversation(*args, **kwargs))
        row = self._primary.get_conversation(conversation_id)
        if isinstance(row, dict):
            self._mirror_call("create_conversation", lambda: self._mirror.upsert_conversation(row))
        return conversation_id

    def get_or_create_conversation(self, *args: Any, **kwargs: Any) -> int:
        conversation_id = int(self._primary.get_or_create_conversation(*args, **kwargs))
        row = self._primary.get_conversation(conversation_id)
        if isinstance(row, dict):
            self._mirror_call("get_or_create_conversation", lambda: self._mirror.upsert_conversation(row))
        return conversation_id

    def set_conversation_external_chat_id(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        out = self._primary.set_conversation_external_chat_id(*args, **kwargs)
        conversation_id = int(kwargs.get("conversation_id") if "conversation_id" in kwargs else args[0])
        row = self._primary.get_conversation(conversation_id)
        if isinstance(row, dict):
            self._mirror_call("set_conversation_external_chat_id", lambda: self._mirror.upsert_conversation(row))
        return out

    def update_conversation_status(self, *args: Any, **kwargs: Any) -> bool:
        updated = bool(self._primary.update_conversation_status(*args, **kwargs))
        if updated:
            conversation_id = int(kwargs.get("conversation_id") if "conversation_id" in kwargs else args[0])
            row = self._primary.get_conversation(conversation_id)
            if isinstance(row, dict):
                self._mirror_call("update_conversation_status", lambda: self._mirror.upsert_conversation(row))
        return updated

    def set_conversation_linkedin_account(self, *args: Any, **kwargs: Any) -> bool:
        updated = bool(self._primary.set_conversation_linkedin_account(*args, **kwargs))
        if updated:
            conversation_id = int(kwargs.get("conversation_id") if "conversation_id" in kwargs else args[0])
            row = self._primary.get_conversation(conversation_id)
            if isinstance(row, dict):
                self._mirror_call("set_conversation_linkedin_account", lambda: self._mirror.upsert_conversation(row))
        return updated

    def add_message(self, *args: Any, **kwargs: Any) -> int:
        message_id = int(self._primary.add_message(*args, **kwargs))
        conversation_id = int(kwargs.get("conversation_id") if "conversation_id" in kwargs else args[0])
        msg_row = self._select_by_id("messages", message_id)
        conv_row = self._primary.get_conversation(conversation_id)
        if isinstance(msg_row, dict):
            self._mirror_call("add_message", lambda: self._mirror.upsert_message(msg_row))
        if isinstance(conv_row, dict):
            self._mirror_call("add_message_conversation", lambda: self._mirror.upsert_conversation(conv_row))
        return message_id

    def log_operation(self, *args: Any, **kwargs: Any) -> None:
        self._primary.log_operation(*args, **kwargs)
        operation = kwargs.get("operation") if "operation" in kwargs else (args[0] if len(args) > 0 else "")
        status = kwargs.get("status") if "status" in kwargs else (args[1] if len(args) > 1 else "ok")
        entity_type = kwargs.get("entity_type") if "entity_type" in kwargs else (args[2] if len(args) > 2 else None)
        entity_id = kwargs.get("entity_id") if "entity_id" in kwargs else (args[3] if len(args) > 3 else None)
        details = kwargs.get("details") if "details" in kwargs else (args[4] if len(args) > 4 else None)
        payload = {
            "operation": operation,
            "status": status,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "details": details or {},
            "created_at": utc_now_iso(),
        }
        self._mirror_call("log_operation", lambda: self._mirror.insert_operation_log(payload))

    def upsert_pre_resume_session(self, *args: Any, **kwargs: Any) -> None:
        self._primary.upsert_pre_resume_session(*args, **kwargs)
        session_id = str(kwargs.get("session_id") if "session_id" in kwargs else args[0])
        row = self._primary.get_pre_resume_session(session_id)
        if isinstance(row, dict):
            self._mirror_call("upsert_pre_resume_session", lambda: self._mirror.upsert_pre_resume_session(row))

    def insert_pre_resume_event(self, *args: Any, **kwargs: Any) -> int:
        event_id = int(self._primary.insert_pre_resume_event(*args, **kwargs))
        row = self._select_by_id("pre_resume_events", event_id)
        if isinstance(row, dict):
            self._mirror_call("insert_pre_resume_event", lambda: self._mirror.upsert_pre_resume_event(row))
        return event_id

    def record_webhook_event(self, *args: Any, **kwargs: Any) -> bool:
        created = bool(self._primary.record_webhook_event(*args, **kwargs))
        if created:
            event_key = kwargs.get("event_key") if "event_key" in kwargs else (args[0] if len(args) > 0 else "")
            source = kwargs.get("source") if "source" in kwargs else (args[1] if len(args) > 1 else "")
            payload = kwargs.get("payload") if "payload" in kwargs else (args[2] if len(args) > 2 else None)
            self._mirror_call(
                "record_webhook_event",
                lambda: self._mirror.insert_webhook_event(event_key=str(event_key), source=str(source), payload=payload),
            )
        return created

    def upsert_job_step_progress(self, *args: Any, **kwargs: Any) -> None:
        self._primary.upsert_job_step_progress(*args, **kwargs)
        job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
        step = str(kwargs.get("step") if "step" in kwargs else args[1])
        rows = self._primary.list_job_step_progress(job_id)
        row = next((item for item in rows if str(item.get("step") or "") == step), None)
        if isinstance(row, dict):
            self._mirror_call("upsert_job_step_progress", lambda: self._mirror.upsert_job_step_progress(row))

    def upsert_candidate_agent_assessment(self, *args: Any, **kwargs: Any) -> None:
        self._primary.upsert_candidate_agent_assessment(*args, **kwargs)
        job_id = int(kwargs.get("job_id") if "job_id" in kwargs else args[0])
        candidate_id = int(kwargs.get("candidate_id") if "candidate_id" in kwargs else args[1])
        agent_key = str(kwargs.get("agent_key") if "agent_key" in kwargs else args[2])
        stage_key = str(kwargs.get("stage_key") if "stage_key" in kwargs else args[4])
        rows = self._primary.list_candidate_assessments(candidate_id=candidate_id, job_id=job_id)
        row = next(
            (
                item
                for item in rows
                if str(item.get("agent_key") or "") == agent_key and str(item.get("stage_key") or "") == stage_key
            ),
            None,
        )
        if isinstance(row, dict):
            self._mirror_call(
                "upsert_candidate_agent_assessment",
                lambda: self._mirror.upsert_candidate_agent_assessment(row),
            )

    def upsert_candidate_signal(self, *args: Any, **kwargs: Any) -> int:
        signal_id = int(self._primary.upsert_candidate_signal(*args, **kwargs))
        row = self._select_by_id("candidate_signals", signal_id)
        if isinstance(row, dict):
            self._mirror_call(
                "upsert_candidate_signal",
                lambda: self._mirror.upsert_candidate_signal(row),
            )
        return signal_id

    def _select_by_id(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        conn = getattr(self._primary, "_conn", None)
        if conn is None:
            return None
        if table_name not in {"messages", "pre_resume_events", "candidate_signals"}:
            return None
        row = conn.execute(f"SELECT * FROM {table_name} WHERE id = ?", (int(row_id),)).fetchone()
        if row is None:
            return None
        row_to_dict = getattr(self._primary, "_row_to_dict", None)
        if callable(row_to_dict):
            return row_to_dict(row)
        return dict(row)
