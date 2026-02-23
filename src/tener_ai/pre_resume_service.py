from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .language import detect_language_from_text


UTC = timezone.utc
TERMINAL_STATUSES = {"resume_received", "not_interested", "unreachable", "stalled"}


DEFAULT_TEMPLATES: Dict[str, Any] = {
    "default_language": "en",
    "intro": {
        "en": (
            "Hi {name}, thanks for your interest in \"{job_title}\". "
            "Before final matching, could you share your latest CV/resume? "
            "Core role focus: {core_profile_summary}."
        ),
        "es": (
            "Hola {name}, gracias por tu interes en \"{job_title}\". "
            "Antes del matching final, puedes compartir tu CV actualizado? "
            "Foco del rol: {core_profile_summary}."
        ),
    },
    "resume_cta": {
        "en": "Please share your CV/resume (file or link) so we can move to final verification.",
        "es": "Comparte tu CV (archivo o link) para pasar a la verificacion final.",
    },
    "resume_ack": {
        "en": "Great, CV received. We are moving to final verification now.",
        "es": "Perfecto, CV recibido. Pasamos a la verificacion final.",
    },
    "not_interested_ack": {
        "en": "Understood, thanks for the reply. We will not send further messages.",
        "es": "Entendido, gracias por responder. No enviaremos mas mensajes.",
    },
    "resume_promised_ack": {
        "en": "Thanks, noted. I will wait for your CV and send one reminder if needed.",
        "es": "Gracias, anotado. Espero tu CV y enviaremos un recordatorio si hace falta.",
    },
    "followups": {
        "1": {
            "en": "Quick follow-up on \"{job_title}\": could you share your CV/resume to continue?",
            "es": "Seguimiento rapido sobre \"{job_title}\": puedes compartir tu CV para continuar?",
        },
        "2": {
            "en": "Second follow-up: if you are interested, please send your CV/resume and we will fast-track.",
            "es": "Segundo seguimiento: si te interesa, comparte tu CV y avanzamos rapido.",
        },
        "3": {
            "en": "Final reminder for \"{job_title}\": send your CV/resume to proceed.",
            "es": "Ultimo recordatorio para \"{job_title}\": envia tu CV para continuar.",
        },
    },
    "intent_answers": {
        "salary": {
            "en": "Compensation depends on scope fit and seniority. If you share expectations, we can confirm range quickly.",
            "es": "La compensacion depende del encaje y seniority. Si compartes expectativas, confirmamos rango rapido.",
        },
        "stack": {
            "en": "Main stack and responsibilities are aligned with the role core profile we shared.",
            "es": "El stack y responsabilidades se alinean con el perfil core que compartimos.",
        },
        "timeline": {
            "en": "Process is active now, and we can move quickly once we receive your CV.",
            "es": "El proceso esta activo y podemos avanzar rapido cuando recibamos tu CV.",
        },
        "send_jd_first": {
            "en": "Sure, I can share role details first. To finalize screening after that, we still need your latest CV.",
            "es": "Claro, puedo compartir detalles primero. Para cerrar screening despues, necesitamos tu CV.",
        },
        "default": {
            "en": "Thanks for the message. I can clarify details and next steps.",
            "es": "Gracias por tu mensaje. Puedo aclarar detalles y siguientes pasos.",
        },
    },
}


def merge_template_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_template_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def parse_resume_links(text: str) -> List[str]:
    links = re.findall(r"https?://[^\s)>\"]+", text or "", flags=re.IGNORECASE)
    selected: List[str] = []
    for link in links:
        lowered = link.lower()
        if any(marker in lowered for marker in ("resume", "cv", ".pdf", ".doc", ".docx", "drive.", "dropbox", "notion.")):
            selected.append(link)
    return selected


@dataclass
class PreResumeSession:
    session_id: str
    candidate_name: str
    job_title: str
    scope_summary: str
    core_profile_summary: str
    language: str
    status: str = "awaiting_reply"
    followups_sent: int = 0
    turns: int = 0
    last_intent: str = "started"
    last_error: Optional[str] = None
    resume_links: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: iso(utc_now()))
    updated_at: str = field(default_factory=lambda: iso(utc_now()))
    next_followup_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "candidate_name": self.candidate_name,
            "job_title": self.job_title,
            "scope_summary": self.scope_summary,
            "core_profile_summary": self.core_profile_summary,
            "language": self.language,
            "status": self.status,
            "followups_sent": self.followups_sent,
            "turns": self.turns,
            "last_intent": self.last_intent,
            "last_error": self.last_error,
            "resume_links": list(self.resume_links),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_followup_at": self.next_followup_at,
        }

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "PreResumeSession":
        return PreResumeSession(
            session_id=str(payload.get("session_id") or ""),
            candidate_name=str(payload.get("candidate_name") or "there"),
            job_title=str(payload.get("job_title") or "this role"),
            scope_summary=str(payload.get("scope_summary") or "role scope"),
            core_profile_summary=str(payload.get("core_profile_summary") or payload.get("scope_summary") or "role scope"),
            language=str(payload.get("language") or "en"),
            status=str(payload.get("status") or "awaiting_reply"),
            followups_sent=int(payload.get("followups_sent") or 0),
            turns=int(payload.get("turns") or 0),
            last_intent=str(payload.get("last_intent") or "started"),
            last_error=str(payload.get("last_error")) if payload.get("last_error") is not None else None,
            resume_links=list(payload.get("resume_links") or []),
            created_at=str(payload.get("created_at") or iso(utc_now())),
            updated_at=str(payload.get("updated_at") or iso(utc_now())),
            next_followup_at=str(payload.get("next_followup_at")) if payload.get("next_followup_at") else None,
        )


class PreResumeCommunicationService:
    def __init__(
        self,
        templates_path: Optional[str] = None,
        max_followups: int = 3,
        followup_delays_hours: Optional[List[int]] = None,
        instruction: str = "",
    ) -> None:
        self.templates = self._load_templates(templates_path)
        self.max_followups = max(1, int(max_followups))
        self.followup_delays_hours = followup_delays_hours or [48, 72, 72]
        self.instruction = instruction
        self.sessions: Dict[str, PreResumeSession] = {}

    def start_session(
        self,
        session_id: str,
        candidate_name: str,
        job_title: str,
        scope_summary: str,
        core_profile_summary: Optional[str] = None,
        language: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        if session_id in self.sessions:
            raise ValueError(f"Session {session_id} already exists")
        current = now or utc_now()
        selected_language = (language or "").strip().lower() or self.templates.get("default_language", "en")
        session = PreResumeSession(
            session_id=session_id,
            candidate_name=candidate_name or "there",
            job_title=job_title or "this role",
            scope_summary=scope_summary or "role scope",
            core_profile_summary=core_profile_summary or scope_summary or "role scope",
            language=selected_language,
        )
        session.next_followup_at = self._next_followup_at(session=session, now=current)
        session.updated_at = iso(current)
        self.sessions[session_id] = session

        outbound = self._render("intro", session.language, session)
        return {
            "event": "session_started",
            "outbound": outbound,
            "state": session.to_dict(),
        }

    def seed_session(self, state: Dict[str, Any]) -> Dict[str, Any]:
        session = PreResumeSession.from_dict(state)
        if not session.session_id:
            raise ValueError("session_id is required in state")
        self.sessions[session.session_id] = session
        return session.to_dict()

    def handle_inbound(self, session_id: str, text: str, now: Optional[datetime] = None) -> Dict[str, Any]:
        session = self._require_session(session_id)
        current = now or utc_now()
        message = text or ""

        if session.status in TERMINAL_STATUSES:
            return {
                "event": "ignored_terminal",
                "intent": "none",
                "outbound": None,
                "state": session.to_dict(),
            }

        if not session.language or session.language == "auto":
            session.language = detect_language_from_text(message, fallback=self.templates.get("default_language", "en"))

        intent, links = self._classify_intent(message)
        for link in links:
            if link not in session.resume_links:
                session.resume_links.append(link)

        session.turns += 1
        session.last_intent = intent
        outbound: Optional[str]

        if intent == "resume_shared":
            session.status = "resume_received"
            session.next_followup_at = None
            outbound = self._render("resume_ack", session.language, session)
        elif intent == "not_interested":
            session.status = "not_interested"
            session.next_followup_at = None
            outbound = self._render("not_interested_ack", session.language, session)
        elif intent == "will_send_later":
            session.status = "resume_promised"
            session.next_followup_at = self._next_followup_at(session=session, now=current)
            outbound = self._render("resume_promised_ack", session.language, session)
        else:
            session.status = "engaged_no_resume"
            session.next_followup_at = self._next_followup_at(session=session, now=current)
            answer = self._render_intent_answer(intent=intent, session=session)
            cta = self._render("resume_cta", session.language, session)
            outbound = f"{answer} {cta}".strip()

        session.updated_at = iso(current)
        return {
            "event": "inbound_processed",
            "intent": intent,
            "resume_links": list(links),
            "outbound": outbound,
            "state": session.to_dict(),
        }

    def build_followup(self, session_id: str, now: Optional[datetime] = None) -> Dict[str, Any]:
        session = self._require_session(session_id)
        current = now or utc_now()

        if session.status in TERMINAL_STATUSES:
            return {
                "event": "followup_skipped",
                "sent": False,
                "reason": "terminal_status",
                "state": session.to_dict(),
            }

        if session.followups_sent >= self.max_followups:
            session.status = "stalled"
            session.next_followup_at = None
            session.updated_at = iso(current)
            return {
                "event": "followup_skipped",
                "sent": False,
                "reason": "max_followups_reached",
                "state": session.to_dict(),
            }

        followup_number = session.followups_sent + 1
        outbound = self._render_followup(followup_number=followup_number, session=session)
        session.followups_sent = followup_number
        session.status = "awaiting_reply"
        session.updated_at = iso(current)
        if session.followups_sent >= self.max_followups:
            session.next_followup_at = None
        else:
            session.next_followup_at = self._next_followup_at(session=session, now=current)

        return {
            "event": "followup_sent",
            "sent": True,
            "followup_number": followup_number,
            "outbound": outbound,
            "state": session.to_dict(),
        }

    def mark_unreachable(self, session_id: str, error: str, now: Optional[datetime] = None) -> Dict[str, Any]:
        session = self._require_session(session_id)
        current = now or utc_now()
        session.status = "unreachable"
        session.last_error = error
        session.next_followup_at = None
        session.updated_at = iso(current)
        return {
            "event": "session_unreachable",
            "state": session.to_dict(),
        }

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = self.sessions.get(session_id)
        return session.to_dict() if session else None

    def _classify_intent(self, text: str) -> Tuple[str, List[str]]:
        normalized = (text or "").strip()
        lowered = normalized.lower()
        links = parse_resume_links(normalized)

        if links:
            return "resume_shared", links
        if any(marker in lowered for marker in ("my cv", "my resume", "attached cv", "attached resume", "here is resume")):
            return "resume_shared", links

        if any(marker in lowered for marker in ("not interested", "no thanks", "stop", "unsubscribe", "not looking")):
            return "not_interested", links

        if any(marker in lowered for marker in ("will send", "send later", "tomorrow", "next week", "later")):
            return "will_send_later", links

        salary_markers = ("salary", "compensation", "pay", "range")
        stack_markers = ("stack", "technology", "tech", "tools", "requirements")
        timeline_markers = ("timeline", "process", "interview", "steps", "when")
        details_markers = ("send jd", "job description", "details first", "share details", "more details")

        if any(marker in lowered for marker in salary_markers):
            return "salary", links
        if any(marker in lowered for marker in stack_markers):
            return "stack", links
        if any(marker in lowered for marker in timeline_markers):
            return "timeline", links
        if any(marker in lowered for marker in details_markers):
            return "send_jd_first", links
        return "default", links

    def _render(self, group: str, language: str, session: PreResumeSession) -> str:
        block = self.templates.get(group, {})
        template = self._pick_template(block, language, fallback="{scope_summary}")
        return template.format(
            name=session.candidate_name,
            job_title=session.job_title,
            scope_summary=session.scope_summary,
            core_profile_summary=session.core_profile_summary,
        )

    def _render_intent_answer(self, intent: str, session: PreResumeSession) -> str:
        answers = self.templates.get("intent_answers", {})
        block = answers.get(intent) or answers.get("default", {})
        language = session.language
        template = self._pick_template(block, language, fallback="")
        return template.format(
            name=session.candidate_name,
            job_title=session.job_title,
            scope_summary=session.scope_summary,
            core_profile_summary=session.core_profile_summary,
        )

    def _render_followup(self, followup_number: int, session: PreResumeSession) -> str:
        followups = self.templates.get("followups", {})
        key = str(followup_number)
        block = followups.get(key) or followups.get(str(self.max_followups), {})
        language = session.language
        template = self._pick_template(block, language, fallback="Please share your CV/resume.")
        return template.format(
            name=session.candidate_name,
            job_title=session.job_title,
            scope_summary=session.scope_summary,
            core_profile_summary=session.core_profile_summary,
        )

    def _pick_template(self, block: Dict[str, Any], language: str, fallback: str) -> str:
        if not isinstance(block, dict) or not block:
            return fallback
        selected = block.get(language) or block.get(self.templates.get("default_language", "en"))
        if isinstance(selected, str):
            return selected
        for value in block.values():
            if isinstance(value, str):
                return value
        return fallback

    def _next_followup_at(self, session: PreResumeSession, now: datetime) -> Optional[str]:
        if session.status in TERMINAL_STATUSES:
            return None
        if session.followups_sent >= self.max_followups:
            return None
        index = min(session.followups_sent, len(self.followup_delays_hours) - 1)
        delay = max(1, int(self.followup_delays_hours[index]))
        return iso(now + timedelta(hours=delay))

    def _require_session(self, session_id: str) -> PreResumeSession:
        session = self.sessions.get(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        return session

    @staticmethod
    def _load_templates(path: Optional[str]) -> Dict[str, Any]:
        if not path:
            return dict(DEFAULT_TEMPLATES)
        file_path = Path(path)
        if not file_path.exists():
            return dict(DEFAULT_TEMPLATES)
        with file_path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            return dict(DEFAULT_TEMPLATES)
        return merge_template_dict(DEFAULT_TEMPLATES, loaded)
