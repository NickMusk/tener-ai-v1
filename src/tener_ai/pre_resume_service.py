from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .language import normalize_language, resolve_conversation_language
from .message_extraction import (
    CandidateMessageExtractionResult,
    classify_pre_resume_intent,
    extract_pre_resume_heuristic_fields,
    parse_resume_links,
)
from .prescreen_policy import PrescreenPolicy, collapse_salary_range_to_expectation


UTC = timezone.utc
TERMINAL_STATUSES = {
    "ready_for_interview",
    "ready_for_screening_call",
    "not_interested",
    "closed_not_fit",
    "unreachable",
    "stalled",
    "delivery_blocked_identity",
}
REMOTE_LOCATION_MARKERS = {"remote", "global", "worldwide", "anywhere", "hybrid", "distributed"}


DEFAULT_TEMPLATES: Dict[str, Any] = {
    "default_language": "en",
    "intro": {
        "en": (
            "Hi {name}, thanks for your interest in \"{job_title}\". First we'll ask a few written qualifying questions, "
            "then request your CV, then send a short async interview. Core role focus: {core_profile_summary}."
        ),
        "uk": (
            "Привіт, {name}! Дякую за інтерес до \"{job_title}\". Спочатку поставимо кілька письмових кваліфікаційних запитань, "
            "потім попросимо ваше CV, а далі надішлемо коротке async interview. Основний фокус ролі: {core_profile_summary}."
        ),
        "es": (
            "Hola {name}, gracias por tu interes en \"{job_title}\". Primero haremos unas preguntas escritas de filtro, "
            "luego pediremos tu CV y despues enviaremos una entrevista asincrona corta. "
            "Foco del rol: {core_profile_summary}."
        ),
    },
    "written_questions_intro": {
        "en": "Please reply in one message if possible:",
        "uk": "Будь ласка, по можливості дайте відповідь одним повідомленням:",
        "es": "Si puedes, responde en un solo mensaje:",
    },
    "cv_request": {
        "en": "Thanks, the written qualifying questions are complete. Please share your CV/resume so we can send the async interview link.",
        "uk": "Дякую, письмові кваліфікаційні запитання завершено. Будь ласка, надішліть ваше CV/резюме, щоб ми могли надіслати посилання на async interview.",
        "es": "Gracias, las preguntas escritas ya estan completas. Comparte tu CV para que podamos enviar el enlace de la entrevista asincrona.",
    },
    "cv_received_pending": {
        "en": "Thanks, CV received. Before I send the async interview link, I still need a few written qualifying answers:",
        "uk": "Дякую, CV отримано. Перш ніж надіслати посилання на async interview, мені ще потрібні кілька письмових кваліфікаційних відповідей:",
        "es": "Gracias, CV recibido. Antes de enviar el enlace de la entrevista asincrona, aun necesito algunas respuestas escritas:",
    },
    "screening_call_ready": {
        "en": "Thanks, I have your CV and the written qualifying questions are complete. The next step is a short async interview.",
        "uk": "Дякую, я вже маю ваше CV, і письмові кваліфікаційні запитання завершено. Наступний крок - коротке async interview.",
        "es": "Gracias, ya tengo tu CV y las preguntas escritas estan completas. El siguiente paso es una entrevista asincrona corta.",
    },
    "not_interested_ack": {
        "en": "Understood, thanks for the reply. We will not send further messages.",
        "uk": "Зрозуміло, дякую за відповідь. Ми більше не надсилатимемо повідомлення.",
        "es": "Entendido, gracias por responder. No enviaremos mas mensajes.",
    },
    "resume_promised_ack": {
        "en": "Thanks, noted. You can send your CV anytime, and I will send one reminder if needed.",
        "uk": "Дякую, зафіксував. Ви можете надіслати своє CV у будь-який момент, і за потреби я надішлю одне нагадування.",
        "es": "Gracias, anotado. Puedes enviar tu CV en cualquier momento y mandare un recordatorio si hace falta.",
    },
    "followups": {
        "1": {
            "en": "Quick follow-up on \"{job_title}\". We still need the written qualifying answers and then your CV before the async interview.",
            "uk": "Швидкий follow-up щодо \"{job_title}\". Нам усе ще потрібні письмові кваліфікаційні відповіді, а потім ваше CV перед async interview.",
            "es": "Seguimiento rapido sobre \"{job_title}\". Aun necesitamos las respuestas escritas y luego tu CV antes de la entrevista asincrona.",
        },
        "2": {
            "en": "Second follow-up for \"{job_title}\". Once the written qualifying questions are complete, please share your CV so we can move quickly.",
            "uk": "Другий follow-up щодо \"{job_title}\". Щойно письмові кваліфікаційні запитання будуть завершені, будь ласка, надішліть своє CV, щоб ми могли рухатися швидко.",
            "es": "Segundo seguimiento para \"{job_title}\". Cuando completemos las preguntas escritas, comparte tu CV para avanzar rapido.",
        },
        "3": {
            "en": "Final reminder for \"{job_title}\". Written qualifying questions first, then CV, then the async interview.",
            "uk": "Останнє нагадування щодо \"{job_title}\". Спочатку письмові кваліфікаційні запитання, потім CV, а далі async interview.",
            "es": "Ultimo recordatorio para \"{job_title}\". Primero preguntas escritas, luego CV y despues la entrevista asincrona.",
        },
    },
    "intent_answers": {
        "salary": {
            "en": "Compensation is checked against role fit and budget. If you share your expected gross monthly salary, I can confirm alignment quickly.",
            "uk": "Компенсація перевіряється щодо відповідності ролі та бюджету. Якщо поділитеся вашим очікуваним gross monthly salary, я швидко підтверджу, чи є узгодження.",
            "es": "La compensacion se revisa contra el encaje y el presupuesto. Si compartes tu expected gross monthly salary, puedo confirmar rapido si hay alineacion.",
        },
        "stack": {
            "en": "Main stack and responsibilities are aligned with the role core profile we shared.",
            "uk": "Основний стек і відповідальність узгоджені з core profile ролі, яким ми поділилися.",
            "es": "El stack principal y las responsabilidades estan alineados con el perfil core que compartimos.",
        },
        "timeline": {
            "en": "The process is written qualifying questions first, then CV, then a short async interview.",
            "uk": "Процес такий: спочатку письмові кваліфікаційні запитання, потім CV, а далі коротке async interview.",
            "es": "El proceso es primero preguntas escritas, luego CV y despues una entrevista asincrona corta.",
        },
        "send_jd_first": {
            "en": "Sure, I can share role details first. The process still stays: written qualifying questions, then CV, then a short async interview.",
            "uk": "Звісно, спочатку можу поділитися деталями ролі. Але процес залишається таким самим: письмові кваліфікаційні запитання, потім CV, а далі коротке async interview.",
            "es": "Claro, puedo compartir detalles primero. El proceso sigue siendo: preguntas escritas, luego CV y despues una entrevista asincrona corta.",
        },
        "default": {
            "en": "Thanks for the message. I can clarify details and next steps.",
            "uk": "Дякую за повідомлення. Можу уточнити деталі та наступні кроки.",
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


def _coerce_boolish(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "si", "sí"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _normalize_session_status(value: Any) -> str:
    status = str(value or "awaiting_reply").strip().lower() or "awaiting_reply"
    if status == "ready_for_screening_call":
        return "ready_for_interview"
    return status


@dataclass
class PreResumeSession:
    session_id: str
    candidate_name: str
    job_title: str
    scope_summary: str
    core_profile_summary: str
    language: str
    job_location: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    salary_currency: Optional[str] = None
    work_authorization_required: bool = False
    status: str = "awaiting_reply"
    followups_sent: int = 0
    turns: int = 0
    last_intent: str = "started"
    last_error: Optional[str] = None
    resume_links: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: iso(utc_now()))
    updated_at: str = field(default_factory=lambda: iso(utc_now()))
    next_followup_at: Optional[str] = None
    must_have_answer: Optional[str] = None
    salary_expectation_gross_monthly: Optional[float] = None
    salary_expectation_currency: Optional[str] = None
    location_confirmed: Optional[bool] = None
    work_authorization_confirmed: Optional[bool] = None
    cv_received: bool = False

    def location_confirmation_required(self) -> bool:
        return PrescreenPolicy.location_confirmation_required(self.job_location)

    def auth_confirmation_required(self) -> bool:
        return PrescreenPolicy.auth_confirmation_required(self.work_authorization_required)

    def prescreen_status(self) -> str:
        return PrescreenPolicy.prescreen_status(
            cv_received=self.cv_received,
            must_have_answer=self.must_have_answer,
            salary_expectation_gross_monthly=self.salary_expectation_gross_monthly,
            job_location=self.job_location,
            location_confirmed=self.location_confirmed,
            work_authorization_required=self.work_authorization_required,
            work_authorization_confirmed=self.work_authorization_confirmed,
        )

    def written_answers_complete(self) -> bool:
        return PrescreenPolicy.written_answers_complete(
            must_have_answer=self.must_have_answer,
            salary_expectation_gross_monthly=self.salary_expectation_gross_monthly,
            job_location=self.job_location,
            location_confirmed=self.location_confirmed,
            work_authorization_required=self.work_authorization_required,
            work_authorization_confirmed=self.work_authorization_confirmed,
        )

    def missing_question_keys(self) -> List[str]:
        return PrescreenPolicy.missing_question_keys(
            must_have_answer=self.must_have_answer,
            salary_expectation_gross_monthly=self.salary_expectation_gross_monthly,
            job_location=self.job_location,
            location_confirmed=self.location_confirmed,
            work_authorization_required=self.work_authorization_required,
            work_authorization_confirmed=self.work_authorization_confirmed,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "candidate_name": self.candidate_name,
            "job_title": self.job_title,
            "scope_summary": self.scope_summary,
            "core_profile_summary": self.core_profile_summary,
            "language": self.language,
            "job_location": self.job_location,
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "salary_currency": self.salary_currency,
            "work_authorization_required": self.work_authorization_required,
            "status": self.status,
            "prescreen_status": self.prescreen_status(),
            "followups_sent": self.followups_sent,
            "turns": self.turns,
            "last_intent": self.last_intent,
            "last_error": self.last_error,
            "resume_links": list(self.resume_links),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_followup_at": self.next_followup_at,
            "must_have_answer": self.must_have_answer,
            "salary_expectation_gross_monthly": self.salary_expectation_gross_monthly,
            "salary_expectation_currency": self.salary_expectation_currency,
            "location_confirmed": self.location_confirmed,
            "work_authorization_confirmed": self.work_authorization_confirmed,
            "cv_received": self.cv_received,
            "required_question_keys": self.missing_question_keys(),
        }

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "PreResumeSession":
        resume_links = list(payload.get("resume_links") or [])
        cv_received = _coerce_boolish(payload.get("cv_received"))
        return PreResumeSession(
            session_id=str(payload.get("session_id") or ""),
            candidate_name=str(payload.get("candidate_name") or "there"),
            job_title=str(payload.get("job_title") or "this role"),
            scope_summary=str(payload.get("scope_summary") or "role scope"),
            core_profile_summary=str(payload.get("core_profile_summary") or payload.get("scope_summary") or "role scope"),
            language=normalize_language(str(payload.get("language") or "en"), fallback="en"),
            job_location=str(payload.get("job_location") or "").strip() or None,
            salary_min=float(payload.get("salary_min")) if payload.get("salary_min") is not None else None,
            salary_max=float(payload.get("salary_max")) if payload.get("salary_max") is not None else None,
            salary_currency=str(payload.get("salary_currency") or "").strip().upper() or None,
            work_authorization_required=bool(_coerce_boolish(payload.get("work_authorization_required"))),
            status=_normalize_session_status(payload.get("status")),
            followups_sent=int(payload.get("followups_sent") or 0),
            turns=int(payload.get("turns") or 0),
            last_intent=str(payload.get("last_intent") or "started"),
            last_error=str(payload.get("last_error")) if payload.get("last_error") is not None else None,
            resume_links=resume_links,
            created_at=str(payload.get("created_at") or iso(utc_now())),
            updated_at=str(payload.get("updated_at") or iso(utc_now())),
            next_followup_at=str(payload.get("next_followup_at")) if payload.get("next_followup_at") else None,
            must_have_answer=str(payload.get("must_have_answer") or "").strip() or None,
            salary_expectation_gross_monthly=collapse_salary_range_to_expectation(
                payload.get("salary_expectation_gross_monthly"),
                payload.get("salary_expectation_gross_monthly"),
            )
            if payload.get("salary_expectation_gross_monthly") is not None
            else collapse_salary_range_to_expectation(
                payload.get("salary_expectation_min"),
                payload.get("salary_expectation_max"),
            ),
            salary_expectation_currency=str(payload.get("salary_expectation_currency") or "").strip().upper() or None,
            location_confirmed=_coerce_boolish(payload.get("location_confirmed")),
            work_authorization_confirmed=_coerce_boolish(payload.get("work_authorization_confirmed")),
            cv_received=bool(cv_received) if cv_received is not None else bool(resume_links),
        )


class PreResumeCommunicationService:
    def __init__(
        self,
        templates_path: Optional[str] = None,
        max_followups: int = 3,
        followup_delays_hours: Optional[List[float]] = None,
        instruction: str = "",
    ) -> None:
        self.templates = self._load_templates(templates_path)
        self.max_followups = max(1, int(max_followups))
        self.followup_delays_hours = [float(x) for x in (followup_delays_hours or [48, 72, 72])]
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
        job_location: Optional[str] = None,
        salary_min: Optional[float] = None,
        salary_max: Optional[float] = None,
        salary_currency: Optional[str] = None,
        work_authorization_required: bool = False,
    ) -> Dict[str, Any]:
        if session_id in self.sessions:
            raise ValueError(f"Session {session_id} already exists")
        current = now or utc_now()
        selected_language = normalize_language(language, fallback=self.templates.get("default_language", "en"))
        session = PreResumeSession(
            session_id=session_id,
            candidate_name=candidate_name or "there",
            job_title=job_title or "this role",
            scope_summary=scope_summary or "role scope",
            core_profile_summary=core_profile_summary or scope_summary or "role scope",
            language=selected_language,
            job_location=str(job_location or "").strip() or None,
            salary_min=float(salary_min) if salary_min is not None else None,
            salary_max=float(salary_max) if salary_max is not None else None,
            salary_currency=str(salary_currency or "").strip().upper() or None,
            work_authorization_required=bool(work_authorization_required),
        )
        session.next_followup_at = self._next_followup_at(session=session, now=current)
        session.updated_at = iso(current)
        self.sessions[session_id] = session

        outbound = self._compose_prompt(session=session, intro=True)
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

    def handle_inbound(
        self,
        session_id: str,
        text: str,
        now: Optional[datetime] = None,
        extraction: CandidateMessageExtractionResult | None = None,
    ) -> Dict[str, Any]:
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

        if extraction is not None:
            session.language = normalize_language(
                extraction.language,
                fallback=self.templates.get("default_language", "en"),
            )
            intent = str(extraction.intent or "default").strip().lower() or "default"
            links = list(extraction.resume_links or [])
        else:
            session.language = resolve_conversation_language(
                latest_message_text=message,
                previous_language=session.language,
                fallback=self.templates.get("default_language", "en"),
            )
            intent, links = self._classify_intent(message)
        for link in links:
            if link not in session.resume_links:
                session.resume_links.append(link)
        if links:
            session.cv_received = True
        if intent == "resume_shared" or bool(getattr(extraction, "resume_shared", False)):
            session.cv_received = True

        session.turns += 1
        session.last_intent = intent

        if extraction is not None:
            extracted = {
                "must_have_answer": extraction.must_have_answer,
                "salary_expectation_gross_monthly": extraction.salary_expectation_gross_monthly,
                "salary_expectation_currency": extraction.salary_expectation_currency,
                "location_confirmed": extraction.location_confirmed,
                "work_authorization_confirmed": extraction.work_authorization_confirmed,
            }
        else:
            extracted = self._extract_structured_answers(session=session, text=message, intent=intent)
        if extracted.get("must_have_answer"):
            session.must_have_answer = str(extracted["must_have_answer"])
        salary_value = extracted.get("salary_expectation_gross_monthly")
        if salary_value is not None:
            session.salary_expectation_gross_monthly = float(salary_value)
        salary_currency = extracted.get("salary_expectation_currency")
        if salary_currency:
            session.salary_expectation_currency = str(salary_currency)
        if extracted.get("location_confirmed") is not None:
            session.location_confirmed = bool(extracted["location_confirmed"])
        if extracted.get("work_authorization_confirmed") is not None:
            session.work_authorization_confirmed = bool(extracted["work_authorization_confirmed"])

        if intent == "not_interested":
            session.status = "not_interested"
            session.next_followup_at = None
            outbound = self._render("not_interested_ack", session.language, session)
        else:
            prescreen_status = session.prescreen_status()
            if prescreen_status == "ready_for_interview":
                session.status = "ready_for_interview"
                session.next_followup_at = None
            elif prescreen_status == "ready_for_cv":
                session.status = "ready_for_cv"
                session.next_followup_at = self._next_followup_at(session=session, now=current)
            elif prescreen_status == "cv_received_pending_answers":
                session.status = "cv_received_pending_answers"
                session.next_followup_at = self._next_followup_at(session=session, now=current)
            else:
                session.status = "resume_promised" if intent == "will_send_later" else "engaged_no_resume"
                session.next_followup_at = self._next_followup_at(session=session, now=current)
            outbound = self._compose_response(session=session, intent=intent)

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
        prompt = self._compose_prompt(session=session, intro=False, include_process=False)
        if prompt:
            outbound = f"{outbound} {prompt}".strip()
        session.followups_sent = followup_number
        session.updated_at = iso(current)
        if session.followups_sent >= self.max_followups:
            idx = min(max(int(self.max_followups) - 1, 0), len(self.followup_delays_hours) - 1)
            delay = float(self.followup_delays_hours[idx])
            session.next_followup_at = iso(current + timedelta(hours=delay))
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
        return classify_pre_resume_intent(text)

    def _extract_structured_answers(self, *, session: PreResumeSession, text: str, intent: str) -> Dict[str, Any]:
        return extract_pre_resume_heuristic_fields(
            text=text,
            intent=intent,
            job_location=session.job_location,
            work_authorization_required=session.auth_confirmation_required(),
            must_have_answer_exists=bool(session.must_have_answer),
            salary_currency=session.salary_expectation_currency or session.salary_currency,
        )

    def _compose_response(self, *, session: PreResumeSession, intent: str) -> str:
        if session.status == "ready_for_interview":
            return self._render("screening_call_ready", session.language, session)
        if intent == "will_send_later":
            ack = self._render("resume_promised_ack", session.language, session)
        else:
            ack = self._render_intent_answer(intent=intent, session=session)
        prompt = self._compose_prompt(session=session, intro=False)
        return f"{ack} {prompt}".strip() if prompt else ack

    def _compose_prompt(self, *, session: PreResumeSession, intro: bool, include_process: bool = True) -> str:
        missing = session.missing_question_keys()
        parts: List[str] = []
        if intro:
            parts.append(self._render("intro", session.language, session))
        if session.status == "ready_for_interview":
            parts.append(self._render("screening_call_ready", session.language, session))
            return " ".join(part for part in parts if part).strip()
        if session.cv_received and missing:
            parts.append(self._render("cv_received_pending", session.language, session))
        elif not intro and include_process and missing:
            parts.append(self._render("written_questions_intro", session.language, session))
        if missing:
            questions = self._build_question_prompts(session=session, keys=missing)
            if questions:
                parts.append(" ".join(questions))
        elif not session.cv_received:
            parts.append(self._render("cv_request", session.language, session))
        return " ".join(part for part in parts if part).strip()

    def _build_question_prompts(self, *, session: PreResumeSession, keys: List[str]) -> List[str]:
        prompts: List[str] = []
        question_number = 1
        for key in keys[:3]:
            if key == "must_have":
                prompts.append(
                    f"{question_number}) What hands-on experience do you have with {session.core_profile_summary or session.scope_summary}?"
                )
            elif key == "salary":
                budget = self._format_budget(session)
                if budget:
                    prompts.append(
                        f"{question_number}) What gross monthly salary are you targeting? Budget on our side is {budget}."
                    )
                else:
                    prompts.append(f"{question_number}) What gross monthly salary are you targeting?")
            elif key == "location_auth":
                prompts.append(f"{question_number}) {self._location_auth_question(session)}")
            question_number += 1
        return prompts

    def _location_auth_question(self, session: PreResumeSession) -> str:
        parts: List[str] = []
        if session.location_confirmation_required():
            location = str(session.job_location or "").strip()
            if location:
                parts.append(f"Are you based in or open to {location}?")
            else:
                parts.append("Can you confirm location alignment for this role?")
        if session.auth_confirmation_required():
            parts.append("Do you have the required work authorization for this role?")
        return " ".join(parts).strip()

    def _render(self, group: str, language: str, session: PreResumeSession) -> str:
        block = self.templates.get(group, {})
        if group == "cv_request" and not block:
            block = self.templates.get("resume_cta", {})
        if group == "screening_call_ready" and not block:
            block = self.templates.get("resume_ack", {})
        template = self._pick_template(block, language, fallback="")
        if not template:
            template = self._default_template_for_group(group)
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
        template = self._pick_template(block, language, fallback="Please reply with the written qualifying answers and then share your CV/resume.")
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
        try:
            delay_hours = float(self.followup_delays_hours[index])
        except (TypeError, ValueError):
            delay_hours = 48.0
        delay_hours = max(delay_hours, 1.0 / 60.0)
        return iso(now + timedelta(hours=delay_hours))

    def _require_session(self, session_id: str) -> PreResumeSession:
        session = self.sessions.get(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        return session

    @staticmethod
    def _default_template_for_group(group: str) -> str:
        fallbacks = {
            "intro": DEFAULT_TEMPLATES["intro"]["en"],
            "written_questions_intro": DEFAULT_TEMPLATES["written_questions_intro"]["en"],
            "cv_request": DEFAULT_TEMPLATES["cv_request"]["en"],
            "cv_received_pending": DEFAULT_TEMPLATES["cv_received_pending"]["en"],
            "screening_call_ready": DEFAULT_TEMPLATES["screening_call_ready"]["en"],
            "not_interested_ack": DEFAULT_TEMPLATES["not_interested_ack"]["en"],
            "resume_promised_ack": DEFAULT_TEMPLATES["resume_promised_ack"]["en"],
        }
        return str(fallbacks.get(group) or "")

    @staticmethod
    def _format_budget(session: PreResumeSession) -> str:
        currency = str(session.salary_currency or "").strip().upper()
        salary_min = session.salary_min
        salary_max = session.salary_max
        if salary_min is None and salary_max is None:
            return ""
        if salary_min is not None and salary_max is not None:
            return f"{int(salary_min):,}-{int(salary_max):,} {currency}".replace(",", " ")
        if salary_min is not None:
            return f"from {int(salary_min):,} {currency}".replace(",", " ")
        return f"up to {int(salary_max or 0):,} {currency}".replace(",", " ")

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
