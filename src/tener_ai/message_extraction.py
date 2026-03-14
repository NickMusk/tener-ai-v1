from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .attachments import AttachmentDescriptor, descriptors_to_text
from .language import normalize_language, resolve_conversation_language
from .prescreen_policy import collapse_salary_range_to_expectation


PRE_RESUME_INTENTS = {
    "resume_shared",
    "not_interested",
    "will_send_later",
    "referral",
    "budget_mismatch",
    "part_time_only",
    "location_mismatch",
    "farewell_only",
    "salary",
    "stack",
    "timeline",
    "send_jd_first",
    "default",
}
FAQ_INTENTS = {
    "salary",
    "stack",
    "timeline",
    "referral",
    "budget_mismatch",
    "part_time_only",
    "location_mismatch",
    "farewell_only",
    "default",
}


def parse_resume_links(text: str) -> List[str]:
    raw_text = str(text or "")
    links = re.findall(r"https?://[^\s)>\"]+", raw_text, flags=re.IGNORECASE)
    selected: List[str] = []
    seen: set[str] = set()
    for link in links:
        lowered = link.lower()
        if any(
            marker in lowered
            for marker in ("resume", "cv", "curriculum", "currículum", ".pdf", ".doc", ".docx", "drive.", "dropbox", "notion.")
        ):
            if link not in seen:
                selected.append(link)
                seen.add(link)
    if selected:
        return selected

    lowered_text = raw_text.lower()
    attachment_markers = (
        "attach",
        "attachment",
        "attached file",
        "attached doc",
        "file",
        "document",
        "cv",
        "resume",
        "résumé",
        "резюме",
        "файл",
        "вложение",
        "adjunto",
        "archivo",
        "curriculum",
        "currículum",
    )
    if links and any(marker in lowered_text for marker in attachment_markers):
        for link in links:
            if link not in seen:
                selected.append(link)
                seen.add(link)
    return selected


def classify_pre_resume_intent(text: str) -> Tuple[str, List[str]]:
    normalized = (text or "").strip()
    lowered = normalized.lower()
    links = parse_resume_links(normalized)

    if links:
        return "resume_shared", links
    if any(
        marker in lowered
        for marker in (
            "my cv",
            "my resume",
            "attached cv",
            "attached resume",
            "attached file",
            "attached document",
            "here is resume",
            "here's my resume",
            "вот резюме",
            "прикрепил файл",
            "прикрепила файл",
            "прикрепил резюме",
            "отправил резюме",
            "adjunto archivo",
            "adjunto documento",
            "adjunto cv",
            "adjunto mi cv",
            "te envío mi cv",
            "aqui esta mi cv",
            "aquí está mi cv",
        )
    ):
        return "resume_shared", links

    if any(
        marker in lowered
        for marker in (
            "not interested",
            "no thanks",
            "stop",
            "unsubscribe",
            "not looking",
            "не интересно",
            "не актуально",
            "не ищу",
            "no me interesa",
            "ya no estoy interesado",
        )
    ):
        return "not_interested", links

    if any(
        marker in lowered
        for marker in (
            "will send",
            "send later",
            "tomorrow",
            "next week",
            "later",
            "пришлю позже",
            "отправлю позже",
            "завтра отправлю",
            "позже",
            "lo envio luego",
            "lo envío luego",
            "te lo envio mañana",
            "te lo envío mañana",
            "despues",
            "después",
        )
    ):
        return "will_send_later", links

    if any(
        marker in lowered
        for marker in (
            "i can refer",
            "i can recommend",
            "i can suggest someone",
            "i know someone",
            "someone else looking",
            "someone who is looking",
            "recommend someone else",
            "refer someone",
            "могу порекомендовать",
            "могу посоветовать",
            "знаю человека",
            "есть кто-то еще",
            "puedo recomendar",
            "conozco a alguien",
            "te puedo referir",
            "puedo sugerir a alguien",
        )
    ):
        return "referral", links

    if any(
        marker in lowered
        for marker in (
            "part time only",
            "only part time",
            "can work only part time",
            "part-time only",
            "solo part time",
            "solo medio tiempo",
            "только part time",
            "только частичная занятость",
            "только на part time",
            "only freelance",
        )
    ):
        return "part_time_only", links

    if any(
        marker in lowered
        for marker in (
            "low budget",
            "budget is low",
            "too low for me",
            "salary is too low",
            "comp is too low",
            "compensation is too low",
            "outside my budget",
            "не подходит по бюджету",
            "слишком низкая зарплата",
            "маленький бюджет",
            "presupuesto bajo",
            "salario muy bajo",
            "muy bajo para mi",
        )
    ):
        return "budget_mismatch", links

    if any(
        marker in lowered
        for marker in (
            "not open to relocate",
            "can't relocate",
            "cannot relocate",
            "want to stay in",
            "prefer to stay in",
            "only in mexico",
            "only in mexico city",
            "solo en",
            "quiero quedarme en",
            "не готов к релокации",
            "не готов переезжать",
            "хочу оставаться в",
        )
    ):
        return "location_mismatch", links

    farewell_markers = (
        "catch you later",
        "take care",
        "sure thanks",
        "thank you so much",
        "thanks",
        "thanks!",
        "thank you",
        "take care too",
        "увидимся",
        "спасибо",
        "до связи",
        "gracias",
        "muchas gracias",
        "hasta luego",
        "cuidate",
        "cuidate!",
    )
    if any(marker in lowered for marker in farewell_markers):
        short_text = re.sub(r"[^a-zа-яё0-9\s]", " ", lowered, flags=re.IGNORECASE)
        if len(short_text.split()) <= 6:
            return "farewell_only", links

    salary_markers = ("salary", "compensation", "pay", "range", "зарплат", "вилка", "salario", "compensación", "compensacion")
    stack_markers = ("stack", "technology", "tech", "tools", "requirements", "стек", "технолог", "tecnolog", "stack técnico")
    timeline_markers = (
        "timeline",
        "process",
        "interview",
        "steps",
        "when",
        "срок",
        "этап",
        "процесс",
        "proceso",
        "entrevista",
        "cuando",
        "cuándo",
        "screening call",
        "async interview",
    )
    details_markers = (
        "send jd",
        "job description",
        "details first",
        "share details",
        "more details",
        "пришлите jd",
        "подробности",
        "описание вакансии",
        "manda jd",
        "descripcion del puesto",
        "descripción del puesto",
    )

    if any(marker in lowered for marker in salary_markers):
        return "salary", links
    if any(marker in lowered for marker in stack_markers):
        return "stack", links
    if any(marker in lowered for marker in timeline_markers):
        return "timeline", links
    if any(marker in lowered for marker in details_markers):
        return "send_jd_first", links
    return "default", links


def classify_faq_intent(message: str) -> str:
    msg = (message or "").lower()
    intent_rules = {
        "salary": ["salary", "compensation", "pay", "вилка", "зарплат", "salario"],
        "stack": ["stack", "technology", "tech", "стек", "tools", "requirements"],
        "timeline": ["timeline", "process", "interview", "срок", "этап", "proceso", "entrevista"],
    }
    for intent, keywords in intent_rules.items():
        if any(k in msg for k in keywords):
            return intent
    return "default"


def normalize_currency(text: str) -> Optional[str]:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return None
    mapping = {
        "$": "USD",
        "usd": "USD",
        "dollar": "USD",
        "dollars": "USD",
        "€": "EUR",
        "eur": "EUR",
        "euro": "EUR",
        "euros": "EUR",
        "£": "GBP",
        "gbp": "GBP",
        "pound": "GBP",
        "pounds": "GBP",
        "aed": "AED",
        "dirham": "AED",
        "dirhams": "AED",
    }
    for marker, code in mapping.items():
        if marker in lowered:
            return code
    return None


def parse_compensation_value(text: str) -> Tuple[Optional[float], Optional[str]]:
    raw = str(text or "")
    currency = normalize_currency(raw)
    matches = re.findall(r"(\d{1,3}(?:[,\s]\d{3})+|\d+(?:\.\d+)?)\s*([kKmM])?", raw)
    values: List[float] = []
    for number, suffix in matches:
        cleaned = number.replace(",", "").replace(" ", "")
        try:
            value = float(cleaned)
        except ValueError:
            continue
        suffix_norm = str(suffix or "").lower()
        if suffix_norm == "k":
            value *= 1000.0
        elif suffix_norm == "m":
            value *= 1000000.0
        elif value < 1000.0:
            continue
        values.append(value)
    if not values:
        return None, currency
    if len(values) == 1:
        return values[0], currency
    collapsed = collapse_salary_range_to_expectation(values[0], values[1])
    return collapsed, currency


def extract_pre_resume_heuristic_fields(
    *,
    text: str,
    intent: str,
    job_location: str | None,
    work_authorization_required: bool,
    must_have_answer_exists: bool,
    salary_currency: str | None,
) -> Dict[str, Any]:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    word_count = len(re.findall(r"[0-9A-Za-zА-Яа-яЁё]+", normalized))
    out: Dict[str, Any] = {}

    salary_value, parsed_currency = parse_compensation_value(normalized)
    if salary_value is not None:
        out["salary_expectation_gross_monthly"] = salary_value
        out["salary_expectation_currency"] = parsed_currency or salary_currency or "USD"

    if work_authorization_required:
        auth_positive = (
            "authorized to work",
            "work authorization",
            "no sponsorship",
            "no visa needed",
            "citizen",
            "green card",
            "eu passport",
            "authorized",
        )
        auth_negative = (
            "need sponsorship",
            "require sponsorship",
            "need visa",
            "not authorized",
            "without authorization",
        )
        if any(marker in lowered for marker in auth_positive):
            out["work_authorization_confirmed"] = True
        elif any(marker in lowered for marker in auth_negative):
            out["work_authorization_confirmed"] = False

    location_text = str(job_location or "").strip().lower()
    location_confirmation_required = bool(location_text) and not any(
        marker in location_text for marker in ("remote", "global", "worldwide", "anywhere", "hybrid", "distributed")
    )
    if location_confirmation_required:
        location_positive = (
            "based in",
            "located in",
            "open to relocate",
            "can relocate",
            "open to move",
        )
        location_negative = (
            "not open to relocate",
            "can't relocate",
            "cannot relocate",
            "not based",
            "want to stay in",
            "prefer to stay in",
            "stay in",
            "only in",
            "quiero quedarme en",
            "solo en",
            "не готов к релокации",
            "не готов переезжать",
            "хочу оставаться в",
        )
        if location_text and location_text in lowered:
            out["location_confirmed"] = True
        elif any(marker in lowered for marker in location_positive):
            out["location_confirmed"] = True
        elif any(marker in lowered for marker in location_negative):
            out["location_confirmed"] = False

    if not must_have_answer_exists:
        if intent in {"default", "stack", "resume_shared", "will_send_later"} and word_count >= 6 and "?" not in normalized:
            out["must_have_answer"] = normalized
        elif any(marker in lowered for marker in ("experience", "hands-on", "worked on", "built", "shipped", "led")) and word_count >= 5:
            out["must_have_answer"] = normalized

    return out


@dataclass
class CandidateMessageExtractionResult:
    mode: str
    language: str
    intent: str
    resume_shared: bool = False
    resume_links: List[str] = field(default_factory=list)
    salary_expectation_gross_monthly: Optional[float] = None
    salary_expectation_currency: Optional[str] = None
    must_have_answer: Optional[str] = None
    location_confirmed: Optional[bool] = None
    work_authorization_confirmed: Optional[bool] = None
    sanitized_text: str = ""
    confidence: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    source: str = "fallback"
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "language": self.language,
            "intent": self.intent,
            "resume_shared": self.resume_shared,
            "resume_links": list(self.resume_links),
            "salary_expectation_gross_monthly": self.salary_expectation_gross_monthly,
            "salary_expectation_currency": self.salary_expectation_currency,
            "must_have_answer": self.must_have_answer,
            "location_confirmed": self.location_confirmed,
            "work_authorization_confirmed": self.work_authorization_confirmed,
            "sanitized_text": self.sanitized_text,
            "confidence": dict(self.confidence or {}),
            "warnings": list(self.warnings or []),
            "source": self.source,
        }


class CandidateMessageExtractionService:
    def __init__(self, llm_client: Any | None = None, instruction: str = "") -> None:
        self.llm_client = llm_client
        self.instruction = str(instruction or "").strip()

    def extract(
        self,
        *,
        mode: str,
        inbound_text: str,
        history: Sequence[Mapping[str, Any]] | None,
        candidate: Mapping[str, Any] | None,
        job: Mapping[str, Any] | None,
        state: Mapping[str, Any] | None = None,
        attachments: Sequence[AttachmentDescriptor] | None = None,
        previous_language: str | None = None,
        fallback_language: str = "en",
        instruction: str = "",
    ) -> CandidateMessageExtractionResult:
        normalized_mode = str(mode or "faq").strip().lower() or "faq"
        fallback = self._fallback_extract(
            mode=normalized_mode,
            inbound_text=inbound_text,
            candidate=candidate,
            job=job,
            state=state,
            attachments=attachments,
            previous_language=previous_language,
            fallback_language=fallback_language,
        )
        client = self.llm_client
        if client is None or not hasattr(client, "generate_candidate_extraction"):
            return fallback
        try:
            raw = client.generate_candidate_extraction(
                mode=normalized_mode,
                instruction=str(instruction or self.instruction or "").strip(),
                job=dict(job or {}),
                candidate=dict(candidate or {}),
                inbound_text=str(inbound_text or ""),
                history=[dict(item) for item in (history or [])],
                state=dict(state or {}),
                attachments=[item.to_dict() for item in (attachments or [])],
                previous_language=str(previous_language or ""),
                fallback_language=str(fallback_language or "en"),
            )
        except Exception as exc:
            fallback.warnings.append(f"llm_extraction_error:{exc}")
            return fallback
        validated = self._validate_llm_result(
            mode=normalized_mode,
            payload=raw,
            fallback=fallback,
            inbound_text=inbound_text,
            attachments=attachments,
        )
        return validated or fallback

    def _fallback_extract(
        self,
        *,
        mode: str,
        inbound_text: str,
        candidate: Mapping[str, Any] | None,
        job: Mapping[str, Any] | None,
        state: Mapping[str, Any] | None,
        attachments: Sequence[AttachmentDescriptor] | None,
        previous_language: str | None,
        fallback_language: str,
    ) -> CandidateMessageExtractionResult:
        human_text = str(inbound_text or "").strip()
        attachment_text = descriptors_to_text(attachments or [], limit=8)
        signal_text = f"{human_text}\n{attachment_text}".strip() if attachment_text else human_text
        if mode == "pre_resume":
            intent, links = classify_pre_resume_intent(signal_text)
            fields = extract_pre_resume_heuristic_fields(
                text=human_text,
                intent=intent,
                job_location=(state or {}).get("job_location") or (job or {}).get("location"),
                work_authorization_required=bool((state or {}).get("work_authorization_required") or (job or {}).get("work_authorization_required")),
                must_have_answer_exists=bool(str((state or {}).get("must_have_answer") or "").strip()),
                salary_currency=(state or {}).get("salary_expectation_currency")
                or (state or {}).get("salary_currency")
                or (job or {}).get("salary_currency"),
            )
        else:
            intent = classify_faq_intent(human_text)
            links = parse_resume_links(signal_text)
            fields = {}
        language = resolve_conversation_language(
            latest_message_text=human_text,
            previous_language=previous_language,
            profile_languages=(candidate or {}).get("languages"),
            fallback=fallback_language,
        )
        return CandidateMessageExtractionResult(
            mode=mode,
            language=normalize_language(language, fallback=fallback_language) or fallback_language,
            intent=intent,
            resume_shared=bool(intent == "resume_shared" or links),
            resume_links=links,
            salary_expectation_gross_monthly=fields.get("salary_expectation_gross_monthly"),
            salary_expectation_currency=fields.get("salary_expectation_currency"),
            must_have_answer=fields.get("must_have_answer"),
            location_confirmed=fields.get("location_confirmed"),
            work_authorization_confirmed=fields.get("work_authorization_confirmed"),
            sanitized_text=str(inbound_text or "").strip(),
            confidence={},
            warnings=[],
            source="fallback",
            raw_payload={},
        )

    def _validate_llm_result(
        self,
        *,
        mode: str,
        payload: Any,
        fallback: CandidateMessageExtractionResult,
        inbound_text: str,
        attachments: Sequence[AttachmentDescriptor] | None,
    ) -> CandidateMessageExtractionResult | None:
        raw = self._coerce_payload_dict(payload)
        if not raw:
            fallback.warnings.append("llm_extraction_empty")
            return None
        allowed_intents = PRE_RESUME_INTENTS if mode == "pre_resume" else FAQ_INTENTS
        intent = str(raw.get("intent") or fallback.intent).strip().lower() or fallback.intent
        if intent not in allowed_intents:
            intent = fallback.intent
        language = normalize_language(str(raw.get("language") or fallback.language), fallback=fallback.language) or fallback.language
        resume_shared = self._coerce_bool(raw.get("resume_shared"))
        if resume_shared is None:
            resume_shared = fallback.resume_shared
        raw_links = raw.get("resume_links")
        resume_links = [str(item).strip() for item in raw_links if str(item).strip()] if isinstance(raw_links, list) else list(fallback.resume_links)
        if resume_shared and not resume_links:
            merged_text = str(inbound_text or "").strip()
            attachment_text = descriptors_to_text(attachments or [], limit=8)
            if attachment_text:
                merged_text = f"{merged_text}\n{attachment_text}".strip()
            resume_links = parse_resume_links(merged_text)
        if resume_links and intent == "default" and mode == "pre_resume":
            intent = "resume_shared"
        has_salary_value = "salary_expectation_gross_monthly" in raw
        has_currency = "salary_expectation_currency" in raw
        has_must_have = "must_have_answer" in raw
        has_location = "location_confirmed" in raw
        has_auth = "work_authorization_confirmed" in raw
        salary_value = self._coerce_float(raw.get("salary_expectation_gross_monthly"))
        currency = normalize_currency(str(raw.get("salary_expectation_currency") or "")) if has_currency else None
        must_have_answer = self._coerce_text(raw.get("must_have_answer")) if has_must_have else fallback.must_have_answer
        sanitized_text = self._coerce_text(raw.get("sanitized_text")) or str(inbound_text or "").strip()
        confidence = self._coerce_confidence(raw.get("confidence"))
        warnings = [str(item).strip() for item in (raw.get("warnings") or []) if str(item).strip()] if isinstance(raw.get("warnings"), list) else []
        location_confirmed = self._coerce_bool(raw.get("location_confirmed")) if has_location else fallback.location_confirmed
        work_authorization_confirmed = (
            self._coerce_bool(raw.get("work_authorization_confirmed"))
            if has_auth
            else fallback.work_authorization_confirmed
        )
        return CandidateMessageExtractionResult(
            mode=mode,
            language=language,
            intent=intent,
            resume_shared=bool(resume_shared or resume_links),
            resume_links=resume_links,
            salary_expectation_gross_monthly=salary_value if has_salary_value else fallback.salary_expectation_gross_monthly,
            salary_expectation_currency=currency if has_currency else fallback.salary_expectation_currency,
            must_have_answer=must_have_answer,
            location_confirmed=location_confirmed,
            work_authorization_confirmed=work_authorization_confirmed,
            sanitized_text=sanitized_text,
            confidence=confidence,
            warnings=warnings,
            source="llm",
            raw_payload=raw,
        )

    @staticmethod
    def _coerce_payload_dict(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return dict(payload)
        text = str(payload or "").strip()
        if not text:
            return {}
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL).strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}

    @staticmethod
    def _coerce_text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return True
        if text in {"0", "false", "no", "n"}:
            return False
        return None

    @staticmethod
    def _coerce_confidence(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        out: Dict[str, float] = {}
        for key, raw in value.items():
            try:
                score = float(raw)
            except (TypeError, ValueError):
                continue
            if score < 0.0:
                score = 0.0
            if score > 1.0:
                score = 1.0
            out[str(key)] = score
        return out
