from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib import error, request


class CandidateLLMResponder:
    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        base_url: str = "https://api.openai.com/v1",
        timeout_seconds: int = 30,
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or "gpt-4o-mini"
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds))

    def generate_candidate_reply(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        fallback_reply: str,
        language: str = "en",
        state: Optional[Dict[str, Any]] = None,
        allow_fallback: bool = True,
    ) -> str:
        fallback = (fallback_reply or "").strip()
        if not self.api_key:
            return fallback if allow_fallback else ""

        payload = self._build_payload(
            mode=mode,
            instruction=instruction,
            job=job,
            candidate=candidate,
            inbound_text=inbound_text,
            history=history,
            fallback_reply=fallback,
            language=(language or "en").lower(),
            state=state or {},
        )
        try:
            content = self._chat_completion(payload)
        except Exception:
            return fallback if allow_fallback else ""
        text = (content or "").strip()
        if text:
            return text
        return fallback if allow_fallback else ""

    def generate_candidate_extraction(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        state: Optional[Dict[str, Any]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        previous_language: str = "",
        fallback_language: str = "en",
    ) -> Dict[str, Any]:
        if not self.api_key:
            return {}
        payload = self._build_extraction_payload(
            mode=mode,
            instruction=instruction,
            job=job,
            candidate=candidate,
            inbound_text=inbound_text,
            history=history,
            state=state or {},
            attachments=attachments or [],
            previous_language=previous_language,
            fallback_language=fallback_language,
        )
        try:
            content = self._chat_completion(payload)
        except Exception:
            return {}
        return self._parse_json_content(content)

    def _build_payload(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        fallback_reply: str,
        language: str,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower()
        multiline_modes = {
            "linkedin_outreach",
            "linkedin_followup",
            "linkedin_interview_invite",
            "linkedin_interview_followup",
        }
        outreach_mode = normalized_mode in multiline_modes
        chat_style_modes = {
            "pre_resume",
            "faq",
            "linkedin_outreach",
            "linkedin_followup",
            "linkedin_interview_invite",
            "linkedin_interview_followup",
        }

        system_rules = [
            "You are Tener AI recruiter communication agent.",
            f"Communication mode: {mode}.",
            f"Reply language must be: {language}.",
            "Output plain text only (no markdown, no JSON, no code blocks).",
            "Use only role-relevant facts from instruction, JD, and conversation context.",
            "Do not invent compensation numbers, interview steps, or policy details that are not provided.",
            (
                "Keep reply concise, natural, and human using short paragraphs with preserved line breaks."
                if outreach_mode
                else "Keep reply concise, natural, and human (2-5 short sentences)."
            ),
            "If mode is pre_resume and resume was not received, include clear CTA to share CV/resume.",
        ]
        if normalized_mode in chat_style_modes:
            system_rules.extend(
                [
                    "Write in informal recruiter chat tone, not corporate copy.",
                    "Use natural rhythm with mixed short and longer lines.",
                    "Avoid perfect polished structure and template transitions.",
                    "Do not use phrases like As an AI or Let me clarify.",
                    "Use context from the conversation instead of generic textbook replies.",
                    "If there was already any previous outbound message in the thread, do not address the candidate by name.",
                    "Never translate or localize the candidate name.",
                    "Do not say thanks for your honesty, thanks for your candor, cheers, warm regards, or similar canned recruiter phrases.",
                    "If the natural answer would be awkward or low confidence, return an empty string.",
                ]
            )
        if instruction.strip():
            system_rules.append(f"Agent instruction:\n{instruction.strip()}")

        trimmed_history = history[-14:]
        user_context = {
            "job": {
                "title": job.get("title"),
                "location": job.get("location"),
                "seniority": job.get("seniority"),
                "jd_text": job.get("jd_text"),
            },
            "candidate": {
                "name": candidate.get("full_name"),
                "headline": candidate.get("headline"),
                "location": candidate.get("location"),
                "languages": candidate.get("languages"),
            },
            "pre_resume_state": state or None,
            "conversation_history": trimmed_history,
            "latest_inbound_message": inbound_text,
            "fallback_reply": fallback_reply,
            "task": "Generate one outbound message for candidate now.",
        }

        max_tokens = 420 if outreach_mode else 220
        return {
            "model": self.model,
            "temperature": 0.3,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": "\n".join(system_rules)},
                {"role": "user", "content": json.dumps(user_context, ensure_ascii=False)},
            ],
        }

    def _build_extraction_payload(
        self,
        *,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, Any]],
        state: Dict[str, Any],
        attachments: List[Dict[str, Any]],
        previous_language: str,
        fallback_language: str,
    ) -> Dict[str, Any]:
        normalized_mode = str(mode or "faq").strip().lower() or "faq"
        allowed_intents = (
            [
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
            ]
            if normalized_mode == "pre_resume"
            else ["salary", "stack", "timeline", "referral", "budget_mismatch", "part_time_only", "location_mismatch", "farewell_only", "default"]
        )
        system_rules = [
            "You are Tener AI structured message extraction agent.",
            f"Extraction mode: {normalized_mode}.",
            "Return strict JSON only.",
            "Do not include markdown fences or commentary.",
            "Infer the candidate's actual message language from human text, not from filenames, URLs, transport strings, or attachment payloads.",
            "Ignore technical artifacts such as att:// links, base64-looking text, provider ids, filenames, and URL fragments unless the human explicitly refers to them semantically.",
            "Extract only what is explicitly stated or strongly implied in the candidate message and immediate context.",
            "If uncertain, keep the field null or use default intent.",
            f"Allowed intents: {', '.join(allowed_intents)}.",
            (
                "For pre_resume extract intent, language, resume signal, expected gross monthly salary, must-have experience answer, location alignment, work authorization, sanitized_text, confidence, warnings."
                if normalized_mode == "pre_resume"
                else "For faq extract intent, language, sanitized_text, confidence, warnings."
            ),
            "Language must be a short code like en, ru, uk, es, pt, de, fr, it, pl, tr, ar.",
            "Use previous_language only as fallback when the latest human text does not clearly indicate language.",
            "Use fallback_language only if both latest text and previous_language are inconclusive.",
            "Confidence must be an object with per-field numeric values from 0 to 1.",
            "Warnings must be a list of short strings.",
        ]
        if instruction.strip():
            system_rules.append(f"Agent instruction:\n{instruction.strip()}")
        trimmed_history = history[-8:]
        user_context = {
            "job": {
                "title": job.get("title"),
                "location": job.get("location"),
                "seniority": job.get("seniority"),
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "salary_currency": job.get("salary_currency"),
                "work_authorization_required": job.get("work_authorization_required"),
            },
            "candidate": {
                "name": candidate.get("full_name"),
                "location": candidate.get("location"),
                "languages": candidate.get("languages"),
            },
            "state": state or None,
            "history": trimmed_history,
            "latest_inbound_message": inbound_text,
            "attachments": attachments,
            "previous_language": previous_language,
            "fallback_language": fallback_language,
            "json_schema": {
                "language": "string",
                "intent": "string",
                "resume_shared": "boolean|null",
                "resume_links": ["string"],
                "salary_expectation_gross_monthly": "number|null",
                "salary_expectation_currency": "string|null",
                "must_have_answer": "string|null",
                "location_confirmed": "boolean|null",
                "work_authorization_confirmed": "boolean|null",
                "sanitized_text": "string",
                "confidence": {"field_name": "0..1"},
                "warnings": ["string"],
            },
        }
        return {
            "model": self.model,
            "temperature": 0.0,
            "max_tokens": 420,
            "messages": [
                {"role": "system", "content": "\n".join(system_rules)},
                {"role": "user", "content": json.dumps(user_context, ensure_ascii=False)},
            ],
        }

    def _chat_completion(self, payload: Dict[str, Any]) -> str:
        url = f"{self.base_url}/chat/completions"
        req = request.Request(
            url=url,
            method="POST",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except error.HTTPError as exc:
            raise RuntimeError(f"OpenAI HTTP error {exc.code}: {self._safe_error_body(exc)}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"OpenAI network error: {exc.reason}") from exc

        parsed = json.loads(raw) if raw else {}
        choices = parsed.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") if isinstance(choices[0], dict) else None
            content = msg.get("content") if isinstance(msg, dict) else ""
            if isinstance(content, str):
                return content
        return ""

    @staticmethod
    def _parse_json_content(content: str) -> Dict[str, Any]:
        text = str(content or "").strip()
        if not text:
            return {}
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _safe_error_body(exc: error.HTTPError) -> str:
        try:
            body = exc.read().decode("utf-8")
            return body[:500] if body else "no_error_body"
        except Exception:
            return "no_error_body"
