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
    ) -> str:
        fallback = (fallback_reply or "").strip()
        if not self.api_key:
            return fallback

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
            return fallback
        text = (content or "").strip()
        return text or fallback

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
        system_rules = [
            "You are Tener AI recruiter communication agent.",
            f"Communication mode: {mode}.",
            f"Reply language must be: {language}.",
            "Output plain text only (no markdown, no JSON, no code blocks).",
            "Use only role-relevant facts from instruction, JD, and conversation context.",
            "Do not invent compensation numbers, interview steps, or policy details that are not provided.",
            "Keep reply concise, natural, and human (2-5 short sentences).",
            "If mode is pre_resume and resume was not received, include clear CTA to share CV/resume.",
        ]
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

        return {
            "model": self.model,
            "temperature": 0.3,
            "max_tokens": 220,
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
    def _safe_error_body(exc: error.HTTPError) -> str:
        try:
            body = exc.read().decode("utf-8")
            return body[:500] if body else "no_error_body"
        except Exception:
            return "no_error_body"
