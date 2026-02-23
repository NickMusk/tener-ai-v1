from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .language import detect_language_from_text, pick_candidate_language
from .matching import MatchingEngine


class SourcingAgent:
    def __init__(self, linkedin_provider: Any) -> None:
        self.linkedin_provider = linkedin_provider

    def find_candidates(self, job: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        query = f"{job.get('title', '')} {job.get('jd_text', '')}"
        return self.linkedin_provider.search_profiles(query=query, limit=limit)


class VerificationAgent:
    def __init__(self, matching_engine: MatchingEngine) -> None:
        self.matching_engine = matching_engine

    def verify_candidate(self, job: Dict[str, Any], profile: Dict[str, Any]) -> Tuple[float, str, Dict[str, Any]]:
        result = self.matching_engine.verify(job=job, profile=profile)
        return result.score, result.status, result.notes


class OutreachAgent:
    def __init__(self, templates_path: str, matching_engine: MatchingEngine) -> None:
        self.templates = self._load_templates(templates_path)
        self.matching_engine = matching_engine

    def compose_intro(self, job: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[str, str]:
        candidate_lang = pick_candidate_language(candidate.get("languages"), fallback=self.templates.get("default_language", "en"))
        template = self._pick_template(self.templates.get("outreach", {}), candidate_lang)
        scope_summary = self.matching_engine.summarize_scope(job)
        msg = template.format(
            name=candidate.get("full_name", "there"),
            job_title=job.get("title", "this role"),
            scope_summary=scope_summary,
        )
        return candidate_lang, msg

    def _pick_template(self, group: Dict[str, str], language: str) -> str:
        if language in group:
            return group[language]
        return group.get(self.templates.get("default_language", "en"), next(iter(group.values())))

    @staticmethod
    def _load_templates(path: str) -> Dict[str, Any]:
        with Path(path).open("r", encoding="utf-8") as f:
            return json.load(f)


class FAQAgent:
    def __init__(self, templates_path: str, matching_engine: MatchingEngine) -> None:
        self.templates = self._load_templates(templates_path)
        self.matching_engine = matching_engine

    def auto_reply(self, inbound_text: str, job: Dict[str, Any], candidate_lang: str | None = None) -> Tuple[str, str, str]:
        lang = candidate_lang or detect_language_from_text(inbound_text, fallback=self.templates.get("default_language", "en"))
        intent = self._classify_intent(inbound_text)
        template = self._pick_template(self.templates.get("faq", {}), intent=intent, language=lang)
        scope_summary = self.matching_engine.summarize_scope(job)
        response = template.format(scope_summary=scope_summary)
        return lang, intent, response

    def _pick_template(self, faq_group: Dict[str, Dict[str, str]], intent: str, language: str) -> str:
        bucket = faq_group.get(intent) or faq_group.get("default", {})
        if language in bucket:
            return bucket[language]
        default_language = self.templates.get("default_language", "en")
        if default_language in bucket:
            return bucket[default_language]
        return next(iter(bucket.values()))

    @staticmethod
    def _classify_intent(message: str) -> str:
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

    @staticmethod
    def _load_templates(path: str) -> Dict[str, Any]:
        with Path(path).open("r", encoding="utf-8") as f:
            return json.load(f)
