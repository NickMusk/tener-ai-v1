from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .language import detect_language_from_text, pick_candidate_language
from .matching import MatchingEngine


class SourcingAgent:
    def __init__(self, linkedin_provider: Any) -> None:
        self.linkedin_provider = linkedin_provider

    def find_candidates(self, job: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 1), 100))
        queries = self._build_queries(job)
        per_query_limit = max(3, min(limit, 25))

        seen: set[str] = set()
        collected: List[Dict[str, Any]] = []

        for query in queries:
            if len(collected) >= limit:
                break

            profiles = self.linkedin_provider.search_profiles(query=query, limit=per_query_limit)
            for profile in profiles:
                key = self._candidate_key(profile)
                if key in seen:
                    continue
                seen.add(key)
                collected.append(profile)
                if len(collected) >= limit:
                    break

        return collected[:limit]

    def send_outreach(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return self.linkedin_provider.send_message(candidate_profile=candidate_profile, message=message)

    def enrich_candidates(self, profiles: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        enriched: List[Dict[str, Any]] = []
        failed = 0
        for profile in profiles:
            try:
                enriched_profile = self.linkedin_provider.enrich_profile(profile)
                if isinstance(enriched_profile, dict):
                    enriched.append(enriched_profile)
                else:
                    enriched.append(profile)
                    failed += 1
            except Exception:
                enriched.append(profile)
                failed += 1
        return enriched, failed

    @staticmethod
    def _candidate_key(profile: Dict[str, Any]) -> str:
        for field in ("linkedin_id", "unipile_profile_id", "attendee_provider_id", "provider_id", "id"):
            value = profile.get(field)
            if isinstance(value, str) and value.strip():
                return f"id:{value.strip().lower()}"
        name = str(profile.get("full_name") or profile.get("name") or "").strip().lower()
        headline = str(profile.get("headline") or "").strip().lower()
        return f"fallback:{name}|{headline}"

    def _build_queries(self, job: Dict[str, Any]) -> List[str]:
        title = str(job.get("title") or "").strip()
        jd_text = str(job.get("jd_text") or "").strip()
        location = str(job.get("location") or "").strip()
        keywords = self._extract_keywords(jd_text, max_items=8)

        candidates = [
            title,
            f"{title} {location}".strip(),
            f"{title} {' '.join(keywords[:4])}".strip(),
            " ".join(keywords[:5]).strip(),
            f"{title} {jd_text[:220]}".strip(),
        ]
        queries: List[str] = []
        seen: set[str] = set()
        for item in candidates:
            query = " ".join(item.split())
            if not query:
                continue
            lowered = query.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            queries.append(query)
        return queries or [title or jd_text]

    @staticmethod
    def _extract_keywords(text: str, max_items: int = 8) -> List[str]:
        stopwords = {
            "and",
            "the",
            "for",
            "with",
            "from",
            "this",
            "that",
            "you",
            "are",
            "will",
            "have",
            "our",
            "your",
            "team",
            "role",
            "need",
            "must",
            "plus",
            "senior",
            "middle",
            "junior",
            "lead",
            "engineer",
            "developer",
        }
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9+.#-]{2,}", text.lower())
        seen: set[str] = set()
        items: List[str] = []
        for token in tokens:
            if token in stopwords:
                continue
            if token in seen:
                continue
            seen.add(token)
            items.append(token)
            if len(items) >= max_items:
                break
        return items


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
