from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .language import detect_language_from_text, pick_candidate_language
from .matching import MatchingEngine


class SourcingAgent:
    def __init__(self, linkedin_provider: Any, instruction: str = "") -> None:
        self.linkedin_provider = linkedin_provider
        self.instruction = instruction

    def find_candidates(self, job: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 1), 200))
        queries = self._build_queries(job)
        per_query_limit = max(10, min(limit, 50))

        seen: set[str] = set()
        collected: List[Dict[str, Any]] = []
        search_errors: List[str] = []

        # Pass 1: broad query set with a larger per-query window.
        for query in queries:
            if len(collected) >= limit:
                break
            try:
                profiles = self.linkedin_provider.search_profiles(query=query, limit=per_query_limit)
            except Exception as exc:
                search_errors.append(f"query={query[:120]} error={exc}")
                continue
            for profile in profiles:
                key = self._candidate_key(profile)
                if key in seen:
                    continue
                seen.add(key)
                collected.append(profile)
                if len(collected) >= limit:
                    break

        # Pass 2: if still below target, rerun with wider windows to reduce duplicate-heavy tops.
        if len(collected) < limit:
            expanded_limit = min(100, max(per_query_limit + 25, int(limit)))
            for query in queries:
                if len(collected) >= limit:
                    break
                try:
                    profiles = self.linkedin_provider.search_profiles(query=query, limit=expanded_limit)
                except Exception as exc:
                    search_errors.append(f"query={query[:120]} error={exc}")
                    continue
                for profile in profiles:
                    key = self._candidate_key(profile)
                    if key in seen:
                        continue
                    seen.add(key)
                    collected.append(profile)
                    if len(collected) >= limit:
                        break

        if not collected and search_errors:
            raise RuntimeError("; ".join(search_errors[:5]))
        return collected[:limit]

    def send_outreach(self, candidate_profile: Dict[str, Any], message: str) -> Dict[str, Any]:
        return self.linkedin_provider.send_message(candidate_profile=candidate_profile, message=message)

    def send_connection_request(self, candidate_profile: Dict[str, Any], message: str | None = None) -> Dict[str, Any]:
        fn = getattr(self.linkedin_provider, "send_connection_request", None)
        if callable(fn):
            return fn(candidate_profile=candidate_profile, message=message)
        return {
            "provider": "unknown",
            "sent": False,
            "reason": "connection_request_not_supported",
        }

    def check_connection_status(self, candidate_profile: Dict[str, Any]) -> Dict[str, Any]:
        fn = getattr(self.linkedin_provider, "check_connection_status", None)
        if callable(fn):
            return fn(candidate_profile=candidate_profile)
        return {
            "provider": "unknown",
            "connected": False,
            "reason": "connection_status_not_supported",
        }

    def fetch_chat_messages(self, chat_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        fn = getattr(self.linkedin_provider, "fetch_chat_messages", None)
        if callable(fn):
            return fn(chat_id=chat_id, limit=limit)
        return []

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
        keywords = self._extract_keywords(jd_text, max_items=14)

        candidates = [
            title,
            f"{title} {location}".strip(),
            f"{title} {' '.join(keywords[:4])}".strip(),
            " ".join(keywords[:5]).strip(),
            " ".join(keywords[5:10]).strip(),
            f"{title} {' '.join(keywords[4:8])}".strip(),
            f"{title} {location} {' '.join(keywords[:6])}".strip(),
            f"{title} {jd_text[:220]}".strip(),
        ]
        for idx in range(0, min(len(keywords), 10), 2):
            chunk = " ".join(keywords[idx : idx + 4]).strip()
            if chunk:
                candidates.append(f"{title} {chunk}".strip())
                if location:
                    candidates.append(f"{title} {location} {chunk}".strip())
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
    def __init__(self, matching_engine: MatchingEngine, instruction: str = "") -> None:
        self.matching_engine = matching_engine
        self.instruction = instruction

    def verify_candidate(self, job: Dict[str, Any], profile: Dict[str, Any]) -> Tuple[float, str, Dict[str, Any]]:
        result = self.matching_engine.verify(job=job, profile=profile)
        return result.score, result.status, result.notes


class OutreachAgent:
    def __init__(self, templates_path: str, matching_engine: MatchingEngine, instruction: str = "") -> None:
        self.templates = self._load_templates(templates_path)
        self.matching_engine = matching_engine
        self.instruction = instruction

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

    def compose_screening_message(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        request_resume: bool,
    ) -> Tuple[str, str]:
        if request_resume:
            return self.compose_resume_request(job=job, candidate=candidate)
        return self.compose_intro(job=job, candidate=candidate)

    def compose_resume_request(self, job: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[str, str]:
        candidate_lang = pick_candidate_language(candidate.get("languages"), fallback=self.templates.get("default_language", "en"))
        group = self.templates.get("outreach_resume_request") or self.templates.get("outreach", {})
        template = self._pick_template(group, candidate_lang)
        scope_summary = self.matching_engine.summarize_scope(job)
        core = self.matching_engine.build_core_profile(job)
        core_summary = ", ".join(core.get("core_skills") or []) or scope_summary
        msg = template.format(
            name=candidate.get("full_name", "there"),
            job_title=job.get("title", "this role"),
            scope_summary=scope_summary,
            core_profile_summary=core_summary,
        )
        return candidate_lang, msg

    def compose_connection_request(self, job: Dict[str, Any], candidate: Dict[str, Any]) -> Tuple[str, str]:
        candidate_lang = pick_candidate_language(candidate.get("languages"), fallback=self.templates.get("default_language", "en"))
        group = self.templates.get("outreach_connect_request") or {}
        if isinstance(group, dict) and group:
            template = self._pick_template(group, candidate_lang)
            scope_summary = self.matching_engine.summarize_scope(job)
            msg = template.format(
                name=candidate.get("full_name", "there"),
                job_title=job.get("title", "this role"),
                scope_summary=scope_summary,
            )
            return candidate_lang, msg

        fallback = {
            "en": "Hi {name}, sending a connection request regarding the role \"{job_title}\". If relevant, happy to share details.",
            "ru": "Привет, {name}! Отправляю запрос в контакты по роли \"{job_title}\". Если релевантно, отправлю детали.",
            "es": "Hola {name}, te envío solicitud de conexión sobre la posición \"{job_title}\". Si encaja, te comparto detalles.",
        }
        template = fallback.get(candidate_lang, fallback["en"])
        return candidate_lang, template.format(
            name=candidate.get("full_name", "there"),
            job_title=job.get("title", "this role"),
        )

    def _pick_template(self, group: Dict[str, str], language: str) -> str:
        if language in group:
            return group[language]
        return group.get(self.templates.get("default_language", "en"), next(iter(group.values())))

    @staticmethod
    def _load_templates(path: str) -> Dict[str, Any]:
        with Path(path).open("r", encoding="utf-8") as f:
            return json.load(f)


class FAQAgent:
    def __init__(self, templates_path: str, matching_engine: MatchingEngine, instruction: str = "") -> None:
        self.templates = self._load_templates(templates_path)
        self.matching_engine = matching_engine
        self.instruction = instruction

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
