from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .language import detect_language_from_text, pick_candidate_language
from .matching import MatchingEngine


class SourcingAgent:
    def __init__(self, linkedin_provider: Any, instruction: str = "", matching_engine: Any | None = None) -> None:
        self.linkedin_provider = linkedin_provider
        self.instruction = instruction
        self.matching_engine = matching_engine

    def find_candidates(
        self,
        job: Dict[str, Any],
        limit: int = 50,
        *,
        exclude_profile_keys: set[str] | None = None,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 1), 200))
        spec = self.build_search_spec(job)
        queries = list(spec.get("fallback_queries") or [])
        collection_target = min(limit * 4, 400)
        per_query_limit = max(25, min(100, max(limit, collection_target // max(len(queries) + 4, 1))))

        seen: set[str] = {
            str(item or "").strip().lower()
            for item in (exclude_profile_keys or set())
            if str(item or "").strip()
        }
        collected: List[Dict[str, Any]] = []
        search_errors: List[str] = []

        structured_search = getattr(self.linkedin_provider, "search_profiles_structured", None)
        if callable(structured_search):
            for stage in self._build_structured_search_stages(spec):
                if len(collected) >= collection_target:
                    break
                try:
                    profiles = structured_search(spec=stage, limit=per_query_limit)
                except Exception as exc:
                    search_errors.append(f"structured_search[{stage.get('stage_key')}] error={exc}")
                    continue
                self._extend_unique_profiles(collected=collected, seen=seen, profiles=profiles or [])

        # Pass 1: text fallback query set.
        for query in queries:
            if len(collected) >= collection_target:
                break
            try:
                profiles = self.linkedin_provider.search_profiles(query=query, limit=per_query_limit)
            except Exception as exc:
                search_errors.append(f"query={query[:120]} error={exc}")
                continue
            self._extend_unique_profiles(collected=collected, seen=seen, profiles=profiles)

        # Pass 2: widen query windows if still below target.
        if len(collected) < collection_target:
            expanded_limit = min(100, max(per_query_limit + 25, int(limit)))
            for query in queries:
                if len(collected) >= collection_target:
                    break
                try:
                    profiles = self.linkedin_provider.search_profiles(query=query, limit=expanded_limit)
                except Exception as exc:
                    search_errors.append(f"query={query[:120]} error={exc}")
                    continue
                self._extend_unique_profiles(collected=collected, seen=seen, profiles=profiles)

        if not collected and search_errors:
            raise RuntimeError("; ".join(search_errors[:5]))
        reranked = self._rerank_profiles(job=job, profiles=collected)
        return reranked[:limit]

    def build_search_preview(self, job: Dict[str, Any]) -> Dict[str, Any]:
        spec = self.build_search_spec(job)
        return {
            "title": spec.get("title_query"),
            "location": spec.get("location"),
            "seniority": spec.get("seniority"),
            "preferred_languages": spec.get("preferred_languages") or [],
            "jd_excerpt": spec.get("jd_excerpt"),
            "extracted_keywords": spec.get("keywords") or [],
            "primary_query": spec.get("title_query"),
            "filters": spec.get("filters") or {},
            "fallback_queries": spec.get("fallback_queries") or [],
            "queries": spec.get("fallback_queries") or [],
        }

    def build_search_spec(self, job: Dict[str, Any]) -> Dict[str, Any]:
        title = str(job.get("title") or "").strip()
        jd_text = str(job.get("jd_text") or "").strip()
        location = str(job.get("location") or "").strip() or None
        seniority = str(job.get("seniority") or "").strip().lower() or None
        preferred_languages = [
            str(item).strip().lower()
            for item in (job.get("preferred_languages") or [])
            if str(item).strip()
        ]
        keywords = self._core_skills(job)
        filters: Dict[str, Any] = {
            "location": location,
            "skills": keywords[:3],
            "profile_language": preferred_languages[:2],
        }
        return {
            "title_query": title or None,
            "location": location,
            "seniority": seniority,
            "preferred_languages": preferred_languages,
            "keywords": keywords,
            "jd_excerpt": jd_text[:280] or None,
            "filters": filters,
            "fallback_queries": self._build_fallback_queries(
                title=title,
                location=location,
                keywords=keywords,
            ),
        }

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
        spec = self.build_search_spec(job)
        return list(spec.get("fallback_queries") or [])

    @staticmethod
    def _extend_unique_profiles(
        *,
        collected: List[Dict[str, Any]],
        seen: set[str],
        profiles: List[Dict[str, Any]],
    ) -> None:
        for profile in profiles or []:
            key = SourcingAgent._candidate_key(profile)
            if key in seen:
                continue
            seen.add(key)
            collected.append(profile)

    def _build_structured_search_stages(self, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters = spec.get("filters") if isinstance(spec.get("filters"), dict) else {}
        location = str(filters.get("location") or "").strip()
        skills = [str(item).strip() for item in (filters.get("skills") or []) if str(item).strip()]
        profile_language = [str(item).strip() for item in (filters.get("profile_language") or []) if str(item).strip()]

        def stage(stage_key: str, *, include_location: bool, include_languages: bool, stage_skills: List[str]) -> Dict[str, Any]:
            next_filters: Dict[str, Any] = {}
            if include_location and location:
                next_filters["location"] = location
            if include_languages and profile_language:
                next_filters["profile_language"] = profile_language[:2]
            if stage_skills:
                next_filters["skills"] = stage_skills[:3]
            return {
                **spec,
                "filters": next_filters,
                "stage_key": stage_key,
            }

        stages = [
            stage("strict", include_location=True, include_languages=True, stage_skills=skills[:3]),
            stage("focused", include_location=True, include_languages=True, stage_skills=skills[:1]),
            stage("location_lang", include_location=True, include_languages=True, stage_skills=[]),
            stage("location_only", include_location=True, include_languages=False, stage_skills=[]),
            stage("title_only", include_location=False, include_languages=False, stage_skills=[]),
        ]
        out: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        for item in stages:
            signature = json.dumps(item.get("filters") or {}, sort_keys=True)
            if signature in seen_keys:
                continue
            seen_keys.add(signature)
            out.append(item)
        return out

    def _rerank_profiles(self, *, job: Dict[str, Any], profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not profiles:
            return []

        scored: List[Tuple[int, float, int, Dict[str, Any]]] = []
        for index, profile in enumerate(profiles):
            bucket, score = self._source_rank(job=job, profile=profile)
            scored.append((bucket, score, -index, profile))

        scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        return [item[3] for item in scored]

    def _source_rank(self, *, job: Dict[str, Any], profile: Dict[str, Any]) -> Tuple[int, float]:
        base_score = 0.0
        must_have_match = 0.0
        if self.matching_engine is not None and hasattr(self.matching_engine, "verify"):
            try:
                result = self.matching_engine.verify(job=job, profile=profile)
            except Exception:
                result = None
            if result is not None:
                base_score = float(getattr(result, "score", 0.0) or 0.0)
                notes = getattr(result, "notes", {}) or {}
                components = notes.get("components") if isinstance(notes.get("components"), dict) else {}
                try:
                    must_have_match = float(components.get("must_have_match") or 0.0)
                except (TypeError, ValueError):
                    must_have_match = 0.0

        location_ok = True
        seniority_ok = True
        if self.matching_engine is not None and hasattr(self.matching_engine, "is_preferred_location"):
            location_ok = bool(
                self.matching_engine.is_preferred_location(
                    job_location=job.get("location"),
                    candidate_location=profile.get("location"),
                )
            )
        if self.matching_engine is not None and hasattr(self.matching_engine, "is_preferred_seniority"):
            try:
                years = int(profile.get("years_experience") or 0)
            except (TypeError, ValueError):
                years = 0
            seniority_ok = bool(
                self.matching_engine.is_preferred_seniority(
                    target=job.get("seniority"),
                    years=years,
                )
            )

        if location_ok and seniority_ok:
            bucket = 3
        elif location_ok:
            bucket = 2
        elif seniority_ok:
            bucket = 1
        else:
            bucket = 0

        adjusted_score = base_score
        if not location_ok:
            adjusted_score -= 1.25
        if not seniority_ok:
            adjusted_score -= 0.85
        if must_have_match < 0.3:
            adjusted_score -= 0.35
        return bucket, adjusted_score

    def _core_skills(self, job: Dict[str, Any], max_items: int = 4) -> List[str]:
        if self.matching_engine is not None:
            try:
                core = self.matching_engine.build_core_profile(job)
            except Exception:
                core = {}
            skills = core.get("core_skills") if isinstance(core, dict) else []
            if isinstance(skills, list):
                cleaned = [str(item).strip().lower() for item in skills if str(item).strip()]
                if cleaned:
                    return cleaned[:max_items]
        return self._extract_keywords(str(job.get("jd_text") or ""), max_items=max_items)

    @staticmethod
    def _build_fallback_queries(*, title: str, location: Optional[str], keywords: List[str]) -> List[str]:
        candidates = [
            title.strip(),
            f"{title} {location or ''}".strip(),
            f"{title} {keywords[0] if keywords else ''}".strip(),
            f"{title} {location or ''} {keywords[0] if keywords else ''}".strip(),
        ]
        out: List[str] = []
        seen: set[str] = set()
        for item in candidates:
            query = " ".join(str(item or "").split()).strip()
            if not query:
                continue
            lowered = query.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(query)
        return out[:4]

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
