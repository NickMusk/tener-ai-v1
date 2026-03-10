from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set


QA_MUST_HAVE_SKILLS = [
    "manual testing",
    "api testing",
    "regression testing",
    "bug reporting",
    "bug triage",
    "test case design",
    "qa",
    "quality assurance",
    "jira",
]

QA_NICE_TO_HAVE_SKILLS = [
    "sql",
    "postman",
    "selenium",
    "playwright",
    "cypress",
    "automation testing",
    "ci/cd",
]

QA_QUESTIONABLE_SKILLS = [
    "go",
    "java",
    "javascript",
    "typescript",
    "node",
    "react",
    "llm",
    "recruiting",
]


@dataclass
class MatchResult:
    score: float
    status: str
    notes: Dict[str, Any]


class MatchingEngine:
    def __init__(self, rules_path: str) -> None:
        self.rules_path = rules_path
        self.rules = self._load_rules()

    def reload(self) -> None:
        self.rules = self._load_rules()

    def verify(self, job: Dict[str, Any], profile: Dict[str, Any]) -> MatchResult:
        rules = self.rules
        weights = rules.get("weights", {})
        candidate_skills = self._candidate_skills(profile)
        requirements = self.build_job_requirements(job)
        core_profile = self.build_core_profile(job)

        missing_fields = [
            field
            for field in rules.get("mandatory_fields", [])
            if self._field_is_missing(profile=profile, field=field, candidate_skills=candidate_skills)
        ]
        if missing_fields:
            explanation = (
                "Candidate rejected: profile is missing mandatory fields "
                + ", ".join(missing_fields)
                + "."
            )
            return MatchResult(
                score=0.0,
                status="rejected",
                notes={
                    "reason": "missing_mandatory_fields",
                    "missing_fields": missing_fields,
                    "human_explanation": explanation,
                },
            )

        required_skills = requirements.get("must_have_skills") or []
        nice_to_have_skills = requirements.get("nice_to_have_skills") or []
        questionable_skills = requirements.get("questionable_skills") or []

        if not required_skills:
            # If no skills are detected in JD, the role is broad; do not penalize heavily.
            must_have_match = 0.6
            matched_skills = []
        else:
            matched_skills = [skill for skill in required_skills if skill in candidate_skills]
            effective_required_count = min(len(required_skills), 6)
            must_have_match = min(1.0, len(matched_skills) / effective_required_count)

        matched_nice_to_have = [skill for skill in nice_to_have_skills if skill in candidate_skills]
        nice_to_have_match = (
            min(1.0, len(matched_nice_to_have) / max(1, len(nice_to_have_skills)))
            if nice_to_have_skills
            else 0.0
        )
        if required_skills:
            skills_match = min(1.0, (must_have_match * 0.9) + (nice_to_have_match * 0.1))
        else:
            skills_match = must_have_match

        target_seniority = str(requirements.get("target_seniority") or "middle").lower()
        years = int(profile.get("years_experience") or 0)
        seniority_match = self._seniority_match(target_seniority, years)

        location_match = self._location_match(job.get("location"), profile.get("location"))
        language_match = self._language_match(job.get("preferred_languages"), profile.get("languages"))

        score = (
            weights.get("skills_match", 0.5) * skills_match
            + weights.get("seniority_match", 0.2) * seniority_match
            + weights.get("location_match", 0.15) * location_match
            + weights.get("language_match", 0.15) * language_match
        )

        status = "verified" if score >= float(rules.get("min_score", 0.65)) else "rejected"
        notes = {
            "core_profile": core_profile,
            "required_skills": list(required_skills),
            "nice_to_have_skills": list(nice_to_have_skills),
            "questionable_skills": list(questionable_skills),
            "matched_skills": matched_skills,
            "matched_nice_to_have_skills": matched_nice_to_have,
            "effective_required_skills_count": min(len(required_skills), 6) if required_skills else 0,
            "target_seniority": target_seniority,
            "candidate_years_experience": years,
            "components": {
                "skills_match": round(skills_match, 3),
                "must_have_match": round(must_have_match, 3),
                "nice_to_have_match": round(nice_to_have_match, 3),
                "seniority_match": round(seniority_match, 3),
                "location_match": round(location_match, 3),
                "language_match": round(language_match, 3),
            },
            "min_score": rules.get("min_score", 0.65),
            "rules_version": rules.get("version", "unknown"),
        }
        notes["human_explanation"] = self._build_human_explanation(
            status=status,
            score=round(score, 3),
            min_score=float(rules.get("min_score", 0.65)),
            required_skills=list(required_skills),
            matched_skills=matched_skills,
            nice_to_have_skills=list(nice_to_have_skills),
            matched_nice_to_have=matched_nice_to_have,
            target_seniority=target_seniority,
            years=years,
            location_match=round(location_match, 3),
            language_match=round(language_match, 3),
        )
        return MatchResult(score=round(score, 3), status=status, notes=notes)

    def build_job_requirements(
        self,
        job: Dict[str, Any],
        *,
        max_must_have: int = 6,
        max_nice_to_have: int = 6,
    ) -> Dict[str, Any]:
        jd_text = (job.get("jd_text") or "").lower()
        title = str(job.get("title") or "").strip()
        target_seniority = (job.get("seniority") or self._infer_seniority(jd_text) or "middle").lower()

        explicit_must_have = self._normalize_skill_list(job.get("must_have_skills"))
        explicit_nice_to_have = self._normalize_skill_list(job.get("nice_to_have_skills"))
        explicit_questionable = self._normalize_skill_list(job.get("questionable_skills"))
        role_family = self._infer_role_family(title=title, jd_text=jd_text)

        if explicit_must_have:
            must_have_skills = explicit_must_have[:max_must_have]
        else:
            extracted_required = self._extract_required_skills(jd_text=jd_text, title=title)
            must_have_skills = (
                extracted_required[:max_must_have]
                if extracted_required
                else self._extract_jd_keywords(
                    self._fallback_requirement_text(jd_text=jd_text, title=title),
                    max_items=max_must_have,
                )
            )

        if explicit_nice_to_have:
            nice_to_have_skills = explicit_nice_to_have[:max_nice_to_have]
        else:
            fallback_keywords = self._extract_nice_to_have_skills(
                jd_text=jd_text,
                title=title,
                max_items=max_must_have + max_nice_to_have,
            )
            must_set = {item.lower() for item in must_have_skills}
            nice_to_have_skills = [item for item in fallback_keywords if item.lower() not in must_set][:max_nice_to_have]

        if explicit_questionable:
            questionable_skills = explicit_questionable
        else:
            questionable_skills = self._extract_questionable_skills(
                jd_text=jd_text,
                title=title,
                must_have_skills=must_have_skills,
                nice_to_have_skills=nice_to_have_skills,
                role_family=role_family,
            )

        return {
            "title": title,
            "target_seniority": target_seniority,
            "must_have_skills": must_have_skills,
            "nice_to_have_skills": nice_to_have_skills,
            "questionable_skills": questionable_skills,
            "location": job.get("location"),
            "preferred_languages": job.get("preferred_languages") or [],
        }

    def build_core_profile(self, job: Dict[str, Any], max_skills: int = 6) -> Dict[str, Any]:
        requirements = self.build_job_requirements(job, max_must_have=max_skills, max_nice_to_have=max_skills)
        return {
            "title": requirements.get("title"),
            "target_seniority": requirements.get("target_seniority"),
            "core_skills": requirements.get("must_have_skills") or [],
            "nice_to_have_skills": requirements.get("nice_to_have_skills") or [],
            "questionable_skills": requirements.get("questionable_skills") or [],
            "location": requirements.get("location"),
            "preferred_languages": requirements.get("preferred_languages") or [],
        }

    def _build_human_explanation(
        self,
        status: str,
        score: float,
        min_score: float,
        required_skills: List[str],
        matched_skills: List[str],
        nice_to_have_skills: List[str],
        matched_nice_to_have: List[str],
        target_seniority: str,
        years: int,
        location_match: float,
        language_match: float,
    ) -> str:
        skills_total = len(required_skills)
        skills_matched = len(matched_skills)
        skills_part = (
            f"skills: {skills_matched}/{skills_total}"
            if skills_total
            else "skills: none explicitly detected in JD (component softened)"
        )
        nice_total = len(nice_to_have_skills)
        nice_matched = len(matched_nice_to_have)
        nice_part = (
            f"nice-to-have: {nice_matched}/{nice_total}"
            if nice_total
            else "nice-to-have: not configured"
        )
        seniority_part = f"seniority: target {target_seniority}, candidate has {years} years of experience"
        location_part = f"location score: {location_match}"
        language_part = f"language score: {language_match}"

        if status == "verified":
            return (
                f"Candidate verified: final score {score} is above threshold {min_score}. "
                f"Factors: {skills_part}; {nice_part}; {seniority_part}; {location_part}; {language_part}."
            )

        gaps: List[str] = []
        if skills_total and skills_matched == 0:
            gaps.append("required JD skills do not match")
        elif skills_total and skills_matched < skills_total:
            missing = [s for s in required_skills if s not in set(matched_skills)]
            if missing:
                gaps.append("missing skills: " + ", ".join(missing[:5]))

        if years <= 1 and target_seniority in {"middle", "senior", "lead"}:
            gaps.append("insufficient experience for target seniority")
        if location_match <= 0.4:
            gaps.append("weak location match")
        if language_match <= 0.3:
            gaps.append("preferred languages are not matched")

        gaps_text = "; ".join(gaps) if gaps else "key components produced a score below threshold"
        return (
            f"Candidate rejected: final score {score} is below threshold {min_score}. "
            f"Reasons: {gaps_text}. "
            f"Factors: {skills_part}; {nice_part}; {seniority_part}; {location_part}; {language_part}."
        )

    def summarize_scope(self, job: Dict[str, Any], max_items: int = 5) -> str:
        core = self.build_core_profile(job=job, max_skills=max_items)
        skills = core.get("core_skills") or []
        if skills:
            return ", ".join(skills)
        title = job.get("title") or "the role"
        return f"key requirements from {title}"

    def _extract_required_skills(self, jd_text: str, title: str = "") -> List[str]:
        role_family = self._infer_role_family(title=title, jd_text=jd_text)
        dictionary = self._skill_dictionary_for_role(role_family)
        requirement_text = self._extract_requirement_scope(jd_text=jd_text, title=title)
        found = self._extract_known_skills(requirement_text, dictionary)
        if role_family == "qa":
            ordered = [skill for skill in QA_MUST_HAVE_SKILLS if skill in found]
            return ordered
        return found

    def _extract_nice_to_have_skills(self, jd_text: str, title: str = "", max_items: int = 12) -> List[str]:
        role_family = self._infer_role_family(title=title, jd_text=jd_text)
        dictionary = self._skill_dictionary_for_role(role_family)
        nice_scope = self._extract_nice_to_have_scope(jd_text=jd_text, title=title)
        found = self._extract_known_skills(nice_scope, dictionary)
        if role_family == "qa":
            ordered = [skill for skill in QA_NICE_TO_HAVE_SKILLS if skill in found]
            if ordered:
                return ordered[:max_items]
        if len(found) >= max_items:
            return found[:max_items]
        if found:
            return found[:max_items]
        return self._extract_jd_keywords(nice_scope, max_items=max_items)

    def _extract_questionable_skills(
        self,
        *,
        jd_text: str,
        title: str,
        must_have_skills: List[str],
        nice_to_have_skills: List[str],
        role_family: str | None,
        max_items: int = 8,
    ) -> List[str]:
        if role_family != "qa":
            return []
        detected = self._extract_known_skills(
            jd_text,
            self._skill_dictionary_for_role(role_family),
        )
        allowed = {item.lower() for item in [*must_have_skills, *nice_to_have_skills]}
        questionable = [
            skill
            for skill in QA_QUESTIONABLE_SKILLS
            if skill in detected and skill not in allowed
        ]
        return questionable[:max_items]

    @staticmethod
    def _normalize_skill_list(values: Any) -> List[str]:
        out: List[str] = []
        seen: Set[str] = set()
        for raw in values or []:
            value = str(raw or "").strip().lower()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    @staticmethod
    def _infer_role_family(title: str, jd_text: str) -> str | None:
        haystack = f"{str(title or '').lower()} {str(jd_text or '').lower()}"
        if any(token in haystack for token in ("manual qa", "qa engineer", "quality assurance", "tester", "testing")):
            return "qa"
        return None

    def _skill_dictionary_for_role(self, role_family: str | None) -> List[str]:
        dictionary = [s.lower() for s in self.rules.get("skill_dictionary", [])]
        dictionary.extend(QA_MUST_HAVE_SKILLS)
        dictionary.extend(QA_NICE_TO_HAVE_SKILLS)
        dictionary.extend(QA_QUESTIONABLE_SKILLS)
        seen: Set[str] = set()
        ordered: List[str] = []
        for skill in dictionary:
            normalized = str(skill or "").strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
        if role_family == "qa":
            prioritized = QA_MUST_HAVE_SKILLS + QA_NICE_TO_HAVE_SKILLS + QA_QUESTIONABLE_SKILLS
            seen.clear()
            out: List[str] = []
            for skill in prioritized + ordered:
                normalized = str(skill or "").strip().lower()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                out.append(normalized)
            return out
        return ordered

    @staticmethod
    def _contains_skill(text: str, skill: str) -> bool:
        escaped = re.escape(str(skill or "").lower())
        if not escaped:
            return False
        pattern = re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])")
        return bool(pattern.search(str(text or "").lower()))

    def _extract_known_skills(self, text: str, dictionary: List[str]) -> List[str]:
        found: List[str] = []
        seen: Set[str] = set()
        for skill in dictionary:
            if skill in seen:
                continue
            if self._contains_skill(text, skill):
                seen.add(skill)
                found.append(skill)
        return found

    def _infer_seniority(self, jd_text: str) -> str | None:
        tokens = {
            "lead": ["lead", "staff", "principal"],
            "senior": ["senior", "sr."],
            "middle": ["middle", "mid-level", "mid level"],
            "junior": ["junior", "jr."],
        }
        for band, markers in tokens.items():
            if any(m in jd_text for m in markers):
                return band
        return None

    def _seniority_match(self, target: str, years: int) -> float:
        bands = self.rules.get("seniority_bands", {})
        target_band = bands.get(target)
        if not target_band:
            return 0.7

        min_years = int(target_band.get("min_years", 0))
        max_years = int(target_band.get("max_years", 99))
        if min_years <= years <= max_years:
            return 1.0
        if years < min_years:
            gap = min_years - years
            return 0.65 if gap == 1 else 0.3
        overage = years - max_years
        if target in {"junior", "middle"}:
            return 0.35 if overage == 1 else 0.15
        return 0.75

    def is_preferred_seniority(self, target: str | None, years: int) -> bool:
        bands = self.rules.get("seniority_bands", {})
        target_band = bands.get(str(target or "").lower())
        if not target_band:
            return True
        min_years = int(target_band.get("min_years", 0))
        max_years = int(target_band.get("max_years", 99))
        if years < min_years:
            return False
        if years <= max_years:
            return True
        if str(target or "").lower() in {"junior", "middle"}:
            return False
        return True

    def _location_match(self, job_location: str | None, candidate_location: str | None) -> float:
        if not job_location:
            return 1.0
        jl = job_location.lower()
        if any(token in jl for token in ("remote", "anywhere", "global", "worldwide")):
            return 1.0
        if not candidate_location:
            return 0.15

        if self.is_preferred_location(job_location=job_location, candidate_location=candidate_location):
            return 1.0
        return 0.15

    def is_preferred_location(self, job_location: str | None, candidate_location: str | None) -> bool:
        if not job_location:
            return True
        jl = str(job_location or "").strip().lower()
        if any(token in jl for token in ("remote", "anywhere", "global", "worldwide")):
            return True
        if not candidate_location:
            return False
        cl = str(candidate_location or "").strip().lower()
        if jl in cl or cl in jl:
            return True
        job_tokens = self._location_tokens(jl)
        cand_tokens = self._location_tokens(cl)
        return bool(job_tokens.intersection(cand_tokens))

    def _language_match(self, preferred_languages: List[str] | None, candidate_languages: List[str] | None) -> float:
        if not preferred_languages:
            return 1.0
        if not candidate_languages:
            return 0.3

        preferred = {x.lower() for x in preferred_languages}
        candidate = {x.lower() for x in candidate_languages}
        overlap = preferred.intersection(candidate)
        if overlap:
            return 1.0
        return 0.3

    def _field_is_missing(self, profile: Dict[str, Any], field: str, candidate_skills: Set[str]) -> bool:
        if field == "skills":
            return len(candidate_skills) == 0
        if field == "years_experience":
            value = profile.get(field)
            return value is None or value == ""
        value = profile.get(field)
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        if isinstance(value, list):
            return len(value) == 0
        return False

    def _candidate_skills(self, profile: Dict[str, Any]) -> Set[str]:
        skills = {s.lower() for s in profile.get("skills", []) if isinstance(s, str) and s.strip()}
        if skills:
            return skills
        headline = str(profile.get("headline") or "").lower()
        dictionary = [s.lower() for s in self.rules.get("skill_dictionary", [])]
        inferred = {skill for skill in dictionary if skill in headline}
        return inferred

    def _extract_jd_keywords(self, jd_text: str, max_items: int = 6) -> List[str]:
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
            "experience",
            "years",
            "strong",
            "good",
            "excellent",
        }
        tokens = [x.strip(".,:;()[]{}!?") for x in jd_text.split()]
        result: List[str] = []
        seen: Set[str] = set()
        for token in tokens:
            if len(token) < 3:
                continue
            if token in stopwords:
                continue
            if token in seen:
                continue
            seen.add(token)
            result.append(token)
            if len(result) >= max_items:
                break
        return result

    @staticmethod
    def _split_jd_fragments(jd_text: str) -> List[str]:
        parts = re.split(r"[\n\r•]+|(?<=[.!?])\s+", jd_text.lower())
        return [part.strip(" -:\t") for part in parts if part and part.strip(" -:\t")]

    def _extract_requirement_scope(self, jd_text: str, title: str = "") -> str:
        required_markers = (
            "requirements",
            "qualification",
            "must have",
            "what you'll need",
            "what you will need",
            "you have",
            "responsibilities",
            "what you'll do",
            "what you will do",
            "experience with",
            "hands-on",
            "required",
        )
        nice_markers = ("nice to have", "preferred", "bonus", "plus", "good to have")
        ignore_markers = (
            "about us",
            "about tener",
            "company",
            "our platform",
            "who we are",
            "recruiting platform",
            "why join",
            "benefits",
            "culture",
        )
        fragments = self._split_jd_fragments(jd_text)
        required_fragments: List[str] = []
        neutral_fragments: List[str] = []
        current_section = "neutral"
        for fragment in fragments:
            if any(marker in fragment for marker in ignore_markers):
                current_section = "ignore"
                continue
            if any(marker in fragment for marker in nice_markers):
                current_section = "nice"
                continue
            if any(marker in fragment for marker in required_markers):
                current_section = "required"
                required_fragments.append(fragment)
                continue
            if current_section == "required":
                required_fragments.append(fragment)
                continue
            if current_section == "nice" or current_section == "ignore":
                continue
            if any(token in fragment for token in ("must", "required", "experience with", "testing", "qa", "quality assurance")):
                required_fragments.append(fragment)
            else:
                neutral_fragments.append(fragment)
        scope = " ".join(required_fragments).strip()
        if scope:
            return scope
        fallback = " ".join(neutral_fragments).strip()
        title_hint = str(title or "").strip().lower()
        return f"{title_hint} {fallback}".strip()

    def _extract_nice_to_have_scope(self, jd_text: str, title: str = "") -> str:
        nice_markers = ("nice to have", "preferred", "bonus", "plus", "good to have")
        fragments = self._split_jd_fragments(jd_text)
        nice_fragments: List[str] = []
        current_section = "neutral"
        for fragment in fragments:
            if any(marker in fragment for marker in nice_markers):
                current_section = "nice"
                nice_fragments.append(fragment)
                continue
            if current_section == "nice":
                nice_fragments.append(fragment)
        scope = " ".join(nice_fragments).strip()
        if scope:
            return scope
        return self._fallback_requirement_text(jd_text=jd_text, title=title)

    def _fallback_requirement_text(self, jd_text: str, title: str = "") -> str:
        title_hint = str(title or "").strip().lower()
        requirement_scope = self._extract_requirement_scope(jd_text=jd_text, title=title)
        return f"{title_hint} {requirement_scope}".strip()

    @staticmethod
    def _location_tokens(value: str) -> Set[str]:
        raw_parts = [part.strip() for part in re.split(r"[,/|-]", value) if part.strip()]
        tokens: Set[str] = set(raw_parts)
        region_aliases = {
            "eastern europe": {
                "eastern europe",
                "romania",
                "ukraine",
                "poland",
                "czech republic",
                "czechia",
                "slovakia",
                "hungary",
                "bulgaria",
                "moldova",
                "estonia",
                "latvia",
                "lithuania",
                "croatia",
                "serbia",
                "slovenia",
                "bosnia",
                "montenegro",
                "north macedonia",
                "albania",
                "georgia",
                "armenia",
            },
        }
        for region, aliases in region_aliases.items():
            if region in value:
                tokens.update(aliases)
        return {token for token in tokens if token}

    def _load_rules(self) -> Dict[str, Any]:
        path = Path(self.rules_path)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
