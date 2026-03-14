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

QA_GENERIC_ROLE_SKILLS = {"qa", "quality assurance"}

QA_SKILL_ALIASES = {
    "manual testing": [
        ("manual testing",),
        ("manual functional",),
        ("exploratory testing",),
        ("web application testing",),
    ],
    "api testing": [
        ("api testing",),
        ("rest api",),
        ("rest apis",),
        ("apis", "test"),
    ],
    "regression testing": [
        ("regression testing",),
        ("regression",),
    ],
    "bug reporting": [
        ("bug reporting",),
        ("bug reports",),
        ("reproduce bugs",),
    ],
    "test case design": [
        ("test case design",),
        ("test cases",),
        ("test scenarios",),
    ],
    "automation testing": [
        ("automation testing",),
        ("automation tools",),
    ],
}

QA_SKILL_PRIORITY = [
    "manual testing",
    "api testing",
    "regression testing",
    "test case design",
    "bug reporting",
    "bug triage",
    "jira",
    "postman",
    "automation testing",
    "sql",
    "selenium",
    "playwright",
    "cypress",
    "qa",
    "quality assurance",
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
            weak_matched_skills: List[str] = []
            skill_evidence: Dict[str, str] = {}
        else:
            skill_match = self._match_skills_with_evidence(profile=profile, skills=required_skills)
            matched_skills = skill_match["strong_matches"]
            weak_matched_skills = skill_match["weak_matches"]
            skill_evidence = skill_match["evidence"]
            effective_required_count = min(len(required_skills), 6)
            must_have_match = min(1.0, float(skill_match["weight"]) / effective_required_count)

        nice_match = self._match_skills_with_evidence(profile=profile, skills=nice_to_have_skills)
        matched_nice_to_have = nice_match["strong_matches"]
        weak_matched_nice_to_have = nice_match["weak_matches"]
        nice_to_have_match = (
            min(1.0, float(nice_match["weight"]) / max(1, len(nice_to_have_skills)))
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
            "weak_skill_matches": weak_matched_skills,
            "skill_evidence": skill_evidence,
            "matched_nice_to_have_skills": matched_nice_to_have,
            "weak_nice_to_have_matches": weak_matched_nice_to_have,
            "nice_to_have_evidence": nice_match["evidence"],
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
        evidence_summary = self._format_skill_evidence_summary(
            strong_matches=matched_skills,
            weak_matches=weak_matched_skills,
            evidence=skill_evidence,
        )
        if evidence_summary:
            notes["human_explanation"] = f"{notes['human_explanation']} Evidence: {evidence_summary}."
        return MatchResult(score=round(score, 3), status=status, notes=notes)

    def match_skills_with_evidence(self, *, profile: Dict[str, Any], skills: List[str]) -> Dict[str, Any]:
        return self._match_skills_with_evidence(profile=profile, skills=skills)

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
            if extracted_required:
                must_have_skills = extracted_required[:max_must_have]
            elif role_family == "qa":
                must_have_skills = []
            else:
                must_have_skills = self._extract_jd_keywords(
                    self._fallback_requirement_text(jd_text=jd_text, title=title),
                    max_items=max_must_have,
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
            return self._ordered_role_skills(
                exact_skills=found,
                alias_skills=self._extract_role_alias_skills(requirement_text, role_family=role_family),
                priority=QA_SKILL_PRIORITY,
                drop_generic_only=True,
            )
        return found

    def _extract_nice_to_have_skills(self, jd_text: str, title: str = "", max_items: int = 12) -> List[str]:
        role_family = self._infer_role_family(title=title, jd_text=jd_text)
        dictionary = self._skill_dictionary_for_role(role_family)
        nice_scope = self._extract_nice_to_have_scope(jd_text=jd_text, title=title)
        if not nice_scope:
            return []
        found = self._extract_known_skills(nice_scope, dictionary)
        if role_family == "qa":
            ordered = self._ordered_role_skills(
                exact_skills=found,
                alias_skills=self._extract_role_alias_skills(nice_scope, role_family=role_family),
                priority=QA_NICE_TO_HAVE_SKILLS + QA_SKILL_PRIORITY,
            )
            ordered = [skill for skill in ordered if skill in QA_NICE_TO_HAVE_SKILLS]
            if ordered:
                return ordered[:max_items]
        if len(found) >= max_items:
            return found[:max_items]
        if found:
            return found[:max_items]
        return []

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

    def _extract_role_alias_skills(self, text: str, *, role_family: str | None) -> List[str]:
        haystack = str(text or "").lower()
        if role_family != "qa" or not haystack:
            return []
        out: List[str] = []
        for skill, aliases in QA_SKILL_ALIASES.items():
            for alias_tokens in aliases:
                if all(token in haystack for token in alias_tokens):
                    out.append(skill)
                    break
        return out

    @staticmethod
    def _ordered_role_skills(
        *,
        exact_skills: List[str],
        alias_skills: List[str],
        priority: List[str],
        drop_generic_only: bool = False,
    ) -> List[str]:
        combined = {str(skill or "").strip().lower() for skill in [*exact_skills, *alias_skills] if str(skill or "").strip()}
        if not combined:
            return []
        ordered: List[str] = []
        seen: Set[str] = set()
        for skill in priority:
            if skill not in combined or skill in seen:
                continue
            seen.add(skill)
            ordered.append(skill)
        extras = [skill for skill in sorted(combined) if skill not in ordered]
        out = ordered + extras
        if drop_generic_only:
            specific = [skill for skill in out if skill not in QA_GENERIC_ROLE_SKILLS]
            return specific or []
        return out

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
        dictionary = [s.lower() for s in self.rules.get("skill_dictionary", [])]
        evidence = self._match_skills_with_evidence(profile=profile, skills=dictionary)
        inferred = {skill.lower() for skill in evidence["strong_matches"] + evidence["weak_matches"]}
        return skills | inferred

    def _match_skills_with_evidence(self, *, profile: Dict[str, Any], skills: List[str]) -> Dict[str, Any]:
        if not skills:
            return {"weight": 0.0, "strong_matches": [], "weak_matches": [], "evidence": {}}

        buckets = self._profile_text_buckets(profile)
        skill_set = {s.lower() for s in profile.get("skills", []) if isinstance(s, str) and s.strip()}
        evidence: Dict[str, str] = {}
        strong_matches: List[str] = []
        weak_matches: List[str] = []
        total_weight = 0.0

        source_order = (
            ("summary", 1.0),
            ("experience", 1.0),
            ("headline", 0.75),
            ("article", 0.6),
            ("generic", 0.5),
        )
        for skill in skills:
            normalized_skill = str(skill or "").strip().lower()
            if not normalized_skill:
                continue

            matched_source = ""
            matched_weight = 0.0
            for source_name, source_weight in source_order:
                if self._text_contains_term(buckets.get(source_name, ""), normalized_skill):
                    matched_source = source_name
                    matched_weight = source_weight
                    break

            if matched_source:
                evidence[normalized_skill] = matched_source
                if matched_weight >= 0.75:
                    strong_matches.append(normalized_skill)
                else:
                    weak_matches.append(normalized_skill)
                total_weight += matched_weight
                continue

            if normalized_skill in skill_set:
                evidence[normalized_skill] = "skills"
                weak_matches.append(normalized_skill)
                total_weight += 0.5

        return {
            "weight": total_weight,
            "strong_matches": strong_matches,
            "weak_matches": weak_matches,
            "evidence": evidence,
        }

    def _profile_text_buckets(self, profile: Dict[str, Any]) -> Dict[str, str]:
        buckets: Dict[str, List[str]] = {
            "summary": [],
            "experience": [],
            "headline": [],
            "article": [],
            "generic": [],
        }
        headline = str(profile.get("headline") or "").strip()
        if headline:
            buckets["headline"].append(headline)

        for key in ("summary", "about", "bio", "description"):
            value = str(profile.get(key) or "").strip()
            if value:
                target = "summary" if key in {"summary", "about", "bio"} else "generic"
                buckets[target].append(value)

        raw = profile.get("raw")
        if isinstance(raw, dict):
            self._collect_profile_text_buckets(raw, buckets=buckets, path=[])

        return {bucket: " ".join(values).lower() for bucket, values in buckets.items() if values}

    def _collect_profile_text_buckets(self, payload: Any, *, buckets: Dict[str, List[str]], path: List[str]) -> None:
        if isinstance(payload, dict):
            for key, value in payload.items():
                self._collect_profile_text_buckets(value, buckets=buckets, path=path + [str(key).lower()])
            return
        if isinstance(payload, list):
            for item in payload:
                self._collect_profile_text_buckets(item, buckets=buckets, path=path)
            return
        if not isinstance(payload, str):
            return

        text = payload.strip()
        if not text:
            return

        bucket = self._classify_profile_text_bucket(path)
        if bucket:
            buckets[bucket].append(text)

    @staticmethod
    def _classify_profile_text_bucket(path: List[str]) -> str | None:
        joined = " ".join(path)
        if any(token in joined for token in ("skill", "endorsement")):
            return None
        if any(token in joined for token in ("summary", "about", "bio")):
            return "summary"
        if any(token in joined for token in ("experience", "position", "employment", "occupation", "work_history", "description")):
            return "experience"
        if any(token in joined for token in ("article", "post", "publication")):
            return "article"
        if "headline" in joined:
            return "headline"
        if any(token in joined for token in ("search", "detail", "raw", "profile", "data")):
            return "generic"
        return None

    @staticmethod
    def _text_contains_term(text: str, term: str) -> bool:
        normalized_text = str(text or "").lower()
        normalized_term = str(term or "").strip().lower()
        if not normalized_text or not normalized_term:
            return False
        pattern = re.sub(r"\s+", r"\\s+", re.escape(normalized_term))
        return re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", normalized_text) is not None

    @staticmethod
    def _format_skill_evidence_summary(
        *,
        strong_matches: List[str],
        weak_matches: List[str],
        evidence: Dict[str, str],
    ) -> str:
        parts: List[str] = []
        if strong_matches:
            strong_sources = ", ".join(f"{skill} via {evidence.get(skill, 'text')}" for skill in strong_matches[:4])
            parts.append(f"text-backed matches: {strong_sources}")
        weak_skills_only = [skill for skill in weak_matches if evidence.get(skill) == "skills"]
        if weak_skills_only:
            parts.append("skills-only signals: " + ", ".join(weak_skills_only[:4]))
        return "; ".join(parts)

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
            if current_section == "nice" or current_section == "ignore":
                continue
            if current_section == "required":
                required_fragments.append(fragment)
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
            if any(
                marker in fragment
                for marker in (
                    "requirements",
                    "qualification",
                    "responsibilities",
                    "what you'll do",
                    "what you will do",
                    "what you'll work on",
                    "what you will work on",
                    "location",
                    "why join",
                )
            ):
                current_section = "neutral"
                continue
            if current_section == "nice":
                nice_fragments.append(fragment)
        scope = " ".join(nice_fragments).strip()
        return scope

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
