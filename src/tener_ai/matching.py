from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Set


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

        missing_fields = [field for field in rules.get("mandatory_fields", []) if not profile.get(field)]
        if missing_fields:
            return MatchResult(
                score=0.0,
                status="rejected",
                notes={"reason": "missing_mandatory_fields", "missing_fields": missing_fields},
            )

        jd_text = (job.get("jd_text") or "").lower()
        required_skills = self._extract_required_skills(jd_text)
        candidate_skills = {s.lower() for s in profile.get("skills", []) if isinstance(s, str)}

        if not required_skills:
            # If no skills are detected in JD, the role is broad; do not penalize heavily.
            skills_match = 0.6
            matched_skills = []
        else:
            matched_skills = sorted(required_skills.intersection(candidate_skills))
            skills_match = len(matched_skills) / len(required_skills)

        target_seniority = (job.get("seniority") or self._infer_seniority(jd_text) or "middle").lower()
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
            "required_skills": sorted(required_skills),
            "matched_skills": matched_skills,
            "target_seniority": target_seniority,
            "candidate_years_experience": years,
            "components": {
                "skills_match": round(skills_match, 3),
                "seniority_match": round(seniority_match, 3),
                "location_match": round(location_match, 3),
                "language_match": round(language_match, 3),
            },
            "min_score": rules.get("min_score", 0.65),
            "rules_version": rules.get("version", "unknown"),
        }
        return MatchResult(score=round(score, 3), status=status, notes=notes)

    def summarize_scope(self, job: Dict[str, Any], max_items: int = 5) -> str:
        jd_text = (job.get("jd_text") or "").lower()
        skills = sorted(self._extract_required_skills(jd_text))[:max_items]
        if skills:
            return ", ".join(skills)
        title = job.get("title") or "the role"
        return f"key requirements from {title}"

    def _extract_required_skills(self, jd_text: str) -> Set[str]:
        dictionary = [s.lower() for s in self.rules.get("skill_dictionary", [])]
        found = {skill for skill in dictionary if skill in jd_text}
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
        if years + 1 >= min_years:
            return 0.7
        return 0.3

    def _location_match(self, job_location: str | None, candidate_location: str | None) -> float:
        if not job_location:
            return 1.0
        if not candidate_location:
            return 0.4

        jl = job_location.lower()
        cl = candidate_location.lower()
        if jl in cl or cl in jl:
            return 1.0

        job_parts = {x.strip() for x in jl.replace("/", ",").split(",") if x.strip()}
        cand_parts = {x.strip() for x in cl.replace("/", ",").split(",") if x.strip()}
        return 0.8 if job_parts.intersection(cand_parts) else 0.4

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

    def _load_rules(self) -> Dict[str, Any]:
        path = Path(self.rules_path)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
