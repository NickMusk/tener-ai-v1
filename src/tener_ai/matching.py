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
            explanation = (
                "Кандидат отклонен: в профиле не хватает обязательных полей "
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
        notes["human_explanation"] = self._build_human_explanation(
            status=status,
            score=round(score, 3),
            min_score=float(rules.get("min_score", 0.65)),
            required_skills=sorted(required_skills),
            matched_skills=matched_skills,
            target_seniority=target_seniority,
            years=years,
            location_match=round(location_match, 3),
            language_match=round(language_match, 3),
        )
        return MatchResult(score=round(score, 3), status=status, notes=notes)

    def _build_human_explanation(
        self,
        status: str,
        score: float,
        min_score: float,
        required_skills: List[str],
        matched_skills: List[str],
        target_seniority: str,
        years: int,
        location_match: float,
        language_match: float,
    ) -> str:
        skills_total = len(required_skills)
        skills_matched = len(matched_skills)
        skills_part = (
            f"скиллы: {skills_matched}/{skills_total}"
            if skills_total
            else "скиллы: в JD не выделены, компонент смягчен"
        )
        seniority_part = f"seniority: целевой уровень {target_seniority}, у кандидата {years} лет опыта"
        location_part = f"локация: коэффициент {location_match}"
        language_part = f"языки: коэффициент {language_match}"

        if status == "verified":
            return (
                f"Кандидат верифицирован: итоговый score {score} выше порога {min_score}. "
                f"Факторы: {skills_part}; {seniority_part}; {location_part}; {language_part}."
            )

        gaps: List[str] = []
        if skills_total and skills_matched == 0:
            gaps.append("не совпали обязательные навыки из JD")
        elif skills_total and skills_matched < skills_total:
            missing = [s for s in required_skills if s not in set(matched_skills)]
            if missing:
                gaps.append("не хватает навыков: " + ", ".join(missing[:5]))

        if years <= 1 and target_seniority in {"middle", "senior", "lead"}:
            gaps.append("недостаточный опыт для целевого seniority")
        if location_match <= 0.4:
            gaps.append("слабое совпадение по локации")
        if language_match <= 0.3:
            gaps.append("нет совпадения по предпочтительным языкам")

        gaps_text = "; ".join(gaps) if gaps else "ключевые компоненты дали score ниже порога"
        return (
            f"Кандидат отклонен: итоговый score {score} ниже порога {min_score}. "
            f"Причины: {gaps_text}. "
            f"Факторы: {skills_part}; {seniority_part}; {location_part}; {language_part}."
        )

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
