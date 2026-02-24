from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_GUIDELINES: Dict[str, Any] = {
    "version": "1.0",
    "defaults": {
        "question_count": 10,
        "time_to_answer": 120,
        "time_to_think": 12,
        "retakes": 1,
        "category_targets": {
            "hard_skills": 0.4,
            "soft_skills": 0.3,
            "cultural_fit": 0.3,
        },
    },
    "company_values": [
        "clear communication",
        "ownership",
        "collaboration",
    ],
    "skill_dictionary": [
        "python",
        "java",
        "javascript",
        "typescript",
        "go",
        "rust",
        "sql",
        "aws",
        "gcp",
        "azure",
        "docker",
        "kubernetes",
        "ml",
        "machine learning",
        "llm",
        "nlp",
    ],
}


class InterviewQuestionGenerator:
    def __init__(self, *, guidelines_path: str, company_profile_path: str, company_name: str) -> None:
        self.guidelines_path = guidelines_path
        self.company_profile_path = company_profile_path
        self.guidelines = self._load_guidelines(guidelines_path)
        self.company_profile = self._load_company_profile(company_profile_path)
        profile_company_name = str(self.company_profile.get("company_name") or "").strip()
        self.company_name = company_name.strip() or profile_company_name or "Tener"

    def generate_for_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        title = str(job.get("title") or "Open Role").strip()
        jd_text = str(job.get("jd_text") or "").strip()
        values = self._company_values()
        top_skills = self._extract_skills(jd_text)

        defaults = self.guidelines.get("defaults") if isinstance(self.guidelines.get("defaults"), dict) else {}
        time_to_answer = max(30, int(self._to_int(defaults.get("time_to_answer"), 120)))
        time_to_think = max(5, int(self._to_int(defaults.get("time_to_think"), 12)))
        retakes = max(0, int(self._to_int(defaults.get("retakes"), 1)))

        mission = str(self.company_profile.get("mission") or "").strip()
        if not mission:
            mission = f"At {self.company_name}, we build teams that deliver measurable impact."
        mission_short = mission[:180].rstrip()

        skills_text = ", ".join(top_skills) if top_skills else "your core technical domain"
        value_primary = values[0] if values else "clear communication"
        value_secondary = values[1] if len(values) > 1 else "ownership"

        desired_count = max(3, min(int(self._to_int(defaults.get("question_count"), 10)), 20))
        category_plan = self._build_category_plan(defaults=defaults, total_questions=desired_count)
        category_index: Dict[str, int] = {"hard_skills": 0, "soft_skills": 0, "cultural_fit": 0}
        questions: List[Dict[str, Any]] = []
        for category in category_plan:
            category_index[category] = category_index.get(category, 0) + 1
            idx = category_index[category]
            if category == "hard_skills":
                item = self._hard_skills_question(
                    index=idx,
                    company_name=self.company_name,
                    job_title=title,
                    skills_text=skills_text,
                )
            elif category == "cultural_fit":
                item = self._cultural_fit_question(
                    index=idx,
                    company_name=self.company_name,
                    mission_short=mission_short,
                    value_primary=value_primary,
                )
            else:
                item = self._soft_skills_question(
                    index=idx,
                    company_name=self.company_name,
                    value_secondary=value_secondary,
                )
            item["category"] = category
            item["timeToAnswer"] = time_to_answer
            item["timeToThink"] = time_to_think
            item["retakes"] = retakes
            questions.append(item)

        payload = {
            "version": str(self.guidelines.get("version") or "1.0"),
            "company_name": self.company_name,
            "job_id": job.get("id"),
            "job_title": title,
            "questions": questions,
            "company_profile": self.company_profile,
        }
        generation_hash = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        return {
            "assessment_name": f"{self.company_name} - {title} Interview",
            "questions": questions,
            "generation_hash": generation_hash,
            "meta": {
                "guidelines_version": str(self.guidelines.get("version") or "1.0"),
                "skills_detected": top_skills,
                "company_values": values,
                "categories": self._count_categories(questions),
            },
        }

    @staticmethod
    def _build_category_plan(*, defaults: Dict[str, Any], total_questions: int) -> List[str]:
        targets = defaults.get("category_targets") if isinstance(defaults.get("category_targets"), dict) else {}
        keys = ["hard_skills", "soft_skills", "cultural_fit"]
        weights: Dict[str, float] = {}
        for key in keys:
            raw = targets.get(key)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = 0.0
            weights[key] = max(0.0, value)
        if sum(weights.values()) <= 0.0:
            weights = {"hard_skills": 0.4, "soft_skills": 0.3, "cultural_fit": 0.3}

        allocated = {k: 0 for k in keys}
        for key in keys:
            allocated[key] = int(total_questions * (weights[key] / sum(weights.values())))

        remaining = total_questions - sum(allocated.values())
        order = sorted(keys, key=lambda k: weights[k], reverse=True)
        cursor = 0
        while remaining > 0:
            pick = order[cursor % len(order)]
            allocated[pick] += 1
            remaining -= 1
            cursor += 1

        plan: List[str] = []
        while len(plan) < total_questions:
            for key in keys:
                if allocated[key] > 0:
                    plan.append(key)
                    allocated[key] -= 1
                    if len(plan) >= total_questions:
                        break
        return plan

    @staticmethod
    def _hard_skills_question(*, index: int, company_name: str, job_title: str, skills_text: str) -> Dict[str, Any]:
        prompts = [
            (
                f"[Hard Skills] At {company_name}, describe the most complex technical problem you solved relevant to {job_title}.",
                "Include constraints, architecture choices, and measurable impact."
            ),
            (
                f"[Hard Skills] For {company_name}, explain how you would design and scale a solution using {skills_text}.",
                "Walk through trade-offs, reliability, performance, and security decisions."
            ),
            (
                f"[Hard Skills] In {company_name}, how do you debug and stabilize production issues in your technical stack?",
                "Share a concrete incident, root cause analysis, and prevention actions."
            ),
            (
                f"[Hard Skills] For {company_name}, tell us about a code quality or architecture improvement you led.",
                "Explain baseline metrics, actions you took, and resulting improvements."
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    @staticmethod
    def _soft_skills_question(*, index: int, company_name: str, value_secondary: str) -> Dict[str, Any]:
        prompts = [
            (
                f"[Soft Skills] At {company_name}, we value {value_secondary}. Tell us about a high-stakes cross-functional collaboration.",
                "Describe your communication strategy, stakeholder alignment, and outcome."
            ),
            (
                f"[Soft Skills] In {company_name}, how do you handle disagreement with product or engineering stakeholders?",
                "Use a real example and explain how you moved the team to a decision."
            ),
            (
                f"[Soft Skills] For {company_name}, describe a time you gave or received difficult feedback.",
                "Focus on your approach, behavior change, and impact on team performance."
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    @staticmethod
    def _cultural_fit_question(*, index: int, company_name: str, mission_short: str, value_primary: str) -> Dict[str, Any]:
        prompts = [
            (
                f"[Cultural Fit] At {company_name}, why does our mission resonate with you?",
                f"Reference this mission in your answer: {mission_short}"
            ),
            (
                f"[Cultural Fit] In {company_name}, we value {value_primary}. Share a situation where you embodied this value.",
                "Explain decisions you made and how they aligned with company culture."
            ),
            (
                f"[Cultural Fit] For {company_name}, what type of team culture helps you deliver your best work?",
                "Be specific about behaviors, accountability, and collaboration norms."
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    @staticmethod
    def _count_categories(questions: List[Dict[str, Any]]) -> Dict[str, int]:
        out: Dict[str, int] = {"hard_skills": 0, "soft_skills": 0, "cultural_fit": 0}
        for question in questions:
            category = str(question.get("category") or "").strip().lower()
            if category in out:
                out[category] += 1
        return out

    def _company_values(self) -> List[str]:
        profile_values = self.company_profile.get("values")
        out = self._to_str_list(profile_values)
        if out:
            return out
        default_values = self.guidelines.get("company_values")
        return self._to_str_list(default_values) or ["clear communication", "ownership", "collaboration"]

    def _extract_skills(self, jd_text: str, max_items: int = 4) -> List[str]:
        dictionary = self._to_str_list(self.guidelines.get("skill_dictionary"))
        text = jd_text.lower()
        found: List[str] = []
        for token in dictionary:
            if token in text and token not in found:
                found.append(token)
                if len(found) >= max_items:
                    break
        if found:
            return found

        # Fallback: pick 3-4 meaningful words from JD.
        raw_tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]{2,}", text)
        stopwords = {
            "and",
            "the",
            "for",
            "with",
            "from",
            "that",
            "this",
            "you",
            "are",
            "our",
            "team",
            "role",
            "will",
            "have",
            "experience",
            "years",
            "required",
            "preferred",
        }
        clean: List[str] = []
        for token in raw_tokens:
            if token in stopwords:
                continue
            if token in clean:
                continue
            clean.append(token)
            if len(clean) >= max_items:
                break
        return clean

    @staticmethod
    def _load_guidelines(path: str) -> Dict[str, Any]:
        base = dict(DEFAULT_GUIDELINES)
        if not path:
            return base
        file_path = Path(path)
        if not file_path.exists():
            return base
        try:
            loaded = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return base
        if not isinstance(loaded, dict):
            return base
        out = dict(base)
        for key, value in loaded.items():
            out[key] = value
        return out

    @staticmethod
    def _load_company_profile(path: str) -> Dict[str, Any]:
        if not path:
            return {}
        file_path = Path(path)
        if not file_path.exists():
            return {}
        try:
            loaded = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return loaded if isinstance(loaded, dict) else {}

    @staticmethod
    def _to_str_list(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        out: List[str] = []
        for item in value:
            token = str(item or "").strip()
            if token:
                out.append(token)
        return out

    @staticmethod
    def _to_int(value: Any, fallback: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(fallback)
