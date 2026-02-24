from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_GUIDELINES: Dict[str, Any] = {
    "version": "1.0",
    "defaults": {
        "question_count": 3,
        "time_to_answer": 120,
        "time_to_think": 12,
        "retakes": 1,
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

        questions = [
            {
                "title": f"At {self.company_name}, we value {value_primary}. Tell us about your most relevant project for the {title} role.",
                "description": (
                    f"Please include context, your role, concrete actions, and measurable outcomes. "
                    f"Connect your story to {self.company_name}'s mission: {mission_short}"
                ),
                "timeToAnswer": time_to_answer,
                "timeToThink": time_to_think,
                "retakes": retakes,
            },
            {
                "title": f"For {self.company_name}, describe a technically challenging problem involving {skills_text}.",
                "description": (
                    "Explain the trade-offs you considered, why you chose your solution, "
                    "and the business or user impact of the final result."
                ),
                "timeToAnswer": time_to_answer,
                "timeToThink": time_to_think,
                "retakes": retakes,
            },
            {
                "title": f"In {self.company_name}, we value {value_secondary}. How do you collaborate across teams under pressure?",
                "description": (
                    "Give a real example of communication with stakeholders, conflict handling, "
                    "and how you kept delivery quality high."
                ),
                "timeToAnswer": time_to_answer,
                "timeToThink": time_to_think,
                "retakes": retakes,
            },
        ]

        desired_count = max(1, min(int(self._to_int(defaults.get("question_count"), 3)), 8))
        questions = questions[:desired_count]

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
            },
        }

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
