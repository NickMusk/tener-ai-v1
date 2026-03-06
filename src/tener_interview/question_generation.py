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
        "golang",
        "rust",
        "sql",
        "aws",
        "gcp",
        "azure",
        "docker",
        "kubernetes",
        "postman",
        "selenium",
        "playwright",
        "cypress",
        "manual testing",
        "api testing",
        "regression testing",
        "test case design",
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
        job_company_name = str(job.get("company") or "").strip()
        company_name = job_company_name or self.company_name

        job_culture_profile = (
            job.get("company_culture_profile")
            if isinstance(job.get("company_culture_profile"), dict)
            else {}
        )
        values = self._company_values(job_culture_profile=job_culture_profile)
        culture_questions = self._culture_interview_questions(job_culture_profile=job_culture_profile)
        top_skills = self._extract_skills(jd_text, max_items=6)

        defaults = self.guidelines.get("defaults") if isinstance(self.guidelines.get("defaults"), dict) else {}
        time_to_answer = max(30, int(self._to_int(defaults.get("time_to_answer"), 120)))
        time_to_think = max(5, int(self._to_int(defaults.get("time_to_think"), 12)))
        retakes = max(0, int(self._to_int(defaults.get("retakes"), 1)))

        mission = str(job_culture_profile.get("summary_200_300_words") or "").strip()
        if not mission:
            mission = str((job_culture_profile.get("mission_orientation") or {}).get("assessment") or "").strip()
        if not mission:
            mission = str(self.company_profile.get("mission") or "").strip()
        if not mission:
            mission = f"At {company_name}, we build teams that deliver measurable impact."
        mission_short = mission[:220].rstrip()

        context = self._derive_job_context(
            title=title,
            jd_text=jd_text,
            skills=top_skills,
            company_name=company_name,
            values=values,
            job_culture_profile=job_culture_profile,
        )

        desired_count = max(3, min(int(self._to_int(defaults.get("question_count"), 10)), 20))
        category_plan = self._build_category_plan(defaults=defaults, total_questions=desired_count)

        category_index: Dict[str, int] = {"hard_skills": 0, "soft_skills": 0, "cultural_fit": 0}
        questions: List[Dict[str, Any]] = []
        seen_titles: Dict[str, int] = {}

        for category in category_plan:
            category_index[category] = category_index.get(category, 0) + 1
            idx = category_index[category]

            if category == "hard_skills":
                item = self._hard_skills_question(
                    index=idx,
                    company_name=company_name,
                    job_title=title,
                    context=context,
                )
            elif category == "cultural_fit":
                item = self._cultural_fit_question(
                    index=idx,
                    company_name=company_name,
                    mission_short=mission_short,
                    value_primary=values[0] if values else "clear communication",
                    profile_question=culture_questions[idx - 1] if idx - 1 < len(culture_questions) else None,
                    context=context,
                )
            else:
                item = self._soft_skills_question(
                    index=idx,
                    company_name=company_name,
                    value_secondary=values[1] if len(values) > 1 else "ownership",
                    context=context,
                )

            if not self._question_is_relevant(item=item, category=category, context=context):
                item = self._fallback_question(category=category, company_name=company_name, job_title=title, context=context)

            # Keep output deterministic while avoiding duplicate titles.
            title_key = re.sub(r"\s+", " ", str(item.get("title") or "").strip().lower())
            seen_titles[title_key] = seen_titles.get(title_key, 0) + 1
            if seen_titles[title_key] > 1:
                item["title"] = f"{str(item.get('title') or '').rstrip()} (Scenario {seen_titles[title_key]})"

            item["category"] = category
            item["timeToAnswer"] = time_to_answer
            item["timeToThink"] = time_to_think
            item["retakes"] = retakes
            questions.append(item)

        payload = {
            "version": str(self.guidelines.get("version") or "1.0"),
            "company_name": company_name,
            "job_id": job.get("id"),
            "job_title": title,
            "questions": questions,
            "company_profile": self.company_profile,
            "role_family": context.get("role_family"),
            "jd_highlights": context.get("highlights"),
        }
        generation_hash = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        return {
            "assessment_name": f"{company_name} - {title} Interview",
            "questions": questions,
            "generation_hash": generation_hash,
            "meta": {
                "guidelines_version": str(self.guidelines.get("version") or "1.0"),
                "company_name": company_name,
                "skills_detected": top_skills,
                "company_values": values,
                "culture_profile_source": "job" if job_culture_profile else "default",
                "categories": self._count_categories(questions),
                "role_family": context.get("role_family"),
                "jd_highlights": context.get("highlights") or [],
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

    def _hard_skills_question(
        self,
        *,
        index: int,
        company_name: str,
        job_title: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        role_family = str(context.get("role_family") or "general")
        primary_req = str(context.get("primary_requirement") or f"the core scope of {job_title}")
        primary_workflow = str(context.get("primary_workflow") or "your highest-risk workflow")
        competency = self._pick_context_item(context.get("competencies"), index, fallback="core skills")
        culture_trait = str(context.get("culture_trait") or "high quality under delivery pressure")
        architecture_emphasis = bool(context.get("architecture_emphasis"))

        if role_family == "qa":
            prompts = [
                (
                    f"[Hard Skills] For {company_name}, how would you build a manual test strategy for {primary_workflow}?",
                    f"Cover acceptance criteria, edge cases, and risk-based prioritization around {primary_req}.",
                ),
                (
                    f"[Hard Skills] Walk us through how you would validate API, UI, and data consistency for {primary_req}.",
                    "Describe concrete test cases, observability, and release gating decisions.",
                ),
                (
                    f"[Hard Skills] Tell us about a severe bug you handled in {competency}.",
                    "Explain reproduction, severity calibration, root-cause collaboration, and prevention steps.",
                ),
                (
                    f"[Hard Skills] In a culture that values {culture_trait}, what would you automate first and what would you keep manual?",
                    "Justify tradeoffs with expected defect detection impact and maintenance cost.",
                ),
            ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        if architecture_emphasis:
            prompts = [
                (
                    f"[Hard Skills] Describe the most complex technical problem you solved relevant to {job_title} at {company_name}.",
                    f"Ground your answer in this scope: {primary_req}.",
                ),
                (
                    f"[Hard Skills] For {company_name}, how would you design and scale {primary_workflow}?",
                    "Walk through reliability, performance, and security tradeoffs.",
                ),
                (
                    "[Hard Skills] How do you debug and stabilize production incidents in your core stack?",
                    "Share a real incident, root cause analysis, and long-term prevention changes.",
                ),
                (
                    "[Hard Skills] Tell us about a code quality or architecture improvement you led.",
                    "Include baseline metrics, intervention choices, and measurable outcomes.",
                ),
            ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        prompts = [
            (
                f"[Hard Skills] Walk us through how you delivered {primary_workflow} in a production environment.",
                f"Focus on implementation choices, constraints, and outcomes tied to {primary_req}.",
            ),
            (
                f"[Hard Skills] What is your approach to validating quality and reliability for {primary_req}?",
                "Explain concrete checks, rollout controls, and failure handling.",
            ),
            (
                f"[Hard Skills] Describe a deep technical challenge involving {competency}.",
                "Cover diagnostics, option analysis, and final resolution.",
            ),
            (
                f"[Hard Skills] In {company_name}, how would you improve delivery quality for {primary_workflow}?",
                "Prioritize high-impact technical improvements and explain why.",
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    def _soft_skills_question(
        self,
        *,
        index: int,
        company_name: str,
        value_secondary: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        role_family = str(context.get("role_family") or "general")
        primary_workflow = str(context.get("primary_workflow") or "cross-functional delivery")
        culture_trait = str(context.get("culture_trait") or value_secondary)

        if role_family == "qa":
            prompts = [
                (
                    f"[Soft Skills] In {company_name}, we value {value_secondary}. Describe a high-stakes bug triage conversation you led.",
                    "Explain how you aligned engineering and product on severity, timeline, and release decision.",
                ),
                (
                    f"[Soft Skills] Tell us about a time you pushed back on a release due to quality risk in {primary_workflow}.",
                    "Describe stakeholder management, evidence you used, and final outcome.",
                ),
                (
                    f"[Soft Skills] Our culture emphasizes {culture_trait}. How do you communicate test findings to drive action quickly?",
                    "Use one concrete example with message framing and impact.",
                ),
            ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        prompts = [
            (
                f"[Soft Skills] We value {value_secondary}. Tell us about a high-stakes cross-functional collaboration.",
                "Describe your communication strategy, stakeholder alignment, and measurable result.",
            ),
            (
                f"[Soft Skills] In {company_name}, how do you handle disagreement with product or engineering stakeholders?",
                "Use a real example and explain how you moved the team to a decision.",
            ),
            (
                f"[Soft Skills] Describe a time you gave or received difficult feedback while delivering {primary_workflow}.",
                "Focus on behavior change and impact on team performance.",
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    def _cultural_fit_question(
        self,
        *,
        index: int,
        company_name: str,
        mission_short: str,
        value_primary: str,
        profile_question: str | None,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        explicit = str(profile_question or "").strip()
        if explicit:
            return {
                "title": f"[Cultural Fit] {explicit}",
                "description": "Use a concrete experience and explain your reasoning.",
            }

        role_family = str(context.get("role_family") or "general")
        primary_workflow = str(context.get("primary_workflow") or "your core scope")
        culture_trait = str(context.get("culture_trait") or value_primary)

        if role_family == "qa":
            prompts = [
                (
                    f"[Cultural Fit] In {company_name}, how do you balance delivery speed and quality when testing critical releases?",
                    "Use one real example with your decision framework.",
                ),
                (
                    f"[Cultural Fit] We value {culture_trait}. Tell us how that shows up in your QA decision-making.",
                    "Describe a concrete tradeoff and why you chose that path.",
                ),
                (
                    f"[Cultural Fit] What team behaviors help you deliver your best work on {primary_workflow}?",
                    "Be specific about accountability, communication cadence, and escalation style.",
                ),
            ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        prompts = [
            (
                f"[Cultural Fit] At {company_name}, why does this mission resonate with you?",
                f"Reference this mission in your answer: {mission_short}",
            ),
            (
                f"[Cultural Fit] In {company_name}, we value {value_primary}. Share a situation where you embodied this value.",
                "Explain decisions you made and how they aligned with company culture.",
            ),
            (
                f"[Cultural Fit] For {company_name}, what type of team culture helps you deliver your best work?",
                "Be specific about behaviors, accountability, and collaboration norms.",
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    def _derive_job_context(
        self,
        *,
        title: str,
        jd_text: str,
        skills: List[str],
        company_name: str,
        values: List[str],
        job_culture_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        text = f"{title}\n{jd_text}".strip().lower()
        role_family = self._detect_role_family(text)
        sentences = self._collect_jd_sentences(jd_text)

        highlight_markers = [
            "must",
            "required",
            "responsib",
            "you will",
            "key",
            "ownership",
            "test",
            "qa",
            "automation",
            "api",
            "quality",
            "collaborat",
            "stakeholder",
            "bug",
            "incident",
            "release",
        ]
        highlights: List[str] = []
        for sentence in sentences:
            low = sentence.lower()
            if any(marker in low for marker in highlight_markers):
                highlights.append(self._compact_snippet(sentence))
            if len(highlights) >= 8:
                break
        if not highlights:
            highlights = [self._compact_snippet(sentence) for sentence in sentences[:4]]

        workflows: List[str] = []
        workflow_markers = [
            "test",
            "validate",
            "triage",
            "debug",
            "release",
            "build",
            "ship",
            "design",
            "review",
            "analyze",
            "document",
            "improve",
        ]
        for sentence in highlights:
            low = sentence.lower()
            if any(marker in low for marker in workflow_markers):
                workflows.append(self._compact_snippet(sentence))

        competencies = skills[:6]
        if not competencies:
            competencies = self._extract_skills(jd_text, max_items=6)

        architecture_emphasis = any(
            marker in text
            for marker in (
                "architecture",
                "distributed",
                "microservices",
                "system design",
                "scalable",
                "high-scale",
            )
        )

        culture_trait = self._culture_context_hint(job_culture_profile=job_culture_profile, values=values)

        keywords = set()
        for token in competencies:
            keywords.add(token.lower())
        for sentence in highlights[:5] + workflows[:5]:
            for token in re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]{3,}", sentence.lower()):
                if token in {"with", "from", "that", "this", "have", "will", "your", "their", "team"}:
                    continue
                keywords.add(token)

        return {
            "role_family": role_family,
            "highlights": highlights,
            "primary_requirement": highlights[0] if highlights else f"delivery scope for {title}",
            "secondary_requirement": highlights[1] if len(highlights) > 1 else highlights[0] if highlights else "core requirements",
            "primary_workflow": workflows[0] if workflows else highlights[0] if highlights else "critical workflow",
            "competencies": competencies,
            "architecture_emphasis": architecture_emphasis,
            "culture_trait": culture_trait,
            "keywords": sorted(keywords),
            "company_name": company_name,
        }

    @staticmethod
    def _compact_snippet(text: str, *, max_words: int = 14, max_chars: int = 96) -> str:
        raw = re.sub(r"\s+", " ", str(text or "")).strip(" \t:;,.!?-")
        if not raw:
            return ""

        first_clause = re.split(r"[,:;]", raw)[0].strip()
        candidate = first_clause or raw

        words = candidate.split()
        if len(words) > max_words:
            candidate = " ".join(words[:max_words]).rstrip(" .")
        if len(candidate) > max_chars:
            candidate = candidate[: max_chars - 3].rstrip(" .") + "..."
        return candidate

    @staticmethod
    def _detect_role_family(text: str) -> str:
        normalized = text.lower()
        qa_markers = ["manual qa", "qa engineer", "quality assurance", "sdet", "test engineer", "software tester", "testing"]
        if any(marker in normalized for marker in qa_markers):
            return "qa"

        eng_markers = ["backend", "frontend", "fullstack", "software engineer", "developer", "platform engineer"]
        if any(marker in normalized for marker in eng_markers):
            return "engineering"

        data_markers = ["data engineer", "machine learning", "ml engineer", "data scientist", "analytics"]
        if any(marker in normalized for marker in data_markers):
            return "data"

        return "general"

    @staticmethod
    def _collect_jd_sentences(jd_text: str) -> List[str]:
        if not jd_text.strip():
            return []
        normalized = jd_text.replace("\r", "\n")
        # Split on line breaks, bullets, and sentence boundaries, but keep hyphenated terms intact.
        raw_parts = re.split(r"(?:\r?\n|•)+|(?<=[.!?])\s+", normalized)
        out: List[str] = []
        seen = set()
        for part in raw_parts:
            clean = re.sub(r"\s+", " ", part).strip(" \t:;,-")
            if len(clean) < 20:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(clean)
            if len(out) >= 60:
                break
        return out

    @staticmethod
    def _pick_context_item(value: Any, index: int, fallback: str) -> str:
        if isinstance(value, list):
            clean = [str(x).strip() for x in value if str(x).strip()]
            if clean:
                return clean[(max(1, index) - 1) % len(clean)]
        return fallback

    def _question_is_relevant(self, *, item: Dict[str, Any], category: str, context: Dict[str, Any]) -> bool:
        title = str(item.get("title") or "").strip()
        description = str(item.get("description") or "").strip()
        if not title or not description:
            return False

        role_family = str(context.get("role_family") or "general")
        architecture_emphasis = bool(context.get("architecture_emphasis"))
        text = f"{title} {description}".lower()

        if role_family == "qa":
            banned = [
                "design and scale",
                "architecture improvement",
                "distributed systems",
                "microservices architecture",
            ]
            if not architecture_emphasis and any(token in text for token in banned):
                return False
            if category == "hard_skills":
                qa_markers = ["test", "qa", "bug", "regression", "release", "api", "quality"]
                if not any(marker in text for marker in qa_markers):
                    return False

        keywords = context.get("keywords")
        if isinstance(keywords, list) and keywords and category != "cultural_fit":
            if not any(str(token).lower() in text for token in keywords[:40]):
                return False

        return True

    def _fallback_question(self, *, category: str, company_name: str, job_title: str, context: Dict[str, Any]) -> Dict[str, Any]:
        role_family = str(context.get("role_family") or "general")
        primary_workflow = str(context.get("primary_workflow") or "your core workflow")

        if category == "hard_skills":
            if role_family == "qa":
                return {
                    "title": f"[Hard Skills] For {company_name}, how would you test and de-risk {primary_workflow}?",
                    "description": "Include test design, prioritization, and release confidence criteria.",
                }
            return {
                "title": f"[Hard Skills] Walk us through how you would deliver {primary_workflow} for {job_title}.",
                "description": "Explain implementation choices, risk controls, and measurable outcomes.",
            }

        if category == "soft_skills":
            return {
                "title": f"[Soft Skills] Describe a difficult stakeholder alignment decision you handled at {company_name}.",
                "description": "Focus on communication choices, decision path, and resulting impact.",
            }

        return {
            "title": f"[Cultural Fit] What team norms at {company_name} help you do your best work?",
            "description": "Be specific about accountability, collaboration, and feedback cadence.",
        }

    @staticmethod
    def _count_categories(questions: List[Dict[str, Any]]) -> Dict[str, int]:
        out: Dict[str, int] = {"hard_skills": 0, "soft_skills": 0, "cultural_fit": 0}
        for question in questions:
            category = str(question.get("category") or "").strip().lower()
            if category in out:
                out[category] += 1
        return out

    def _company_values(self, *, job_culture_profile: Dict[str, Any] | None = None) -> List[str]:
        profile_values = self._to_str_list((job_culture_profile or {}).get("culture_values"))
        if profile_values:
            return profile_values
        profile_values = self._to_str_list((job_culture_profile or {}).get("values"))
        if profile_values:
            return profile_values
        profile_values = self.company_profile.get("values")
        out = self._to_str_list(profile_values)
        if out:
            return out
        default_values = self.guidelines.get("company_values")
        return self._to_str_list(default_values) or ["clear communication", "ownership", "collaboration"]

    def _culture_interview_questions(self, *, job_culture_profile: Dict[str, Any] | None = None) -> List[str]:
        explicit = self._to_str_list((job_culture_profile or {}).get("culture_interview_questions"))
        if explicit:
            return explicit[:3]
        return []

    def _culture_context_hint(self, *, job_culture_profile: Dict[str, Any], values: List[str]) -> str:
        performance = str((job_culture_profile.get("performance_expectations") or {}).get("assessment") or "").strip()
        if performance:
            return performance[:120].rstrip(" .")
        work_style = self._to_str_list(job_culture_profile.get("work_style"))
        if work_style:
            return work_style[0][:120].rstrip(" .")
        if values:
            return values[0]
        return "clear communication"

    def _extract_skills(self, jd_text: str, max_items: int = 4) -> List[str]:
        dictionary = self._to_str_list(self.guidelines.get("skill_dictionary"))
        text = jd_text.lower()
        found: List[str] = []
        for token in dictionary:
            if self._skill_present(token=token, text=text) and token not in found:
                # Normalize aliases to avoid noisy variants in output.
                normalized = "go" if token == "golang" else token
                if normalized not in found:
                    found.append(normalized)
                if len(found) >= max_items:
                    break
        if found:
            return found

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
            "candidate",
            "candidates",
            "engineer",
            "engineering",
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
    def _skill_present(*, token: str, text: str) -> bool:
        token_norm = token.strip().lower()
        if not token_norm:
            return False
        if token_norm == "go":
            # Avoid matching "go" as a generic verb in regular prose.
            go_patterns = [r"\bgolang\b", r"\bgo\s+language\b", r"\blanguage\s+go\b"]
            return any(re.search(pattern, text) for pattern in go_patterns)
        return re.search(rf"\b{re.escape(token_norm)}\b", text) is not None

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
