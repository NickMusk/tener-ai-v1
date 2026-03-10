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
        "bug reporting",
        "automation testing",
        "ml",
        "machine learning",
        "llm",
        "nlp",
    ],
}

QA_FOCUS_AREAS = [
    "web applications and APIs",
    "end-to-end candidate pipelines",
    "external integrations",
    "release readiness",
    "AI-driven workflows",
    "recruiter dashboards",
    "candidate data consistency",
]


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
        requirements = self._job_requirements(job)
        top_skills = list(requirements.get("must_have_skills") or []) or self._extract_skills(jd_text, max_items=6)
        explicit_behavioral = self._behavioral_profile_questions(job_culture_profile=job_culture_profile)

        defaults = self.guidelines.get("defaults") if isinstance(self.guidelines.get("defaults"), dict) else {}
        time_to_answer = max(30, int(self._to_int(defaults.get("time_to_answer"), 120)))
        time_to_think = max(5, int(self._to_int(defaults.get("time_to_think"), 12)))
        retakes = max(0, int(self._to_int(defaults.get("retakes"), 1)))

        context = self._derive_job_context(
            title=title,
            jd_text=jd_text,
            company_name=company_name,
            values=values,
            requirements=requirements,
            job_culture_profile=job_culture_profile,
        )

        desired_count = max(3, min(int(self._to_int(defaults.get("question_count"), 10)), 20))
        category_plan = self._build_category_plan(defaults=defaults, total_questions=desired_count)

        category_index: Dict[str, int] = {"hard_skills": 0, "soft_skills": 0, "cultural_fit": 0}
        questions: List[Dict[str, Any]] = []
        seen_titles: Dict[str, int] = {}

        for category in category_plan:
            category_index[category] = category_index.get(category, 0) + 1
            if category == "hard_skills":
                item = self._hard_skills_question(
                    index=category_index[category],
                    company_name=company_name,
                    job_title=title,
                    context=context,
                )
            else:
                explicit_question = None
                if category == "cultural_fit":
                    explicit_idx = category_index[category] - 1
                    if explicit_idx < len(explicit_behavioral):
                        explicit_question = explicit_behavioral[explicit_idx]
                item = self._behavioral_question(
                    index=category_index[category],
                    category=category,
                    company_name=company_name,
                    context=context,
                    explicit_question=explicit_question,
                )

            if not self._question_is_relevant(item=item, category=category, context=context):
                item = self._fallback_question(category=category, company_name=company_name, job_title=title, context=context)

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
            "requirements": requirements,
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
        primary_workflow = self._pick_context_item(context.get("workflows"), index, fallback=f"core workflows for {job_title}")
        secondary_workflow = self._pick_context_item(context.get("risk_areas"), index, fallback=primary_workflow)
        primary_skill = self._pick_context_item(context.get("core_skills"), index, fallback="core execution")
        secondary_skill = self._pick_context_item(context.get("supporting_skills"), index + 1, fallback="reliable delivery")
        nice_skill = self._pick_context_item(context.get("nice_to_have_skills"), index, fallback="automation")
        skill_pair = self._join_terms([primary_skill, secondary_skill], max_items=2)

        if role_family == "qa":
            prompts = [
                (
                    f"How would you design manual coverage for {primary_workflow} at {company_name}?",
                    f"Explain scope, acceptance criteria, edge cases, and how you would use {skill_pair} before release.",
                ),
                (
                    f"Walk us through how you would validate UI, API, and data consistency for {secondary_workflow}.",
                    "Describe the checks you would run, the evidence you would collect, and the release risks you would watch.",
                ),
                (
                    f"Tell us about a severe issue you investigated in {secondary_workflow}.",
                    f"Cover reproduction, isolation, debugging path, and how {primary_skill} helped you get to root cause.",
                ),
                (
                    f"What would your regression strategy look like for a release touching {primary_workflow} and {secondary_workflow}?",
                    "Describe prioritization under time pressure, high-risk paths, and how you would decide whether to ship.",
                ),
                (
                    f"How would you test AI-driven or partially nondeterministic behavior inside {primary_workflow}?",
                    "Explain how you separate acceptable variance from a real defect and how you keep recruiter trust high.",
                ),
                (
                    f"What would you automate first, and what would you keep manual, for {primary_workflow}?",
                    f"Use {nice_skill} only if it clearly improves coverage, speed, or confidence without creating brittle maintenance cost.",
                ),
            ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        prompts = [
            (
                f"At {company_name}, walk us through how you would deliver {primary_workflow} in production.",
                f"Focus on implementation choices, constraints, and outcomes tied to {primary_skill}.",
            ),
            (
                f"What is your approach to validating quality and reliability for {primary_workflow}?",
                "Explain concrete checks, rollout controls, and failure handling.",
            ),
            (
                f"Describe a deep technical challenge involving {primary_skill}.",
                "Cover diagnostics, option analysis, and final resolution.",
            ),
            (
                f"In {company_name}, how would you improve delivery quality for {secondary_workflow}?",
                "Prioritize high-impact technical improvements and explain why.",
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    def _behavioral_question(
        self,
        *,
        index: int,
        category: str,
        company_name: str,
        context: Dict[str, Any],
        explicit_question: str | None,
    ) -> Dict[str, Any]:
        if explicit_question:
            return {
                "title": explicit_question,
                "description": "Use a concrete example, the tradeoff you faced, and the outcome.",
            }

        role_family = str(context.get("role_family") or "general")
        primary_workflow = self._pick_context_item(context.get("workflows"), index, fallback="cross-functional delivery")
        secondary_workflow = self._pick_context_item(context.get("risk_areas"), index + 1, fallback=primary_workflow)
        value_primary = self._pick_context_item(context.get("company_values"), index, fallback="ownership")
        value_secondary = self._pick_context_item(context.get("company_values"), index + 1, fallback="clear communication")
        release_signal = self._pick_context_item(context.get("release_signals"), index, fallback="quality risk")

        if role_family == "qa":
            if category == "soft_skills":
                prompts = [
                    (
                        f"Tell us about a time you pushed back on a release because of {release_signal}.",
                        f"Explain how you aligned product and engineering, what evidence you used, and what happened next in {primary_workflow}.",
                    ),
                    (
                        f"Describe a high-stakes bug triage conversation you led around {secondary_workflow}.",
                        "Focus on severity calibration, stakeholder alignment, and how you kept the team moving.",
                    ),
                    (
                        f"How do you communicate test findings so people act quickly without feeling blocked?",
                        f"Use one real example tied to {primary_workflow} and show how you framed risk and next steps.",
                    ),
                    (
                        f"Describe a disagreement you had with engineers or product about release readiness.",
                        "Walk through the evidence, the decision path, and how you handled the relationship after the call.",
                    ),
                ]
            else:
                prompts = [
                    (
                        f"Describe a time you protected quality under pressure in a fast-moving environment.",
                        f"Show how your judgment balanced speed, correctness, and accountability during {primary_workflow}.",
                    ),
                    (
                        f"What evidence do you need before you are comfortable recommending a release?",
                        f"Answer in the context of {secondary_workflow} and be specific about your quality bar.",
                    ),
                    (
                        f"What team behaviors help you do your best QA work?",
                        f"Be concrete about accountability, escalation style, and collaboration norms when testing {primary_workflow}.",
                    ),
                    (
                        f"In a startup that values {value_primary} and {value_secondary}, how do you avoid being either a bottleneck or a rubber stamp?",
                        "Use a real example or a concrete operating model that shows your decision-making style.",
                    ),
                ]
            title, description = prompts[(index - 1) % len(prompts)]
            return {"title": title, "description": description}

        prompts = [
            (
                f"At {company_name}, we value {value_secondary}. Tell us about a high-stakes cross-functional collaboration.",
                "Describe your communication strategy, stakeholder alignment, and measurable result.",
            ),
            (
                f"In {company_name}, we value {value_primary}. Share a situation where you embodied this value.",
                "Explain the tradeoff you faced and how it affected the outcome.",
            ),
        ]
        title, description = prompts[(index - 1) % len(prompts)]
        return {"title": title, "description": description}

    def _derive_job_context(
        self,
        *,
        title: str,
        jd_text: str,
        company_name: str,
        values: List[str],
        requirements: Dict[str, List[str]],
        job_culture_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        text = f"{title}\n{jd_text}".strip().lower()
        role_family = self._detect_role_family(text)

        highlights = self._extract_highlights(jd_text)
        workflows = self._extract_workflows(jd_text, role_family=role_family)
        risk_areas = self._extract_risk_areas(jd_text, role_family=role_family, workflows=workflows)
        core_skills = list(requirements.get("must_have_skills") or [])
        nice_to_have_skills = list(requirements.get("nice_to_have_skills") or [])
        questionable_skills = {str(item).strip().lower() for item in (requirements.get("questionable_skills") or []) if str(item).strip()}
        supporting_skills = [skill for skill in core_skills + nice_to_have_skills if skill.lower() not in questionable_skills]
        culture_trait = self._culture_context_hint(job_culture_profile=job_culture_profile, values=values)
        release_signals = self._release_signals(job_culture_profile=job_culture_profile, workflows=workflows, risk_areas=risk_areas)

        keywords = set()
        for token in supporting_skills[:8]:
            keywords.add(token.lower())
        for sentence in workflows[:4] + risk_areas[:4]:
            for token in re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]{3,}", sentence.lower()):
                if token in {"with", "from", "that", "this", "have", "will", "your", "their", "team"}:
                    continue
                keywords.add(token)

        return {
            "role_family": role_family,
            "highlights": highlights,
            "workflows": workflows,
            "risk_areas": risk_areas,
            "primary_requirement": highlights[0] if highlights else f"delivery scope for {title}",
            "primary_workflow": workflows[0] if workflows else f"critical workflows for {title}",
            "core_skills": core_skills,
            "nice_to_have_skills": nice_to_have_skills,
            "supporting_skills": supporting_skills or core_skills or nice_to_have_skills,
            "questionable_skills": sorted(questionable_skills),
            "culture_trait": culture_trait,
            "keywords": sorted(keywords),
            "company_name": company_name,
            "company_values": values,
            "release_signals": release_signals,
        }

    def _extract_highlights(self, jd_text: str) -> List[str]:
        sentences = self._collect_jd_sentences(jd_text)
        highlights: List[str] = []
        for sentence in sentences:
            low = sentence.lower()
            if self._is_noise_sentence(sentence):
                continue
            if any(marker in low for marker in ("responsib", "requirement", "test", "release", "integrat", "workflow")):
                highlights.append(self._compact_snippet(sentence))
            if len(highlights) >= 8:
                break
        if highlights:
            return highlights
        return [self._compact_snippet(sentence) for sentence in sentences[:4] if not self._is_noise_sentence(sentence)]

    def _extract_workflows(self, jd_text: str, *, role_family: str) -> List[str]:
        bullet_lines = self._section_bullets(
            jd_text,
            sections={"responsibilities", "what you'll work on", "what youll work on", "examples of areas you will test"},
        )
        scored: List[tuple[int, int, str]] = []
        for line in bullet_lines:
            if self._is_noise_sentence(line):
                continue
            low = line.lower()
            if any(marker in low for marker in ("test", "validate", "verify", "reproduce", "release", "integrat", "workflow", "dashboard", "pipeline")):
                normalized = self._normalize_workflow(line)
                score = 0
                if any(marker in low for marker in ("api", "platform", "integrat", "workflow", "dashboard", "pipeline", "release")):
                    score += 4
                if any(marker in low for marker in ("ai", "ml")):
                    score += 2
                if any(marker in low for marker in ("test case", "bug")):
                    score += 1
                scored.append((score, -len(scored), normalized))
        if scored:
            ordered = [item[2] for item in sorted(scored, reverse=True)]
            return self._dedupe_keep_order(ordered)[:6]
        if role_family == "qa":
            return list(QA_FOCUS_AREAS[:4])
        return []

    def _extract_risk_areas(self, jd_text: str, *, role_family: str, workflows: List[str]) -> List[str]:
        low = jd_text.lower()
        out = list(workflows[:3])
        if "ai" in low or "ml" in low:
            out.append("AI-driven workflows")
        if "integrat" in low:
            out.append("external integrations")
        if "dashboard" in low:
            out.append("recruiter dashboards")
        if "release" in low:
            out.append("release readiness")
        if "data pipeline" in low or "candidate data" in low:
            out.append("candidate data consistency")
        if role_family == "qa" and not out:
            out.extend(QA_FOCUS_AREAS[:4])
        return self._dedupe_keep_order(out)[:6]

    @staticmethod
    def _normalize_workflow(text: str) -> str:
        low = str(text or "").lower()
        if "web applications" in low and "api" in low:
            return "web applications and APIs"
        if "ai-driven" in low and ("interview" in low or "scoring" in low or "evaluation" in low):
            return "AI-driven interviews and scoring workflows"
        if "integrat" in low and any(token in low for token in ("ats", "linkedin", "messaging", "hiring platform")):
            return "ATS, LinkedIn, and messaging integrations"
        if "candidate pipeline" in low:
            return "end-to-end candidate pipelines"
        if "candidate data" in low:
            return "candidate data pipelines"
        if "dashboard" in low:
            return "recruiter dashboards"
        if "release" in low:
            return "release readiness"
        if "test case" in low:
            return "test cases and acceptance coverage"
        if "manual functional" in low or "exploratory" in low or "regression" in low:
            return "manual functional, regression, and exploratory testing"
        if "reproduce bugs" in low or "document" in low:
            return "bug reproduction and reporting"
        return InterviewQuestionGenerator._compact_snippet(str(text or ""))

    @staticmethod
    def _compact_snippet(text: str, *, max_words: int = 14, max_chars: int = 96) -> str:
        raw = re.sub(r"\s+", " ", str(text or "")).strip(" \t:;,.!?-")
        if not raw:
            return ""
        first_clause = re.split(r"[:;]", raw)[0].strip()
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
            if len(out) >= 80:
                break
        return out

    def _question_is_relevant(self, *, item: Dict[str, Any], category: str, context: Dict[str, Any]) -> bool:
        title = str(item.get("title") or "").strip()
        description = str(item.get("description") or "").strip()
        if not title or not description:
            return False

        text = f"{title} {description}".lower()
        banned_fragments = [
            "we are looking for",
            "why join",
            "our goal is",
            "the environment reads as",
            "decision-making appears",
            "mission intensity looks",
            "summary_200_300_words",
        ]
        if any(fragment in text for fragment in banned_fragments):
            return False

        questionable = context.get("questionable_skills")
        if isinstance(questionable, list):
            for skill in questionable:
                if str(skill).strip().lower() in text and category == "hard_skills":
                    return False

        role_family = str(context.get("role_family") or "general")
        if role_family == "qa" and category == "hard_skills":
            if not any(marker in text for marker in ("test", "validate", "release", "bug", "workflow", "api", "integration")):
                return False
        if category == "cultural_fit" and any(token in text for token in ("underperform", "management style")):
            return False
        return True

    def _fallback_question(self, *, category: str, company_name: str, job_title: str, context: Dict[str, Any]) -> Dict[str, Any]:
        primary_workflow = self._pick_context_item(context.get("workflows"), 1, fallback=f"core workflows for {job_title}")
        if category == "hard_skills":
            return {
                "title": f"How would you test and de-risk {primary_workflow} at {company_name}?",
                "description": "Include scope, prioritization, risk controls, and the release evidence you would require.",
            }
        if category == "soft_skills":
            return {
                "title": "Describe a time you aligned stakeholders around a quality-risk decision.",
                "description": "Focus on your communication choices, evidence, and final outcome.",
            }
        return {
            "title": "How do you maintain a strong quality bar without slowing the team down?",
            "description": "Use a concrete example and explain your decision framework.",
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

    def _behavioral_profile_questions(self, *, job_culture_profile: Dict[str, Any] | None = None) -> List[str]:
        explicit = self._to_str_list((job_culture_profile or {}).get("culture_interview_questions"))
        out: List[str] = []
        for question in explicit:
            normalized = re.sub(r"\s+", " ", question).strip()
            if len(normalized) < 20:
                continue
            low = normalized.lower()
            if any(token in low for token in ("management style", "underperform", "why does this mission", "why this company")):
                continue
            out.append(normalized)
            if len(out) >= 3:
                break
        return out

    def _culture_context_hint(self, *, job_culture_profile: Dict[str, Any], values: List[str]) -> str:
        performance = str((job_culture_profile.get("performance_expectations") or {}).get("assessment") or "").strip()
        if performance:
            compact = self._compact_snippet(performance, max_words=12, max_chars=80)
            if compact and not self._is_noise_sentence(compact):
                return compact
        work_style = self._to_str_list(job_culture_profile.get("work_style"))
        for item in work_style:
            compact = self._compact_snippet(item, max_words=12, max_chars=80)
            if compact and not self._is_noise_sentence(compact):
                return compact
        if values:
            return values[0]
        return "clear communication"

    def _job_requirements(self, job: Dict[str, Any]) -> Dict[str, List[str]]:
        must_have = self._to_str_list(job.get("must_have_skills"))
        nice_to_have = self._to_str_list(job.get("nice_to_have_skills"))
        questionable = self._to_str_list(job.get("questionable_skills"))
        if must_have or nice_to_have or questionable:
            return {
                "must_have_skills": must_have[:6],
                "nice_to_have_skills": [item for item in nice_to_have if item not in set(must_have)][:6],
                "questionable_skills": questionable[:6],
            }
        extracted = self._extract_skills(str(job.get("jd_text") or ""), max_items=6)
        return {
            "must_have_skills": extracted[:6],
            "nice_to_have_skills": [],
            "questionable_skills": [],
        }

    def _extract_skills(self, jd_text: str, max_items: int = 4) -> List[str]:
        dictionary = self._to_str_list(self.guidelines.get("skill_dictionary"))
        text = jd_text.lower()
        found: List[str] = []
        for token in dictionary:
            if self._skill_present(token=token, text=text) and token not in found:
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
            if token in stopwords or token in clean:
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
            go_patterns = [r"\bgolang\b", r"\bgo\s+language\b", r"\blanguage\s+go\b"]
            return any(re.search(pattern, text) for pattern in go_patterns)
        return re.search(rf"\b{re.escape(token_norm)}\b", text) is not None

    @staticmethod
    def _section_bullets(jd_text: str, sections: set[str]) -> List[str]:
        lines = [re.sub(r"\s+", " ", raw).strip() for raw in str(jd_text or "").replace("\r", "\n").split("\n")]
        current = ""
        out: List[str] = []
        for line in lines:
            if not line:
                continue
            normalized = line.lower().strip(" :")
            if normalized in sections:
                current = normalized
                continue
            if len(line) < 4:
                continue
            if line.startswith(("•", "-", "*")):
                if current in sections:
                    out.append(line.lstrip("•-* ").strip())
                continue
            if current in sections and not line.endswith(":"):
                out.append(line)
        return out

    @staticmethod
    def _release_signals(*, job_culture_profile: Dict[str, Any], workflows: List[str], risk_areas: List[str]) -> List[str]:
        signals: List[str] = []
        for item in (job_culture_profile.get("hiring_signals") or []):
            token = str(item).strip().lower()
            if not token:
                continue
            if "quality bar" in token:
                signals.append("quality risk")
            elif "correctness" in token:
                signals.append("correctness risk")
        if signals:
            return InterviewQuestionGenerator._dedupe_keep_order(signals)[:4]
        fallback = workflows[:2] + risk_areas[:2]
        return fallback or ["quality risk"]

    @staticmethod
    def _join_terms(items: List[str], *, max_items: int = 2) -> str:
        clean = [str(item).strip() for item in items if str(item).strip()]
        if not clean:
            return "core workflows"
        top = clean[:max_items]
        if len(top) == 1:
            return top[0]
        if len(top) == 2:
            return f"{top[0]} and {top[1]}"
        return ", ".join(top[:-1]) + f", and {top[-1]}"

    @staticmethod
    def _pick_context_item(value: Any, index: int, fallback: str) -> str:
        if isinstance(value, list):
            clean = [str(x).strip() for x in value if str(x).strip()]
            if clean:
                return clean[(max(1, index) - 1) % len(clean)]
        return fallback

    @staticmethod
    def _dedupe_keep_order(values: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for value in values:
            token = str(value or "").strip()
            if not token:
                continue
            key = token.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(token)
        return out

    @staticmethod
    def _is_noise_sentence(text: str) -> bool:
        low = str(text or "").strip().lower()
        if not low:
            return True
        noise_markers = (
            "about tener",
            "tener.ai is building",
            "our goal is",
            "we are looking for",
            "why join",
            "work with experienced founders",
            "opportunity to grow",
            "build a product redefining",
        )
        return any(marker in low for marker in noise_markers)

    @staticmethod
    def _load_guidelines(path: str) -> Dict[str, Any]:
        base = dict(DEFAULT_GUIDELINES)
        base["defaults"] = dict(DEFAULT_GUIDELINES["defaults"])
        base["defaults"]["category_targets"] = dict(DEFAULT_GUIDELINES["defaults"]["category_targets"])
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
            if key == "defaults" and isinstance(value, dict):
                merged_defaults = dict(base["defaults"])
                merged_defaults.update(value)
                targets = value.get("category_targets") if isinstance(value.get("category_targets"), dict) else {}
                merged_defaults["category_targets"] = {**base["defaults"]["category_targets"], **targets}
                out["defaults"] = merged_defaults
            else:
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
