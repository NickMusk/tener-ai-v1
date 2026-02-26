from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .candidate_scoring import CandidateScoringPolicy
from .db import AGENT_DEFAULT_NAMES, Database
from .llm_responder import CandidateLLMResponder
from .matching import MatchingEngine


UTC = timezone.utc


class CandidateProfileService:
    def __init__(
        self,
        *,
        db: Database,
        matching_engine: MatchingEngine,
        scoring_policy: CandidateScoringPolicy,
        llm_responder: CandidateLLMResponder | None = None,
        explanation_cache_ttl_seconds: int = 900,
    ) -> None:
        self.db = db
        self.matching_engine = matching_engine
        self.scoring_policy = scoring_policy
        self.llm_responder = llm_responder
        self.explanation_cache_ttl_seconds = max(30, int(explanation_cache_ttl_seconds))
        self._explanation_cache: Dict[str, Dict[str, Any]] = {}

    def list_candidate_resume_links(self, candidate_id: int) -> List[str]:
        sessions = self.db.list_pre_resume_sessions_for_candidate(candidate_id=int(candidate_id), limit=500)
        return self._collect_resume_links(sessions=sessions)

    def build_candidate_profile(
        self,
        *,
        candidate_id: int,
        selected_job_id: Optional[int] = None,
        include_audit: bool = False,
        include_explanation: bool = True,
    ) -> Dict[str, Any]:
        candidate = self.db.get_candidate(candidate_id)
        if not candidate:
            raise ValueError("candidate not found")

        matches = self.db.list_candidate_matches(candidate_id=int(candidate_id))
        assessments_all = self.db.list_candidate_assessments(candidate_id=int(candidate_id))
        sessions = self.db.list_pre_resume_sessions_for_candidate(candidate_id=int(candidate_id), limit=500)
        conversations = self.db.list_conversations_for_candidate(candidate_id=int(candidate_id), limit=500)
        pre_resume_events = self.db.list_pre_resume_events_for_candidate(candidate_id=int(candidate_id), limit=1000)
        logs = self.db.list_logs_for_candidate(candidate_id=int(candidate_id), limit=500)

        assessments_by_job: Dict[int, List[Dict[str, Any]]] = {}
        for item in assessments_all:
            job_id = int(item.get("job_id") or 0)
            assessments_by_job.setdefault(job_id, []).append(item)

        sessions_by_job: Dict[int, List[Dict[str, Any]]] = {}
        for row in sessions:
            job_id = int(row.get("job_id") or 0)
            sessions_by_job.setdefault(job_id, []).append(row)

        events_by_job: Dict[int, List[Dict[str, Any]]] = {}
        for row in pre_resume_events:
            job_id = int(row.get("job_id") or 0)
            events_by_job.setdefault(job_id, []).append(row)

        conversations_by_job: Dict[int, List[Dict[str, Any]]] = {}
        conversation_ids_by_job: Dict[int, set[int]] = {}
        for row in conversations:
            job_id = int(row.get("job_id") or 0)
            conversations_by_job.setdefault(job_id, []).append(row)
            conversation_id = int(row.get("conversation_id") or 0)
            if conversation_id > 0:
                conversation_ids_by_job.setdefault(job_id, set()).add(conversation_id)

        jobs_payload: List[Dict[str, Any]] = []
        global_signals: List[Dict[str, Any]] = []

        for match in matches:
            job_id = int(match.get("job_id") or 0)
            job = {
                "id": job_id,
                "title": str(match.get("job_title") or "").strip(),
                "company": str(match.get("job_company") or "").strip() or None,
                "jd_text": str(match.get("job_jd_text") or "").strip(),
                "location": str(match.get("job_location") or "").strip() or None,
                "preferred_languages": match.get("job_preferred_languages") if isinstance(match.get("job_preferred_languages"), list) else [],
                "seniority": str(match.get("job_seniority") or "").strip() or None,
            }
            candidate_assessments = list(assessments_by_job.get(job_id, []))
            scorecard = self.db.build_agent_scorecard(
                assessments=candidate_assessments,
                candidate_row=match,
            )
            current_status_key, current_status_label = self.db.derive_candidate_current_status(match)
            overall_scoring = self.scoring_policy.compute_overall(
                scorecard=scorecard,
                current_status_key=current_status_key,
            )
            job_events = list(events_by_job.get(job_id, []))
            job_conversations = list(conversations_by_job.get(job_id, []))
            resumes_for_job = self._collect_resume_links(sessions=sessions_by_job.get(job_id, []))
            fit_breakdown = self._build_fit_breakdown(
                job=job,
                candidate=candidate,
                match=match,
                scorecard=scorecard,
                pre_resume_events=job_events,
                conversations=job_conversations,
                resume_links=resumes_for_job,
            )
            job_conversation_ids = conversation_ids_by_job.get(job_id, set())
            job_logs = self._filter_logs_for_job(logs=logs, job_id=job_id, conversation_ids=job_conversation_ids)
            signals = self._build_signals_timeline(
                job_id=job_id,
                scorecard=scorecard,
                overall_scoring=overall_scoring,
                assessments=candidate_assessments,
                pre_resume_events=job_events,
                logs=job_logs,
            )
            culture_analysis = fit_breakdown.get("culture_fit") if isinstance(fit_breakdown.get("culture_fit"), dict) else {}
            if culture_analysis:
                signals.insert(
                    0,
                    {
                        "kind": "culture_analysis",
                        "job_id": job_id,
                        "created_at": datetime.now(UTC).isoformat(),
                        "agent_key": "culture_analyst",
                        "agent_name": AGENT_DEFAULT_NAMES.get("culture_analyst", "Harper AI (Culture Analyst)"),
                        "status": "generated",
                        "reason": str((culture_analysis.get("analysis") or {}).get("summary") or "Culture fit analysis updated."),
                        "signals": {
                            "alignment_highlights": culture_analysis.get("alignment_highlights") or [],
                            "concerns": culture_analysis.get("concerns") or [],
                            "predictive_signals": culture_analysis.get("predictive_signals") or [],
                        },
                    },
                )
            global_signals.extend(signals)
            explanation = self._build_fit_explanation(
                candidate=candidate,
                job=job,
                overall_scoring=overall_scoring,
                fit_breakdown=fit_breakdown,
                signals=signals[:25],
                include_explanation=include_explanation,
            )
            jobs_payload.append(
                {
                    "job": job,
                    "match": {
                        "score": match.get("score"),
                        "status": match.get("status"),
                        "created_at": match.get("match_created_at"),
                        "verification_notes": match.get("verification_notes") if isinstance(match.get("verification_notes"), dict) else {},
                    },
                    "conversation": (conversations_by_job.get(job_id) or [None])[0],
                    "current_status": {
                        "key": current_status_key,
                        "label": current_status_label,
                    },
                    "scorecard": scorecard,
                    "overall_scoring": overall_scoring,
                    "fit_breakdown": fit_breakdown,
                    "fit_explanation": explanation,
                    "resumes": {
                        "links": resumes_for_job,
                        "latest_link": resumes_for_job[0] if resumes_for_job else None,
                    },
                    "signals_timeline": signals,
                }
            )

        jobs_payload.sort(
            key=lambda item: self._safe_parse_time(
                str((item.get("match") or {}).get("created_at") or ""),
            ),
            reverse=True,
        )

        selected_job = selected_job_id if selected_job_id is not None else None
        selected_exists = False
        if selected_job is not None:
            selected_exists = any(int((item.get("job") or {}).get("id") or 0) == int(selected_job) for item in jobs_payload)
        if not selected_exists:
            selected_job = int((jobs_payload[0].get("job") or {}).get("id") or 0) if jobs_payload else None

        global_signals.sort(
            key=lambda item: self._safe_parse_time(str(item.get("created_at") or "")),
            reverse=True,
        )
        resume_links = self._collect_resume_links(sessions=sessions)
        payload: Dict[str, Any] = {
            "candidate": candidate,
            "selected_job_id": selected_job,
            "jobs": jobs_payload,
            "resume_links": resume_links,
            "signals_timeline": global_signals[:500],
            "summary": {
                "jobs_total": len(jobs_payload),
                "conversations_total": len(conversations),
                "resume_links_total": len(resume_links),
            },
            "generated_at": datetime.now(UTC).isoformat(),
        }
        if include_audit:
            payload["audit"] = {
                "raw_matches": matches,
                "raw_assessments": assessments_all,
                "raw_pre_resume_sessions": sessions,
                "raw_pre_resume_events": pre_resume_events,
                "raw_logs": logs,
                "raw_conversations": conversations,
            }
        return payload

    def create_demo_profile(self, *, job_id: Optional[int] = None) -> Dict[str, Any]:
        chosen_job = self.db.get_job(int(job_id)) if job_id is not None else None
        if not chosen_job:
            jobs = self.db.list_jobs(limit=100)
            if jobs:
                chosen_job = jobs[0]
        if not chosen_job:
            new_job_id = self.db.insert_job(
                title="Demo Fullstack Engineer",
                company="DemoCo",
                jd_text="Need Python, AWS, Docker, CI/CD, communication, ownership.",
                location="Remote",
                preferred_languages=["en"],
                seniority="senior",
            )
            chosen_job = self.db.get_job(new_job_id) or {}

        assert isinstance(chosen_job, dict)
        job_id_int = int(chosen_job.get("id") or 0)
        if job_id_int <= 0:
            raise ValueError("job not found for demo profile")

        core_profile = self.matching_engine.build_core_profile(chosen_job)
        required = [str(item).strip() for item in (core_profile.get("core_skills") or []) if str(item).strip()][:6]
        if not required:
            required = ["python", "aws", "docker", "ci/cd"]

        demo_fixture = {
            "candidate": {
                "full_name": "Tkachenko Victor",
                "headline": "Front-end Practice Lead / Engineering Manager",
                "location": "Dnipro, Ukraine",
                "languages": ["en", "uk"],
                "skills": [
                    "html5",
                    "angular",
                    "react",
                    "vue",
                    "backbone",
                    "jquery",
                    "core web vitals",
                    "browser internals",
                    "spa",
                    "ssr",
                    "pwa",
                    "bootstrap",
                    "qunit",
                    "sinon",
                    "agile",
                    "scrum",
                    "ci/cd",
                    "frontend architecture",
                    "mentoring",
                    "hiring",
                ],
                "years_experience": 13,
            },
            "nice_to_have_skills": [
                "frontend leadership",
                "core web vitals optimization",
                "mentoring and coaching",
                "cross team release coordination",
            ],
            "company_culture_profile": {
                "values": [
                    "high ownership",
                    "direct communication",
                    "mentorship culture",
                    "fast but disciplined execution",
                ],
                "team_style": "autonomous squads with strong cross-functional collaboration and frequent peer reviews",
                "decision_style": "pragmatic decisions backed by metrics and measurable delivery outcomes",
                "delivery_environment": "high throughput product environment with continuous releases",
            },
            "predictive_behavior_signals": [
                "Reduced customer site load time from around 4 seconds to near 1 second, indicating sustained performance ownership under production constraints.",
                "Built and scaled a frontend department to six engineers, a strong proxy for team enablement and hiring maturity.",
                "Ran in-house training tracks with roughly 30 participants and around 10 hires, suggesting repeatable mentorship and onboarding impact.",
                "Maintained tutoring cohorts with a hiring conversion near 40 percent, which correlates with strong communication clarity and coaching consistency.",
            ],
            "assessment": {
                "sourcing_vetting": {
                    "score": 82.0,
                    "status": "qualified",
                    "reason": "Strong senior frontend leadership depth and measurable engineering outcomes.",
                },
                "communication": {
                    "score": 85.0,
                    "status": "in_dialogue",
                    "reason": "Communication is concise and concrete with clear ownership language.",
                },
                "interview_evaluation": {
                    "score": 84.0,
                    "status": "scored",
                    "reason": "Interview discussion shows practical architecture judgment and strong mentoring signals.",
                },
            },
            "chat": {
                "outbound": "Hey, thanks for sharing your resume. We reviewed it and your profile is now in interview review.",
                "inbound_preview": "Shared my CV and examples of frontend performance and leadership work",
                "event_reason": "Candidate shared CV quickly and highlighted measurable leadership and web performance impact.",
            },
            "resume_links": [
                "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
            ],
            "human_explanation": (
                "Demo profile mirrors Victor Tkachenko CV: senior frontend leadership, proven Core Web Vitals outcomes, "
                "team scaling, and long-term mentoring footprint."
            ),
        }

        candidate_skills_norm = {str(item).strip().lower() for item in demo_fixture["candidate"]["skills"] if str(item).strip()}
        matched = [item for item in required if str(item).strip().lower() in candidate_skills_norm]
        if not matched and required:
            matched = required[:1]
        missing = [item for item in required if item not in set(matched)]
        nice_to_have = list(demo_fixture["nice_to_have_skills"])
        company_culture_profile = dict(demo_fixture["company_culture_profile"])
        predictive_behavior_signals = list(demo_fixture["predictive_behavior_signals"])
        demo_linkedin_id = f"demo-profile-candidate-{job_id_int}"
        demo_candidate_id = self.db.upsert_candidate(
            {
                "linkedin_id": demo_linkedin_id,
                "full_name": str(demo_fixture["candidate"]["full_name"]),
                "headline": str(demo_fixture["candidate"]["headline"]),
                "location": str(demo_fixture["candidate"]["location"]),
                "languages": list(demo_fixture["candidate"]["languages"]),
                "skills": sorted(set(list(demo_fixture["candidate"]["skills"]) + matched)),
                "years_experience": int(demo_fixture["candidate"]["years_experience"]),
                "raw": {},
            },
            source="demo",
        )
        notes = {
            "core_profile": core_profile,
            "required_skills": required,
            "matched_skills": matched,
            "nice_to_have_skills": nice_to_have,
            "company_culture_profile": company_culture_profile,
            "predictive_behavior_signals": predictive_behavior_signals,
            "components": {
                "skills_match": round(float(len(matched)) / float(max(len(required), 1)), 3),
                "seniority_match": 1.0 if int(demo_fixture["candidate"]["years_experience"]) >= 8 else 0.8,
                "location_match": 0.6,
                "language_match": 1.0,
            },
            "human_explanation": str(demo_fixture["human_explanation"]),
            "rules_version": "demo_fixture",
        }
        self.db.create_candidate_match(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            score=0.81,
            status="verified",
            verification_notes=notes,
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            agent_key="sourcing_vetting",
            agent_name=AGENT_DEFAULT_NAMES["sourcing_vetting"],
            stage_key="vetting",
            score=float((demo_fixture["assessment"]["sourcing_vetting"] or {}).get("score") or 0.0),
            status=str((demo_fixture["assessment"]["sourcing_vetting"] or {}).get("status") or "qualified"),
            reason=str(
                (demo_fixture["assessment"]["sourcing_vetting"] or {}).get("reason")
                or "Strong technical alignment for core stack and seniority."
            ),
            details={"matched_required_skills": matched, "missing_required_skills": missing},
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            agent_key="communication",
            agent_name=AGENT_DEFAULT_NAMES["communication"],
            stage_key="dialogue",
            score=float((demo_fixture["assessment"]["communication"] or {}).get("score") or 0.0),
            status=str((demo_fixture["assessment"]["communication"] or {}).get("status") or "in_dialogue"),
            reason=str(
                (demo_fixture["assessment"]["communication"] or {}).get("reason")
                or "Replies are clear and cooperative, with minor ambiguity on availability."
            ),
            details={
                "quality_adjustment": 3.5,
                "quality_signals": {
                    "word_count": 68,
                    "unique_word_ratio": 0.81,
                    "turns": 5,
                    "followups_sent": 1,
                    "filler_count": 0,
                    "specificity_markers": 4,
                },
            },
        )
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            agent_key="interview_evaluation",
            agent_name=AGENT_DEFAULT_NAMES["interview_evaluation"],
            stage_key="interview_results",
            score=float((demo_fixture["assessment"]["interview_evaluation"] or {}).get("score") or 0.0),
            status=str((demo_fixture["assessment"]["interview_evaluation"] or {}).get("status") or "scored"),
            reason=str(
                (demo_fixture["assessment"]["interview_evaluation"] or {}).get("reason")
                or "Interview indicates solid ownership and architecture decision quality."
            ),
            details={
                "technical_score": 85,
                "soft_skills_score": 84,
                "culture_fit_score": 86,
                "score_confidence": 0.73,
                "interview_signals": [
                    "Connected architecture choices to measurable product metrics and delivery constraints.",
                    "Described mentoring and hiring processes with concrete operational details.",
                ],
            },
        )
        self.db.update_candidate_match_status(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            status="interview_scored",
            extra_notes={
                "interview_status": "scored",
                "interview_total_score": 84.0,
                "interview_session_id": None,
            },
        )
        conversation_id = self.db.get_or_create_conversation(
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            channel="linkedin",
        )
        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=str((demo_fixture["chat"] or {}).get("outbound") or ""),
            candidate_language="en",
            meta={"type": "demo_seed", "auto": True},
        )
        session_state = {
            "session_id": f"pre-{conversation_id}",
            "candidate_name": str(demo_fixture["candidate"]["full_name"]),
            "job_title": str(chosen_job.get("title") or "Demo role"),
            "scope_summary": ", ".join(required),
            "core_profile_summary": ", ".join(required),
            "language": "en",
            "status": "resume_received",
            "followups_sent": 1,
            "turns": 5,
            "last_intent": "resume_shared",
            "last_error": None,
            "resume_links": list(demo_fixture["resume_links"]),
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "next_followup_at": None,
            "awaiting_pre_vetting_opt_in": False,
        }
        self.db.upsert_pre_resume_session(
            session_id=session_state["session_id"],
            conversation_id=conversation_id,
            job_id=job_id_int,
            candidate_id=demo_candidate_id,
            state=session_state,
            instruction="demo fixture",
        )
        self.db.insert_pre_resume_event(
            session_id=session_state["session_id"],
            conversation_id=conversation_id,
            event_type="inbound_processed",
            intent="resume_shared",
            inbound_text=str((demo_fixture["chat"] or {}).get("inbound_preview") or "Attached my resume"),
            outbound_text="Great, resume received",
            state_status="resume_received",
            details={
                "source": "demo_fixture",
                "signal": str((demo_fixture["chat"] or {}).get("event_reason") or ""),
            },
        )
        self.db.log_operation(
            operation="candidate.profile.demo_seed",
            status="ok",
            entity_type="candidate",
            entity_id=str(demo_candidate_id),
            details={"job_id": job_id_int, "candidate_id": demo_candidate_id},
        )
        return {
            "candidate_id": demo_candidate_id,
            "job_id": job_id_int,
            "conversation_id": conversation_id,
            "profile_path": f"/candidate/{demo_candidate_id}?job_id={job_id_int}",
        }

    def _build_fit_breakdown(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        match: Dict[str, Any],
        scorecard: Dict[str, Any],
        pre_resume_events: List[Dict[str, Any]],
        conversations: List[Dict[str, Any]],
        resume_links: List[str],
    ) -> Dict[str, Any]:
        notes = match.get("verification_notes") if isinstance(match.get("verification_notes"), dict) else {}
        core_profile = notes.get("core_profile") if isinstance(notes.get("core_profile"), dict) else {}
        candidate_skills = [str(item).strip() for item in (candidate.get("skills") or []) if str(item).strip()]
        candidate_skills_norm = {item.lower() for item in candidate_skills}
        required_skills_raw = notes.get("required_skills") if isinstance(notes.get("required_skills"), list) else []
        required_skills = [str(item).strip() for item in required_skills_raw if str(item).strip()]
        if not required_skills:
            required_skills = [str(item).strip() for item in (core_profile.get("core_skills") or []) if str(item).strip()]
        matched_from_notes = notes.get("matched_skills") if isinstance(notes.get("matched_skills"), list) else []
        matched_skills = [str(item).strip() for item in matched_from_notes if str(item).strip()]
        if not matched_skills:
            matched_skills = [item for item in required_skills if item.lower() in candidate_skills_norm]
        required_norm = {item.lower(): item for item in required_skills}
        matched_norm = {item.lower(): item for item in matched_skills}
        missing_must_have = [label for key, label in required_norm.items() if key not in matched_norm]
        nice_raw = notes.get("nice_to_have_skills") if isinstance(notes.get("nice_to_have_skills"), list) else []
        nice_to_have = [str(item).strip() for item in nice_raw if str(item).strip()]
        if not nice_to_have:
            core_skills = [str(item).strip() for item in (core_profile.get("core_skills") or []) if str(item).strip()]
            nice_to_have = [item for item in core_skills if item.lower() not in required_norm]
        missing_nice = [item for item in nice_to_have if item.lower() not in candidate_skills_norm]
        matched_nice = [item for item in nice_to_have if item.lower() in candidate_skills_norm]
        culture_fit = self._build_culture_fit(
            job=job,
            candidate=candidate,
            match=match,
            scorecard=scorecard,
            pre_resume_events=pre_resume_events,
            conversations=conversations,
            resume_links=resume_links,
        )
        risks: List[Dict[str, Any]] = []
        if missing_must_have:
            risks.append(
                {
                    "type": "missing_must_have",
                    "severity": "high",
                    "message": f"Missing must-have skills: {', '.join(missing_must_have[:6])}",
                }
            )
        components = notes.get("components") if isinstance(notes.get("components"), dict) else {}
        location_match = self._safe_float(components.get("location_match"), None)
        language_match = self._safe_float(components.get("language_match"), None)
        if location_match is not None and location_match <= 0.4:
            risks.append({"type": "location_match", "severity": "medium", "message": "Weak location alignment for this role."})
        if language_match is not None and language_match <= 0.3:
            risks.append(
                {
                    "type": "language_match",
                    "severity": "medium",
                    "message": "Preferred language alignment is weak.",
                }
            )
        interview_score = ((scorecard.get("interview_evaluation") or {}).get("latest_score") if isinstance(scorecard, dict) else None)
        if interview_score is None:
            risks.append(
                {
                    "type": "interview_pending",
                    "severity": "low",
                    "message": "Interview score is not available yet.",
                }
            )
        return {
            "must_have": {
                "required": required_skills,
                "matched": matched_skills,
                "missing": missing_must_have,
                "match_ratio": round(float(len(matched_skills)) / float(max(len(required_skills), 1)), 3),
            },
            "nice_to_have": {
                "expected": nice_to_have,
                "matched": matched_nice,
                "missing": missing_nice,
            },
            "culture_fit": culture_fit,
            "candidate_snapshot": {
                "skills": candidate_skills,
                "years_experience": candidate.get("years_experience"),
                "location": candidate.get("location"),
                "languages": candidate.get("languages") if isinstance(candidate.get("languages"), list) else [],
            },
            "risk_flags": risks,
            "score_snapshot": {
                "match_score": round(self._safe_float(match.get("score"), 0.0) * 100.0, 2),
                "match_status": str(match.get("status") or ""),
            },
        }

    def _build_fit_explanation(
        self,
        *,
        candidate: Dict[str, Any],
        job: Dict[str, Any],
        overall_scoring: Dict[str, Any],
        fit_breakdown: Dict[str, Any],
        signals: List[Dict[str, Any]],
        include_explanation: bool,
    ) -> Dict[str, Any]:
        fallback = self._fallback_fit_explanation(
            candidate=candidate,
            job=job,
            overall_scoring=overall_scoring,
            fit_breakdown=fit_breakdown,
        )
        if not include_explanation:
            return {"source": "fallback", "text": fallback, "cached": False}
        key_payload = {
            "candidate_id": candidate.get("id"),
            "job_id": job.get("id"),
            "overall": overall_scoring,
            "fit_breakdown": fit_breakdown,
            "signals": signals,
        }
        cache_key = hashlib.sha1(json.dumps(key_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
        cached = self._explanation_cache.get(cache_key)
        now = time.time()
        if cached and (now - float(cached.get("ts") or 0.0)) <= self.explanation_cache_ttl_seconds:
            return {"source": str(cached.get("source") or "cache"), "text": str(cached.get("text") or fallback), "cached": True}

        if self.llm_responder is None:
            self._explanation_cache[cache_key] = {"ts": now, "source": "fallback", "text": fallback}
            return {"source": "fallback", "text": fallback, "cached": False}

        instruction = (
            "Generate a concise recruiter style fit explanation for one candidate in plain text.\n"
            "Use 4 short paragraphs.\n"
            "Paragraph 1: overall fit summary.\n"
            "Paragraph 2: must have and nice to have breakdown.\n"
            "Paragraph 3: culture fit alignment and team environment match using culture analyst inputs.\n"
            "Paragraph 4: key risks and recommendation.\n"
            "Be specific to provided signals and avoid generic wording.\n"
            "Reference resume, chat, and interview signals when available."
        )
        inbound_text = json.dumps(
            {
                "overall_scoring": overall_scoring,
                "fit_breakdown": fit_breakdown,
                "signals": signals[:20],
            },
            ensure_ascii=False,
        )
        generated = self.llm_responder.generate_candidate_reply(
            mode="candidate_profile_fit",
            instruction=instruction,
            job=job,
            candidate=candidate,
            inbound_text=inbound_text,
            history=[],
            fallback_reply=fallback,
            language="en",
            state={"candidate_profile_fit": True},
        )
        out_text = str(generated or fallback).strip() or fallback
        source = "llm" if out_text != fallback else "fallback"
        self._explanation_cache[cache_key] = {"ts": now, "source": source, "text": out_text}
        return {"source": source, "text": out_text, "cached": False}

    @staticmethod
    def _fallback_fit_explanation(
        *,
        candidate: Dict[str, Any],
        job: Dict[str, Any],
        overall_scoring: Dict[str, Any],
        fit_breakdown: Dict[str, Any],
    ) -> str:
        status = str(overall_scoring.get("overall_status") or "review")
        overall = overall_scoring.get("overall_score")
        overall_text = f"{overall:.1f}" if isinstance(overall, (int, float)) else "N/A"
        must = fit_breakdown.get("must_have") if isinstance(fit_breakdown.get("must_have"), dict) else {}
        required = must.get("required") if isinstance(must.get("required"), list) else []
        matched = must.get("matched") if isinstance(must.get("matched"), list) else []
        missing = must.get("missing") if isinstance(must.get("missing"), list) else []
        risk_flags = fit_breakdown.get("risk_flags") if isinstance(fit_breakdown.get("risk_flags"), list) else []
        culture_fit = fit_breakdown.get("culture_fit") if isinstance(fit_breakdown.get("culture_fit"), dict) else {}
        cultural_highlights = (
            culture_fit.get("alignment_highlights") if isinstance(culture_fit.get("alignment_highlights"), list) else []
        )
        analysis = culture_fit.get("analysis") if isinstance(culture_fit.get("analysis"), dict) else {}
        analysis_agent = str(analysis.get("agent_name") or AGENT_DEFAULT_NAMES.get("culture_analyst", "Harper AI (Culture Analyst)")).strip()
        analysis_summary = str(analysis.get("summary") or "").strip()
        top_risks = [str((item or {}).get("message") or "").strip() for item in risk_flags if isinstance(item, dict)]
        top_risks = [item for item in top_risks if item]
        name = str(candidate.get("full_name") or "Candidate").strip() or "Candidate"
        title = str(job.get("title") or "the role").strip() or "the role"
        return (
            f"{name} currently sits at overall status {status} with score {overall_text} for {title}.\n"
            f"Must-have coverage is {len(matched)}/{max(len(required), 1)}; matched: {', '.join(matched[:6]) or 'none'}; "
            f"missing: {', '.join(missing[:6]) or 'none'}.\n"
            f"{analysis_agent} culture analysis: {analysis_summary or ', '.join(str(item) for item in cultural_highlights[:4]) or 'no strong cultural insights yet'}.\n"
            f"Main risks: {', '.join(top_risks[:3]) or 'no critical risks detected from current data'}."
        )

    def _build_signals_timeline(
        self,
        *,
        job_id: int,
        scorecard: Dict[str, Any],
        overall_scoring: Dict[str, Any],
        assessments: List[Dict[str, Any]],
        pre_resume_events: List[Dict[str, Any]],
        logs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        weights = overall_scoring.get("weights") if isinstance(overall_scoring.get("weights"), dict) else {}
        inputs = overall_scoring.get("inputs") if isinstance(overall_scoring.get("inputs"), dict) else {}
        present_weights = 0.0
        for key, value in inputs.items():
            if value is None:
                continue
            present_weights += self._safe_float(weights.get(key), 0.0)
        latest_stage_by_agent: Dict[str, str] = {}
        for agent_key, bucket in (scorecard or {}).items():
            if not isinstance(bucket, dict):
                continue
            stage = str(bucket.get("latest_stage") or "").strip()
            if stage:
                latest_stage_by_agent[str(agent_key)] = stage

        for item in assessments:
            agent_key = str(item.get("agent_key") or "").strip()
            stage_key = str(item.get("stage_key") or "").strip()
            score = self._safe_float(item.get("score"), None)
            contribution = None
            contributes = False
            if (
                score is not None
                and agent_key in latest_stage_by_agent
                and latest_stage_by_agent.get(agent_key) == stage_key
                and inputs.get(agent_key) is not None
                and present_weights > 0.0
            ):
                weight = self._safe_float(weights.get(agent_key), 0.0)
                contribution = round((weight * score) / present_weights, 2)
                contributes = True
            out.append(
                {
                    "kind": "assessment_signal",
                    "job_id": job_id,
                    "created_at": item.get("updated_at"),
                    "agent_key": agent_key,
                    "agent_name": item.get("agent_name") or AGENT_DEFAULT_NAMES.get(agent_key),
                    "stage_key": stage_key,
                    "status": item.get("status"),
                    "score": score,
                    "reason": item.get("reason"),
                    "contributes_to_overall": contributes,
                    "contribution_score_points": contribution,
                    "signals": item.get("details") if isinstance(item.get("details"), dict) else {},
                }
            )

        for event in pre_resume_events:
            details = event.get("details") if isinstance(event.get("details"), dict) else {}
            out.append(
                {
                    "kind": "pre_resume_event",
                    "job_id": int(event.get("job_id") or job_id),
                    "created_at": event.get("created_at"),
                    "event_type": event.get("event_type"),
                    "intent": event.get("intent"),
                    "state_status": event.get("state_status"),
                    "inbound_preview": str(event.get("inbound_text") or "")[:180] or None,
                    "outbound_preview": str(event.get("outbound_text") or "")[:180] or None,
                    "reason": details.get("reason") if isinstance(details, dict) else None,
                    "signals": details,
                }
            )

        for item in logs:
            details = item.get("details") if isinstance(item.get("details"), dict) else {}
            operation = str(item.get("operation") or "").strip().lower()
            status = str(item.get("status") or "").strip().lower()
            error_text = str(details.get("error") or "")
            if (
                operation == "agent.interview.sync"
                and status == "error"
                and "INTERVIEW_SESSION_NOT_FOUND" in error_text
            ):
                continue
            out.append(
                {
                    "kind": "operation_log",
                    "job_id": int(details.get("job_id") or job_id),
                    "created_at": item.get("created_at"),
                    "operation": item.get("operation"),
                    "status": item.get("status"),
                    "entity_type": item.get("entity_type"),
                    "entity_id": item.get("entity_id"),
                    "signals": details,
                }
            )
        out.sort(
            key=lambda item: self._safe_parse_time(str(item.get("created_at") or "")),
            reverse=True,
        )
        return out[:400]

    def _build_culture_fit(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        match: Dict[str, Any],
        scorecard: Dict[str, Any],
        pre_resume_events: List[Dict[str, Any]],
        conversations: List[Dict[str, Any]],
        resume_links: List[str],
    ) -> Dict[str, Any]:
        notes = match.get("verification_notes") if isinstance(match.get("verification_notes"), dict) else {}
        raw_profile = notes.get("company_culture_profile") if isinstance(notes.get("company_culture_profile"), dict) else {}
        jd_text = str(job.get("jd_text") or "").lower()
        values = [str(x).strip() for x in (raw_profile.get("values") or []) if str(x).strip()]
        if not values:
            inferred = []
            if any(token in jd_text for token in ("ownership", "autonomy", "owner", "self directed")):
                inferred.append("high ownership")
            if any(token in jd_text for token in ("fast", "move fast", "speed", "quickly")):
                inferred.append("fast execution")
            if any(token in jd_text for token in ("communication", "written", "stakeholder", "clarity")):
                inferred.append("clear communication")
            if any(token in jd_text for token in ("product", "customer", "impact")):
                inferred.append("product thinking")
            if any(token in jd_text for token in ("architecture", "scalable", "quality", "testing")):
                inferred.append("engineering rigor")
            values = inferred

        candidate_skills = [str(item).strip().lower() for item in (candidate.get("skills") or []) if str(item).strip()]
        communication_score = self._safe_float(((scorecard.get("communication") or {}).get("latest_score")), None)
        interview_score = self._safe_float(((scorecard.get("interview_evaluation") or {}).get("latest_score")), None)
        alignment: List[str] = []
        concerns: List[str] = []
        evidence: List[str] = []
        predictive_signals_raw = notes.get("predictive_behavior_signals") if isinstance(notes.get("predictive_behavior_signals"), list) else []
        predictive_signals = [str(x).strip() for x in predictive_signals_raw if str(x).strip()]
        chat_signal_lines = self._extract_chat_signal_lines(pre_resume_events=pre_resume_events, conversations=conversations)
        interview_details = (scorecard.get("interview_evaluation") or {}).get("stages")
        interview_latest = interview_details[0] if isinstance(interview_details, list) and interview_details else {}
        interview_signals = (
            interview_latest.get("signals") if isinstance(interview_latest.get("signals"), dict) else {}
        )

        if communication_score is not None and communication_score >= 75:
            evidence.append(f"Communication score {communication_score:.1f} indicates clear async communication")
            alignment.append("strong communication fit for distributed startup environment")
        elif communication_score is not None and communication_score < 60:
            concerns.append("communication signal is below expected bar for high context async collaboration")
        if interview_score is not None and interview_score >= 80:
            evidence.append(f"Interview score {interview_score:.1f} supports decision quality and ownership")
            alignment.append("demonstrates ownership and architecture judgement")
        elif interview_score is not None and interview_score < 65:
            concerns.append("interview signal indicates inconsistent ownership or decision quality")
        if any(skill in candidate_skills for skill in ("llm", "ai", "rag")):
            alignment.append("brings direct AI product exposure relevant to agentic workflow roadmap")
        if resume_links:
            evidence.append(f"Resume submitted ({len(resume_links)} file(s)) and included in culture analysis context")
        if chat_signal_lines:
            evidence.append(f"Conversation analysis used {len(chat_signal_lines)} communication excerpts")
            if any("team" in line.lower() or "deployment" in line.lower() or "roadmap" in line.lower() for line in chat_signal_lines):
                alignment.append("asks team or delivery context questions, a positive long term retention pattern")
        if isinstance(interview_signals, dict):
            depth = interview_signals.get("interview_signals")
            if isinstance(depth, list) and depth:
                evidence.extend([str(x).strip() for x in depth[:2] if str(x).strip()])

        style = str(raw_profile.get("team_style") or "").strip()
        decision_style = str(raw_profile.get("decision_style") or "").strip()
        if style:
            evidence.append(f"Company team style: {style}")
        if decision_style:
            evidence.append(f"Decision style: {decision_style}")
        if predictive_signals:
            evidence.extend(predictive_signals[:3])
            alignment.append("predictive behavior signals align with historical retention patterns")
        if not alignment:
            concerns.append("limited direct evidence for culture alignment from resume, chat, and interview data")

        analysis = self._build_culture_agent_analysis(
            job=job,
            candidate=candidate,
            company_culture_profile=raw_profile,
            values=values,
            resume_links=resume_links,
            chat_signal_lines=chat_signal_lines,
            interview_score=interview_score,
            interview_signals=interview_signals if isinstance(interview_signals, dict) else {},
            alignment=alignment,
            concerns=concerns,
            predictive_signals=predictive_signals,
        )

        return {
            "company_values": values,
            "alignment_highlights": alignment[:6],
            "concerns": concerns[:4],
            "evidence": evidence[:8],
            "predictive_signals": predictive_signals[:6],
            "analysis": analysis,
        }

    @staticmethod
    def _extract_chat_signal_lines(
        *,
        pre_resume_events: List[Dict[str, Any]],
        conversations: List[Dict[str, Any]],
    ) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for event in pre_resume_events:
            inbound = str(event.get("inbound_text") or "").strip()
            outbound = str(event.get("outbound_text") or "").strip()
            for text in (inbound, outbound):
                if not text:
                    continue
                key = text.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(text[:240])
        for row in conversations:
            last_message = str(row.get("last_message") or "").strip()
            if not last_message:
                continue
            key = last_message.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(last_message[:240])
        return out[:12]

    def _build_culture_agent_analysis(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        company_culture_profile: Dict[str, Any],
        values: List[str],
        resume_links: List[str],
        chat_signal_lines: List[str],
        interview_score: float | None,
        interview_signals: Dict[str, Any],
        alignment: List[str],
        concerns: List[str],
        predictive_signals: List[str],
    ) -> Dict[str, Any]:
        agent_name = AGENT_DEFAULT_NAMES.get("culture_analyst", "Harper AI (Culture Analyst)")
        fallback = (
            f"{agent_name} sees culture fit as "
            f"{'strong' if alignment else 'unclear'} based on resume submission, chat behavior, and interview evidence."
        )
        if predictive_signals:
            fallback = f"{fallback} Key predictive signals: {'; '.join(predictive_signals[:2])}"

        if self.llm_responder is not None:
            instruction = (
                "You are the Culture Analyst for candidate evaluation.\n"
                "Write one concise paragraph in plain text.\n"
                "Explain culture fit against company culture profile using ONLY available resume, chat, and interview signals.\n"
                "Include at least one specific behavioral signal and one risk or caveat.\n"
                "No bullet points."
            )
            inbound_text = json.dumps(
                {
                    "company_culture_profile": company_culture_profile,
                    "company_values": values,
                    "resume_links_count": len(resume_links),
                    "chat_signals": chat_signal_lines[:8],
                    "interview_score": interview_score,
                    "interview_signals": interview_signals,
                    "alignment_highlights": alignment[:6],
                    "concerns": concerns[:4],
                    "predictive_signals": predictive_signals[:6],
                },
                ensure_ascii=False,
            )
            generated = self.llm_responder.generate_candidate_reply(
                mode="candidate_profile_culture_fit",
                instruction=instruction,
                job=job,
                candidate=candidate,
                inbound_text=inbound_text,
                history=[],
                fallback_reply=fallback,
                language="en",
                state={"culture_analyst": True},
            )
            summary = str(generated or fallback).strip() or fallback
            source = "llm" if summary != fallback else "fallback"
        else:
            summary = fallback
            source = "fallback"

        return {
            "agent_key": "culture_analyst",
            "agent_name": agent_name,
            "source": source,
            "summary": summary,
            "inputs": {
                "resume_links_count": len(resume_links),
                "chat_excerpt_count": len(chat_signal_lines),
                "interview_score": interview_score,
            },
        }

    @staticmethod
    def _filter_logs_for_job(logs: List[Dict[str, Any]], job_id: int, conversation_ids: set[int]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in logs:
            details = item.get("details") if isinstance(item.get("details"), dict) else {}
            details_job_id = int(details.get("job_id") or 0)
            if details_job_id == int(job_id):
                out.append(item)
                continue
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if entity_type == "conversation":
                try:
                    entity_id = int(item.get("entity_id") or 0)
                except (TypeError, ValueError):
                    entity_id = 0
                if entity_id > 0 and entity_id in conversation_ids:
                    out.append(item)
        return out

    @staticmethod
    def _collect_resume_links(sessions: List[Dict[str, Any]]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        ordered = sorted(
            sessions,
            key=lambda item: CandidateProfileService._safe_parse_time(str(item.get("updated_at") or "")),
            reverse=True,
        )
        for row in ordered:
            candidates = []
            resume_links = row.get("resume_links")
            if isinstance(resume_links, list):
                candidates.extend(resume_links)
            state_json = row.get("state_json") if isinstance(row.get("state_json"), dict) else {}
            nested = state_json.get("resume_links")
            if isinstance(nested, list):
                candidates.extend(nested)
            for raw in candidates:
                link = str(raw or "").strip()
                if not link or link in seen:
                    continue
                seen.add(link)
                out.append(link)
        return out

    @staticmethod
    def _safe_parse_time(raw: str) -> datetime:
        text = str(raw or "").strip()
        if not text:
            return datetime.min.replace(tzinfo=UTC)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min.replace(tzinfo=UTC)

    @staticmethod
    def _safe_float(value: Any, fallback: float | None) -> float | None:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return fallback
        return num
