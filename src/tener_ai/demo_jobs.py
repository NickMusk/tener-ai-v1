from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .db import AGENT_DEFAULT_NAMES, Database
from .pre_resume_service import PreResumeCommunicationService


UTC = timezone.utc
MAIN_DASHBOARD_DEMO_SEED_KEY = "interexy-middle-js-v1"
MAIN_DASHBOARD_DEMO_COMPANY = "Interexy"
MAIN_DASHBOARD_DEMO_TITLE = "Middle JS Developer"
MAIN_DASHBOARD_DEMO_COMPANY_WEBSITE = "https://interexy.com/"
MAIN_DASHBOARD_DEMO_LOCATION = "Warsaw, Poland / Hybrid"
MAIN_DASHBOARD_DEMO_ACCOUNT_PROVIDER_ID = "interexy-demo-recruiter-pl"
MAIN_DASHBOARD_DEMO_ACCOUNT_LABEL = "Demo Recruiter PL"
MAIN_DASHBOARD_DEMO_PREFIX = "seed-interexy-middle-js"

STAGE_PLAN = [
    ("queued", 92),
    ("queued_delivery", 12),
    ("connect_sent", 28),
    ("dialogue", 26),
    ("cv_received", 18),
    ("interview_pending", 9),
    ("interview_passed", 4),
    ("interview_failed", 3),
    ("closed", 8),
]

FIRST_NAMES = [
    "Aleksandra",
    "Jakub",
    "Marta",
    "Piotr",
    "Anna",
    "Michal",
    "Katarzyna",
    "Pawel",
    "Monika",
    "Tomasz",
    "Natalia",
    "Kamil",
    "Karolina",
    "Mateusz",
    "Agnieszka",
    "Adrian",
    "Julia",
    "Dawid",
    "Patrycja",
    "Szymon",
]
LAST_NAMES = [
    "Kowalski",
    "Nowak",
    "Wisniewski",
    "Wojcik",
    "Kowalczyk",
    "Kaminski",
    "Lewandowski",
    "Zielinski",
    "Szymanski",
    "Wozniak",
]
LOCATIONS = [
    "Warsaw, Poland",
    "Krakow, Poland",
    "Wroclaw, Poland",
    "Gdansk, Poland",
    "Poznan, Poland",
    "Lodz, Poland",
]
HEADLINES = [
    "Frontend Engineer | React, TypeScript, Product Delivery",
    "JavaScript Developer | React, Node.js, REST APIs",
    "React Engineer | TypeScript, Testing, Product Teams",
    "Full Stack JavaScript Engineer | React, Node.js, APIs",
]
EXTRA_SKILLS = [
    "next.js",
    "playwright",
    "graphql",
    "aws",
    "docker",
    "cypress",
    "redux",
    "vite",
    "jest",
    "analytics",
]
REQUIRED_SKILLS = ["javascript", "typescript", "react", "node.js", "rest api", "testing"]
NICE_TO_HAVE_SKILLS = ["next.js", "playwright", "graphql", "aws", "product analytics"]
QUESTIONABLE_SKILLS = ["wordpress", "php only", "jquery only"]
QUESTIONS = [
    {
        "category": "hard_skills",
        "title": "React Architecture Under Delivery Pressure",
        "question": (
            "Walk through how you would structure a medium-sized React and TypeScript feature for Interexy "
            "so it ships quickly without collapsing under future changes."
        ),
        "time_to_answer": 180,
        "time_to_think": 20,
    },
    {
        "category": "hard_skills",
        "title": "Node.js API Ownership",
        "question": (
            "Describe a Node.js API or integration you owned end to end. What broke in production, "
            "what did you change, and how did you keep the system observable?"
        ),
        "time_to_answer": 180,
        "time_to_think": 20,
    },
    {
        "category": "hard_skills",
        "title": "Testing Strategy For Product Teams",
        "question": (
            "Interexy ships client-facing features fast. Explain which parts you would cover with unit, "
            "integration, and end-to-end tests and which parts you would leave lighter."
        ),
        "time_to_answer": 150,
        "time_to_think": 20,
    },
    {
        "category": "soft_skills",
        "title": "Debugging Communication",
        "question": (
            "Tell us about a bug that affected users. How did you communicate the issue, tradeoffs, "
            "and fix plan to product and non-engineering stakeholders?"
        ),
        "time_to_answer": 150,
        "time_to_think": 15,
    },
    {
        "category": "cultural_fit",
        "title": "Ownership In Async Teams",
        "question": (
            "Interexy works with direct written communication and strong ownership. "
            "Give an example of a feature or incident where you drove progress without waiting for detailed instructions."
        ),
        "time_to_answer": 150,
        "time_to_think": 15,
    },
]
INTERVIEW_META = {
    "categories": {"hard_skills": 3, "soft_skills": 1, "cultural_fit": 1},
    "estimated_minutes": 18,
    "language": "en",
    "company_name": MAIN_DASHBOARD_DEMO_COMPANY,
    "role_title": MAIN_DASHBOARD_DEMO_TITLE,
}
BASE_SCORE_MAP = {
    "queued": 0.73,
    "queued_delivery": 0.76,
    "connect_sent": 0.79,
    "dialogue": 0.82,
    "cv_received": 0.86,
    "interview_pending": 0.89,
    "interview_passed": 0.94,
    "interview_failed": 0.85,
    "closed": 0.80,
}
MATCH_STATUS_MAP = {
    "queued": "verified",
    "queued_delivery": "verified",
    "connect_sent": "outreach_pending_connection",
    "dialogue": "outreach_sent",
    "cv_received": "resume_received",
    "interview_pending": "interview_invited",
    "interview_passed": "interview_scored",
    "interview_failed": "interview_scored",
    "closed": "outreach_sent",
}
CLOSED_OUTCOMES = [
    "not_interested",
    "not_interested",
    "not_interested",
    "not_interested",
    "not_interested",
    "unreachable",
    "unreachable",
    "stalled",
]


def is_main_dashboard_demo_job(job: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(job, dict):
        return False
    culture_profile = job.get("company_culture_profile")
    if isinstance(culture_profile, dict):
        seed_meta = culture_profile.get("seed_meta")
        if isinstance(seed_meta, dict):
            return str(seed_meta.get("seed_key") or "").strip().lower() == MAIN_DASHBOARD_DEMO_SEED_KEY
    return False


class MainDashboardDemoJobSeeder:
    def __init__(
        self,
        *,
        db: Any,
        pre_resume_service: Optional[PreResumeCommunicationService] = None,
        interview_assessment_preparer: Optional[Callable[[int], Dict[str, Any]]] = None,
        postgres_dsn: str = "",
    ) -> None:
        self.db = db
        self.pre_resume_service = pre_resume_service
        self.interview_assessment_preparer = interview_assessment_preparer
        self.postgres_dsn = str(postgres_dsn or "").strip()
        self.base_ts = datetime(2026, 3, 12, 8, 0, tzinfo=UTC)

    @property
    def primary_db(self) -> Database:
        primary = getattr(self.db, "_primary", self.db)
        if not isinstance(primary, Database):
            raise RuntimeError("demo seeder requires sqlite primary database")
        return primary

    def ensure_seeded(self, *, force_reseed: bool = False) -> Dict[str, Any]:
        existing = self._find_existing_job()
        created = False
        if existing is None:
            job_id = self.db.insert_job(
                title=MAIN_DASHBOARD_DEMO_TITLE,
                company=MAIN_DASHBOARD_DEMO_COMPANY,
                company_website=MAIN_DASHBOARD_DEMO_COMPANY_WEBSITE,
                jd_text=self._job_description(),
                location=MAIN_DASHBOARD_DEMO_LOCATION,
                preferred_languages=["en", "pl"],
                seniority="middle",
                must_have_skills=REQUIRED_SKILLS,
                nice_to_have_skills=NICE_TO_HAVE_SKILLS,
                questionable_skills=QUESTIONABLE_SKILLS,
                salary_min=21000,
                salary_max=26000,
                salary_currency="PLN",
                work_authorization_required=True,
                linkedin_routing_mode="manual",
            )
            created = True
        else:
            job_id = int(existing.get("id") or 0)
            if force_reseed or not self._is_seed_complete(job_id=job_id):
                self._reset_seed_job(job_id=job_id)
            else:
                summary = self._build_summary(job_id=job_id)
                return {
                    "status": "ok",
                    "created": False,
                    "seeded": False,
                    "job_id": job_id,
                    "job": self.db.get_job(job_id),
                    "seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY,
                    "summary": summary,
                }

        self._upsert_job_metadata(job_id=job_id)
        account_id = self._ensure_account()
        self.db.replace_job_linkedin_account_assignments(job_id=job_id, account_ids=[account_id])
        assessment_payload = self._prepare_interview_assessment(job_id=job_id)
        self._seed_candidates(job_id=job_id, account_id=account_id)
        summary = self._build_summary(job_id=job_id)
        self._seed_job_progress(job_id=job_id, summary=summary, interview_assessment=assessment_payload)
        self._seed_operation_logs(job_id=job_id, summary=summary, interview_assessment=assessment_payload)
        self.db.log_operation(
            operation="admin.seed.full_demo_job",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "job_id": job_id,
                "seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY,
                "created": created,
                "force_reseed": bool(force_reseed),
                "summary": summary,
            },
        )
        return {
            "status": "ok",
            "created": created,
            "seeded": True,
            "job_id": job_id,
            "job": self.db.get_job(job_id),
            "seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY,
            "summary": summary,
        }

    def _find_existing_job(self) -> Optional[Dict[str, Any]]:
        for item in self.db.list_jobs(limit=500, include_archived=True):
            if is_main_dashboard_demo_job(item):
                return item
        return None

    def _is_seed_complete(self, *, job_id: int) -> bool:
        summary = self._build_summary(job_id=job_id)
        expected = dict(STAGE_PLAN)
        return int(summary.get("total_candidates") or 0) == 200 and summary.get("ats_counts") == expected

    def _upsert_job_metadata(self, *, job_id: int) -> None:
        with self.primary_db.transaction() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET
                    title = ?,
                    company = ?,
                    company_website = ?,
                    jd_text = ?,
                    location = ?,
                    preferred_languages = ?,
                    seniority = ?,
                    must_have_skills = ?,
                    nice_to_have_skills = ?,
                    questionable_skills = ?,
                    salary_min = ?,
                    salary_max = ?,
                    salary_currency = ?,
                    work_authorization_required = ?,
                    linkedin_routing_mode = ?,
                    archived_at = NULL
                WHERE id = ?
                """,
                (
                    MAIN_DASHBOARD_DEMO_TITLE,
                    MAIN_DASHBOARD_DEMO_COMPANY,
                    MAIN_DASHBOARD_DEMO_COMPANY_WEBSITE,
                    self._job_description(),
                    MAIN_DASHBOARD_DEMO_LOCATION,
                    json.dumps(["en", "pl"]),
                    "middle",
                    json.dumps(REQUIRED_SKILLS),
                    json.dumps(NICE_TO_HAVE_SKILLS),
                    json.dumps(QUESTIONABLE_SKILLS),
                    21000,
                    26000,
                    "PLN",
                    1,
                    "manual",
                    int(job_id),
                ),
            )
        self.db.upsert_job_culture_profile(
            job_id=int(job_id),
            status="generated",
            company_name=MAIN_DASHBOARD_DEMO_COMPANY,
            company_website=MAIN_DASHBOARD_DEMO_COMPANY_WEBSITE,
            profile={
                "values": ["weekly shipping", "direct feedback", "ownership", "clear writing"],
                "team_style": (
                    "Small product squad with one product manager, one designer, "
                    "and four engineers delivering weekly releases."
                ),
                "ideal_candidate_traits": [
                    "Owns frontend features end to end",
                    "Explains tradeoffs explicitly",
                    "Comfortable with production bugs and iteration",
                    "Works well in English-first async communication",
                ],
                "culture_interview_questions": [item["title"] for item in QUESTIONS[-2:]],
                "seed_meta": {
                    "seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY,
                    "seed_version": 1,
                    "ideal_close_summary": (
                        "Search launched on day 1, shortlist stabilized on day 3, resumes reviewed on day 5, "
                        "async interview scored by day 8, offer accepted on day 11."
                    ),
                },
            },
            sources=[{"kind": "server_seed", "label": "deterministic demo data"}],
            warnings=[],
            search_queries=[
                "Middle JavaScript Developer Poland React TypeScript",
                "Interexy frontend product engineer Poland",
            ],
            error=None,
        )

    def _ensure_account(self) -> int:
        return int(
            self.db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id=MAIN_DASHBOARD_DEMO_ACCOUNT_PROVIDER_ID,
                status="connected",
                label=MAIN_DASHBOARD_DEMO_ACCOUNT_LABEL,
                metadata={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "synthetic": True},
            )
        )

    def _reset_seed_job(self, *, job_id: int) -> None:
        candidate_ids = [
            int(item.get("candidate_id") or 0)
            for item in self.db.list_candidates_for_job(job_id)
            if int(item.get("candidate_id") or 0) > 0
        ]
        with self.primary_db.transaction() as conn:
            conn.execute(
                "DELETE FROM messages WHERE conversation_id IN (SELECT id FROM conversations WHERE job_id = ?)",
                (int(job_id),),
            )
            conn.execute(
                "DELETE FROM pre_resume_events WHERE conversation_id IN (SELECT id FROM conversations WHERE job_id = ?)",
                (int(job_id),),
            )
            conn.execute("DELETE FROM pre_resume_sessions WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM candidate_prescreens WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM candidate_agent_assessments WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM candidate_signals WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM resume_assets WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM outbound_actions WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM outreach_account_events WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM conversations WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM job_candidates WHERE job_id = ?", (int(job_id),))
            conn.execute("DELETE FROM job_step_progress WHERE job_id = ?", (int(job_id),))
            conn.execute(
                "DELETE FROM operation_logs WHERE entity_type = 'job' AND entity_id = ?",
                (str(job_id),),
            )
            if candidate_ids:
                placeholders = ",".join(["?"] * len(candidate_ids))
                conn.execute(
                    f"""
                    DELETE FROM candidates
                    WHERE id IN ({placeholders})
                      AND linkedin_id LIKE ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM job_candidates m
                          WHERE m.candidate_id = candidates.id
                      )
                    """,
                    tuple(candidate_ids) + (f"{MAIN_DASHBOARD_DEMO_PREFIX}-%",),
                )

    def _prepare_interview_assessment(self, *, job_id: int) -> Dict[str, Any]:
        fallback = {
            "status": "seeded",
            "assessment_name": f"{MAIN_DASHBOARD_DEMO_COMPANY} - {MAIN_DASHBOARD_DEMO_TITLE} Interview",
            "questions": QUESTIONS,
            "meta": INTERVIEW_META,
        }
        if self.interview_assessment_preparer is None:
            return fallback
        try:
            prepared = self.interview_assessment_preparer(int(job_id))
        except Exception as exc:
            return {"status": "error", "reason": "prepare_failed", "details": {"message": str(exc)}, **fallback}
        payload = dict(fallback)
        payload["provider_prepare"] = prepared
        if str(prepared.get("status") or "").strip().lower() == "ok":
            payload["status"] = "ok"
        return payload

    def _seed_candidates(self, *, job_id: int, account_id: int) -> None:
        index = 0
        for stage, count in STAGE_PLAN:
            for local_index in range(count):
                self._seed_candidate(
                    job_id=job_id,
                    account_id=account_id,
                    stage=stage,
                    global_index=index,
                    local_index=local_index,
                )
                index += 1

    def _seed_candidate(self, *, job_id: int, account_id: int, stage: str, global_index: int, local_index: int) -> None:
        profile = self._candidate_profile(global_index)
        candidate_id = int(self.db.upsert_candidate(profile, source="seed"))
        notes = self._verification_notes(stage=stage, profile=profile, global_index=global_index)
        self.db.create_candidate_match(
            job_id=int(job_id),
            candidate_id=candidate_id,
            score=self._match_score(stage=stage, global_index=global_index),
            status=MATCH_STATUS_MAP[stage],
            verification_notes=notes,
        )
        self._record_sourcing_assessment(
            candidate_id=candidate_id,
            job_id=job_id,
            stage=stage,
            global_index=global_index,
        )
        if stage == "queued":
            return
        conversation_id = int(self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin"))
        self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
        if stage == "queued_delivery":
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            self.db.create_outbound_action(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                action_type="linkedin_message",
                account_id=account_id,
                payload={
                    "planned_action_kind": "message",
                    "delivery_mode": "message_only",
                    "type": "outreach_after_connection",
                    "template": "product_intro_short",
                },
                priority=40,
            )
            self._record_communication_assessment(
                candidate_id=candidate_id,
                job_id=job_id,
                stage=stage,
                global_index=global_index,
            )
            self.db.insert_outreach_account_event(
                event_key=self._event_key(candidate_id=candidate_id, suffix="queued"),
                account_id=account_id,
                event_type="message_queued",
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
            )
            return
        if stage == "connect_sent":
            self.db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")
            self.db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content=(
                    "Connection request sent for Interexy Middle JS Developer. "
                    "Shared short intro and product context."
                ),
                candidate_language="en",
                meta={"type": "connect_request", "delivery_status": "pending_connection"},
            )
            self._record_communication_assessment(
                candidate_id=candidate_id,
                job_id=job_id,
                stage=stage,
                global_index=global_index,
            )
            self.db.insert_outreach_account_event(
                event_key=self._event_key(candidate_id=candidate_id, suffix="connect"),
                account_id=account_id,
                event_type="connection_requested",
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
            )
            return
        if stage == "dialogue":
            self._seed_dialogue_candidate(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                account_id=account_id,
                stage=stage,
                global_index=global_index,
            )
            return
        if stage in {"cv_received", "interview_pending", "interview_passed", "interview_failed"}:
            self._seed_resume_candidate(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                account_id=account_id,
                stage=stage,
                global_index=global_index,
            )
            return
        if stage == "closed":
            self._seed_closed_candidate(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                account_id=account_id,
                global_index=global_index,
                outcome=CLOSED_OUTCOMES[local_index],
            )

    def _seed_dialogue_candidate(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        account_id: int,
        stage: str,
        global_index: int,
    ) -> None:
        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=(
                "Hi, your React and TypeScript background looks relevant for Interexy. "
                "Open to a quick intro?"
            ),
            candidate_language="en",
            meta={"type": "outreach_after_connection", "delivery_status": "sent"},
        )
        self.db.add_message(
            conversation_id=conversation_id,
            direction="inbound",
            content=(
                "Potentially yes. Can you share more about product scope, salary range, "
                "and hybrid expectations in Warsaw?"
            ),
            candidate_language="en",
            meta={},
        )
        session = self._start_pre_resume_session(
            session_id="pre-%s-%s" % (job_id, candidate_id),
            candidate_name=str(self.db.get_candidate(candidate_id).get("full_name") or "Candidate"),
            language="en",
        )
        state = dict(session["state"])
        state.update({"status": "engaged_no_resume", "turns": 2, "last_intent": "asked_role_details"})
        self.db.upsert_pre_resume_session(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state=state,
            instruction="deterministic demo seed",
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="session_started",
            intent=None,
            inbound_text=None,
            outbound_text=session["intro_message"],
            state_status="awaiting_reply",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="candidate_replied",
            intent="asked_role_details",
            inbound_text="Asked for scope, salary, and location details.",
            outbound_text=None,
            state_status="engaged_no_resume",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.upsert_candidate_prescreen(
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            status="incomplete",
            must_have_answers_json=[
                {"question": "Commercial React and TypeScript experience", "answer": "4 years", "status": "met"},
                {"question": "Node.js API ownership", "answer": "Worked on partner integrations", "status": "met"},
            ],
            salary_expectation_gross_monthly=((22000 + (global_index % 4) * 500) + (24500 + (global_index % 4) * 500)) / 2,
            salary_expectation_currency="PLN",
            location_confirmed=True,
            work_authorization_confirmed=True,
            cv_received=False,
            summary="Dialogue in progress. Candidate asked for more scope before sharing CV.",
            notes="Healthy mid-funnel conversation.",
        )
        self._record_communication_assessment(
            candidate_id=candidate_id,
            job_id=job_id,
            stage=stage,
            global_index=global_index,
        )
        self.db.insert_outreach_account_event(
            event_key=self._event_key(candidate_id=candidate_id, suffix="dialogue-sent"),
            account_id=account_id,
            event_type="message_sent",
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.insert_outreach_account_event(
            event_key=self._event_key(candidate_id=candidate_id, suffix="dialogue-reply"),
            account_id=account_id,
            event_type="reply_received",
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )

    def _seed_resume_candidate(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        account_id: int,
        stage: str,
        global_index: int,
    ) -> None:
        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        cv_url = "https://example.com/cv/%s-%03d.pdf" % (MAIN_DASHBOARD_DEMO_PREFIX, global_index + 1)
        self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=(
                "Thanks for connecting. First a few written qualifying questions, then CV, "
                "then a short screening call."
            ),
            candidate_language="en",
            meta={"type": "outreach_after_connection", "delivery_status": "sent"},
        )
        self.db.add_message(
            conversation_id=conversation_id,
            direction="inbound",
            content="Looks aligned. Sharing my CV here: %s" % cv_url,
            candidate_language="en",
            meta={},
        )
        followup_text = "Great, CV received. We are moving you to screening review."
        if stage == "interview_pending":
            followup_text = "You are moving to the async interview stage. Expect a short structured interview link."
        elif stage == "interview_passed":
            followup_text = "Interview score landed above bar. We are preparing shortlist and offer discussion."
        elif stage == "interview_failed":
            followup_text = "Thanks again. We completed the interview review and will close the loop shortly."
        self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=followup_text,
            candidate_language="en",
            meta={"type": "interview_invite" if stage == "interview_pending" else "followup", "delivery_status": "sent"},
        )
        session = self._start_pre_resume_session(
            session_id="pre-%s-%s" % (job_id, candidate_id),
            candidate_name=str(self.db.get_candidate(candidate_id).get("full_name") or "Candidate"),
            language="en",
        )
        state = dict(session["state"])
        state.update(
            {
                "status": "resume_received",
                "turns": 3,
                "last_intent": "resume_shared",
                "resume_links": [cv_url],
                "cv_received": True,
            }
        )
        self.db.upsert_pre_resume_session(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state=state,
            instruction="deterministic demo seed",
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="session_started",
            intent=None,
            inbound_text=None,
            outbound_text=session["intro_message"],
            state_status="awaiting_reply",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="resume_shared",
            intent="resume_shared",
            inbound_text="Candidate shared CV and compensation expectations.",
            outbound_text=None,
            state_status="resume_received",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "cv_url": cv_url},
        )
        self.db.upsert_candidate_prescreen(
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            status="ready_for_screening_call",
            must_have_answers_json=[
                {"question": "Commercial React and TypeScript ownership", "answer": "4 to 6 years in SaaS teams", "status": "met"},
                {"question": "Node.js API work", "answer": "Built and maintained partner and product APIs", "status": "met"},
                {"question": "Testing discipline", "answer": "Uses Jest or Playwright on core codepaths", "status": "met"},
            ],
            salary_expectation_gross_monthly=((23000 + (global_index % 4) * 500) + (25500 + (global_index % 4) * 500)) / 2,
            salary_expectation_currency="PLN",
            location_confirmed=True,
            work_authorization_confirmed=True,
            cv_received=True,
            summary="Written screen complete and CV attached.",
            notes="Ready for screening call and interview handling.",
        )
        self._record_communication_assessment(
            candidate_id=candidate_id,
            job_id=job_id,
            stage=stage,
            global_index=global_index,
        )
        self.db.insert_outreach_account_event(
            event_key=self._event_key(candidate_id=candidate_id, suffix="resume-sent"),
            account_id=account_id,
            event_type="message_sent",
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.insert_outreach_account_event(
            event_key=self._event_key(candidate_id=candidate_id, suffix="resume-reply"),
            account_id=account_id,
            event_type="reply_received",
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        if stage in {"interview_pending", "interview_passed", "interview_failed"}:
            passed = stage == "interview_passed"
            total_score = float(self._verification_notes(stage=stage, profile=self.db.get_candidate(candidate_id) or {}, global_index=global_index).get("interview_total_score") or 84.0)
            self._record_interview_assessment(
                candidate_id=candidate_id,
                job_id=job_id,
                stage=stage,
                total_score=total_score,
                passed=passed,
            )

    def _seed_closed_candidate(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        account_id: int,
        global_index: int,
        outcome: str,
    ) -> None:
        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content="Hi, your JS profile looks relevant for Interexy. Open to a quick conversation?",
            candidate_language="en",
            meta={"type": "outreach_after_connection", "delivery_status": "sent"},
        )
        inbound = ""
        if outcome == "not_interested":
            inbound = "Thanks, but I am not considering a move right now."
        elif outcome == "stalled":
            inbound = "This is interesting, but timing is bad. Maybe later in the quarter."
        if inbound:
            self.db.add_message(
                conversation_id=conversation_id,
                direction="inbound",
                content=inbound,
                candidate_language="en",
                meta={},
            )
        session = self._start_pre_resume_session(
            session_id="pre-%s-%s" % (job_id, candidate_id),
            candidate_name=str(self.db.get_candidate(candidate_id).get("full_name") or "Candidate"),
            language="en",
        )
        state = dict(session["state"])
        state.update(
            {
                "status": outcome,
                "turns": 1 if outcome == "unreachable" else 2,
                "last_intent": outcome,
                "last_error": "followup_window_expired" if outcome == "stalled" else None,
            }
        )
        self.db.upsert_pre_resume_session(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            job_id=job_id,
            candidate_id=candidate_id,
            state=state,
            instruction="deterministic demo seed",
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="session_started",
            intent=None,
            inbound_text=None,
            outbound_text=session["intro_message"],
            state_status="awaiting_reply",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        self.db.insert_pre_resume_event(
            session_id=session["session_id"],
            conversation_id=conversation_id,
            event_type="outcome_recorded",
            intent=outcome,
            inbound_text=outcome.replace("_", " "),
            outbound_text=None,
            state_status=outcome,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY},
        )
        if outcome != "unreachable":
            self.db.upsert_candidate_prescreen(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                status=outcome,
                must_have_answers_json=[],
                salary_expectation_currency="PLN",
                cv_received=False,
                summary="Candidate exited the funnel before CV review.",
                notes=outcome.replace("_", " ").title(),
            )
        self._record_communication_assessment(
            candidate_id=candidate_id,
            job_id=job_id,
            stage="closed",
            global_index=global_index,
        )
        self.db.insert_outreach_account_event(
            event_key=self._event_key(candidate_id=candidate_id, suffix="closed"),
            account_id=account_id,
            event_type="message_sent",
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "outcome": outcome},
        )

    def _record_sourcing_assessment(self, *, candidate_id: int, job_id: int, stage: str, global_index: int) -> None:
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="sourcing_vetting",
            agent_name=AGENT_DEFAULT_NAMES["sourcing_vetting"],
            stage_key="vetting",
            score=round(self._match_score(stage=stage, global_index=global_index) * 100.0, 1),
            status="qualified",
            reason="Strong match on JavaScript, TypeScript, React, and product delivery expectations.",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "stage": stage},
        )

    def _record_communication_assessment(
        self,
        *,
        candidate_id: int,
        job_id: int,
        stage: str,
        global_index: int,
    ) -> None:
        status_map = {
            "queued_delivery": "queued",
            "connect_sent": "outreach_pending_connection",
            "dialogue": "in_dialogue",
            "cv_received": "cv_received",
            "interview_pending": "interview_pending",
            "interview_passed": "offer_ready",
            "interview_failed": "interview_failed",
            "closed": "closed",
        }
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="communication",
            agent_name=AGENT_DEFAULT_NAMES["communication"],
            stage_key="dialogue",
            score=round(self._match_score(stage=stage, global_index=global_index) * 100.0 - 3.0, 1),
            status=status_map.get(stage, "review"),
            reason="Communication trail seeded to reflect realistic recruiter-candidate interaction.",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "stage": stage},
        )

    def _record_interview_assessment(
        self,
        *,
        candidate_id: int,
        job_id: int,
        stage: str,
        total_score: float,
        passed: bool,
    ) -> None:
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="interview_evaluation",
            agent_name=AGENT_DEFAULT_NAMES["interview_evaluation"],
            stage_key="interview_results",
            score=float(total_score),
            status="scored" if stage in {"interview_passed", "interview_failed"} else "in_progress",
            reason="Seeded interview outcome aligned with the full-job demo storyline.",
            details={"seed_key": MAIN_DASHBOARD_DEMO_SEED_KEY, "stage": stage, "passed": bool(passed)},
        )

    def _build_summary(self, *, job_id: int) -> Dict[str, Any]:
        items = self.db.list_candidates_for_job(job_id)
        ats_counts = Counter()
        current_status = Counter()
        for item in items:
            stage_payload = self.db.derive_candidate_ats_stage(item)
            ats_counts[str(stage_payload.get("ats_stage_key") or "")] += 1
            current_status[str(item.get("current_status_key") or "")] += 1
        return {
            "job_id": int(job_id),
            "total_candidates": len(items),
            "ats_counts": {key: int(ats_counts.get(key) or 0) for key, _ in STAGE_PLAN},
            "current_status_counts": dict(sorted((str(k), int(v)) for k, v in current_status.items())),
            "questions": [item["title"] for item in QUESTIONS],
            "selected_candidate": "Aleksandra Wisniewska",
        }

    def _seed_job_progress(
        self,
        *,
        job_id: int,
        summary: Dict[str, Any],
        interview_assessment: Dict[str, Any],
    ) -> None:
        source_total = 1284
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="culture_profile",
            status="success",
            output={
                "status": "ok",
                "company": MAIN_DASHBOARD_DEMO_COMPANY,
                "summary": "Culture profile loaded: weekly shipping, direct feedback, ownership, English-first written communication.",
            },
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="interview_assessment",
            status="success" if str(interview_assessment.get("status") or "").strip().lower() in {"ok", "seeded"} else "error",
            output=interview_assessment,
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="source",
            status="success",
            output={
                "status": "ok",
                "profiles_found": source_total,
                "top_matches": 200,
                "country": "Poland",
                "title": MAIN_DASHBOARD_DEMO_TITLE,
            },
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="enrich",
            status="success",
            output={"status": "ok", "attempted": 200, "enriched": 200, "linkedin_urls_attached": 200},
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="verify",
            status="success",
            output={
                "status": "ok",
                "verified": 200,
                "rejected": source_total - 200,
                "core_signal": "JavaScript + TypeScript + React + Node.js + product delivery",
            },
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="add",
            status="success",
            output={
                "status": "ok",
                "added": 200,
                "summary": "200 deterministic LinkedIn candidates added to the funnel.",
            },
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="outreach",
            status="success",
            output={
                "status": "ok",
                "queued_delivery": int(summary["ats_counts"]["queued_delivery"]),
                "connect_sent": int(summary["ats_counts"]["connect_sent"]),
                "dialogue": int(summary["ats_counts"]["dialogue"]),
                "cv_received": int(summary["ats_counts"]["cv_received"]),
                "closed": int(summary["ats_counts"]["closed"]),
            },
        )
        self.db.upsert_job_step_progress(
            job_id=job_id,
            step="workflow",
            status="success",
            output={
                "status": "ok",
                "job_id": int(job_id),
                "searched": source_total,
                "shortlisted": 200,
                "ats_distribution": summary["ats_counts"],
                "ideal_close": {
                    "selected_candidate": "Aleksandra Wisniewska",
                    "accepted_offer_in_days": 11,
                    "why": (
                        "Best mix of React ownership, Node.js API experience, testing discipline, "
                        "and direct written communication."
                    ),
                },
            },
        )

    def _seed_operation_logs(
        self,
        *,
        job_id: int,
        summary: Dict[str, Any],
        interview_assessment: Dict[str, Any],
    ) -> None:
        rows = [
            (
                "agent.sourcing.search",
                {
                    "job_id": job_id,
                    "profiles_found": 1284,
                    "target_market": "Poland",
                    "core_stack": REQUIRED_SKILLS[:4],
                },
                self._iso(self.base_ts),
            ),
            (
                "agent.sourcing.shortlist",
                {"job_id": job_id, "shortlisted": 200, "rejected": 1084, "score_threshold": 0.73},
                self._iso(self.base_ts + timedelta(minutes=12)),
            ),
            (
                "agent.enrichment.batch",
                {"job_id": job_id, "attempted": 200, "enriched": 200, "linkedin_urls_attached": 200},
                self._iso(self.base_ts + timedelta(minutes=22)),
            ),
            (
                "agent.verification.batch",
                {"job_id": job_id, "verified": 200, "must_have_skills": REQUIRED_SKILLS},
                self._iso(self.base_ts + timedelta(minutes=36)),
            ),
            (
                "agent.outreach.queue",
                {
                    "job_id": job_id,
                    "queued_delivery": summary["ats_counts"]["queued_delivery"],
                    "connect_sent": summary["ats_counts"]["connect_sent"],
                },
                self._iso(self.base_ts + timedelta(hours=1, minutes=5)),
            ),
            (
                "agent.outreach.replies",
                {
                    "job_id": job_id,
                    "dialogue": summary["ats_counts"]["dialogue"],
                    "cv_received": summary["ats_counts"]["cv_received"],
                    "closed": summary["ats_counts"]["closed"],
                },
                self._iso(self.base_ts + timedelta(days=2, minutes=10)),
            ),
            (
                "job.interview_assessment.prepare",
                {"job_id": job_id, "questions": [item["title"] for item in QUESTIONS], "prepare": interview_assessment},
                self._iso(self.base_ts + timedelta(days=3)),
            ),
            (
                "job.interview.invites",
                {
                    "job_id": job_id,
                    "interview_pending": summary["ats_counts"]["interview_pending"],
                    "interview_passed": summary["ats_counts"]["interview_passed"],
                    "interview_failed": summary["ats_counts"]["interview_failed"],
                },
                self._iso(self.base_ts + timedelta(days=7)),
            ),
            (
                "job.offer.shortlist",
                {"job_id": job_id, "finalists": 4, "selected_candidate": "Aleksandra Wisniewska"},
                self._iso(self.base_ts + timedelta(days=9)),
            ),
            (
                "job.offer.accepted",
                {"job_id": job_id, "candidate_name": "Aleksandra Wisniewska", "accepted_at_day": 11},
                self._iso(self.base_ts + timedelta(days=11)),
            ),
        ]
        with self.primary_db.transaction() as conn:
            for operation, details, created_at in rows:
                conn.execute(
                    """
                    INSERT INTO operation_logs (operation, entity_type, entity_id, status, details, created_at)
                    VALUES (?, 'job', ?, 'ok', ?, ?)
                    """,
                    (operation, str(job_id), json.dumps(details), created_at),
                )

    def _candidate_profile(self, global_index: int) -> Dict[str, Any]:
        if global_index == 186:
            full_name = "Aleksandra Wisniewska"
            slug = "aleksandra-wisniewska-4525"
        else:
            full_name = self._candidate_name(global_index)
            slug = self._slugify(full_name) + "-%04d" % (4100 + global_index)
        extras = [
            EXTRA_SKILLS[global_index % len(EXTRA_SKILLS)],
            EXTRA_SKILLS[(global_index + 3) % len(EXTRA_SKILLS)],
        ]
        return {
            "linkedin_id": "%s-%03d" % (MAIN_DASHBOARD_DEMO_PREFIX, global_index + 1),
            "provider_id": "%s-provider-%03d" % (MAIN_DASHBOARD_DEMO_PREFIX, global_index + 1),
            "unipile_profile_id": "%s-unipile-%03d" % (MAIN_DASHBOARD_DEMO_PREFIX, global_index + 1),
            "linkedin_public_url": "https://www.linkedin.com/in/%s" % slug,
            "full_name": full_name,
            "headline": HEADLINES[global_index % len(HEADLINES)],
            "location": LOCATIONS[global_index % len(LOCATIONS)],
            "languages": ["en", "pl"],
            "skills": REQUIRED_SKILLS + extras,
            "years_experience": 3 + (global_index % 5),
        }

    def _verification_notes(self, *, stage: str, profile: Dict[str, Any], global_index: int) -> Dict[str, Any]:
        notes: Dict[str, Any] = {
            "required_skills": REQUIRED_SKILLS,
            "matched_skills": REQUIRED_SKILLS,
            "nice_to_have_skills": [profile.get("skills", [])[-2], "product analytics"],
            "decision": "strong_yes" if stage in {"cv_received", "interview_pending", "interview_passed"} else "yes",
            "market": "Poland",
            "language_signal": "strong_english",
            "summary": (
                "%s matches the %s profile with React, TypeScript, Node.js, and product delivery experience."
                % (str(profile.get("full_name") or "Candidate"), MAIN_DASHBOARD_DEMO_TITLE)
            ),
        }
        if stage == "interview_pending":
            notes.update(
                {
                    "interview_status": "in_progress" if global_index % 2 else "invited",
                    "interview_summary": "Interview started after successful written screening and CV review.",
                }
            )
        elif stage == "interview_passed":
            total = 86.0 + float(global_index % 4) * 2.0
            if str(profile.get("full_name") or "") == "Aleksandra Wisniewska":
                total = 94.0
            notes.update(
                {
                    "interview_status": "scored",
                    "interview_total_score": total,
                    "interview_summary": "Strong ownership, pragmatic React architecture, and crisp production tradeoffs.",
                    "ideal_close_label": "Offer accepted" if str(profile.get("full_name") or "") == "Aleksandra Wisniewska" else "Final shortlist",
                }
            )
        elif stage == "interview_failed":
            total_fail = 67.0 + float(global_index % 3) * 3.0
            notes.update(
                {
                    "interview_status": "scored",
                    "interview_total_score": total_fail,
                    "interview_summary": "Solid baseline but below bar on production ownership and debugging depth.",
                }
            )
        return notes

    def _start_pre_resume_session(self, *, session_id: str, candidate_name: str, language: str) -> Dict[str, Any]:
        if self.pre_resume_service is not None:
            result = self.pre_resume_service.start_session(
                session_id=session_id,
                candidate_name=candidate_name,
                job_title=MAIN_DASHBOARD_DEMO_TITLE,
                scope_summary=", ".join(REQUIRED_SKILLS),
                core_profile_summary=", ".join(REQUIRED_SKILLS[:4]),
                language=language,
                job_location=MAIN_DASHBOARD_DEMO_LOCATION,
                salary_min=21000,
                salary_max=26000,
                salary_currency="PLN",
                work_authorization_required=True,
            )
            return {
                "session_id": session_id,
                "state": dict(result.get("state") or {}),
                "intro_message": str(result.get("outbound") or "").strip(),
            }
        return {
            "session_id": session_id,
            "state": {
                "session_id": session_id,
                "candidate_name": candidate_name,
                "job_title": MAIN_DASHBOARD_DEMO_TITLE,
                "scope_summary": ", ".join(REQUIRED_SKILLS),
                "language": language,
                "status": "awaiting_reply",
                "followups_sent": 0,
                "turns": 1,
                "resume_links": [],
                "cv_received": False,
            },
            "intro_message": "Would you be open to a quick async screening step?",
        }

    @staticmethod
    def _candidate_name(global_index: int) -> str:
        first = FIRST_NAMES[global_index % len(FIRST_NAMES)]
        last = LAST_NAMES[(global_index // len(FIRST_NAMES)) % len(LAST_NAMES)]
        return "%s %s" % (first, last)

    @staticmethod
    def _slugify(value: str) -> str:
        return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-").replace("--", "-")

    @staticmethod
    def _job_description() -> str:
        return (
            "Interexy is hiring a Middle JS Developer in Poland for product-facing web applications. "
            "The role needs strong JavaScript, TypeScript, React, Node.js, REST API work, testing discipline, "
            "English communication, and comfort shipping client-facing features in a fast product team."
        )

    @staticmethod
    def _match_score(*, stage: str, global_index: int) -> float:
        return round(BASE_SCORE_MAP[stage] + (global_index % 7) * 0.004, 3)

    @staticmethod
    def _event_key(*, candidate_id: int, suffix: str) -> str:
        return "%s:%s:%s" % (MAIN_DASHBOARD_DEMO_SEED_KEY, candidate_id, suffix)

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()


def seed_full_demo_job(
    *,
    db: Any,
    pre_resume_service: Optional[PreResumeCommunicationService] = None,
    interview_assessment_preparer: Optional[Callable[[int], Dict[str, Any]]] = None,
    force_reseed: bool = False,
    postgres_dsn: str = "",
) -> Dict[str, Any]:
    return MainDashboardDemoJobSeeder(
        db=db,
        pre_resume_service=pre_resume_service,
        interview_assessment_preparer=interview_assessment_preparer,
        postgres_dsn=postgres_dsn,
    ).ensure_seeded(force_reseed=force_reseed)


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed deterministic full demo job data.")
    parser.add_argument("--db-path", default=os.environ.get("TENER_DB_PATH", str(Path("runtime") / "tener_v1.sqlite3")))
    parser.add_argument("--force-reseed", action="store_true", help="Reset the existing seeded job before rebuilding it.")
    parser.add_argument(
        "--templates-path",
        default=str(Path(__file__).resolve().parents[2] / "config" / "outreach_templates.json"),
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_cli_parser()
    args = parser.parse_args(argv)
    db = Database(str(args.db_path))
    db.init_schema()
    pre_resume = PreResumeCommunicationService(templates_path=str(args.templates_path))
    payload = seed_full_demo_job(
        db=db,
        pre_resume_service=pre_resume,
        force_reseed=bool(args.force_reseed),
        postgres_dsn=str(os.environ.get("TENER_DB_DSN", "") or ""),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
