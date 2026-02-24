from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
from uuid import uuid4
from typing import Any, Dict, List

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database
from .instructions import AgentEvaluationPlaybook
from .pre_resume_service import PreResumeCommunicationService

DEFAULT_FORCED_TEST_SCORE = 0.99
TERMINAL_PRE_RESUME_STATUSES = {"resume_received", "not_interested", "unreachable", "stalled"}
ACTIVE_INTERVIEW_STATUSES = {"invited", "in_progress"}
TERMINAL_INTERVIEW_STATUSES = {"completed", "scored", "failed", "expired", "canceled"}
AGENT_ROLES = {
    "sourcing_vetting": "Reed AI (Talent Scout)",
    "communication": "Casey AI (Hiring Coordinator)",
    "interview_evaluation": "Jordan AI (Lead Interviewer)",
    "culture_analyst": "Harper AI (Culture Analyst)",
    "job_architect": "Spencer AI (Job Architect)",
}


@dataclass
class WorkflowSummary:
    job_id: int
    searched: int
    verified: int
    needs_resume: int
    rejected: int
    outreached: int
    outreach_sent: int
    outreach_pending_connection: int
    outreach_failed: int
    conversation_ids: List[int]


class WorkflowService:
    def __init__(
        self,
        db: Database,
        sourcing_agent: SourcingAgent,
        verification_agent: VerificationAgent,
        outreach_agent: OutreachAgent,
        faq_agent: FAQAgent,
        pre_resume_service: PreResumeCommunicationService | None = None,
        llm_responder: Any | None = None,
        interview_client: Any | None = None,
        agent_evaluation_playbook: AgentEvaluationPlaybook | None = None,
        contact_all_mode: bool = False,
        require_resume_before_final_verify: bool = False,
        stage_instructions: Dict[str, str] | None = None,
        forced_test_ids_path: str | None = None,
        forced_test_score: float = DEFAULT_FORCED_TEST_SCORE,
        interview_invite_ttl_hours: int = 72,
        interview_max_followups: int = 2,
        interview_followup_delays_hours: List[float] | None = None,
    ) -> None:
        self.db = db
        self.sourcing_agent = sourcing_agent
        self.verification_agent = verification_agent
        self.outreach_agent = outreach_agent
        self.faq_agent = faq_agent
        self.pre_resume_service = pre_resume_service
        self.llm_responder = llm_responder
        self.interview_client = interview_client
        self.agent_evaluation_playbook = agent_evaluation_playbook
        self.contact_all_mode = contact_all_mode
        self.require_resume_before_final_verify = require_resume_before_final_verify
        self.stage_instructions = dict(stage_instructions or {})
        self.forced_test_ids_path = (forced_test_ids_path or "").strip() or None
        try:
            self.forced_test_score = float(forced_test_score)
        except (TypeError, ValueError):
            self.forced_test_score = DEFAULT_FORCED_TEST_SCORE
        try:
            self.interview_invite_ttl_hours = max(1, int(interview_invite_ttl_hours))
        except (TypeError, ValueError):
            self.interview_invite_ttl_hours = 72
        try:
            self.interview_max_followups = max(0, int(interview_max_followups))
        except (TypeError, ValueError):
            self.interview_max_followups = 2
        delays_raw = interview_followup_delays_hours or [24.0, 48.0]
        parsed_delays: List[float] = []
        for raw in delays_raw:
            try:
                parsed_delays.append(max(1.0 / 60.0, float(raw)))
            except (TypeError, ValueError):
                continue
        self.interview_followup_delays_hours = parsed_delays or [24.0, 48.0]
        self.test_jobs_forced_only = str(os.environ.get("TENER_TEST_JOBS_FORCED_ONLY", "true")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        raw_keywords = str(
            os.environ.get(
                "TENER_TEST_JOB_KEYWORDS",
                "test,testing,smoke,sandbox,debug,verify,staging,qa,check,probe,demo,тест",
            )
        )
        self.test_job_keywords = [x.strip().lower() for x in raw_keywords.split(",") if x.strip()]

    def source_candidates(self, job_id: int, limit: int = 30, test_mode: bool | None = None) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        forced_test_ids = self._load_forced_test_identifiers()
        forced_only = self._effective_test_mode(job=job, test_mode=test_mode, forced_identifiers=forced_test_ids)
        profiles = self.sourcing_agent.find_candidates(job=job, limit=limit)
        profiles = self._inject_forced_test_candidates(
            job=job,
            profiles=profiles,
            limit=limit,
            forced_identifiers=forced_test_ids,
            forced_only=forced_only,
        )
        included = sorted(
            {
                matched
                for profile in profiles
                for matched in [self._forced_test_identifier_for_profile(profile, forced_test_ids)]
                if matched
            }
        )

        self.db.log_operation(
            operation="agent.sourcing.search",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "profiles_found": len(profiles),
                "limit": limit,
                "forced_test_ids_file": self.forced_test_ids_path,
                "forced_test_ids_configured": forced_test_ids,
                "forced_test_ids_included": included,
                "test_mode_active": forced_only,
                "test_mode_requested": test_mode,
            },
        )
        return {
            "job_id": job_id,
            "profiles": profiles,
            "total": len(profiles),
            "test_mode_active": forced_only,
            "test_mode_requested": test_mode,
            "instruction": self.stage_instructions.get("sourcing", ""),
        }

    def verify_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        forced_test_ids = self._load_forced_test_identifiers()
        enrich_result = self.enrich_profiles(job_id=job_id, profiles=profiles)
        enriched_profiles = enrich_result["profiles"]

        items: List[Dict[str, Any]] = []
        verified = 0
        needs_resume = 0
        rejected = 0

        for profile in enriched_profiles:
            score, status, notes = self.verification_agent.verify_candidate(job=job, profile=profile)
            forced_identifier = self._forced_test_identifier_for_profile(profile, forced_test_ids)
            if forced_identifier:
                score = max(float(score), self.forced_test_score)
                status = "verified"
                notes = dict(notes or {})
                notes["forced_test_candidate"] = True
                notes["forced_test_identifier"] = forced_identifier
                notes["forced_score"] = self.forced_test_score
                notes["human_explanation"] = (
                    "Forced test candidate prioritized: "
                    f"score set to {self.forced_test_score}."
                )
            if self.contact_all_mode and status == "rejected":
                status = "needs_resume"
                notes = dict(notes)
                notes["pre_resume_status"] = "rejected"
                notes["screening_outcome"] = "needs_resume"
                existing = str(notes.get("human_explanation") or "").strip()
                if existing:
                    notes["human_explanation"] = (
                        existing
                        + " Decision at this stage: request CV/resume and clarify experience before final verdict."
                    )
                else:
                    notes["human_explanation"] = (
                        "Insufficient confirmed profile data. Requesting CV/resume for final decision."
                    )
            record = {
                "profile": profile,
                "score": score,
                "status": status,
                "notes": notes,
            }
            items.append(record)

            if status == "verified":
                verified += 1
            elif status == "needs_resume":
                needs_resume += 1
            else:
                rejected += 1

            entity_id = str(profile.get("linkedin_id") or profile.get("id") or "unknown")
            self.db.log_operation(
                operation="agent.verification.evaluate",
                status="ok",
                entity_type="candidate_profile",
                entity_id=entity_id,
                details={"job_id": job_id, "result": status, "score": score},
            )

        return {
            "job_id": job_id,
            "items": items,
            "total": len(items),
            "verified": verified,
            "needs_resume": needs_resume,
            "rejected": rejected,
            "enriched_total": enrich_result["total"],
            "enrich_failed": enrich_result["failed"],
            "instruction": self.stage_instructions.get("verification", ""),
        }

    def enrich_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        self._get_job_or_raise(job_id)
        enriched_profiles, failed = self.sourcing_agent.enrich_candidates(profiles)
        forced_ids = self._load_forced_test_identifiers()
        forced_preserved = 0
        if forced_ids and enriched_profiles:
            stabilized: List[Dict[str, Any]] = []
            for idx, enriched in enumerate(enriched_profiles):
                profile = dict(enriched) if isinstance(enriched, dict) else {}
                source_profile = profiles[idx] if idx < len(profiles) and isinstance(profiles[idx], dict) else {}
                forced_identifier = self._forced_test_identifier_for_profile(source_profile, forced_ids)
                if forced_identifier and not self._forced_test_identifier_for_profile(profile, [forced_identifier]):
                    profile = self._mark_forced_test_candidate(profile=profile, identifier=forced_identifier)
                    forced_preserved += 1
                stabilized.append(profile)
            enriched_profiles = stabilized
        self.db.log_operation(
            operation="agent.sourcing.enrich",
            status="ok" if failed == 0 else "partial",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "input_profiles": len(profiles),
                "enriched": len(enriched_profiles),
                "failed": failed,
                "forced_markers_preserved": forced_preserved,
            },
        )
        return {
            "job_id": job_id,
            "profiles": enriched_profiles,
            "total": len(enriched_profiles),
            "failed": failed,
            "instruction": self.stage_instructions.get("enrich", ""),
        }

    def add_verified_candidates(self, job_id: int, verified_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        self._get_job_or_raise(job_id)

        added: List[Dict[str, Any]] = []
        for item in verified_items:
            profile = item.get("profile") if isinstance(item, dict) else None
            if not isinstance(profile, dict):
                continue

            score = float(item.get("score") or 0.0)
            notes = item.get("notes") if isinstance(item.get("notes"), dict) else {}
            screening_status = str(item.get("status") or "verified").strip().lower()
            if screening_status not in {"verified", "needs_resume", "rejected"}:
                screening_status = "verified"

            candidate_id = self.db.upsert_candidate(profile, source="linkedin")
            self.db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=score,
                status=screening_status,
                verification_notes=notes,
            )
            self._record_sourcing_vetting_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                screening_status=screening_status,
                match_score=score,
                notes=notes,
            )
            self._upsert_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="interview_evaluation",
                stage_key="interview_results",
                score=None,
                status="not_started",
                reason="Interview step has not started yet.",
                details={"source": "workflow.add"},
            )
            self.db.log_operation(
                operation="agent.add.persist",
                status="ok",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={"job_id": job_id, "score": score, "status": screening_status},
            )
            added.append({"candidate_id": candidate_id, "profile": profile, "score": score, "status": screening_status})

        return {
            "job_id": job_id,
            "added": added,
            "total": len(added),
            "instruction": self.stage_instructions.get("add", ""),
        }

    def outreach_candidates(self, job_id: int, candidate_ids: List[int], test_mode: bool | None = None) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        forced_identifiers = self._load_forced_test_identifiers()
        forced_lookup = self._build_forced_identifier_lookup(job=job, forced_identifiers=forced_identifiers)
        forced_only = self._effective_test_mode(job=job, test_mode=test_mode, forced_identifiers=forced_identifiers)

        out_items: List[Dict[str, Any]] = []
        conversation_ids: List[int] = []
        sent = 0
        pending_connection = 0
        failed = 0
        test_filter_skipped = 0

        for raw_id in candidate_ids:
            try:
                candidate_id = int(raw_id)
            except (TypeError, ValueError):
                failed += 1
                continue

            candidate = self.db.get_candidate(candidate_id)
            if not candidate:
                failed += 1
                continue
            match = self.db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
            if forced_only:
                forced_identifier = self._forced_test_identifier_for_profile(candidate, forced_lookup)
                if not forced_identifier:
                    forced_identifier = self._forced_test_identifier_from_match(match, forced_lookup)
                if not forced_identifier:
                    test_filter_skipped += 1
                    self.db.log_operation(
                        operation="agent.outreach.test_filter_skip",
                        status="skipped",
                        entity_type="candidate",
                        entity_id=str(candidate_id),
                        details={
                            "job_id": job_id,
                            "reason": "test_job_forced_only",
                            "forced_test_ids_file": self.forced_test_ids_path,
                        },
                    )
                    continue

            screening_status = str((match or {}).get("status") or "")
            request_resume = self.require_resume_before_final_verify or screening_status == "needs_resume"
            conversation_id = self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            language = str((candidate.get("languages") or ["en"])[0]).lower()
            message = ""
            pre_resume_session_id = None
            if request_resume and self.pre_resume_service is not None:
                pre_resume_session_id = f"pre-{conversation_id}"
                session = self.db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)
                if session and isinstance(session.get("state_json"), dict):
                    if self.pre_resume_service.get_session(pre_resume_session_id) is None:
                        self.pre_resume_service.seed_session(session["state_json"])
                    session_state = session["state_json"]
                    language = str(session_state.get("language") or language)
                else:
                    started = self.pre_resume_service.start_session(
                        session_id=pre_resume_session_id,
                        candidate_name=str(candidate.get("full_name") or "there"),
                        job_title=str(job.get("title") or "this role"),
                        scope_summary=self.outreach_agent.matching_engine.summarize_scope(job),
                        core_profile_summary=", ".join(
                            self.outreach_agent.matching_engine.build_core_profile(job).get("core_skills") or []
                        )
                        or self.outreach_agent.matching_engine.summarize_scope(job),
                        language=str((candidate.get("languages") or ["en"])[0]).lower(),
                    )
                    session_state = started["state"]
                    language = str(session_state.get("language") or "en")
                    message = str(started.get("outbound") or "")
                    self.db.insert_pre_resume_event(
                        session_id=pre_resume_session_id,
                        conversation_id=conversation_id,
                        event_type="session_started",
                        intent="started",
                        inbound_text=None,
                        outbound_text=message,
                        state_status=session_state.get("status"),
                        details={"job_id": job_id, "candidate_id": candidate_id},
                    )
                self.db.upsert_pre_resume_session(
                    session_id=pre_resume_session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=session_state,
                    instruction=self.stage_instructions.get("pre_resume", ""),
                )
                if not message:
                    language, message = self.outreach_agent.compose_screening_message(
                        job=job,
                        candidate=candidate,
                        request_resume=True,
                    )
            else:
                language, message = self.outreach_agent.compose_screening_message(
                    job=job,
                    candidate=candidate,
                    request_resume=request_resume,
                )

            connect_request = None
            delivery_status = "failed"
            try:
                delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
            except Exception as exc:
                delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}

            if delivery.get("sent"):
                sent += 1
                delivery_status = "sent"
                self.db.update_conversation_status(conversation_id=conversation_id, status="active")
                self.db.update_candidate_match_status(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    status="outreach_sent",
                    extra_notes={"outreach_state": "sent"},
                )
            elif self._is_connection_required_error(delivery):
                _, connect_message = self.outreach_agent.compose_connection_request(job=job, candidate=candidate)
                try:
                    connect_request = self.sourcing_agent.send_connection_request(
                        candidate_profile=candidate,
                        message=connect_message,
                    )
                except Exception as exc:
                    connect_request = {"sent": False, "provider": "linkedin", "error": str(exc)}

                if connect_request.get("sent"):
                    pending_connection += 1
                    delivery_status = "pending_connection"
                    self.db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")
                    self.db.update_candidate_match_status(
                        job_id=job_id,
                        candidate_id=candidate_id,
                        status="outreach_pending_connection",
                        extra_notes={
                            "outreach_state": "waiting_connection",
                            "connect_request": connect_request,
                        },
                    )
                    self.db.log_operation(
                        operation="agent.outreach.connect_request",
                        status="ok",
                        entity_type="candidate",
                        entity_id=str(candidate_id),
                        details={"job_id": job_id, "connect_request": connect_request},
                    )
                else:
                    failed += 1
                    self.db.log_operation(
                        operation="agent.outreach.connect_request",
                        status="error",
                        entity_type="candidate",
                        entity_id=str(candidate_id),
                        details={"job_id": job_id, "connect_request": connect_request, "delivery": delivery},
                    )
            else:
                failed += 1
                self.db.log_operation(
                    operation="agent.outreach.delivery_error",
                    status="error",
                    entity_type="candidate",
                    entity_id=str(candidate_id),
                    details={"job_id": job_id, "delivery": delivery},
                )

            external_chat_id = str(delivery.get("chat_id") or "").strip()
            chat_binding = None
            if external_chat_id:
                chat_binding = self.db.set_conversation_external_chat_id(
                    conversation_id=conversation_id,
                    external_chat_id=external_chat_id,
                )
                binding_status = str((chat_binding or {}).get("status") or "")
                if binding_status not in {"set", "rebound_same_candidate"}:
                    self.db.log_operation(
                        operation="agent.outreach.chat_binding",
                        status="partial",
                        entity_type="conversation",
                        entity_id=str(conversation_id),
                        details={"candidate_id": candidate_id, "chat_binding": chat_binding},
                    )

            self.db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content=message,
                candidate_language=language,
                meta={
                    "type": "outreach" if delivery_status == "sent" else "outreach_pending_connection",
                    "auto": True,
                    "delivery": delivery,
                    "delivery_status": delivery_status,
                    "connect_request": connect_request,
                    "pending_delivery": delivery_status == "pending_connection",
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                    "external_chat_id": external_chat_id or None,
                    "chat_binding": chat_binding,
                },
            )
            self.db.log_operation(
                operation="agent.outreach.send",
                status="ok" if delivery_status in {"sent", "pending_connection"} else "error",
                entity_type="conversation",
                entity_id=str(conversation_id),
                details={
                    "candidate_id": candidate_id,
                    "language": language,
                    "delivery": delivery,
                    "delivery_status": delivery_status,
                    "connect_request": connect_request,
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                    "external_chat_id": external_chat_id or None,
                    "chat_binding": chat_binding,
                },
            )
            self._record_communication_outreach_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                delivery_status=delivery_status,
                delivery=delivery,
                connect_request=connect_request,
                request_resume=request_resume,
            )

            out_items.append(
                {
                    "candidate_id": candidate_id,
                    "conversation_id": conversation_id,
                    "language": language,
                    "delivery": delivery,
                    "delivery_status": delivery_status,
                    "connect_request": connect_request,
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                    "external_chat_id": external_chat_id or None,
                    "chat_binding": chat_binding,
                }
            )
            conversation_ids.append(conversation_id)

        return {
            "job_id": job_id,
            "items": out_items,
            "conversation_ids": conversation_ids,
            "sent": sent,
            "pending_connection": pending_connection,
            "failed": failed,
            "test_filter_skipped": test_filter_skipped,
            "test_job_forced_only_active": forced_only,
            "test_mode_requested": test_mode,
            "total": len(out_items),
            "instruction": self.stage_instructions.get("outreach", ""),
        }

    def poll_pending_connections(self, job_id: int | None = None, limit: int = 200) -> Dict[str, Any]:
        rows = self.db.list_conversations_by_status(status="waiting_connection", limit=limit, job_id=job_id)
        checked = 0
        connected = 0
        sent = 0
        still_waiting = 0
        failed = 0
        items: List[Dict[str, Any]] = []

        for row in rows:
            checked += 1
            conversation_id = int(row["conversation_id"])
            candidate_id = int(row["candidate_id"])
            candidate = self.db.get_candidate(candidate_id)
            if not candidate:
                failed += 1
                items.append({"conversation_id": conversation_id, "status": "candidate_missing"})
                continue

            try:
                connection = self.sourcing_agent.check_connection_status(candidate_profile=candidate)
            except Exception as exc:
                connection = {"connected": False, "error": str(exc)}

            if not connection.get("connected"):
                still_waiting += 1
                items.append(
                    {
                        "conversation_id": conversation_id,
                        "candidate_id": candidate_id,
                        "status": "waiting_connection",
                        "connection": connection,
                    }
                )
                continue

            connected += 1
            send_result = self._deliver_pending_outreach_message(conversation_id=conversation_id, candidate=candidate)
            if send_result.get("sent"):
                sent += 1
            else:
                failed += 1
            items.append(
                {
                    "conversation_id": conversation_id,
                    "candidate_id": candidate_id,
                    "status": "connected",
                    "connection": connection,
                    "delivery": send_result,
                }
            )

        self.db.log_operation(
            operation="agent.outreach.poll_connections",
            status="ok" if failed == 0 else "partial",
            entity_type="job" if job_id is not None else "system",
            entity_id=str(job_id) if job_id is not None else None,
            details={
                "checked": checked,
                "connected": connected,
                "sent": sent,
                "still_waiting": still_waiting,
                "failed": failed,
            },
        )

        return {
            "job_id": job_id,
            "checked": checked,
            "connected": connected,
            "sent": sent,
            "still_waiting": still_waiting,
            "failed": failed,
            "items": items,
        }

    def add_manual_test_account(
        self,
        job_id: int,
        full_name: str,
        language: str = "en",
        linkedin_id: str | None = None,
        location: str | None = None,
        headline: str | None = None,
        external_chat_id: str | None = None,
        scope_summary: str | None = None,
    ) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        name = full_name.strip()
        if not name:
            raise ValueError("full_name is required")

        normalized_lang = (language or "en").strip().lower() or "en"
        account_id = (linkedin_id or "").strip() or f"manual-{uuid4().hex[:12]}"
        profile = {
            "linkedin_id": account_id,
            "full_name": name,
            "headline": (headline or "Manual Test Candidate").strip(),
            "location": (location or job.get("location") or "Remote").strip(),
            "languages": [normalized_lang],
            "skills": [],
            "years_experience": 0,
            "raw": {"manual": True},
        }

        candidate_id = self.db.upsert_candidate(profile, source="manual")
        self.db.create_candidate_match(
            job_id=job_id,
            candidate_id=candidate_id,
            score=0.0,
            status="needs_resume",
            verification_notes={
                "manual": True,
                "human_explanation": "Manual test account. Screening is deferred until CV is received.",
            },
        )
        conversation_id = self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="manual")
        chat_id = (external_chat_id or "").strip() or f"manual-chat-{conversation_id}"
        self.db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id=chat_id)

        session_id = f"pre-{conversation_id}"
        initial_outbound = ""
        state: Dict[str, Any] | None = None

        if self.pre_resume_service is not None:
            session = self.db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)
            if session and isinstance(session.get("state_json"), dict):
                state = session["state_json"]
                if self.pre_resume_service.get_session(session_id) is None:
                    self.pre_resume_service.seed_session(state)
            else:
                started = self.pre_resume_service.start_session(
                    session_id=session_id,
                    candidate_name=name,
                    job_title=str(job.get("title") or "this role"),
                    scope_summary=(scope_summary or str(job.get("jd_text") or "")).strip() or "Role details will be provided.",
                    core_profile_summary=", ".join(
                        self.outreach_agent.matching_engine.build_core_profile(job).get("core_skills") or []
                    )
                    or self.outreach_agent.matching_engine.summarize_scope(job),
                    language=normalized_lang,
                )
                state = started.get("state") if isinstance(started.get("state"), dict) else None
                initial_outbound = str(started.get("outbound") or "").strip()
                if state:
                    self.db.insert_pre_resume_event(
                        session_id=session_id,
                        conversation_id=conversation_id,
                        event_type="session_started",
                        intent="started",
                        inbound_text=None,
                        outbound_text=initial_outbound or None,
                        state_status=state.get("status"),
                        details={"job_id": job_id, "candidate_id": candidate_id, "source": "manual"},
                    )
            if state:
                self.db.upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=state,
                    instruction=self.stage_instructions.get("pre_resume", ""),
                )

        if not initial_outbound:
            _, initial_outbound = self.outreach_agent.compose_screening_message(
                job=job,
                candidate=profile,
                request_resume=True,
            )

        outbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=initial_outbound,
            candidate_language=normalized_lang,
            meta={
                "type": "manual_account_start",
                "auto": True,
                "delivery": {"sent": True, "provider": "manual", "chat_id": chat_id, "mock": True},
                "session_id": session_id,
            },
        )
        self.db.log_operation(
            operation="agent.manual_account.added",
            status="ok",
            entity_type="conversation",
            entity_id=str(conversation_id),
            details={"job_id": job_id, "candidate_id": candidate_id, "session_id": session_id},
        )
        self.db.log_operation(
            operation="agent.pre_resume.reply",
            status="ok",
            entity_type="message",
            entity_id=str(outbound_id),
            details={
                "conversation_id": conversation_id,
                "intent": "started",
                "language": normalized_lang,
                "session_id": session_id,
                "delivery": {"sent": True, "provider": "manual", "chat_id": chat_id, "mock": True},
            },
        )
        self._record_communication_outreach_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            delivery_status="sent",
            delivery={"sent": True, "provider": "manual", "chat_id": chat_id, "mock": True},
            connect_request=None,
            request_resume=True,
        )

        return {
            "job_id": job_id,
            "candidate_id": candidate_id,
            "conversation_id": conversation_id,
            "session_id": session_id,
            "external_chat_id": chat_id,
            "candidate": profile,
            "initial_outbound": initial_outbound,
        }

    def execute_job_workflow(self, job_id: int, limit: int = 30, test_mode: bool | None = None) -> WorkflowSummary:
        job = self._get_job_or_raise(job_id)
        forced_test_ids = self._load_forced_test_identifiers()
        effective_test_mode = self._effective_test_mode(
            job=job,
            test_mode=test_mode,
            forced_identifiers=forced_test_ids,
        )

        self.db.log_operation(
            operation="workflow.execute.start",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={"limit": limit, "test_mode_active": effective_test_mode, "test_mode_requested": test_mode},
        )

        source_result = self.source_candidates(job_id=job_id, limit=limit, test_mode=effective_test_mode)
        verify_result = self.verify_profiles(job_id=job_id, profiles=source_result["profiles"])

        if self.contact_all_mode:
            eligible_items = [item for item in verify_result["items"] if item.get("status") in {"verified", "needs_resume"}]
        else:
            eligible_items = [item for item in verify_result["items"] if item.get("status") == "verified"]
        add_result = self.add_verified_candidates(job_id=job_id, verified_items=eligible_items)
        outreach_result = self.outreach_candidates(
            job_id=job_id,
            candidate_ids=[x["candidate_id"] for x in add_result["added"]],
            test_mode=effective_test_mode,
        )

        summary = WorkflowSummary(
            job_id=job_id,
            searched=source_result["total"],
            verified=verify_result["verified"],
            needs_resume=verify_result.get("needs_resume", 0),
            rejected=verify_result["rejected"],
            outreached=outreach_result["total"],
            outreach_sent=outreach_result["sent"],
            outreach_pending_connection=outreach_result.get("pending_connection", 0),
            outreach_failed=outreach_result["failed"],
            conversation_ids=outreach_result["conversation_ids"],
        )

        self.db.log_operation(
            operation="workflow.execute.finish",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "searched": summary.searched,
                "verified": summary.verified,
                "needs_resume": summary.needs_resume,
                "rejected": summary.rejected,
                "outreached": summary.outreached,
                "outreach_sent": summary.outreach_sent,
                "outreach_pending_connection": summary.outreach_pending_connection,
                "outreach_failed": summary.outreach_failed,
                "test_mode_active": effective_test_mode,
                "test_mode_requested": test_mode,
            },
        )
        return summary

    def process_inbound_message(self, conversation_id: int, text: str) -> Dict[str, Any]:
        conversation = self.db.get_conversation(conversation_id)
        if not conversation:
            raise ValueError(f"Conversation {conversation_id} not found")

        job = self.db.get_job(int(conversation["job_id"]))
        if not job:
            raise ValueError("Conversation is linked to missing job")
        candidate = self.db.get_candidate(int(conversation["candidate_id"]))
        if not candidate:
            raise ValueError("Conversation is linked to missing candidate")
        match = self.db.get_candidate_match(
            job_id=int(conversation["job_id"]),
            candidate_id=int(conversation["candidate_id"]),
        )

        messages = self.db.list_messages(conversation_id)
        previous_lang = None
        for item in reversed(messages):
            if item.get("candidate_language"):
                previous_lang = item["candidate_language"]
                break
        llm_history = self._build_llm_history(messages=messages, latest_inbound=text)

        inbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="inbound",
            content=text,
            candidate_language=previous_lang,
            meta={"type": "candidate_message"},
        )
        self.db.log_operation(
            operation="conversation.inbound.received",
            status="ok",
            entity_type="message",
            entity_id=str(inbound_id),
            details={"conversation_id": conversation_id},
        )

        pre_resume = self.db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)
        if pre_resume and self.pre_resume_service is not None:
            session_id = str(pre_resume.get("session_id") or "")
            state = pre_resume.get("state_json")
            if session_id and isinstance(state, dict):
                if self.pre_resume_service.get_session(session_id) is None:
                    self.pre_resume_service.seed_session(state)
                result = self.pre_resume_service.handle_inbound(session_id=session_id, text=text)
                state_out = result.get("state") if isinstance(result.get("state"), dict) else state
                if isinstance(state_out, dict):
                    self.db.upsert_pre_resume_session(
                        session_id=session_id,
                        conversation_id=conversation_id,
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        state=state_out,
                        instruction=self.stage_instructions.get("pre_resume", ""),
                    )
                outbound = str(result.get("outbound") or "").strip()
                intent = str(result.get("intent") or "default")
                language = str((state_out or {}).get("language") or previous_lang or "en")
                outbound = self._maybe_llm_reply(
                    mode="pre_resume",
                    instruction=self.stage_instructions.get("pre_resume", ""),
                    job=job,
                    candidate=candidate,
                    inbound_text=text,
                    history=llm_history,
                    fallback_reply=outbound,
                    language=language,
                    state=state_out if isinstance(state_out, dict) else None,
                )
                outbound = self._append_interview_opt_in_prompt(
                    outbound=outbound,
                    language=language,
                    state=state_out if isinstance(state_out, dict) else None,
                    match=match,
                )
                self.db.insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    event_type="inbound_processed",
                    intent=intent,
                    inbound_text=text,
                    outbound_text=outbound or None,
                    state_status=(state_out or {}).get("status"),
                    details={"result_event": result.get("event")},
                )

                if outbound:
                    delivery = self._send_auto_reply(candidate=candidate, message=outbound, conversation=conversation)
                    outbound_id = self.db.add_message(
                        conversation_id=conversation_id,
                        direction="outbound",
                        content=outbound,
                        candidate_language=language,
                        meta={
                            "type": "pre_resume_auto_reply",
                            "intent": intent,
                            "auto": True,
                            "session_id": session_id,
                            "state_status": (state_out or {}).get("status"),
                            "delivery": delivery,
                        },
                    )
                    self.db.log_operation(
                        operation="agent.pre_resume.reply",
                        status="ok" if delivery.get("sent") else "error",
                        entity_type="message",
                        entity_id=str(outbound_id),
                        details={
                            "conversation_id": conversation_id,
                            "intent": intent,
                            "language": language,
                            "session_id": session_id,
                            "delivery": delivery,
                        },
                    )

                if (state_out or {}).get("status") == "resume_received":
                    self.db.update_candidate_match_status(
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        status="resume_received",
                        extra_notes={"resume_received_at": (state_out or {}).get("updated_at")},
                    )
                    self.db.log_operation(
                        operation="candidate.resume.received",
                        status="ok",
                        entity_type="candidate",
                        entity_id=str(conversation["candidate_id"]),
                        details={"conversation_id": conversation_id, "session_id": session_id},
                    )
                self._record_communication_dialogue_assessment(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    mode="pre_resume",
                    intent=intent,
                    state=state_out if isinstance(state_out, dict) else None,
                    inbound_text=text,
                )

                interview_result: Dict[str, Any] | None = None
                if intent == "pre_vetting_opt_in":
                    interview_result = self._send_interview_invite_after_opt_in(
                        job=job,
                        candidate=candidate,
                        conversation=conversation,
                        language=language,
                        match=match,
                    )

                response = {
                    "language": language,
                    "intent": intent,
                    "reply": outbound,
                    "mode": "pre_resume",
                    "state": state_out,
                }
                if interview_result:
                    response["interview"] = interview_result
                return response

        lang, intent, reply = self.faq_agent.auto_reply(inbound_text=text, job=job, candidate_lang=previous_lang)
        reply = self._maybe_llm_reply(
            mode="faq",
            instruction=self.stage_instructions.get("faq", ""),
            job=job,
            candidate=candidate,
            inbound_text=text,
            history=llm_history,
            fallback_reply=reply,
            language=lang,
            state=None,
        )
        delivery = self._send_auto_reply(candidate=candidate, message=reply, conversation=conversation)
        outbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=reply,
            candidate_language=lang,
            meta={"type": "faq_auto_reply", "intent": intent, "auto": True, "delivery": delivery},
        )
        self.db.log_operation(
            operation="agent.faq.reply",
            status="ok" if delivery.get("sent") else "error",
            entity_type="message",
            entity_id=str(outbound_id),
            details={"conversation_id": conversation_id, "intent": intent, "language": lang, "delivery": delivery},
        )
        self._record_communication_dialogue_assessment(
            job_id=int(conversation["job_id"]),
            candidate_id=int(conversation["candidate_id"]),
            mode="faq",
            intent=intent,
            state=None,
            inbound_text=text,
        )

        return {"language": lang, "intent": intent, "reply": reply}

    def process_provider_inbound_message(
        self,
        external_chat_id: str,
        text: str,
        sender_provider_id: str | None = None,
    ) -> Dict[str, Any]:
        conversation = self.db.get_conversation_by_external_chat_id(external_chat_id) if external_chat_id else None
        if not conversation and sender_provider_id:
            candidate = self.db.get_candidate_by_linkedin_id(sender_provider_id)
            if candidate:
                conversation = self.db.get_latest_conversation_for_candidate(int(candidate["id"]))
        if not conversation:
            return {"processed": False, "reason": "conversation_not_found"}
        result = self.process_inbound_message(conversation_id=int(conversation["id"]), text=text)
        return {
            "processed": True,
            "conversation_id": int(conversation["id"]),
            "external_chat_id": conversation.get("external_chat_id"),
            "result": result,
        }

    def process_connection_event(
        self,
        sender_provider_id: str | None = None,
        external_chat_id: str | None = None,
    ) -> Dict[str, Any]:
        conversation = self.db.get_conversation_by_external_chat_id(external_chat_id) if external_chat_id else None
        if not conversation and sender_provider_id:
            candidate = self.db.get_candidate_by_linkedin_id(sender_provider_id)
            if candidate:
                conversation = self.db.get_latest_conversation_for_candidate(int(candidate["id"]))
        if not conversation:
            return {"processed": False, "reason": "conversation_not_found"}
        candidate = self.db.get_candidate(int(conversation["candidate_id"]))
        if not candidate:
            return {"processed": False, "reason": "candidate_not_found", "conversation_id": int(conversation["id"])}
        if str(conversation.get("status") or "") != "waiting_connection":
            return {"processed": False, "reason": "conversation_not_waiting_connection", "conversation_id": int(conversation["id"])}
        delivery = self._deliver_pending_outreach_message(conversation_id=int(conversation["id"]), candidate=candidate)
        return {
            "processed": True,
            "conversation_id": int(conversation["id"]),
            "delivery": delivery,
        }

    def poll_provider_inbound_messages(
        self,
        job_id: int | None = None,
        limit: int = 100,
        per_chat_limit: int = 20,
    ) -> Dict[str, Any]:
        fetch_fn = getattr(self.sourcing_agent, "fetch_chat_messages", None)
        if not callable(fetch_fn):
            return {
                "job_id": job_id,
                "conversations_checked": 0,
                "messages_scanned": 0,
                "processed": 0,
                "duplicates": 0,
                "ignored": 0,
                "errors": 0,
                "items": [],
                "reason": "provider_inbound_poll_not_supported",
            }

        safe_limit = max(1, min(int(limit or 100), 500))
        safe_per_chat = max(1, min(int(per_chat_limit or 20), 50))
        rows = self.db.list_conversations_overview(limit=max(safe_limit * 4, 200), job_id=job_id)

        rows_to_poll: List[Dict[str, Any]] = []
        for row in rows:
            external_chat_id = str(row.get("external_chat_id") or "").strip()
            if not external_chat_id:
                continue
            if str(row.get("channel") or "").lower() != "linkedin":
                continue
            rows_to_poll.append(row)
            if len(rows_to_poll) >= safe_limit:
                break

        conversations_checked = 0
        messages_scanned = 0
        processed = 0
        duplicates = 0
        ignored = 0
        errors = 0
        items: List[Dict[str, Any]] = []

        for row in rows_to_poll:
            conversations_checked += 1
            conversation_id = int(row["conversation_id"])
            external_chat_id = str(row.get("external_chat_id") or "").strip()
            candidate = self.db.get_candidate(int(row["candidate_id"]))

            try:
                messages = fetch_fn(external_chat_id, limit=safe_per_chat) or []
            except Exception as exc:
                errors += 1
                items.append(
                    {
                        "conversation_id": conversation_id,
                        "external_chat_id": external_chat_id,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                self.db.log_operation(
                    operation="poll.unipile.inbound.error",
                    status="error",
                    entity_type="conversation",
                    entity_id=str(conversation_id),
                    details={"external_chat_id": external_chat_id, "error": str(exc)},
                )
                continue

            if not isinstance(messages, list):
                ignored += 1
                continue

            for message in messages:
                if not isinstance(message, dict):
                    ignored += 1
                    continue
                messages_scanned += 1
                if not self._is_inbound_provider_message(message=message, candidate=candidate):
                    ignored += 1
                    continue

                text = str(message.get("text") or "").strip()
                if not text:
                    ignored += 1
                    continue

                provider_message_id = str(message.get("provider_message_id") or "").strip()
                sender_provider_id = str(message.get("sender_provider_id") or "").strip()
                occurred_at = str(message.get("created_at") or "").strip()
                dedupe_tail = provider_message_id or hashlib.sha256(
                    f"{external_chat_id}|{sender_provider_id}|{occurred_at}|{text}".encode("utf-8")
                ).hexdigest()
                event_key = f"poll-unipile:{external_chat_id}:{dedupe_tail}"
                is_new = self.db.record_webhook_event(
                    event_key=event_key,
                    source="unipile_poll",
                    payload=(message.get("raw") if isinstance(message.get("raw"), dict) else message),
                )
                if not is_new:
                    duplicates += 1
                    continue

                try:
                    result = self.process_inbound_message(conversation_id=conversation_id, text=text)
                except Exception as exc:
                    errors += 1
                    items.append(
                        {
                            "conversation_id": conversation_id,
                            "external_chat_id": external_chat_id,
                            "provider_message_id": provider_message_id or None,
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    self.db.log_operation(
                        operation="poll.unipile.inbound.error",
                        status="error",
                        entity_type="conversation",
                        entity_id=str(conversation_id),
                        details={
                            "external_chat_id": external_chat_id,
                            "provider_message_id": provider_message_id or None,
                            "error": str(exc),
                        },
                    )
                    continue

                processed += 1
                items.append(
                    {
                        "conversation_id": conversation_id,
                        "external_chat_id": external_chat_id,
                        "provider_message_id": provider_message_id or None,
                        "status": "processed",
                        "result_mode": str(result.get("mode") or "faq"),
                    }
                )
                self.db.log_operation(
                    operation="poll.unipile.inbound.processed",
                    status="ok",
                    entity_type="conversation",
                    entity_id=str(conversation_id),
                    details={
                        "external_chat_id": external_chat_id,
                        "provider_message_id": provider_message_id or None,
                        "result_mode": str(result.get("mode") or "faq"),
                    },
                )

        return {
            "job_id": job_id,
            "conversations_checked": conversations_checked,
            "messages_scanned": messages_scanned,
            "processed": processed,
            "duplicates": duplicates,
            "ignored": ignored,
            "errors": errors,
            "items": items,
        }

    def run_due_pre_resume_followups(self, job_id: int | None = None, limit: int = 100) -> Dict[str, Any]:
        if self.pre_resume_service is None:
            return {
                "processed": 0,
                "sent": 0,
                "skipped": 0,
                "errors": 0,
                "total_due": 0,
                "items": [],
                "reason": "pre_resume_service_not_configured",
            }

        safe_limit = max(1, min(int(limit or 100), 500))
        rows = self.db.list_pre_resume_sessions(limit=max(safe_limit * 5, 200), job_id=job_id)
        now = datetime.now(timezone.utc)

        due_rows: List[Dict[str, Any]] = []
        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            if status in TERMINAL_PRE_RESUME_STATUSES:
                continue
            next_followup_at = str(row.get("next_followup_at") or "").strip()
            if not next_followup_at:
                continue
            due_at = self._parse_iso_datetime(next_followup_at)
            if due_at is None:
                continue
            if due_at <= now:
                due_rows.append(row)
                if len(due_rows) >= safe_limit:
                    break

        sent = 0
        skipped = 0
        errors = 0
        items: List[Dict[str, Any]] = []

        for row in due_rows:
            session_id = str(row.get("session_id") or "")
            if not session_id:
                errors += 1
                continue
            conversation_id = int(row["conversation_id"])
            job_ref = int(row["job_id"])
            candidate_id = int(row["candidate_id"])
            state_json = row.get("state_json") if isinstance(row.get("state_json"), dict) else {}
            if self.pre_resume_service.get_session(session_id) is None and state_json:
                self.pre_resume_service.seed_session(state_json)

            try:
                result = self.pre_resume_service.build_followup(session_id=session_id)
            except Exception as exc:
                errors += 1
                items.append(
                    {
                        "session_id": session_id,
                        "conversation_id": conversation_id,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                self.db.log_operation(
                    operation="agent.pre_resume.followup.error",
                    status="error",
                    entity_type="conversation",
                    entity_id=str(conversation_id),
                    details={"session_id": session_id, "error": str(exc)},
                )
                continue

            state = result.get("state") if isinstance(result.get("state"), dict) else {}
            self.db.upsert_pre_resume_session(
                session_id=session_id,
                conversation_id=conversation_id,
                job_id=job_ref,
                candidate_id=candidate_id,
                state=state,
                instruction=self.stage_instructions.get("pre_resume", ""),
            )
            self._record_communication_dialogue_assessment(
                job_id=job_ref,
                candidate_id=candidate_id,
                mode="pre_resume",
                intent="followup",
                state=state if isinstance(state, dict) else None,
                inbound_text=None,
            )
            event_type = "followup_sent" if result.get("sent") else "followup_skipped"
            self.db.insert_pre_resume_event(
                session_id=session_id,
                conversation_id=conversation_id,
                event_type=event_type,
                intent=None,
                inbound_text=None,
                outbound_text=result.get("outbound"),
                state_status=state.get("status") if isinstance(state, dict) else None,
                details={"reason": result.get("reason"), "source": "scheduler"},
            )

            if not result.get("sent"):
                skipped += 1
                items.append(
                    {
                        "session_id": session_id,
                        "conversation_id": conversation_id,
                        "status": "skipped",
                        "reason": result.get("reason"),
                    }
                )
                continue

            outbound = str(result.get("outbound") or "").strip()
            if not outbound:
                skipped += 1
                items.append(
                    {
                        "session_id": session_id,
                        "conversation_id": conversation_id,
                        "status": "skipped",
                        "reason": "empty_outbound",
                    }
                )
                continue

            candidate = self.db.get_candidate(candidate_id)
            conversation = self.db.get_conversation(conversation_id)
            if not candidate or not conversation:
                errors += 1
                items.append(
                    {
                        "session_id": session_id,
                        "conversation_id": conversation_id,
                        "status": "error",
                        "reason": "missing_candidate_or_conversation",
                    }
                )
                continue

            language = str((state or {}).get("language") or (candidate.get("languages") or ["en"])[0]).lower()
            delivery = self._send_auto_reply(candidate=candidate, message=outbound, conversation=conversation)
            outbound_id = self.db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content=outbound,
                candidate_language=language,
                meta={
                    "type": "pre_resume_followup",
                    "auto": True,
                    "session_id": session_id,
                    "delivery": delivery,
                },
            )
            self.db.log_operation(
                operation="agent.pre_resume.followup",
                status="ok" if delivery.get("sent") else "error",
                entity_type="message",
                entity_id=str(outbound_id),
                details={
                    "session_id": session_id,
                    "conversation_id": conversation_id,
                    "job_id": job_ref,
                    "candidate_id": candidate_id,
                    "delivery": delivery,
                },
            )
            if delivery.get("sent"):
                sent += 1
                status = "sent"
            else:
                errors += 1
                status = "delivery_error"

            items.append(
                {
                    "session_id": session_id,
                    "conversation_id": conversation_id,
                    "status": status,
                    "delivery": delivery,
                }
            )

        return {
            "processed": len(due_rows),
            "sent": sent,
            "skipped": skipped,
            "errors": errors,
            "total_due": len(due_rows),
            "items": items,
        }

    def sync_interview_progress(
        self,
        job_id: int | None = None,
        limit: int = 100,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        if self.interview_client is None:
            return {
                "processed": 0,
                "updated": 0,
                "errors": 0,
                "items": [],
                "reason": "interview_client_not_configured",
            }

        safe_limit = max(1, min(int(limit or 100), 500))
        rows = self._list_candidates_with_interview_sessions(job_id=job_id, limit=max(safe_limit * 3, 300))
        processed = 0
        updated = 0
        errors = 0
        items: List[Dict[str, Any]] = []

        for row in rows:
            if processed >= safe_limit:
                break
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            session_id = str((notes or {}).get("interview_session_id") or "").strip()
            if not session_id:
                continue
            processed += 1
            try:
                payload = self.interview_client.refresh_session(session_id=session_id, force=force_refresh)
            except Exception:
                try:
                    payload = self.interview_client.get_session(session_id=session_id)
                except Exception as exc:
                    errors += 1
                    items.append(
                        {
                            "job_id": int(row["job_id"]),
                            "candidate_id": int(row["candidate_id"]),
                            "session_id": session_id,
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    self.db.log_operation(
                        operation="agent.interview.sync",
                        status="error",
                        entity_type="candidate",
                        entity_id=str(row["candidate_id"]),
                        details={"job_id": int(row["job_id"]), "session_id": session_id, "error": str(exc)},
                    )
                    continue

            interview_status = str(payload.get("status") or "").strip().lower()
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            total_score = summary.get("total_score")
            try:
                total_score = float(total_score) if total_score is not None else None
            except (TypeError, ValueError):
                total_score = None

            changed = self._apply_interview_progress_update(
                job_id=int(row["job_id"]),
                candidate_id=int(row["candidate_id"]),
                notes=notes or {},
                interview_status=interview_status,
                session_id=session_id,
                entry_url=str((notes or {}).get("interview_entry_url") or payload.get("entry_url") or "").strip() or None,
                total_score=total_score,
                current_match_status=str(row.get("status") or "needs_resume"),
            )
            if changed:
                updated += 1
            items.append(
                {
                    "job_id": int(row["job_id"]),
                    "candidate_id": int(row["candidate_id"]),
                    "session_id": session_id,
                    "interview_status": interview_status or None,
                    "updated": changed,
                    "total_score": total_score,
                }
            )
            self.db.log_operation(
                operation="agent.interview.sync",
                status="ok",
                entity_type="candidate",
                entity_id=str(row["candidate_id"]),
                details={
                    "job_id": int(row["job_id"]),
                    "session_id": session_id,
                    "interview_status": interview_status or None,
                    "updated": changed,
                    "total_score": total_score,
                },
            )

        return {
            "processed": processed,
            "updated": updated,
            "errors": errors,
            "items": items,
        }

    def run_due_interview_followups(self, job_id: int | None = None, limit: int = 100) -> Dict[str, Any]:
        if self.interview_client is None:
            return {
                "processed": 0,
                "sent": 0,
                "skipped": 0,
                "errors": 0,
                "items": [],
                "reason": "interview_client_not_configured",
            }

        self.sync_interview_progress(job_id=job_id, limit=max(limit * 2, 100), force_refresh=False)

        safe_limit = max(1, min(int(limit or 100), 500))
        now = datetime.now(timezone.utc)
        rows = self._list_candidates_with_interview_sessions(job_id=job_id, limit=max(safe_limit * 5, 500))
        due_rows: List[Dict[str, Any]] = []
        for row in rows:
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            if not isinstance(notes, dict):
                continue
            session_id = str(notes.get("interview_session_id") or "").strip()
            entry_url = str(notes.get("interview_entry_url") or "").strip()
            interview_status = str(notes.get("interview_status") or "").strip().lower()
            if not session_id or not entry_url:
                continue
            if interview_status not in ACTIVE_INTERVIEW_STATUSES:
                continue
            followups_sent = self._safe_int(notes.get("interview_followups_sent"), 0)
            if followups_sent >= self.interview_max_followups:
                continue
            due_at = self._parse_iso_datetime(str(notes.get("interview_next_followup_at") or "").strip())
            if due_at is None or due_at > now:
                continue
            due_rows.append(row)
            if len(due_rows) >= safe_limit:
                break

        sent = 0
        skipped = 0
        errors = 0
        items: List[Dict[str, Any]] = []
        for row in due_rows:
            job_ref = int(row["job_id"])
            candidate_id = int(row["candidate_id"])
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            session_id = str((notes or {}).get("interview_session_id") or "").strip()
            entry_url = str((notes or {}).get("interview_entry_url") or "").strip()
            interview_status = str((notes or {}).get("interview_status") or "").strip().lower() or "invited"
            followups_sent = self._safe_int((notes or {}).get("interview_followups_sent"), 0)
            followup_number = followups_sent + 1

            candidate = self.db.get_candidate(candidate_id)
            conversation = self.db.get_latest_conversation_for_candidate(candidate_id)
            job = self.db.get_job(job_ref)
            if not candidate or not conversation or not job:
                errors += 1
                items.append(
                    {
                        "job_id": job_ref,
                        "candidate_id": candidate_id,
                        "session_id": session_id,
                        "status": "error",
                        "reason": "missing_candidate_or_conversation_or_job",
                    }
                )
                continue

            language = self._candidate_primary_language(candidate)
            message = self._compose_interview_followup_message(
                job=job,
                candidate=candidate,
                entry_url=entry_url,
                language=language,
                followup_number=followup_number,
            )
            delivery = self._send_auto_reply(candidate=candidate, message=message, conversation=conversation)
            outbound_id = self.db.add_message(
                conversation_id=int(conversation["id"]),
                direction="outbound",
                content=message,
                candidate_language=language,
                meta={
                    "type": "interview_followup",
                    "auto": True,
                    "session_id": session_id,
                    "followup_number": followup_number,
                    "delivery": delivery,
                },
            )
            status = "sent" if delivery.get("sent") else "delivery_error"
            if delivery.get("sent"):
                sent += 1
            else:
                errors += 1

            next_followup_at = self._next_interview_followup_at(
                followups_sent=followup_number,
                now=datetime.now(timezone.utc),
            )
            updates = {
                "interview_status": interview_status,
                "interview_followups_sent": followup_number,
                "interview_last_followup_at": datetime.now(timezone.utc).isoformat(),
                "interview_next_followup_at": next_followup_at,
            }
            mapped_status = self._match_status_for_interview(interview_status=interview_status)
            if mapped_status:
                self.db.update_candidate_match_status(
                    job_id=job_ref,
                    candidate_id=candidate_id,
                    status=mapped_status,
                    extra_notes=updates,
                )
            else:
                self.db.update_candidate_match_status(
                    job_id=job_ref,
                    candidate_id=candidate_id,
                    status=str(row.get("status") or "needs_resume"),
                    extra_notes=updates,
                )

            self.db.log_operation(
                operation="agent.interview.followup",
                status="ok" if delivery.get("sent") else "error",
                entity_type="message",
                entity_id=str(outbound_id),
                details={
                    "job_id": job_ref,
                    "candidate_id": candidate_id,
                    "session_id": session_id,
                    "followup_number": followup_number,
                    "delivery": delivery,
                },
            )
            items.append(
                {
                    "job_id": job_ref,
                    "candidate_id": candidate_id,
                    "session_id": session_id,
                    "status": status,
                    "followup_number": followup_number,
                    "delivery": delivery,
                }
            )

        skipped = max(0, len(due_rows) - sent - errors)
        return {
            "processed": len(due_rows),
            "sent": sent,
            "skipped": skipped,
            "errors": errors,
            "items": items,
        }

    def _send_interview_invite_after_opt_in(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        conversation: Dict[str, Any],
        language: str,
        match: Dict[str, Any] | None,
    ) -> Dict[str, Any] | None:
        if self.interview_client is None:
            return None
        start_fn = getattr(self.interview_client, "start_session", None)
        if not callable(start_fn):
            return None

        notes = (match or {}).get("verification_notes") if isinstance((match or {}).get("verification_notes"), dict) else {}
        session_id_existing = str((notes or {}).get("interview_session_id") or "").strip()
        if session_id_existing:
            return {
                "started": False,
                "reason": "session_already_exists",
                "session_id": session_id_existing,
                "entry_url": (notes or {}).get("interview_entry_url"),
                "status": (notes or {}).get("interview_status"),
            }

        job_id = self._safe_int(job.get("id"), self._safe_int(conversation.get("job_id"), 0))
        candidate_id = self._safe_int(candidate.get("id"), self._safe_int(conversation.get("candidate_id"), 0))
        conversation_id = self._safe_int(conversation.get("id"), 0)
        if job_id <= 0 or candidate_id <= 0 or conversation_id <= 0:
            return {"started": False, "reason": "missing_ids"}

        try:
            started = start_fn(
                job_id=job_id,
                candidate_id=candidate_id,
                candidate_name=str(candidate.get("full_name") or "").strip(),
                conversation_id=conversation_id,
                language=str(language or "en").strip().lower() or "en",
                ttl_hours=self.interview_invite_ttl_hours,
            )
        except Exception as exc:
            self.db.log_operation(
                operation="agent.interview.invite",
                status="error",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={"job_id": job_id, "error": str(exc)},
            )
            return {"started": False, "reason": "start_session_failed", "error": str(exc)}

        session_id = str(started.get("session_id") or "").strip()
        entry_url = str(started.get("entry_url") or "").strip()
        interview_status = str(started.get("status") or "invited").strip().lower() or "invited"
        if not session_id or not entry_url:
            self.db.log_operation(
                operation="agent.interview.invite",
                status="error",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={"job_id": job_id, "reason": "missing_session_or_entry_url", "payload": started},
            )
            return {"started": False, "reason": "missing_session_or_entry_url", "payload": started}

        message = self._compose_interview_invite_message(
            job=job,
            candidate=candidate,
            entry_url=entry_url,
            language=language,
        )
        delivery = self._send_auto_reply(candidate=candidate, message=message, conversation=conversation)
        outbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=message,
            candidate_language=str(language or "en").strip().lower() or "en",
            meta={
                "type": "interview_invite",
                "auto": True,
                "session_id": session_id,
                "interview_status": interview_status,
                "entry_url": entry_url,
                "delivery": delivery,
            },
        )

        now = datetime.now(timezone.utc)
        updates = {
            "interview_session_id": session_id,
            "interview_entry_url": entry_url,
            "interview_status": interview_status,
            "interview_invited_at": now.isoformat(),
            "interview_followups_sent": 0,
            "interview_next_followup_at": self._next_interview_followup_at(followups_sent=0, now=now),
            "interview_provider": ((started.get("provider") or {}).get("name") if isinstance(started.get("provider"), dict) else None),
        }
        mapped_status = self._match_status_for_interview(interview_status=interview_status)
        self.db.update_candidate_match_status(
            job_id=job_id,
            candidate_id=candidate_id,
            status=mapped_status or str((match or {}).get("status") or "needs_resume"),
            extra_notes=updates,
        )
        self._upsert_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="interview_evaluation",
            stage_key="interview_results",
            score=None,
            status=interview_status if interview_status in {"invited", "in_progress"} else "not_started",
            reason="Interview invite created; waiting for candidate completion.",
            details={"session_id": session_id, "entry_url": entry_url},
        )
        self.db.log_operation(
            operation="agent.interview.invite",
            status="ok" if delivery.get("sent") else "error",
            entity_type="message",
            entity_id=str(outbound_id),
            details={
                "job_id": job_id,
                "candidate_id": candidate_id,
                "conversation_id": conversation_id,
                "session_id": session_id,
                "entry_url": entry_url,
                "delivery": delivery,
            },
        )
        return {
            "started": True,
            "session_id": session_id,
            "entry_url": entry_url,
            "status": interview_status,
            "delivery": delivery,
        }

    def _append_interview_opt_in_prompt(
        self,
        outbound: str,
        language: str,
        state: Dict[str, Any] | None,
        match: Dict[str, Any] | None,
    ) -> str:
        text = str(outbound or "").strip()
        if not text or self.interview_client is None:
            return text
        notes = (match or {}).get("verification_notes") if isinstance((match or {}).get("verification_notes"), dict) else {}
        if str((notes or {}).get("interview_session_id") or "").strip():
            return text

        state_status = str((state or {}).get("status") or "").strip().lower()
        if state_status in TERMINAL_PRE_RESUME_STATUSES or state_status == "interview_opt_in":
            return text
        lowered = text.lower()
        if "interview link" in lowered or "pre-vetting" in lowered or "pre vetting" in lowered:
            return text

        prompts = {
            "en": 'If you want to continue, reply "I agree to async pre-vetting" and I will send the interview link.',
            "ru": 'Если хотите продолжить, ответьте "согласен(а) на асинхронный pre-vetting", и я отправлю ссылку на интервью.',
            "es": 'Si quieres continuar, responde "acepto pre-vetting asincrono" y te envio el enlace de entrevista.',
        }
        lang = str(language or "en").strip().lower()
        prompt = prompts.get(lang, prompts["en"])
        return f"{text} {prompt}".strip()

    def _compose_interview_invite_message(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        entry_url: str,
        language: str,
    ) -> str:
        name = str(candidate.get("full_name") or "there")
        title = str(job.get("title") or "this role")
        templates = {
            "en": (
                "Great, {name}. Here is your async pre-vetting interview link for \"{title}\": {url}. "
                "Please complete it and reply here once done."
            ),
            "ru": (
                "Отлично, {name}. Вот ссылка на асинхронное pre-vetting интервью по роли \"{title}\": {url}. "
                "Пройдите его и напишите здесь, когда завершите."
            ),
            "es": (
                "Perfecto, {name}. Aqui tienes el enlace del interview async pre-vetting para \"{title}\": {url}. "
                "Completalo y responde aqui cuando termines."
            ),
        }
        lang = str(language or "en").strip().lower()
        template = templates.get(lang, templates["en"])
        return template.format(name=name, title=title, url=entry_url)

    def _compose_interview_followup_message(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        entry_url: str,
        language: str,
        followup_number: int,
    ) -> str:
        name = str(candidate.get("full_name") or "there")
        title = str(job.get("title") or "this role")
        first = {
            "en": 'Quick follow-up on your async pre-vetting for "{title}": {url}. Let me know if you need help.',
            "ru": 'Короткий follow-up по вашему async pre-vetting для "{title}": {url}. Если нужна помощь, напишите.',
            "es": 'Seguimiento rapido sobre tu async pre-vetting para "{title}": {url}. Avisa si necesitas ayuda.',
        }
        second = {
            "en": 'Final reminder for "{title}": please complete the async pre-vetting here: {url}.',
            "ru": 'Финальное напоминание по "{title}": пожалуйста, пройдите async pre-vetting по ссылке: {url}.',
            "es": 'Recordatorio final para "{title}": completa el async pre-vetting aqui: {url}.',
        }
        lang = str(language or "en").strip().lower()
        pool = first if int(followup_number) <= 1 else second
        template = pool.get(lang, pool["en"])
        return template.format(name=name, title=title, url=entry_url)

    def _apply_interview_progress_update(
        self,
        job_id: int,
        candidate_id: int,
        notes: Dict[str, Any],
        interview_status: str,
        session_id: str,
        entry_url: str | None,
        total_score: float | None,
        current_match_status: str,
    ) -> bool:
        status = str(interview_status or "").strip().lower()
        if not status:
            return False
        existing_status = str(notes.get("interview_status") or "").strip().lower()
        existing_score = notes.get("interview_total_score")
        changed = existing_status != status
        if total_score is not None:
            try:
                changed = changed or float(existing_score) != float(total_score)
            except (TypeError, ValueError):
                changed = True

        update_notes: Dict[str, Any] = {
            "interview_session_id": session_id,
            "interview_status": status,
        }
        if entry_url:
            update_notes["interview_entry_url"] = entry_url
        if total_score is not None:
            update_notes["interview_total_score"] = float(total_score)
            update_notes["interview_score"] = float(total_score)
            update_notes["final_interview_score"] = float(total_score)
            update_notes["interview_scored_at"] = datetime.now(timezone.utc).isoformat()
        if status in TERMINAL_INTERVIEW_STATUSES:
            update_notes["interview_next_followup_at"] = None

        mapped_status = self._match_status_for_interview(interview_status=status)
        self.db.update_candidate_match_status(
            job_id=job_id,
            candidate_id=candidate_id,
            status=mapped_status or str(current_match_status or "needs_resume"),
            extra_notes=update_notes,
        )

        if status == "scored" and total_score is not None:
            self._upsert_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="interview_evaluation",
                stage_key="interview_results",
                score=float(total_score),
                status="scored",
                reason="Interview scored and synced from interview module.",
                details={"session_id": session_id, "source": "interview_sync"},
            )
        elif status in {"invited", "in_progress"}:
            self._upsert_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="interview_evaluation",
                stage_key="interview_results",
                score=None,
                status=status,
                reason="Interview is active and awaiting completion.",
                details={"session_id": session_id, "source": "interview_sync"},
            )
        elif status in {"failed", "expired", "canceled"}:
            self._upsert_agent_assessment(
                job_id=job_id,
                candidate_id=candidate_id,
                agent_key="interview_evaluation",
                stage_key="interview_results",
                score=None,
                status="failed",
                reason=f"Interview ended with status {status}.",
                details={"session_id": session_id, "source": "interview_sync"},
            )

        return changed

    def _list_candidates_with_interview_sessions(self, job_id: int | None, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 2000))
        job_ids: List[int] = []
        if job_id is not None:
            job_ids = [int(job_id)]
        else:
            for job in self.db.list_jobs(limit=300):
                try:
                    job_ids.append(int(job.get("id")))
                except (TypeError, ValueError):
                    continue

        out: List[Dict[str, Any]] = []
        for job_ref in job_ids:
            rows = self.db.list_candidates_for_job(job_ref)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
                if not str((notes or {}).get("interview_session_id") or "").strip():
                    continue
                enriched = dict(row)
                enriched["job_id"] = job_ref
                out.append(enriched)
                if len(out) >= safe_limit:
                    return out
        return out

    def _next_interview_followup_at(self, followups_sent: int, now: datetime) -> str | None:
        if self.interview_max_followups <= 0:
            return None
        if int(followups_sent) >= int(self.interview_max_followups):
            return None
        idx = min(max(int(followups_sent), 0), len(self.interview_followup_delays_hours) - 1)
        delay = float(self.interview_followup_delays_hours[idx])
        return (now + timedelta(hours=delay)).astimezone(timezone.utc).isoformat()

    @staticmethod
    def _match_status_for_interview(interview_status: str) -> str | None:
        status = str(interview_status or "").strip().lower()
        mapping = {
            "created": "interview_invited",
            "invited": "interview_invited",
            "in_progress": "interview_in_progress",
            "completed": "interview_completed",
            "scored": "interview_scored",
            "failed": "interview_failed",
            "expired": "interview_failed",
            "canceled": "interview_failed",
        }
        return mapping.get(status)

    @staticmethod
    def _candidate_primary_language(candidate: Dict[str, Any]) -> str:
        langs = candidate.get("languages")
        if isinstance(langs, list):
            for item in langs:
                lang = str(item or "").strip().lower()
                if lang:
                    return lang
        return "en"

    @staticmethod
    def _safe_int(value: Any, fallback: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(fallback)

    def _record_sourcing_vetting_assessment(
        self,
        job_id: int,
        candidate_id: int,
        screening_status: str,
        match_score: float,
        notes: Dict[str, Any] | None,
    ) -> None:
        normalized_status = str(screening_status or "").strip().lower()
        raw_score = self._normalize_percentage(match_score * 100.0)
        status_map = {
            "verified": ("qualified", max(raw_score, 75.0)),
            "needs_resume": ("conditional", max(min(raw_score, 74.0), 50.0)),
            "rejected": ("not_matched", min(raw_score, 45.0)),
        }
        assessment_status, score = status_map.get(normalized_status, ("review", raw_score))
        explanation = ""
        if isinstance(notes, dict):
            explanation = str(notes.get("human_explanation") or "").strip()
        if not explanation:
            explanation = f"Screening status: {normalized_status or 'unknown'}."

        self._upsert_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="sourcing_vetting",
            stage_key="vetting",
            score=score,
            status=assessment_status,
            reason=explanation,
            details={
                "screening_status": normalized_status,
                "match_score": round(raw_score, 2),
            },
        )

    def _record_communication_outreach_assessment(
        self,
        job_id: int,
        candidate_id: int,
        delivery_status: str,
        delivery: Dict[str, Any] | None,
        connect_request: Dict[str, Any] | None,
        request_resume: bool,
    ) -> None:
        normalized = str(delivery_status or "").strip().lower()
        if normalized == "sent":
            score = 74.0 if request_resume else 70.0
            status = "contacted"
            reason = "Initial outreach delivered."
        elif normalized == "pending_connection":
            score = 45.0
            status = "pending_connection"
            reason = "Message blocked until candidate accepts connection request."
        else:
            score = 20.0
            status = "delivery_failed"
            reason = "Outreach delivery failed."

        delivery_error = str((delivery or {}).get("error") or "").strip()
        if delivery_error:
            reason = f"{reason} Provider error: {delivery_error}"

        self._upsert_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="communication",
            stage_key="outreach",
            score=score,
            status=status,
            reason=reason,
            details={
                "delivery_status": normalized or "unknown",
                "delivery": delivery or {},
                "connect_request": connect_request or {},
                "request_resume": bool(request_resume),
            },
        )

    def _record_communication_dialogue_assessment(
        self,
        job_id: int,
        candidate_id: int,
        mode: str,
        intent: str | None,
        state: Dict[str, Any] | None,
        inbound_text: str | None,
    ) -> None:
        normalized_mode = str(mode or "").strip().lower()
        normalized_intent = str(intent or "").strip().lower()
        state_status = str((state or {}).get("status") or "").strip().lower()

        if normalized_mode == "pre_resume":
            mapping = {
                "resume_received": (96.0, "cv_received", "Candidate shared CV/resume."),
                "interview_opt_in": (86.0, "interview_opt_in", "Candidate confirmed async pre-vetting interview."),
                "resume_promised": (83.0, "resume_promised", "Candidate promised to share CV later."),
                "engaged_no_resume": (78.0, "in_dialogue", "Candidate is engaged in dialogue before CV."),
                "awaiting_reply": (70.0, "awaiting_reply", "Awaiting candidate response after follow-up."),
                "not_interested": (35.0, "not_interested", "Candidate is not interested."),
                "stalled": (25.0, "stalled", "Dialogue stalled without response."),
                "unreachable": (15.0, "unreachable", "Candidate unreachable through current channel."),
            }
            score, status, reason = mapping.get(
                state_status,
                (66.0, "in_dialogue", f"Dialogue update captured (status: {state_status or 'unknown'})."),
            )
        else:
            score = 68.0
            status = "in_dialogue"
            reason = f"FAQ dialogue handled (intent: {normalized_intent or 'default'})."

        self._upsert_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key="communication",
            stage_key="dialogue",
            score=score,
            status=status,
            reason=reason,
            details={
                "mode": normalized_mode or "faq",
                "intent": normalized_intent or "default",
                "state_status": state_status or None,
                "inbound_preview": (str(inbound_text or "").strip()[:200] or None),
            },
        )

    def _upsert_agent_assessment(
        self,
        job_id: int,
        candidate_id: int,
        agent_key: str,
        stage_key: str,
        score: float | None,
        status: str,
        reason: str,
        details: Dict[str, Any] | None = None,
    ) -> None:
        self.db.upsert_candidate_agent_assessment(
            job_id=job_id,
            candidate_id=candidate_id,
            agent_key=agent_key,
            agent_name=self._agent_name(agent_key),
            stage_key=stage_key,
            score=self._normalize_percentage(score) if score is not None else None,
            status=str(status or "unknown"),
            reason=reason,
            instruction=self._agent_evaluation_instruction(agent_key=agent_key, stage_key=stage_key),
            details=details or {},
        )

    def _agent_name(self, agent_key: str) -> str:
        fallback = AGENT_ROLES.get(agent_key, agent_key.replace("_", " ").title())
        if self.agent_evaluation_playbook is None:
            return fallback
        from_book = self.agent_evaluation_playbook.get_agent_name(agent_key, fallback="")
        return from_book or fallback

    def _agent_evaluation_instruction(self, agent_key: str, stage_key: str) -> str:
        if self.agent_evaluation_playbook is None:
            return ""
        return self.agent_evaluation_playbook.get_instruction(agent_key=agent_key, stage_key=stage_key, fallback="")

    @staticmethod
    def _normalize_percentage(value: float | None) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        if numeric < 0.0:
            return 0.0
        if numeric > 100.0:
            return 100.0
        return round(numeric, 2)

    def _send_auto_reply(
        self,
        candidate: Dict[str, Any],
        message: str,
        conversation: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        if str(candidate.get("source") or "").lower() == "manual":
            return {
                "sent": True,
                "provider": "manual",
                "chat_id": (conversation or {}).get("external_chat_id"),
                "mock": True,
            }
        try:
            return self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
        except Exception as exc:
            return {"sent": False, "provider": "linkedin", "error": str(exc)}

    def _maybe_llm_reply(
        self,
        mode: str,
        instruction: str,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        inbound_text: str,
        history: List[Dict[str, str]],
        fallback_reply: str,
        language: str,
        state: Dict[str, Any] | None,
    ) -> str:
        fallback = (fallback_reply or "").strip()
        if not fallback:
            return fallback
        if self.llm_responder is None:
            return fallback
        source = "fallback"
        reason = "llm_unavailable"
        try:
            generated = self.llm_responder.generate_candidate_reply(
                mode=mode,
                instruction=instruction,
                job=job,
                candidate=candidate,
                inbound_text=inbound_text,
                history=history,
                fallback_reply=fallback,
                language=language,
                state=state,
            )
        except Exception as exc:
            self.db.log_operation(
                operation="agent.llm.reply.error",
                status="error",
                entity_type="candidate",
                entity_id=str(candidate.get("id") or candidate.get("linkedin_id") or "unknown"),
                details={"mode": mode, "error": str(exc)},
            )
            return fallback
        generated_text = str(generated or "").strip()
        if generated_text:
            source = "llm"
            reason = "generated"
        else:
            generated_text = fallback
            reason = "empty_generation"

        final_text = self._sanitize_reply_text(generated_text)
        if not final_text:
            final_text = fallback
            source = "fallback"
            reason = "empty_after_sanitize"

        if self._contains_template_placeholders(final_text):
            final_text = fallback
            source = "fallback"
            reason = "template_placeholder"

        if mode == "pre_resume" and self._should_require_resume_cta(state):
            if not self._has_resume_cta(final_text):
                cta = self._extract_resume_cta(fallback, language=language)
                if cta:
                    final_text = f"{final_text.rstrip()} {cta}".strip()
                source = "llm_guarded" if source == "llm" else source
                reason = "resume_cta_enforced"

        self.db.log_operation(
            operation="agent.llm.reply",
            status="ok" if source in {"llm", "llm_guarded"} else "partial",
            entity_type="candidate",
            entity_id=str(candidate.get("id") or candidate.get("linkedin_id") or "unknown"),
            details={
                "mode": mode,
                "source": source,
                "reason": reason,
                "language": language,
                "used_fallback": source == "fallback",
            },
        )

        return final_text or fallback

    @staticmethod
    def _sanitize_reply_text(text: str, limit: int = 600) -> str:
        normalized = " ".join(str(text or "").strip().split())
        if not normalized:
            return ""
        if len(normalized) <= limit:
            return normalized
        clipped = normalized[:limit].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0]
        return clipped.strip()

    @staticmethod
    def _contains_template_placeholders(text: str) -> bool:
        return bool(re.search(r"\{[a-zA-Z_][^{}]{0,40}\}", str(text or "")))

    @staticmethod
    def _should_require_resume_cta(state: Dict[str, Any] | None) -> bool:
        if not isinstance(state, dict):
            return True
        status = str(state.get("status") or "").strip().lower()
        return status not in {"resume_received", "not_interested", "unreachable", "stalled"}

    @staticmethod
    def _has_resume_cta(text: str) -> bool:
        lowered = str(text or "").lower()
        markers = (
            "cv",
            "resume",
            "résumé",
            "curriculum",
            "currículum",
            "резюме",
        )
        return any(marker in lowered for marker in markers)

    @classmethod
    def _extract_resume_cta(cls, text: str, language: str = "en") -> str:
        fallback_by_lang = {
            "en": "Please share your CV/resume so we can proceed.",
            "ru": "Пожалуйста, отправьте ваше резюме, чтобы мы могли продолжить.",
            "es": "Comparte tu CV para que podamos continuar.",
        }
        source = str(text or "").strip()
        if source:
            parts = [x.strip() for x in re.split(r"(?<=[.!?])\s+", source) if x.strip()]
            for part in parts:
                if cls._has_resume_cta(part):
                    return part
            if cls._has_resume_cta(source):
                return source
        return fallback_by_lang.get(str(language or "en").lower(), fallback_by_lang["en"])

    @staticmethod
    def _build_llm_history(messages: List[Dict[str, Any]], latest_inbound: str) -> List[Dict[str, str]]:
        history: List[Dict[str, str]] = []
        for msg in messages[-14:]:
            direction = str(msg.get("direction") or "outbound")
            role = "candidate" if direction == "inbound" else "agent"
            content = str(msg.get("content") or "").strip()
            if not content:
                continue
            history.append({"role": role, "content": content})
        if latest_inbound.strip():
            history.append({"role": "candidate", "content": latest_inbound.strip()})
        return history

    @staticmethod
    def _parse_iso_datetime(value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _is_connection_required_error(delivery: Dict[str, Any]) -> bool:
        if not isinstance(delivery, dict):
            return False
        if delivery.get("sent"):
            return False
        reason = str(delivery.get("reason") or "").lower()
        error = str(delivery.get("error") or "").lower()
        text = f"{reason} {error}"
        needles = (
            "no_connection_with_recipient",
            "recipient cannot be reached",
            "not to be first degree",
            "not first degree",
            "first degree connection",
        )
        return any(token in text for token in needles)

    def _deliver_pending_outreach_message(self, conversation_id: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        messages = self.db.list_messages(conversation_id=conversation_id)
        pending = None
        for msg in reversed(messages):
            if str(msg.get("direction") or "") != "outbound":
                continue
            meta = msg.get("meta") if isinstance(msg.get("meta"), dict) else {}
            if str(meta.get("delivery_status") or "") == "pending_connection":
                pending = msg
                break
            if str(meta.get("type") or "") == "outreach_pending_connection":
                pending = msg
                break
        if not pending:
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            return {"sent": False, "reason": "pending_message_not_found"}

        message = str(pending.get("content") or "").strip()
        if not message:
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            return {"sent": False, "reason": "pending_message_empty"}

        try:
            delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
        except Exception as exc:
            delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}

        if delivery.get("sent"):
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            chat_id = str(delivery.get("chat_id") or "").strip()
            chat_binding = None
            if chat_id:
                chat_binding = self.db.set_conversation_external_chat_id(
                    conversation_id=conversation_id,
                    external_chat_id=chat_id,
                )
                binding_status = str((chat_binding or {}).get("status") or "")
                if binding_status not in {"set", "rebound_same_candidate"}:
                    self.db.log_operation(
                        operation="agent.outreach.chat_binding",
                        status="partial",
                        entity_type="conversation",
                        entity_id=str(conversation_id),
                        details={"chat_binding": chat_binding},
                    )
            conversation = self.db.get_conversation(conversation_id)
            if conversation:
                self.db.update_candidate_match_status(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    status="outreach_sent",
                    extra_notes={"outreach_state": "sent_after_connection"},
                )
                self._record_communication_outreach_assessment(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    delivery_status="sent",
                    delivery=delivery,
                    connect_request=None,
                    request_resume=True,
                )
            self.db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content=message,
                candidate_language=str((candidate.get("languages") or ["en"])[0]).lower(),
                meta={
                    "type": "outreach_after_connection",
                    "auto": True,
                    "delivery": delivery,
                    "trigger": "connection_accepted",
                    "chat_binding": chat_binding,
                },
            )
            self.db.log_operation(
                operation="agent.outreach.send_after_connection",
                status="ok",
                entity_type="conversation",
                entity_id=str(conversation_id),
                details={"delivery": delivery},
            )
            return delivery

        if self._is_connection_required_error(delivery):
            self.db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")
        else:
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
        conversation = self.db.get_conversation(conversation_id)
        if conversation:
            self._record_communication_outreach_assessment(
                job_id=int(conversation["job_id"]),
                candidate_id=int(conversation["candidate_id"]),
                delivery_status="failed",
                delivery=delivery,
                connect_request=None,
                request_resume=True,
            )

        self.db.log_operation(
            operation="agent.outreach.send_after_connection",
            status="error",
            entity_type="conversation",
            entity_id=str(conversation_id),
            details={"delivery": delivery},
        )
        return delivery

    def _inject_forced_test_candidates(
        self,
        job: Dict[str, Any],
        profiles: List[Dict[str, Any]],
        limit: int,
        forced_identifiers: List[str],
        forced_only: bool = False,
    ) -> List[Dict[str, Any]]:
        if not forced_identifiers:
            return profiles

        target_limit = max(1, min(int(limit or 1), 100))
        forced_profiles: List[Dict[str, Any]] = []
        seen_forced: set[str] = set()

        for identifier in forced_identifiers:
            forced = self._resolve_forced_test_candidate(identifier=identifier, job=job)
            if not forced:
                continue
            marked = self._mark_forced_test_candidate(forced, identifier=identifier)
            key = self._profile_identity_key(marked)
            if key in seen_forced:
                continue
            seen_forced.add(key)
            forced_profiles.append(marked)

        if forced_only:
            return forced_profiles[:target_limit]

        merged: List[Dict[str, Any]] = []
        seen = set()

        for forced in forced_profiles:
            key = self._profile_identity_key(forced)
            if key in seen:
                continue
            seen.add(key)
            merged.append(forced)
            if len(merged) >= target_limit:
                return merged[:target_limit]

        for profile in profiles:
            key = self._profile_identity_key(profile)
            if key in seen:
                continue
            matched_identifier = self._forced_test_identifier_for_profile(profile, forced_identifiers)
            if matched_identifier:
                profile = self._mark_forced_test_candidate(profile, identifier=matched_identifier)
                key = self._profile_identity_key(profile)
                if key in seen:
                    continue
            seen.add(key)
            merged.append(profile)
            if len(merged) >= target_limit:
                break

        return merged[:target_limit]

    def _effective_test_mode(self, job: Dict[str, Any], test_mode: bool | None, forced_identifiers: List[str]) -> bool:
        if not forced_identifiers:
            return False
        if test_mode is not None:
            return bool(test_mode)
        return self._is_test_job(job) and self.test_jobs_forced_only

    def _resolve_forced_test_candidate(self, identifier: str, job: Dict[str, Any]) -> Dict[str, Any] | None:
        provider = getattr(self.sourcing_agent, "linkedin_provider", None)
        search_fn = getattr(provider, "search_profiles", None) if provider is not None else None

        fallback = {
            "linkedin_id": identifier,
            "unipile_profile_id": identifier,
            "attendee_provider_id": identifier,
            "full_name": f"Forced Test Candidate ({identifier})",
            "headline": "Test candidate",
            "location": str(job.get("location") or "Remote"),
            "languages": ["en"],
            "skills": [],
            "years_experience": 8,
            "raw": {
                "public_identifier": identifier,
                "forced_test_candidate": True,
                "forced_test_identifier": identifier,
                "source": "workflow_fallback",
            },
        }

        if not callable(search_fn):
            return fallback

        try:
            found = search_fn(query=identifier, limit=20) or []
        except Exception:
            return fallback

        if not isinstance(found, list) or not found:
            return fallback

        for profile in found:
            if not isinstance(profile, dict):
                continue
            matched = self._forced_test_identifier_for_profile(profile, [identifier])
            if matched == identifier:
                return profile

        for profile in found:
            if not isinstance(profile, dict):
                continue
            raw = profile.get("raw") if isinstance(profile.get("raw"), dict) else {}
            public_identifier = str(raw.get("public_identifier") or "").strip().lower()
            if public_identifier == identifier:
                return profile

        return fallback

    @staticmethod
    def _mark_forced_test_candidate(profile: Dict[str, Any], identifier: str) -> Dict[str, Any]:
        out = dict(profile)
        raw = out.get("raw") if isinstance(out.get("raw"), dict) else {}
        raw = dict(raw)
        raw["forced_test_candidate"] = True
        raw["forced_test_identifier"] = identifier
        raw.setdefault("public_identifier", identifier)
        out["raw"] = raw

        if not str(out.get("linkedin_id") or "").strip():
            out["linkedin_id"] = identifier
        if not str(out.get("attendee_provider_id") or "").strip():
            out["attendee_provider_id"] = str(out.get("linkedin_id") or identifier)
        if not str(out.get("unipile_profile_id") or "").strip():
            out["unipile_profile_id"] = str(out.get("linkedin_id") or identifier)
        if not str(out.get("full_name") or "").strip():
            out["full_name"] = f"Forced Test Candidate ({identifier})"
        if not isinstance(out.get("languages"), list) or not out.get("languages"):
            out["languages"] = ["en"]
        if not str(out.get("location") or "").strip():
            out["location"] = "Remote"
        return out

    def _forced_test_identifier_for_profile(self, profile: Dict[str, Any], forced_identifiers: List[str]) -> str | None:
        if not forced_identifiers:
            return None
        targets = {x.strip().lower() for x in forced_identifiers if x and x.strip()}
        if not targets:
            return None

        for field in ("linkedin_id", "attendee_provider_id", "unipile_profile_id", "provider_id"):
            value = str(profile.get(field) or "").strip().lower()
            if not value:
                continue
            if value in targets:
                return value
            for identifier in targets:
                if identifier and identifier in value:
                    return identifier

        raw = profile.get("raw")
        if not isinstance(raw, dict):
            raw_profile = profile.get("raw_profile")
            if isinstance(raw_profile, dict):
                raw = raw_profile
        if not isinstance(raw, dict):
            return None

        direct_identifier = str(raw.get("forced_test_identifier") or "").strip().lower()
        if direct_identifier and direct_identifier in targets:
            return direct_identifier
        if bool(raw.get("forced_test_candidate")):
            direct_public = str(raw.get("public_identifier") or "").strip().lower()
            if direct_public and direct_public in targets:
                return direct_public

        buckets: List[Dict[str, Any]] = []
        stack: List[Dict[str, Any]] = [raw]
        seen_obj_ids: set[int] = set()
        while stack:
            bucket = stack.pop()
            bucket_id = id(bucket)
            if bucket_id in seen_obj_ids:
                continue
            seen_obj_ids.add(bucket_id)
            buckets.append(bucket)
            for key in ("raw", "detail", "search", "data"):
                nested = bucket.get(key)
                if isinstance(nested, dict):
                    stack.append(nested)

        for bucket in buckets:
            forced_identifier = str(bucket.get("forced_test_identifier") or "").strip().lower()
            if forced_identifier and forced_identifier in targets:
                return forced_identifier
            public_identifier = str(bucket.get("public_identifier") or "").strip().lower()
            if public_identifier and public_identifier in targets:
                return public_identifier
            if bool(bucket.get("forced_test_candidate")) and len(targets) == 1:
                return next(iter(targets))
        return None

    def _load_forced_test_identifiers(self) -> List[str]:
        path_raw = self.forced_test_ids_path or ""
        if not path_raw:
            return []
        path = Path(path_raw)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            return []
        out: List[str] = []
        seen: set[str] = set()
        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].strip().lower()
            if not line:
                continue
            if line in seen:
                continue
            seen.add(line)
            out.append(line)
        return out

    def _build_forced_identifier_lookup(self, job: Dict[str, Any], forced_identifiers: List[str]) -> List[str]:
        if not forced_identifiers:
            return []
        out: set[str] = {x.strip().lower() for x in forced_identifiers if x and x.strip()}
        for identifier in list(out):
            resolved = self._resolve_forced_test_candidate(identifier=identifier, job=job)
            if not isinstance(resolved, dict):
                continue
            for field in ("linkedin_id", "attendee_provider_id", "unipile_profile_id", "provider_id"):
                value = str(resolved.get(field) or "").strip().lower()
                if value:
                    out.add(value)
            raw = resolved.get("raw")
            if isinstance(raw, dict):
                public_identifier = str(raw.get("public_identifier") or "").strip().lower()
                if public_identifier:
                    out.add(public_identifier)
        return sorted(out)

    @staticmethod
    def _forced_test_identifier_from_match(match: Dict[str, Any] | None, forced_identifiers: List[str]) -> str | None:
        if not isinstance(match, dict):
            return None
        targets = {x.strip().lower() for x in forced_identifiers if x and x.strip()}
        if not targets:
            return None
        notes = match.get("verification_notes")
        if not isinstance(notes, dict):
            return None
        forced_identifier = str(notes.get("forced_test_identifier") or "").strip().lower()
        if forced_identifier and forced_identifier in targets:
            return forced_identifier
        if bool(notes.get("forced_test_candidate")) and len(targets) == 1:
            return next(iter(targets))
        return None

    def _is_test_job(self, job: Dict[str, Any]) -> bool:
        if not self.test_job_keywords:
            return False
        title = str(job.get("title") or "").strip().lower()
        jd_text = str(job.get("jd_text") or "").strip().lower()
        text = f"{title}\n{jd_text}"
        return any(keyword in text for keyword in self.test_job_keywords)

    @staticmethod
    def _is_inbound_provider_message(message: Dict[str, Any], candidate: Dict[str, Any] | None) -> bool:
        direction = str(message.get("direction") or "").strip().lower()
        inbound_markers = {"inbound", "incoming", "received", "from_them"}
        outbound_markers = {"outbound", "sent", "from_me", "self"}
        if direction in inbound_markers:
            return True
        if direction in outbound_markers:
            return False

        for marker in ("is_sender", "is_self", "from_me", "self"):
            value = message.get(marker)
            if isinstance(value, bool):
                if value:
                    return False
                return True

        sender_provider_id = str(message.get("sender_provider_id") or "").strip().lower()
        if sender_provider_id and isinstance(candidate, dict):
            for field in ("linkedin_id", "attendee_provider_id", "unipile_profile_id"):
                candidate_id = str(candidate.get(field) or "").strip().lower()
                if candidate_id and sender_provider_id == candidate_id:
                    return True
        return False

    @staticmethod
    def _profile_identity_key(profile: Dict[str, Any]) -> str:
        for field in ("linkedin_id", "unipile_profile_id", "attendee_provider_id", "provider_id", "id"):
            value = profile.get(field)
            if isinstance(value, str) and value.strip():
                return f"id:{value.strip().lower()}"
        raw = profile.get("raw")
        if isinstance(raw, dict):
            public_identifier = raw.get("public_identifier")
            if isinstance(public_identifier, str) and public_identifier.strip():
                return f"public:{public_identifier.strip().lower()}"
        name = str(profile.get("full_name") or profile.get("name") or "").strip().lower()
        headline = str(profile.get("headline") or "").strip().lower()
        return f"fallback:{name}|{headline}"

    def _get_job_or_raise(self, job_id: int) -> Dict[str, Any]:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        return job
