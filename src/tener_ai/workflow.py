from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4
from typing import Any, Dict, List

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database
from .pre_resume_service import PreResumeCommunicationService

FORCED_TEST_PUBLIC_IDENTIFIER = "olena-bachek-b8523121a"
FORCED_TEST_SCORE = 0.99


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
        contact_all_mode: bool = False,
        require_resume_before_final_verify: bool = False,
        stage_instructions: Dict[str, str] | None = None,
    ) -> None:
        self.db = db
        self.sourcing_agent = sourcing_agent
        self.verification_agent = verification_agent
        self.outreach_agent = outreach_agent
        self.faq_agent = faq_agent
        self.pre_resume_service = pre_resume_service
        self.llm_responder = llm_responder
        self.contact_all_mode = contact_all_mode
        self.require_resume_before_final_verify = require_resume_before_final_verify
        self.stage_instructions = dict(stage_instructions or {})
        self.forced_test_public_identifier = FORCED_TEST_PUBLIC_IDENTIFIER
        self.forced_test_score = FORCED_TEST_SCORE

    def source_candidates(self, job_id: int, limit: int = 30) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        profiles = self.sourcing_agent.find_candidates(job=job, limit=limit)
        profiles = self._inject_forced_test_candidate(job=job, profiles=profiles, limit=limit)
        forced_included = any(self._is_forced_test_candidate(p) for p in profiles)

        self.db.log_operation(
            operation="agent.sourcing.search",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "profiles_found": len(profiles),
                "limit": limit,
                "forced_test_public_identifier": self.forced_test_public_identifier,
                "forced_test_included": forced_included,
            },
        )
        return {
            "job_id": job_id,
            "profiles": profiles,
            "total": len(profiles),
            "instruction": self.stage_instructions.get("sourcing", ""),
        }

    def verify_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        enrich_result = self.enrich_profiles(job_id=job_id, profiles=profiles)
        enriched_profiles = enrich_result["profiles"]

        items: List[Dict[str, Any]] = []
        verified = 0
        needs_resume = 0
        rejected = 0

        for profile in enriched_profiles:
            score, status, notes = self.verification_agent.verify_candidate(job=job, profile=profile)
            if self._is_forced_test_candidate(profile):
                score = max(float(score), self.forced_test_score)
                status = "verified"
                notes = dict(notes or {})
                notes["forced_test_candidate"] = True
                notes["forced_test_public_identifier"] = self.forced_test_public_identifier
                notes["forced_score"] = self.forced_test_score
                notes["human_explanation"] = (
                    "Тестовый кандидат принудительно приоритизирован: "
                    f"score установлен на {self.forced_test_score}."
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
                        + " Решение на этом этапе: запросить CV и уточнить опыт перед финальным вердиктом."
                    )
                else:
                    notes["human_explanation"] = (
                        "Недостаточно подтвержденных данных в профиле. Запрашиваем CV для финального решения."
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
        self.db.log_operation(
            operation="agent.sourcing.enrich",
            status="ok" if failed == 0 else "partial",
            entity_type="job",
            entity_id=str(job_id),
            details={"input_profiles": len(profiles), "enriched": len(enriched_profiles), "failed": failed},
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

    def outreach_candidates(self, job_id: int, candidate_ids: List[int]) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)

        out_items: List[Dict[str, Any]] = []
        conversation_ids: List[int] = []
        sent = 0
        pending_connection = 0
        failed = 0

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
            if external_chat_id:
                self.db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id=external_chat_id)

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
                },
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

        return {
            "job_id": job_id,
            "candidate_id": candidate_id,
            "conversation_id": conversation_id,
            "session_id": session_id,
            "external_chat_id": chat_id,
            "candidate": profile,
            "initial_outbound": initial_outbound,
        }

    def execute_job_workflow(self, job_id: int, limit: int = 30) -> WorkflowSummary:
        self._get_job_or_raise(job_id)

        self.db.log_operation(
            operation="workflow.execute.start",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={"limit": limit},
        )

        source_result = self.source_candidates(job_id=job_id, limit=limit)
        verify_result = self.verify_profiles(job_id=job_id, profiles=source_result["profiles"])

        if self.contact_all_mode:
            eligible_items = [item for item in verify_result["items"] if item.get("status") in {"verified", "needs_resume"}]
        else:
            eligible_items = [item for item in verify_result["items"] if item.get("status") == "verified"]
        add_result = self.add_verified_candidates(job_id=job_id, verified_items=eligible_items)
        outreach_result = self.outreach_candidates(
            job_id=job_id,
            candidate_ids=[x["candidate_id"] for x in add_result["added"]],
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

                return {
                    "language": language,
                    "intent": intent,
                    "reply": outbound,
                    "mode": "pre_resume",
                    "state": state_out,
                }

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
        if not generated_text:
            return fallback
        return generated_text

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
            if chat_id:
                self.db.set_conversation_external_chat_id(conversation_id=conversation_id, external_chat_id=chat_id)
            conversation = self.db.get_conversation(conversation_id)
            if conversation:
                self.db.update_candidate_match_status(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    status="outreach_sent",
                    extra_notes={"outreach_state": "sent_after_connection"},
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

        self.db.log_operation(
            operation="agent.outreach.send_after_connection",
            status="error",
            entity_type="conversation",
            entity_id=str(conversation_id),
            details={"delivery": delivery},
        )
        return delivery

    def _inject_forced_test_candidate(
        self,
        job: Dict[str, Any],
        profiles: List[Dict[str, Any]],
        limit: int,
    ) -> List[Dict[str, Any]]:
        forced = self._resolve_forced_test_candidate(job=job)
        if not forced:
            return profiles

        target_limit = max(1, min(int(limit or 1), 100))
        forced = self._mark_forced_test_candidate(forced)

        merged: List[Dict[str, Any]] = [forced]
        seen = {self._profile_identity_key(forced)}

        for profile in profiles:
            key = self._profile_identity_key(profile)
            if key in seen:
                continue
            if self._is_forced_test_candidate(profile):
                profile = self._mark_forced_test_candidate(profile)
                key = self._profile_identity_key(profile)
            merged.append(profile)
            seen.add(key)
            if len(merged) >= target_limit:
                break

        return merged[:target_limit]

    def _resolve_forced_test_candidate(self, job: Dict[str, Any]) -> Dict[str, Any] | None:
        provider = getattr(self.sourcing_agent, "linkedin_provider", None)
        if provider is None:
            return None
        provider_name = provider.__class__.__name__.lower()
        # Keep this hardcoded override for real Unipile runs and avoid perturbing mock/offline tests.
        if "unipile" not in provider_name:
            return None

        search_fn = getattr(provider, "search_profiles", None)
        if not callable(search_fn):
            return None

        fallback = {
            "linkedin_id": self.forced_test_public_identifier,
            "unipile_profile_id": self.forced_test_public_identifier,
            "attendee_provider_id": self.forced_test_public_identifier,
            "full_name": "Olena Bachek (Test)",
            "headline": "Senior Backend Engineer",
            "location": str(job.get("location") or "Remote"),
            "languages": ["en"],
            "skills": [],
            "years_experience": 8,
            "raw": {
                "public_identifier": self.forced_test_public_identifier,
                "forced_test_candidate": True,
                "source": "workflow_fallback",
            },
        }

        try:
            found = search_fn(query=self.forced_test_public_identifier, limit=20) or []
        except Exception:
            return fallback

        if not isinstance(found, list) or not found:
            return fallback

        for profile in found:
            if isinstance(profile, dict) and self._is_forced_test_candidate(profile):
                return profile

        for profile in found:
            if not isinstance(profile, dict):
                continue
            raw = profile.get("raw") if isinstance(profile.get("raw"), dict) else {}
            public_identifier = str(raw.get("public_identifier") or "").strip().lower()
            if public_identifier == self.forced_test_public_identifier:
                return profile

        first = found[0]
        if isinstance(first, dict):
            return first
        return fallback

    def _mark_forced_test_candidate(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(profile)
        raw = out.get("raw") if isinstance(out.get("raw"), dict) else {}
        raw = dict(raw)
        raw["forced_test_candidate"] = True
        raw["forced_test_public_identifier"] = self.forced_test_public_identifier
        raw.setdefault("public_identifier", self.forced_test_public_identifier)
        out["raw"] = raw

        if not str(out.get("linkedin_id") or "").strip():
            out["linkedin_id"] = self.forced_test_public_identifier
        if not str(out.get("attendee_provider_id") or "").strip():
            out["attendee_provider_id"] = str(out.get("linkedin_id") or self.forced_test_public_identifier)
        if not str(out.get("unipile_profile_id") or "").strip():
            out["unipile_profile_id"] = str(out.get("linkedin_id") or self.forced_test_public_identifier)
        if not str(out.get("full_name") or "").strip():
            out["full_name"] = "Olena Bachek (Test)"
        if not isinstance(out.get("languages"), list) or not out.get("languages"):
            out["languages"] = ["en"]
        if not str(out.get("location") or "").strip():
            out["location"] = "Remote"
        return out

    def _is_forced_test_candidate(self, profile: Dict[str, Any]) -> bool:
        target = self.forced_test_public_identifier
        for field in ("linkedin_id", "attendee_provider_id", "unipile_profile_id", "provider_id"):
            value = str(profile.get(field) or "").strip().lower()
            if value == target:
                return True

        raw = profile.get("raw")
        if not isinstance(raw, dict):
            return False

        if bool(raw.get("forced_test_candidate")):
            return True

        buckets = [raw]
        if isinstance(raw.get("detail"), dict):
            buckets.append(raw["detail"])
        if isinstance(raw.get("search"), dict):
            buckets.append(raw["search"])

        for bucket in buckets:
            public_identifier = str(bucket.get("public_identifier") or "").strip().lower()
            if public_identifier == target:
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
