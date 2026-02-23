from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database
from .pre_resume_service import PreResumeCommunicationService


@dataclass
class WorkflowSummary:
    job_id: int
    searched: int
    verified: int
    needs_resume: int
    rejected: int
    outreached: int
    outreach_sent: int
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
        self.contact_all_mode = contact_all_mode
        self.require_resume_before_final_verify = require_resume_before_final_verify
        self.stage_instructions = dict(stage_instructions or {})

    def source_candidates(self, job_id: int, limit: int = 30) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        profiles = self.sourcing_agent.find_candidates(job=job, limit=limit)

        self.db.log_operation(
            operation="agent.sourcing.search",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={"profiles_found": len(profiles), "limit": limit},
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

            try:
                delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
                if delivery.get("sent") is False:
                    failed += 1
                    self.db.log_operation(
                        operation="agent.outreach.delivery_error",
                        status="error",
                        entity_type="candidate",
                        entity_id=str(candidate_id),
                        details={"job_id": job_id, "delivery": delivery},
                    )
                else:
                    sent += 1
            except Exception as exc:
                delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}
                failed += 1
                self.db.log_operation(
                    operation="agent.outreach.delivery_error",
                    status="error",
                    entity_type="candidate",
                    entity_id=str(candidate_id),
                    details={"job_id": job_id, "error": str(exc)},
                )

            self.db.add_message(
                conversation_id=conversation_id,
                direction="outbound",
                content=message,
                candidate_language=language,
                meta={
                    "type": "outreach",
                    "auto": True,
                    "delivery": delivery,
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                },
            )
            self.db.log_operation(
                operation="agent.outreach.send",
                status="ok" if delivery.get("sent") else "error",
                entity_type="conversation",
                entity_id=str(conversation_id),
                details={
                    "candidate_id": candidate_id,
                    "language": language,
                    "delivery": delivery,
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                },
            )

            out_items.append(
                {
                    "candidate_id": candidate_id,
                    "conversation_id": conversation_id,
                    "language": language,
                    "delivery": delivery,
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                }
            )
            conversation_ids.append(conversation_id)

        return {
            "job_id": job_id,
            "items": out_items,
            "conversation_ids": conversation_ids,
            "sent": sent,
            "failed": failed,
            "total": len(out_items),
            "instruction": self.stage_instructions.get("outreach", ""),
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

        messages = self.db.list_messages(conversation_id)
        previous_lang = None
        for item in reversed(messages):
            if item.get("candidate_language"):
                previous_lang = item["candidate_language"]
                break

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
                        },
                    )
                    self.db.log_operation(
                        operation="agent.pre_resume.reply",
                        status="ok",
                        entity_type="message",
                        entity_id=str(outbound_id),
                        details={"conversation_id": conversation_id, "intent": intent, "language": language, "session_id": session_id},
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
        outbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="outbound",
            content=reply,
            candidate_language=lang,
            meta={"type": "faq_auto_reply", "intent": intent, "auto": True},
        )
        self.db.log_operation(
            operation="agent.faq.reply",
            status="ok",
            entity_type="message",
            entity_id=str(outbound_id),
            details={"conversation_id": conversation_id, "intent": intent, "language": lang},
        )

        return {"language": lang, "intent": intent, "reply": reply}

    def _get_job_or_raise(self, job_id: int) -> Dict[str, Any]:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        return job
