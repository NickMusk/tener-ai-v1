from __future__ import annotations

from typing import Any
from dataclasses import dataclass
from typing import Dict, List

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database


@dataclass
class WorkflowSummary:
    job_id: int
    searched: int
    verified: int
    rejected: int
    outreached: int
    conversation_ids: List[int]


class WorkflowService:
    def __init__(
        self,
        db: Database,
        sourcing_agent: SourcingAgent,
        verification_agent: VerificationAgent,
        outreach_agent: OutreachAgent,
        faq_agent: FAQAgent,
    ) -> None:
        self.db = db
        self.sourcing_agent = sourcing_agent
        self.verification_agent = verification_agent
        self.outreach_agent = outreach_agent
        self.faq_agent = faq_agent

    def execute_job_workflow(self, job_id: int, limit: int = 30) -> WorkflowSummary:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        self.db.log_operation(
            operation="workflow.execute.start",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={"limit": limit},
        )

        profiles = self.sourcing_agent.find_candidates(job=job, limit=limit)
        self.db.log_operation(
            operation="agent.sourcing.search",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={"profiles_found": len(profiles)},
        )

        verified_count = 0
        rejected_count = 0
        outreached_count = 0
        conversation_ids: List[int] = []

        for profile in profiles:
            candidate_id = self.db.upsert_candidate(profile, source="linkedin")
            score, status, notes = self.verification_agent.verify_candidate(job=job, profile=profile)

            self.db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=score,
                status=status,
                verification_notes=notes,
            )
            self.db.log_operation(
                operation="agent.verification.evaluate",
                status="ok",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={
                    "job_id": job_id,
                    "result": status,
                    "score": score,
                },
            )

            if status != "verified":
                rejected_count += 1
                continue

            verified_count += 1
            candidate = self.db.get_candidate(candidate_id)
            if not candidate:
                continue

            language, message = self.outreach_agent.compose_intro(job=job, candidate=candidate)
            conversation_id = self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")
            delivery: Dict[str, Any]
            try:
                delivery = self.sourcing_agent.send_outreach(candidate_profile=profile, message=message)
            except Exception as exc:
                delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}
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
                meta={"type": "outreach", "auto": True, "delivery": delivery},
            )
            self.db.log_operation(
                operation="agent.outreach.send",
                status="ok",
                entity_type="conversation",
                entity_id=str(conversation_id),
                details={"candidate_id": candidate_id, "language": language, "delivery": delivery},
            )

            conversation_ids.append(conversation_id)
            outreached_count += 1

        summary = WorkflowSummary(
            job_id=job_id,
            searched=len(profiles),
            verified=verified_count,
            rejected=rejected_count,
            outreached=outreached_count,
            conversation_ids=conversation_ids,
        )

        self.db.log_operation(
            operation="workflow.execute.finish",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "searched": summary.searched,
                "verified": summary.verified,
                "rejected": summary.rejected,
                "outreached": summary.outreached,
            },
        )

        return summary

    def process_inbound_message(self, conversation_id: int, text: str) -> Dict[str, str]:
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
