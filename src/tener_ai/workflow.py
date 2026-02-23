from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

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
        return {"job_id": job_id, "profiles": profiles, "total": len(profiles)}

    def verify_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        enrich_result = self.enrich_profiles(job_id=job_id, profiles=profiles)
        enriched_profiles = enrich_result["profiles"]

        items: List[Dict[str, Any]] = []
        verified = 0
        rejected = 0

        for profile in enriched_profiles:
            score, status, notes = self.verification_agent.verify_candidate(job=job, profile=profile)
            record = {
                "profile": profile,
                "score": score,
                "status": status,
                "notes": notes,
            }
            items.append(record)

            if status == "verified":
                verified += 1
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
            "rejected": rejected,
            "enriched_total": enrich_result["total"],
            "enrich_failed": enrich_result["failed"],
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
        return {"job_id": job_id, "profiles": enriched_profiles, "total": len(enriched_profiles), "failed": failed}

    def add_verified_candidates(self, job_id: int, verified_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        self._get_job_or_raise(job_id)

        added: List[Dict[str, Any]] = []
        for item in verified_items:
            profile = item.get("profile") if isinstance(item, dict) else None
            if not isinstance(profile, dict):
                continue

            score = float(item.get("score") or 0.0)
            notes = item.get("notes") if isinstance(item.get("notes"), dict) else {}

            candidate_id = self.db.upsert_candidate(profile, source="linkedin")
            self.db.create_candidate_match(
                job_id=job_id,
                candidate_id=candidate_id,
                score=score,
                status="verified",
                verification_notes=notes,
            )
            self.db.log_operation(
                operation="agent.add.persist",
                status="ok",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={"job_id": job_id, "score": score},
            )
            added.append({"candidate_id": candidate_id, "profile": profile, "score": score})

        return {"job_id": job_id, "added": added, "total": len(added)}

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

            language, message = self.outreach_agent.compose_intro(job=job, candidate=candidate)
            conversation_id = self.db.get_or_create_conversation(job_id=job_id, candidate_id=candidate_id, channel="linkedin")

            try:
                delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
                if delivery.get("sent") is False:
                    failed += 1
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
                meta={"type": "outreach", "auto": True, "delivery": delivery},
            )
            self.db.log_operation(
                operation="agent.outreach.send",
                status="ok",
                entity_type="conversation",
                entity_id=str(conversation_id),
                details={"candidate_id": candidate_id, "language": language, "delivery": delivery},
            )

            out_items.append(
                {
                    "candidate_id": candidate_id,
                    "conversation_id": conversation_id,
                    "language": language,
                    "delivery": delivery,
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

        verified_items = [item for item in verify_result["items"] if item.get("status") == "verified"]
        add_result = self.add_verified_candidates(job_id=job_id, verified_items=verified_items)
        outreach_result = self.outreach_candidates(
            job_id=job_id,
            candidate_ids=[x["candidate_id"] for x in add_result["added"]],
        )

        summary = WorkflowSummary(
            job_id=job_id,
            searched=source_result["total"],
            verified=verify_result["verified"],
            rejected=verify_result["rejected"],
            outreached=outreach_result["total"],
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

    def _get_job_or_raise(self, job_id: int) -> Dict[str, Any]:
        job = self.db.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        return job
