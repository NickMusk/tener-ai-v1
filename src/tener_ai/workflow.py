from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import io
import hashlib
import os
from pathlib import Path
import re
from urllib import request as urlrequest
import zipfile
from uuid import uuid4
from typing import Any, Dict, List

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .attachments import (
    AttachmentDescriptor,
    descriptors_to_text,
    extract_attachment_descriptors_from_values,
    extract_resume_urls,
    is_resume_like_name_or_url,
)
from .db import Database, utc_now_iso
from .instructions import AgentEvaluationPlaybook
from .linkedin_limits import (
    effective_daily_connect_limit,
    effective_daily_message_limit,
    policy_allowed_connects_today,
    policy_daily_new_threads_cap,
    policy_weekly_connect_cap,
)
from .linkedin_provider import UnipileLinkedInProvider
from .language import normalize_language, resolve_conversation_language, resolve_outbound_language
from .message_extraction import CandidateMessageExtractionService, parse_resume_links
from .pre_resume_service import PreResumeCommunicationService

DEFAULT_FORCED_TEST_SCORE = 0.99
TERMINAL_PRE_RESUME_STATUSES = {
    "ready_for_interview",
    "ready_for_screening_call",
    "not_interested",
    "unreachable",
    "stalled",
    "delivery_blocked_identity",
}
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


class JobOperationBlockedError(RuntimeError):
    pass


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
        linkedin_outreach_policy: Dict[str, Any] | None = None,
        managed_linkedin_enabled: bool = True,
        managed_linkedin_dispatch_inline: bool = True,
        managed_unipile_api_key: str = "",
        managed_unipile_base_url: str = "https://api.unipile.com",
        managed_unipile_timeout_seconds: int = 30,
    ) -> None:
        self.db = db
        self.sourcing_agent = sourcing_agent
        self.verification_agent = verification_agent
        self.outreach_agent = outreach_agent
        self.faq_agent = faq_agent
        self.pre_resume_service = pre_resume_service
        self.llm_responder = llm_responder
        self.message_extraction_service = CandidateMessageExtractionService(llm_client=llm_responder)
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
                "test,smoke,sandbox,debug,verify,staging,check,probe,demo,тест",
            )
        )
        self.test_job_keywords = [x.strip().lower() for x in raw_keywords.split(",") if x.strip()]
        self.linkedin_outreach_policy = dict(linkedin_outreach_policy or {})
        self.managed_linkedin_enabled = bool(managed_linkedin_enabled)
        self.managed_linkedin_dispatch_inline = bool(managed_linkedin_dispatch_inline)
        self.managed_unipile_api_key = str(managed_unipile_api_key or "").strip()
        self.managed_unipile_base_url = str(managed_unipile_base_url or "https://api.unipile.com").strip()
        try:
            self.managed_unipile_timeout_seconds = max(5, int(managed_unipile_timeout_seconds))
        except (TypeError, ValueError):
            self.managed_unipile_timeout_seconds = 30

    def _persist_step_progress(self, *, job_id: int, step: str, status: str, output: Dict[str, Any] | None = None) -> None:
        self.db.upsert_job_step_progress(
            job_id=int(job_id),
            step=str(step),
            status=str(status),
            output=output or {},
        )

    def _reset_step_progress(self, *, job_id: int, steps: List[str]) -> None:
        for step in steps:
            self._persist_step_progress(job_id=job_id, step=step, status="idle", output={})

    @staticmethod
    def _is_removed_provider_error(exc: Exception | str) -> bool:
        text = str(exc or "").lower()
        return "account not found" in text or ("errors/resource_not_found" in text and "requested resource" in text)

    @staticmethod
    def _is_operational_linkedin_account(account: Dict[str, Any] | None) -> bool:
        if not isinstance(account, dict):
            return False
        if str(account.get("status") or "").strip().lower() != "connected":
            return False
        metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
        return not bool(metadata.get("removed_from_provider"))

    def _list_active_linkedin_accounts(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        return [
            row
            for row in self.db.list_linkedin_accounts(limit=limit, status="connected")
            if self._is_operational_linkedin_account(row)
        ]

    def _list_dispatchable_linkedin_accounts(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        return self._list_active_linkedin_accounts(limit=limit)

    @staticmethod
    def _source_account_priority(account: Dict[str, Any]) -> int:
        metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
        connection_params = metadata.get("connection_params") if isinstance(metadata.get("connection_params"), dict) else {}
        im_params = connection_params.get("im") if isinstance(connection_params.get("im"), dict) else {}
        premium_features = im_params.get("premiumFeatures")
        features: List[str] = []
        if isinstance(premium_features, list):
            features.extend(str(item or "").strip().lower() for item in premium_features if str(item or "").strip())
        if isinstance(metadata.get("premium_features"), list):
            features.extend(str(item or "").strip().lower() for item in (metadata.get("premium_features") or []) if str(item or "").strip())
        joined = " ".join(features)
        if "recruiter" in joined:
            return 0
        if any(token in joined for token in ("sales", "navigator", "premium")):
            return 1
        return 2

    def _mark_linkedin_account_removed(self, *, account: Dict[str, Any], reason: str) -> None:
        account_id = int(account.get("id") or 0)
        if account_id <= 0:
            return
        self.db.update_linkedin_account_status(
            account_id=account_id,
            status="removed",
            metadata={
                "removed_from_provider": True,
                "removed_from_provider_at": utc_now_iso(),
                "removed_reason": str(reason or "").strip() or "provider_account_not_found",
            },
        )

    def _job_candidate_profile_keys(self, *, job_id: int) -> set[str]:
        keys: set[str] = set()
        for row in self.db.list_candidates_for_job(job_id):
            linkedin_id = str(row.get("linkedin_id") or "").strip().lower()
            if linkedin_id:
                keys.add(f"id:{linkedin_id}")
        return keys

    def source_candidates(
        self,
        job_id: int,
        limit: int = 30,
        test_mode: bool | None = None,
        *,
        exclude_profile_keys: set[str] | None = None,
    ) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        self._assert_job_automation_allowed(job, operation="source_candidates")
        forced_test_ids = self._load_forced_test_identifiers()
        forced_only = self._effective_test_mode(job=job, test_mode=test_mode, forced_identifiers=forced_test_ids)
        provider = getattr(self.sourcing_agent, "linkedin_provider", None)
        original_account_id = getattr(provider, "account_id", None) if provider is not None and hasattr(provider, "account_id") else None
        profiles: List[Dict[str, Any]] = []
        search_errors: List[str] = []
        seen_profile_keys: set[str] = {
            str(item or "").strip().lower()
            for item in (exclude_profile_keys or set())
            if str(item or "").strip()
        }
        forced_seed_profiles: List[Dict[str, Any]] = []
        if forced_only:
            profiles = self._inject_forced_test_candidates(
                job=job,
                profiles=[],
                limit=limit,
                forced_identifiers=forced_test_ids,
                forced_only=True,
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
                    "test_mode_active": True,
                    "test_mode_requested": test_mode,
                    "excluded_profile_keys": len(exclude_profile_keys or set()),
                    "forced_only_short_circuit": True,
                },
            )
            return {
                "job_id": job_id,
                "profiles": profiles,
                "total": len(profiles),
                "test_mode_active": True,
                "test_mode_requested": test_mode,
                "instruction": self.stage_instructions.get("sourcing", ""),
            }
        if forced_test_ids and max(1, min(int(limit or 1), 100)) == 1:
            forced_seed_profiles = self._inject_forced_test_candidates(
                job=job,
                profiles=[],
                limit=limit,
                forced_identifiers=forced_test_ids,
                forced_only=True,
            )
            for profile in forced_seed_profiles:
                key = self._profile_identity_key(profile)
                if key in seen_profile_keys:
                    continue
                seen_profile_keys.add(key)
                profiles.append(profile)
        source_accounts = sorted(
            self._list_dispatchable_linkedin_accounts(limit=500),
            key=self._source_account_priority,
        )
        if provider is not None and hasattr(provider, "account_id") and source_accounts:
            try:
                for account in source_accounts:
                    if len(profiles) >= limit:
                        break
                    provider_account_id = str(account.get("provider_account_id") or "").strip()
                    if not provider_account_id:
                        continue
                    setattr(provider, "account_id", provider_account_id)
                    try:
                        batch = self.sourcing_agent.find_candidates(
                            job=job,
                            limit=max(1, limit - len(profiles)),
                            exclude_profile_keys=seen_profile_keys,
                        )
                    except Exception as exc:
                        if self._is_removed_provider_error(exc):
                            self._mark_linkedin_account_removed(account=account, reason=str(exc))
                        search_errors.append(f"account_id={provider_account_id} error={exc}")
                        continue
                    if not batch:
                        continue
                    for profile in batch:
                        key = self.sourcing_agent._candidate_key(profile)
                        if key in seen_profile_keys:
                            continue
                        seen_profile_keys.add(key)
                        profiles.append(profile)
                        if len(profiles) >= limit:
                            break
            finally:
                setattr(provider, "account_id", original_account_id)
            if not profiles and search_errors:
                raise RuntimeError("; ".join(search_errors[:5]))
        elif provider is not None and hasattr(provider, "account_id"):
            raise RuntimeError("no_active_linkedin_accounts")
        elif len(profiles) < limit:
            profiles.extend(
                self.sourcing_agent.find_candidates(
                    job=job,
                    limit=max(1, limit - len(profiles)),
                    exclude_profile_keys=seen_profile_keys,
                )
            )
        if forced_seed_profiles:
            retained_search_profiles = self._exclude_forced_test_profiles(
                profiles=profiles[len(forced_seed_profiles) :],
                forced_identifiers=forced_test_ids,
            )
            profiles = (forced_seed_profiles + retained_search_profiles)[: max(1, min(int(limit or 1), 100))]
        else:
            profiles = self._exclude_forced_test_profiles(
                profiles=profiles,
                forced_identifiers=forced_test_ids,
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
                "excluded_profile_keys": len(exclude_profile_keys or set()),
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

    def top_up_job_candidates(self, job_id: int, limit: int = 30, test_mode: bool | None = None) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        self._assert_job_automation_allowed(job, operation="top_up_job_candidates")
        forced_test_ids = self._load_forced_test_identifiers()
        effective_test_mode = self._effective_test_mode(
            job=job,
            test_mode=test_mode,
            forced_identifiers=forced_test_ids,
        )
        exclude_profile_keys = self._job_candidate_profile_keys(job_id=job_id)

        self.db.log_operation(
            operation="workflow.source_top_up.start",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details={
                "limit": limit,
                "existing_candidates": len(exclude_profile_keys),
                "test_mode_active": effective_test_mode,
                "test_mode_requested": test_mode,
            },
        )
        self._reset_step_progress(job_id=job_id, steps=["source", "enrich", "verify", "add", "workflow"])
        self._persist_step_progress(
            job_id=job_id,
            step="workflow",
            status="running",
            output={"mode": "source_top_up", "limit": limit, "test_mode_requested": test_mode},
        )

        steps_order = ["source", "enrich", "verify", "add"]
        source_result: Dict[str, Any] = {}
        enrich_result: Dict[str, Any] = {}
        verify_result: Dict[str, Any] = {}
        add_result: Dict[str, Any] = {}
        current_step = "source"

        try:
            self._persist_step_progress(job_id=job_id, step="source", status="running", output={})
            source_result = self.source_candidates(
                job_id=job_id,
                limit=limit,
                test_mode=effective_test_mode,
                exclude_profile_keys=exclude_profile_keys,
            )
            self._persist_step_progress(job_id=job_id, step="source", status="success", output=source_result)

            current_step = "enrich"
            self._persist_step_progress(job_id=job_id, step="enrich", status="running", output={})
            enrich_result = self.enrich_profiles(job_id=job_id, profiles=source_result["profiles"])
            self._persist_step_progress(job_id=job_id, step="enrich", status="success", output=enrich_result)

            current_step = "verify"
            self._persist_step_progress(job_id=job_id, step="verify", status="running", output={})
            verify_result = self._verify_enriched_profiles(
                job_id=job_id,
                enriched_profiles=enrich_result["profiles"],
                enrich_result=enrich_result,
            )
            self._persist_step_progress(job_id=job_id, step="verify", status="success", output=verify_result)

            if self.contact_all_mode:
                eligible_items = [item for item in verify_result["items"] if item.get("status") in {"verified", "needs_resume"}]
            else:
                eligible_items = [item for item in verify_result["items"] if item.get("status") == "verified"]

            current_step = "add"
            self._persist_step_progress(job_id=job_id, step="add", status="running", output={})
            add_result = self.add_verified_candidates(job_id=job_id, verified_items=eligible_items)
            self._persist_step_progress(job_id=job_id, step="add", status="success", output=add_result)
        except Exception as exc:
            self._persist_step_progress(job_id=job_id, step=current_step, status="error", output={"error": str(exc)})
            current_index = steps_order.index(current_step) if current_step in steps_order else -1
            for step in steps_order[current_index + 1 :]:
                self._persist_step_progress(
                    job_id=job_id,
                    step=step,
                    status="skipped",
                    output={"reason": "upstream_step_failed", "failed_step": current_step, "mode": "source_top_up"},
                )
            self._persist_step_progress(
                job_id=job_id,
                step="workflow",
                status="error",
                output={"error": str(exc), "failed_step": current_step, "mode": "source_top_up"},
            )
            raise

        result = {
            "job_id": job_id,
            "mode": "source_top_up",
            "requested_limit": int(limit or 0),
            "existing_candidates": len(exclude_profile_keys),
            "searched": int(source_result.get("total") or 0),
            "verified": int(verify_result.get("verified") or 0),
            "needs_resume": int(verify_result.get("needs_resume") or 0),
            "rejected": int(verify_result.get("rejected") or 0),
            "added": int(add_result.get("total") or 0),
            "candidate_ids": [int(item.get("candidate_id") or 0) for item in (add_result.get("added") or []) if int(item.get("candidate_id") or 0) > 0],
            "test_mode_requested": test_mode,
        }
        self.db.log_operation(
            operation="workflow.source_top_up.finish",
            status="ok",
            entity_type="job",
            entity_id=str(job_id),
            details=result,
        )
        self._persist_step_progress(job_id=job_id, step="workflow", status="success", output=result)
        return result

    def verify_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        enrich_result = self.enrich_profiles(job_id=job_id, profiles=profiles)
        return self._verify_enriched_profiles(
            job_id=job_id,
            enriched_profiles=enrich_result["profiles"],
            enrich_result=enrich_result,
        )

    def _verify_enriched_profiles(
        self,
        *,
        job_id: int,
        enriched_profiles: List[Dict[str, Any]],
        enrich_result: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        self._assert_job_automation_allowed(job, operation="verify_profiles")
        job_culture_profile = (
            job.get("company_culture_profile")
            if isinstance(job.get("company_culture_profile"), dict)
            else {}
        )
        forced_test_ids = self._load_forced_test_identifiers()
        enrich_summary = enrich_result or {"total": len(enriched_profiles), "failed": 0}

        items: List[Dict[str, Any]] = []
        verified = 0
        needs_resume = 0
        rejected = 0

        for profile in enriched_profiles:
            score, status, notes = self.verification_agent.verify_candidate(job=job, profile=profile)
            notes = dict(notes or {})
            if job_culture_profile and not isinstance(notes.get("company_culture_profile"), dict):
                notes["company_culture_profile"] = job_culture_profile
            forced_identifier = self._forced_test_identifier_for_profile(profile, forced_test_ids)
            if forced_identifier:
                score = max(float(score), self.forced_test_score)
                status = "verified"
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
            "enriched_total": enrich_summary["total"],
            "enrich_failed": enrich_summary["failed"],
            "instruction": self.stage_instructions.get("verification", ""),
        }

    def enrich_profiles(self, job_id: int, profiles: List[Dict[str, Any]]) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        self._assert_job_automation_allowed(job, operation="enrich_profiles")
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
        job = self._get_job_or_raise(job_id)
        self._assert_job_automation_allowed(job, operation="add_verified_candidates")

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
        self._assert_job_automation_allowed(job, operation="outreach_candidates")
        try:
            if self._managed_linkedin_available():
                return self._outreach_candidates_managed(job_id=job_id, candidate_ids=candidate_ids, test_mode=test_mode)
            return self._outreach_candidates_direct(job_id=job_id, candidate_ids=candidate_ids, test_mode=test_mode)
        except JobOperationBlockedError:
            raise
        except Exception as exc:
            self.db.log_operation(
                operation="agent.outreach.execute",
                status="error",
                entity_type="job",
                entity_id=str(job_id),
                details={"error": str(exc), "candidate_ids_count": len(candidate_ids or [])},
            )
            return {
                "job_id": job_id,
                "items": [],
                "conversation_ids": [],
                "sent": 0,
                "pending_connection": 0,
                "failed": len(candidate_ids or []),
                "total": 0,
                "error": str(exc),
                "instruction": self.stage_instructions.get("outreach", ""),
            }

    def _outreach_candidates_managed(
        self,
        job_id: int,
        candidate_ids: List[int],
        test_mode: bool | None = None,
        selection_state_override: Dict[str, Any] | None = None,
        dispatch_inline_override: bool | None = None,
    ) -> Dict[str, Any]:
        job = self._get_job_or_raise(job_id)
        forced_identifiers = self._load_forced_test_identifiers()
        forced_lookup = self._build_forced_identifier_lookup(job=job, forced_identifiers=forced_identifiers)
        forced_only = self._effective_test_mode(job=job, test_mode=test_mode, forced_identifiers=forced_identifiers)

        out_items: List[Dict[str, Any]] = []
        conversation_ids: List[int] = []
        queued_action_ids: List[int] = []
        selection_state = selection_state_override or self._build_linkedin_account_selection_state(job_id=job_id)
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
            if self._is_non_test_forced_candidate(
                candidate=candidate,
                match=match,
                forced_identifiers=forced_lookup,
                forced_only=forced_only,
            ):
                self.db.log_operation(
                    operation="agent.outreach.production_filter_skip",
                    status="skipped",
                    entity_type="candidate",
                    entity_id=str(candidate_id),
                    details={
                        "job_id": job_id,
                        "reason": "forced_test_candidate_excluded",
                    },
                )
                continue
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
            language = resolve_outbound_language(candidate, fallback="en")
            message = ""
            session_state: Dict[str, Any] | None = None
            started_pre_resume_session = False
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
                        language=resolve_outbound_language(candidate, fallback="en"),
                        job_location=str(job.get("location") or "").strip() or None,
                        salary_min=self._safe_float(job.get("salary_min"), None),
                        salary_max=self._safe_float(job.get("salary_max"), None),
                        salary_currency=str(job.get("salary_currency") or "").strip() or None,
                        work_authorization_required=bool(job.get("work_authorization_required")),
                    )
                    session_state = started["state"]
                    language = str(session_state.get("language") or "en")
                    message = str(started.get("outbound") or "")
                    started_pre_resume_session = True
                self.db.upsert_pre_resume_session(
                    session_id=pre_resume_session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=session_state,
                    instruction=self.stage_instructions.get("pre_resume", ""),
                )
                if isinstance(session_state, dict):
                    self._sync_candidate_prescreen_from_state(
                        job=job,
                        candidate_id=candidate_id,
                        conversation_id=conversation_id,
                        state=session_state,
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

            message = self._compose_linkedin_outreach_message(
                job=job,
                candidate=candidate,
                language=language,
                fallback_message=message,
                request_resume=request_resume,
                state=session_state,
            )
            if started_pre_resume_session and pre_resume_session_id and isinstance(session_state, dict):
                self.db.insert_pre_resume_event(
                    session_id=pre_resume_session_id,
                    conversation_id=conversation_id,
                    event_type="session_started",
                    intent="started",
                    inbound_text=None,
                    outbound_text=message,
                    state_status=session_state.get("status"),
                        details={"job_id": job_id, "candidate_id": candidate_id, "source": "outreach"},
                )

            conversation = self.db.get_conversation(conversation_id)
            delivery_mode = self._determine_initial_outreach_delivery_mode(
                action_type="outreach_initial",
                conversation=conversation,
            )
            planned_action_kind = self._planned_action_kind_for_delivery_mode(delivery_mode)
            priority = self._outreach_priority(match=match)
            action_id, assigned_account_id = self._queue_managed_outbound_action(
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                action_type="outreach_initial",
                priority=priority,
                payload={
                    "language": language,
                    "message": message,
                    "request_resume": bool(request_resume),
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                    "delivery_mode": delivery_mode,
                    "planned_action_kind": planned_action_kind,
                    "allow_forced_test_candidate": bool(forced_only),
                },
                selection_state=selection_state,
            )
            queued_action_ids.append(action_id)
            out_items.append(
                {
                    "candidate_id": candidate_id,
                    "conversation_id": conversation_id,
                    "language": language,
                    "delivery_status": "queued",
                    "request_resume": request_resume,
                    "screening_status": screening_status or None,
                    "pre_resume_session_id": pre_resume_session_id,
                    "delivery_mode": delivery_mode,
                    "planned_action_kind": planned_action_kind,
                    "action_id": action_id,
                    "linkedin_account_id": assigned_account_id,
                }
            )
            conversation_ids.append(conversation_id)

        dispatch_inline = self.managed_linkedin_dispatch_inline if dispatch_inline_override is None else bool(dispatch_inline_override)
        sent = 0
        pending_connection = 0
        dispatch_error = None
        if dispatch_inline and queued_action_ids:
            try:
                dispatched = self.dispatch_outbound_actions(limit=len(queued_action_ids), action_ids=queued_action_ids, job_id=job_id)
            except Exception as exc:
                dispatch_error = str(exc)
                self.db.log_operation(
                    operation="agent.outreach.dispatch_inline",
                    status="error",
                    entity_type="job",
                    entity_id=str(job_id),
                    details={"error": dispatch_error, "queued_action_ids": queued_action_ids[:20]},
                )
            else:
                sent = int(dispatched.get("sent") or 0)
                pending_connection = int(dispatched.get("pending_connection") or 0)
                failed += int(dispatched.get("failed") or 0)
                by_action_id = {
                    int(item.get("action_id") or 0): item
                    for item in (dispatched.get("items") or [])
                    if isinstance(item, dict) and int(item.get("action_id") or 0) > 0
                }
                for item in out_items:
                    action_id = int(item.get("action_id") or 0)
                    result = by_action_id.get(action_id)
                    if not result:
                        continue
                    item["delivery_status"] = result.get("delivery_status") or item.get("delivery_status")
                    item["delivery"] = result.get("delivery")
                    item["connect_request"] = result.get("connect_request")
                    item["external_chat_id"] = result.get("external_chat_id")
                    item["chat_binding"] = result.get("chat_binding")
                    item["linkedin_account_id"] = result.get("linkedin_account_id")
        if dispatch_error:
            for item in out_items:
                item["dispatch_error"] = dispatch_error
        return {
            "job_id": job_id,
            "items": out_items,
            "conversation_ids": conversation_ids,
            "sent": sent,
            "pending_connection": pending_connection,
            "failed": failed,
            "queued": len(queued_action_ids),
            "dispatch_error": dispatch_error,
            "test_filter_skipped": test_filter_skipped,
            "test_job_forced_only_active": forced_only,
            "test_mode_requested": test_mode,
            "total": len(out_items),
            "instruction": self.stage_instructions.get("outreach", ""),
        }

    def _outreach_candidates_direct(self, job_id: int, candidate_ids: List[int], test_mode: bool | None = None) -> Dict[str, Any]:
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
            if self._is_non_test_forced_candidate(
                candidate=candidate,
                match=match,
                forced_identifiers=forced_lookup,
                forced_only=forced_only,
            ):
                self.db.log_operation(
                    operation="agent.outreach.production_filter_skip",
                    status="skipped",
                    entity_type="candidate",
                    entity_id=str(candidate_id),
                    details={
                        "job_id": job_id,
                        "reason": "forced_test_candidate_excluded",
                    },
                )
                continue
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
            language = resolve_outbound_language(candidate, fallback="en")
            message = ""
            session_state: Dict[str, Any] | None = None
            started_pre_resume_session = False
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
                        language=resolve_outbound_language(candidate, fallback="en"),
                        job_location=str(job.get("location") or "").strip() or None,
                        salary_min=self._safe_float(job.get("salary_min"), None),
                        salary_max=self._safe_float(job.get("salary_max"), None),
                        salary_currency=str(job.get("salary_currency") or "").strip() or None,
                        work_authorization_required=bool(job.get("work_authorization_required")),
                    )
                    session_state = started["state"]
                    language = str(session_state.get("language") or "en")
                    message = str(started.get("outbound") or "")
                    started_pre_resume_session = True
                self.db.upsert_pre_resume_session(
                    session_id=pre_resume_session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=session_state,
                    instruction=self.stage_instructions.get("pre_resume", ""),
                )
                if isinstance(session_state, dict):
                    self._sync_candidate_prescreen_from_state(
                        job=job,
                        candidate_id=candidate_id,
                        conversation_id=conversation_id,
                        state=session_state,
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

            message = self._compose_linkedin_outreach_message(
                job=job,
                candidate=candidate,
                language=language,
                fallback_message=message,
                request_resume=request_resume,
                state=session_state,
            )
            if started_pre_resume_session and pre_resume_session_id and isinstance(session_state, dict):
                self.db.insert_pre_resume_event(
                    session_id=pre_resume_session_id,
                    conversation_id=conversation_id,
                    event_type="session_started",
                    intent="started",
                    inbound_text=None,
                    outbound_text=message,
                    state_status=session_state.get("status"),
                        details={"job_id": job_id, "candidate_id": candidate_id, "source": "outreach"},
                )

            conversation = self.db.get_conversation(conversation_id)
            delivery_mode = self._determine_initial_outreach_delivery_mode(
                action_type="outreach_initial",
                conversation=conversation,
            )
            planned_action_kind = self._planned_action_kind_for_delivery_mode(delivery_mode)
            connect_request = None
            delivery_status = "failed"
            if delivery_mode == "connect_first":
                delivery = {"sent": False, "provider": "linkedin", "reason": "connect_first"}
            else:
                try:
                    delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
                except Exception as exc:
                    delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}

            if delivery_mode != "connect_first" and delivery.get("sent"):
                sent += 1
                delivery_status = "sent"
                self.db.update_conversation_status(conversation_id=conversation_id, status="active")
                self.db.update_candidate_match_status(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    status="outreach_sent",
                    extra_notes={"outreach_state": "sent"},
                )
            elif delivery_mode == "connect_first" or self._is_connection_required_error(delivery):
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
                elif str(connect_request.get("reason") or "").strip().lower() == "connection_request_not_supported":
                    try:
                        delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
                    except Exception as exc:
                        delivery = {"sent": False, "provider": "linkedin", "error": str(exc)}
                    if delivery.get("sent"):
                        sent += 1
                        delivery_status = "sent"
                        connect_request = None
                        self.db.update_conversation_status(conversation_id=conversation_id, status="active")
                        self.db.update_candidate_match_status(
                            job_id=job_id,
                            candidate_id=candidate_id,
                            status="outreach_sent",
                            extra_notes={
                                "outreach_state": "sent",
                                "delivery_fallback": "message_without_connect",
                            },
                        )
                    else:
                        failed += 1
                        self.db.log_operation(
                            operation="agent.outreach.connect_request",
                            status="error",
                            entity_type="candidate",
                            entity_id=str(candidate_id),
                            details={
                                "job_id": job_id,
                                "connect_request": connect_request,
                                "delivery": delivery,
                                "fallback": "message_without_connect",
                            },
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
                    "delivery_mode": delivery_mode,
                    "planned_action_kind": planned_action_kind,
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
                    "delivery_mode": delivery_mode,
                    "planned_action_kind": planned_action_kind,
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
                    "delivery_mode": delivery_mode,
                    "planned_action_kind": planned_action_kind,
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
        skipped = 0
        items: List[Dict[str, Any]] = []

        for row in rows:
            checked += 1
            conversation_id = int(row["conversation_id"])
            candidate_id = int(row["candidate_id"])
            job = self.db.get_job(int(row["job_id"]))
            if not job:
                failed += 1
                items.append({"conversation_id": conversation_id, "status": "job_missing"})
                continue
            if self._job_is_paused(job):
                skipped += 1
                items.append(
                    {
                        "conversation_id": conversation_id,
                        "candidate_id": candidate_id,
                        "status": "job_paused",
                    }
                )
                continue
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
                "skipped": skipped,
            },
        )

        return {
            "job_id": job_id,
            "checked": checked,
            "connected": connected,
            "sent": sent,
            "still_waiting": still_waiting,
            "failed": failed,
            "skipped": skipped,
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
        self._assert_job_automation_allowed(job, operation="add_manual_test_account")
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
        started_pre_resume_session = False

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
                    job_location=str(job.get("location") or "").strip() or None,
                    salary_min=self._safe_float(job.get("salary_min"), None),
                    salary_max=self._safe_float(job.get("salary_max"), None),
                    salary_currency=str(job.get("salary_currency") or "").strip() or None,
                    work_authorization_required=bool(job.get("work_authorization_required")),
                )
                state = started.get("state") if isinstance(started.get("state"), dict) else None
                initial_outbound = str(started.get("outbound") or "").strip()
                started_pre_resume_session = bool(state)
            if state:
                self.db.upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=state,
                    instruction=self.stage_instructions.get("pre_resume", ""),
                )
                self._sync_candidate_prescreen_from_state(
                    job=job,
                    candidate_id=candidate_id,
                    conversation_id=conversation_id,
                    state=state,
                )

        if not initial_outbound:
            _, initial_outbound = self.outreach_agent.compose_screening_message(
                job=job,
                candidate=profile,
                request_resume=True,
            )
        initial_outbound = self._compose_linkedin_outreach_message(
            job=job,
            candidate=profile,
            language=normalized_lang,
            fallback_message=initial_outbound,
            request_resume=True,
            state=state,
        )
        if started_pre_resume_session and state:
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
        self._assert_job_automation_allowed(job, operation="execute_job_workflow")
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
        self._reset_step_progress(job_id=job_id, steps=["source", "enrich", "verify", "add", "outreach", "workflow"])
        self._persist_step_progress(job_id=job_id, step="workflow", status="running", output={"limit": limit, "test_mode_requested": test_mode})

        steps_order = ["source", "enrich", "verify", "add", "outreach"]
        source_result: Dict[str, Any] = {}
        enrich_result: Dict[str, Any] = {}
        verify_result: Dict[str, Any] = {}
        add_result: Dict[str, Any] = {}
        outreach_result: Dict[str, Any] = {}
        current_step = "source"

        try:
            self._persist_step_progress(job_id=job_id, step="source", status="running", output={})
            source_result = self.source_candidates(job_id=job_id, limit=limit, test_mode=effective_test_mode)
            self._persist_step_progress(job_id=job_id, step="source", status="success", output=source_result)

            current_step = "enrich"
            self._persist_step_progress(job_id=job_id, step="enrich", status="running", output={})
            enrich_result = self.enrich_profiles(job_id=job_id, profiles=source_result["profiles"])
            self._persist_step_progress(job_id=job_id, step="enrich", status="success", output=enrich_result)

            current_step = "verify"
            self._persist_step_progress(job_id=job_id, step="verify", status="running", output={})
            verify_result = self._verify_enriched_profiles(
                job_id=job_id,
                enriched_profiles=enrich_result["profiles"],
                enrich_result=enrich_result,
            )
            self._persist_step_progress(job_id=job_id, step="verify", status="success", output=verify_result)

            if self.contact_all_mode:
                eligible_items = [item for item in verify_result["items"] if item.get("status") in {"verified", "needs_resume"}]
            else:
                eligible_items = [item for item in verify_result["items"] if item.get("status") == "verified"]

            current_step = "add"
            self._persist_step_progress(job_id=job_id, step="add", status="running", output={})
            add_result = self.add_verified_candidates(job_id=job_id, verified_items=eligible_items)
            self._persist_step_progress(job_id=job_id, step="add", status="success", output=add_result)

            current_step = "outreach"
            self._persist_step_progress(job_id=job_id, step="outreach", status="running", output={})
            outreach_result = self.outreach_candidates(
                job_id=job_id,
                candidate_ids=[x["candidate_id"] for x in add_result["added"]],
                test_mode=effective_test_mode,
            )
            outreach_status = (
                "error"
                if outreach_result["failed"] > 0
                and outreach_result["sent"] == 0
                and outreach_result.get("pending_connection", 0) == 0
                else "success"
            )
            self._persist_step_progress(job_id=job_id, step="outreach", status=outreach_status, output=outreach_result)
        except Exception as exc:
            self._persist_step_progress(job_id=job_id, step=current_step, status="error", output={"error": str(exc)})
            current_index = steps_order.index(current_step) if current_step in steps_order else -1
            for step in steps_order[current_index + 1 :]:
                self._persist_step_progress(
                    job_id=job_id,
                    step=step,
                    status="skipped",
                    output={"reason": "upstream_step_failed", "failed_step": current_step},
                )
            self._persist_step_progress(job_id=job_id, step="workflow", status="error", output={"error": str(exc), "failed_step": current_step})
            raise

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
        self._persist_step_progress(
            job_id=job_id,
            step="workflow",
            status="success",
            output={
                "job_id": summary.job_id,
                "searched": summary.searched,
                "verified": summary.verified,
                "needs_resume": summary.needs_resume,
                "rejected": summary.rejected,
                "outreached": summary.outreached,
                "outreach_sent": summary.outreach_sent,
                "outreach_pending_connection": summary.outreach_pending_connection,
                "outreach_failed": summary.outreach_failed,
                "conversation_ids": summary.conversation_ids,
                "test_mode_requested": test_mode,
            },
        )
        return summary

    def process_inbound_message(
        self,
        conversation_id: int,
        text: str,
        inbound_meta: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
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
        pre_resume = self.db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)

        messages = self.db.list_messages(conversation_id)
        previous_lang = None
        for item in reversed(messages):
            if item.get("candidate_language"):
                previous_lang = item["candidate_language"]
                break
        llm_history = self._build_llm_history(messages=messages, latest_inbound=text)

        normalized_meta = self._normalize_inbound_meta(inbound_meta)
        capture_meta = inbound_meta if isinstance(inbound_meta, dict) else normalized_meta
        extraction_mode = "pre_resume" if pre_resume and self.pre_resume_service is not None else "faq"
        extraction_state = pre_resume.get("state_json") if isinstance((pre_resume or {}).get("state_json"), dict) else None
        extraction_attachments = extract_attachment_descriptors_from_values([capture_meta or {}, normalized_meta or {}], limit=8)
        extraction = self.message_extraction_service.extract(
            mode=extraction_mode,
            inbound_text=text,
            history=llm_history,
            candidate=candidate,
            job=job,
            state=extraction_state,
            attachments=extraction_attachments,
            previous_language=previous_lang,
            fallback_language="en",
            instruction=self.stage_instructions.get(extraction_mode, ""),
        )
        inbound_language = normalize_language(
            extraction.language,
            fallback=resolve_conversation_language(
                latest_message_text=text,
                previous_language=previous_lang,
                profile_languages=candidate.get("languages"),
                fallback="en",
            ),
        ) or "en"
        message_meta = dict(normalized_meta)
        message_meta["extraction"] = extraction.to_dict()
        inbound_id = self.db.add_message(
            conversation_id=conversation_id,
            direction="inbound",
            content=text,
            candidate_language=inbound_language,
            meta=message_meta,
        )
        self._capture_resume_assets_from_inbound(
            job=job,
            candidate=candidate,
            conversation=conversation,
            inbound_message_id=inbound_id,
            inbound_text=text,
            inbound_meta=capture_meta,
        )
        self.db.log_operation(
            operation="conversation.inbound.received",
            status="ok",
            entity_type="message",
            entity_id=str(inbound_id),
            details={"conversation_id": conversation_id},
        )
        account_id = int(conversation.get("linkedin_account_id") or 0)
        self._record_outreach_account_event(
            event_key=f"message:{inbound_id}:reply_received",
            account_id=account_id,
            event_type="reply_received",
            job_id=int(conversation["job_id"]),
            candidate_id=int(conversation["candidate_id"]),
            conversation_id=conversation_id,
            details={"message_id": inbound_id},
        )
        if str(conversation.get("status") or "").strip().lower() == "waiting_connection":
            self._record_outreach_account_event(
                event_key=f"conversation:{conversation_id}:connect_accepted",
                account_id=account_id,
                event_type="connect_accepted",
                job_id=int(conversation["job_id"]),
                candidate_id=int(conversation["candidate_id"]),
                conversation_id=conversation_id,
                details={"source": "inbound_message", "message_id": inbound_id},
            )
        job_paused = self._job_is_paused(job)

        if pre_resume and self.pre_resume_service is not None:
            session_id = str(pre_resume.get("session_id") or "")
            state = pre_resume.get("state_json")
            if session_id and isinstance(state, dict):
                if self.pre_resume_service.get_session(session_id) is None:
                    self.pre_resume_service.seed_session(state)
                result = self.pre_resume_service.handle_inbound(session_id=session_id, text=text, extraction=extraction)
                state_out = result.get("state") if isinstance(result.get("state"), dict) else state
                outbound = str(result.get("outbound") or "").strip()
                intent = str(extraction.intent or result.get("intent") or "default")
                language = normalize_language(
                    extraction.language or str((state_out or {}).get("language") or inbound_language or ""),
                    fallback="en",
                )
                if isinstance(state_out, dict):
                    state_out["language"] = language
                    state_out["last_extraction"] = extraction.to_dict()
                if not job_paused:
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
                if isinstance(state_out, dict):
                    self.db.upsert_pre_resume_session(
                        session_id=session_id,
                        conversation_id=conversation_id,
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        state=state_out,
                        instruction=self.stage_instructions.get("pre_resume", ""),
                    )
                    self.pre_resume_service.seed_session(state_out)
                    self._sync_candidate_prescreen_from_state(
                        job=job,
                        candidate_id=int(conversation["candidate_id"]),
                        conversation_id=conversation_id,
                        state=state_out,
                    )
                state_status = str((state_out or {}).get("status") or "").strip().lower()
                current_match_status = str((match or {}).get("status") or "").strip().lower()
                if current_match_status in {
                    "verified",
                    "needs_resume",
                    "outreach_pending_connection",
                    "outreach_sent",
                    "outreached",
                }:
                    self.db.update_conversation_status(conversation_id=conversation_id, status="active")
                    self.db.update_candidate_match_status(
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        status="in_dialogue",
                        extra_notes={"last_candidate_reply_at": utc_now_iso()},
                    )
                    if isinstance(match, dict):
                        match["status"] = "in_dialogue"
                interview_result: Dict[str, Any] | None = None
                prescreen_status = str((state_out or {}).get("prescreen_status") or "").strip().lower()
                should_attempt_interview = prescreen_status == "ready_for_interview"
                if should_attempt_interview and not job_paused:
                    interview_result = self._send_interview_invite(
                        job=job,
                        candidate=candidate,
                        conversation=conversation,
                        language=language,
                        match=match,
                    )
                    if isinstance(state_out, dict):
                        state_out["awaiting_pre_vetting_opt_in"] = False
                    if isinstance(interview_result, dict) and interview_result.get("started"):
                        outbound = str(interview_result.get("message") or "").strip() or outbound
                    if isinstance(state_out, dict):
                        self.db.upsert_pre_resume_session(
                            session_id=session_id,
                            conversation_id=conversation_id,
                            job_id=int(conversation["job_id"]),
                            candidate_id=int(conversation["candidate_id"]),
                            state=state_out,
                            instruction=self.stage_instructions.get("pre_resume", ""),
                        )
                        self.pre_resume_service.seed_session(state_out)

                self.db.insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    event_type="inbound_processed",
                    intent=intent,
                    inbound_text=text,
                    outbound_text=outbound or None,
                    state_status=(state_out or {}).get("status"),
                    details={
                        "result_event": result.get("event"),
                        "prescreen_status": (state_out or {}).get("prescreen_status"),
                        "extraction": extraction.to_dict(),
                    },
                )

                if (
                    outbound
                    and not job_paused
                    and not (isinstance(interview_result, dict) and interview_result.get("started"))
                ):
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

                prescreen_match_status = self._match_status_from_prescreen_status(prescreen_status)
                effective_match_status = str((match or {}).get("status") or current_match_status or "").strip().lower()
                if prescreen_match_status and not (
                    isinstance(interview_result, dict) and interview_result.get("started")
                ):
                    self.db.update_candidate_match_status(
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        status=prescreen_match_status,
                        extra_notes=(
                            {"prescreen_status": prescreen_status or None}
                            if prescreen_match_status == "must_have_approved"
                            else {
                                "resume_received_at": (state_out or {}).get("updated_at"),
                                "prescreen_status": prescreen_status or None,
                            }
                        ),
                    )
                    if isinstance(match, dict):
                        match["status"] = prescreen_match_status
                    if prescreen_match_status == "must_have_approved":
                        self.db.log_operation(
                            operation="candidate.prescreen.must_have_approved",
                            status="ok",
                            entity_type="candidate",
                            entity_id=str(conversation["candidate_id"]),
                            details={"conversation_id": conversation_id, "session_id": session_id},
                        )
                    elif prescreen_status == "ready_for_interview":
                        self.db.log_operation(
                            operation="candidate.prescreen.completed",
                            status="ok",
                            entity_type="candidate",
                            entity_id=str(conversation["candidate_id"]),
                            details={"conversation_id": conversation_id, "session_id": session_id},
                        )
                    else:
                        self.db.log_operation(
                            operation="candidate.resume.received",
                            status="ok",
                            entity_type="candidate",
                            entity_id=str(conversation["candidate_id"]),
                            details={"conversation_id": conversation_id, "session_id": session_id},
                        )
                    should_record_resume_received = prescreen_match_status == "resume_received_pending_must_have" or (
                        prescreen_match_status == "resume_received"
                        and effective_match_status not in {"resume_received_pending_must_have", "resume_received"}
                    )
                    if should_record_resume_received:
                        self._record_outreach_account_event(
                            event_key=f"message:{inbound_id}:resume_received",
                            account_id=account_id,
                            event_type="resume_received",
                            job_id=int(conversation["job_id"]),
                            candidate_id=int(conversation["candidate_id"]),
                            conversation_id=conversation_id,
                            details={"message_id": inbound_id, "session_id": session_id},
                        )
                elif state_status == "not_interested":
                    self.db.update_candidate_match_status(
                        job_id=int(conversation["job_id"]),
                        candidate_id=int(conversation["candidate_id"]),
                        status="rejected",
                        extra_notes={"rejected_at": utc_now_iso(), "rejection_reason": "candidate_not_interested"},
                    )
                self._record_communication_dialogue_assessment(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    mode="pre_resume",
                    intent=intent,
                    state=state_out if isinstance(state_out, dict) else None,
                    inbound_text=text,
                )

                response = {
                    "language": language,
                    "intent": intent,
                    "reply": "" if job_paused else outbound,
                    "mode": "paused" if job_paused else "pre_resume",
                    "state": self._public_pre_resume_state(state_out),
                }
                if job_paused:
                    response["job_paused"] = True
                if interview_result is not None:
                    response["interview"] = interview_result
                return response

        lang, intent, reply = self.faq_agent.auto_reply(
            inbound_text=text,
            job=job,
            candidate_lang=inbound_language,
            extracted_language=extraction.language,
            extracted_intent=extraction.intent,
        )
        if job_paused:
            self._record_communication_dialogue_assessment(
                job_id=int(conversation["job_id"]),
                candidate_id=int(conversation["candidate_id"]),
                mode="paused",
                intent=intent,
                state=None,
                inbound_text=text,
            )
            return {"language": lang, "intent": intent, "reply": "", "mode": "paused", "job_paused": True}
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

    def _sync_candidate_prescreen_from_state(
        self,
        *,
        job: Dict[str, Any],
        candidate_id: int,
        conversation_id: int,
        state: Dict[str, Any] | None,
    ) -> None:
        state_payload = state if isinstance(state, dict) else {}
        if not state_payload:
            return
        must_have_answer = str(state_payload.get("must_have_answer") or "").strip()
        must_have_answers = [{"question": "must_have_experience", "answer": must_have_answer}] if must_have_answer else []
        salary_currency = str(
            state_payload.get("salary_expectation_currency") or state_payload.get("salary_currency") or job.get("salary_currency") or ""
        ).strip().upper() or None
        self.db.upsert_candidate_prescreen(
            job_id=int(job.get("id") or job.get("job_id") or 0),
            candidate_id=int(candidate_id),
            conversation_id=int(conversation_id),
            status=str(state_payload.get("prescreen_status") or state_payload.get("status") or "incomplete"),
            must_have_answers_json=must_have_answers,
            salary_expectation_min=self._safe_float(state_payload.get("salary_expectation_min"), None),
            salary_expectation_max=self._safe_float(state_payload.get("salary_expectation_max"), None),
            salary_expectation_currency=salary_currency,
            location_confirmed=self._safe_bool(state_payload.get("location_confirmed"), None),
            work_authorization_confirmed=self._safe_bool(state_payload.get("work_authorization_confirmed"), None),
            cv_received=bool(state_payload.get("cv_received")) or bool(state_payload.get("resume_links")),
            summary=(
                "Written prescreen complete. Ready for interview."
                if str(state_payload.get("prescreen_status") or "").strip().lower() == "ready_for_interview"
                else None
            ),
            notes=None,
            updated_at=str(state_payload.get("updated_at") or utc_now_iso()),
        )

    def _normalize_inbound_meta(self, inbound_meta: Dict[str, Any] | None) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": "candidate_message"}
        if not isinstance(inbound_meta, dict):
            return out
        for key in ("provider", "provider_message_id", "occurred_at", "event_type", "event_id"):
            value = inbound_meta.get(key)
            if isinstance(value, str) and value.strip():
                out[key] = value.strip()
        descriptors = self._extract_attachment_descriptors_from_inbound_meta(inbound_meta, limit=12)
        if descriptors:
            out["attachments"] = [item.to_dict() for item in descriptors]
        return out

    def _capture_resume_assets_from_inbound(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        conversation: Dict[str, Any],
        inbound_message_id: int,
        inbound_text: str,
        inbound_meta: Dict[str, Any] | None,
    ) -> None:
        try:
            job_id = int(job.get("id") or conversation.get("job_id") or 0)
            candidate_id = int(candidate.get("id") or conversation.get("candidate_id") or 0)
            conversation_id = int(conversation.get("id") or 0)
            if job_id <= 0 or candidate_id <= 0 or conversation_id <= 0:
                return

            descriptors = self._extract_attachment_descriptors_from_inbound_meta(inbound_meta, limit=16)
            links = parse_resume_links(inbound_text)
            for url in extract_resume_urls(descriptors):
                if url not in links:
                    links.append(url)

            if not links and not any(self._is_resume_like_descriptor(item) for item in descriptors):
                return

            provider = str((inbound_meta or {}).get("provider") or "linkedin").strip().lower() or "linkedin"
            provider_message_id = str((inbound_meta or {}).get("provider_message_id") or "").strip() or None
            observed_at = str((inbound_meta or {}).get("occurred_at") or "").strip() or utc_now_iso()

            for idx, url in enumerate(links):
                normalized_url = str(url or "").strip()
                if not normalized_url:
                    continue
                source_id = f"link:{hashlib.sha1(normalized_url.encode('utf-8')).hexdigest()}"
                self._store_resume_asset(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    conversation_id=conversation_id,
                    source_type="message_link",
                    source_id=source_id,
                    provider=provider,
                    provider_message_id=provider_message_id,
                    file_name=None,
                    mime_type=None,
                    file_size_bytes=None,
                    remote_url=normalized_url,
                    attachment_provider_file_id=None,
                    observed_at=observed_at,
                    inbound_message_id=inbound_message_id,
                    rank=idx,
                )

            rank_base = len(links)
            for idx, descriptor in enumerate(descriptors):
                if not self._is_resume_like_descriptor(descriptor):
                    continue
                name = str(descriptor.name or "").strip() or None
                url = str(descriptor.url or "").strip() or None
                source_identity = str(descriptor.provider_file_id or "").strip()
                if not source_identity:
                    source_identity = hashlib.sha1(
                        f"{provider_message_id or ''}|{name or ''}|{url or ''}|{idx}".encode("utf-8")
                    ).hexdigest()
                source_id = f"attachment:{source_identity}"
                self._store_resume_asset(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    conversation_id=conversation_id,
                    source_type="message_attachment",
                    source_id=source_id,
                    provider=provider,
                    provider_message_id=provider_message_id,
                    file_name=name,
                    mime_type=str(descriptor.mime_type or "").strip().lower() or None,
                    file_size_bytes=descriptor.size_bytes,
                    remote_url=url,
                    attachment_provider_file_id=source_identity if str(source_identity).strip() else None,
                    observed_at=observed_at,
                    inbound_message_id=inbound_message_id,
                    rank=rank_base + idx,
                )
        except Exception as exc:
            self.db.log_operation(
                operation="candidate.resume.capture",
                status="error",
                entity_type="message",
                entity_id=str(inbound_message_id),
                details={"error": str(exc)},
            )

    def _store_resume_asset(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        source_type: str,
        source_id: str,
        provider: str,
        provider_message_id: str | None,
        file_name: str | None,
        mime_type: str | None,
        file_size_bytes: int | None,
        remote_url: str | None,
        attachment_provider_file_id: str | None,
        observed_at: str,
        inbound_message_id: int,
        rank: int,
    ) -> None:
        asset_key = self._resume_asset_key(
            job_id=job_id,
            candidate_id=candidate_id,
            source_type=source_type,
            source_id=source_id,
            remote_url=remote_url,
            file_name=file_name,
        )
        status = "received"
        storage_path: str | None = None
        content_sha256: str | None = None
        extracted_text: str | None = None
        parsed_payload: Dict[str, Any] = {}
        processing_error: str | None = None
        resolved_mime = str(mime_type or "").strip().lower() or None
        resolved_size = file_size_bytes

        if (
            remote_url
            and remote_url.startswith("att://")
            and provider in {"unipile", "unipile_poll"}
            and provider_message_id
            and attachment_provider_file_id
        ):
            try:
                payload, downloaded_mime = self._download_provider_attachment_payload(
                    provider=provider,
                    provider_message_id=provider_message_id,
                    attachment_id=attachment_provider_file_id,
                )
                if not resolved_mime:
                    resolved_mime = downloaded_mime
                resolved_size = len(payload)
                storage_path, content_sha256 = self._persist_resume_payload(
                    candidate_id=candidate_id,
                    job_id=job_id,
                    asset_key=asset_key,
                    file_name=file_name,
                    mime_type=resolved_mime,
                    payload=payload,
                )
                extracted_text, extractor_hint, parse_error = self._extract_resume_text(
                    payload=payload,
                    file_name=file_name,
                    mime_type=resolved_mime,
                )
                parsed_payload = {
                    "extractor": extractor_hint,
                    "text_length": len(extracted_text or ""),
                    "parse_error": parse_error,
                    "download_source": "provider_attachment",
                }
                if extracted_text:
                    status = "processed"
                else:
                    status = "stored_unparsed"
                    processing_error = parse_error or "resume_text_not_extracted"
            except Exception as exc:
                status = "download_failed"
                processing_error = str(exc)
        elif remote_url and self._skip_remote_resume_fetch(remote_url):
            status = "stored_unparsed"
            processing_error = "remote_fetch_skipped_for_mock_url"
        elif remote_url:
            try:
                payload, downloaded_mime = self._download_resume_payload(remote_url)
                if not resolved_mime:
                    resolved_mime = downloaded_mime
                resolved_size = len(payload)
                storage_path, content_sha256 = self._persist_resume_payload(
                    candidate_id=candidate_id,
                    job_id=job_id,
                    asset_key=asset_key,
                    file_name=file_name,
                    mime_type=resolved_mime,
                    payload=payload,
                )
                extracted_text, extractor_hint, parse_error = self._extract_resume_text(
                    payload=payload,
                    file_name=file_name,
                    mime_type=resolved_mime,
                )
                parsed_payload = {
                    "extractor": extractor_hint,
                    "text_length": len(extracted_text or ""),
                    "parse_error": parse_error,
                }
                if extracted_text:
                    status = "processed"
                else:
                    status = "stored_unparsed"
                    processing_error = parse_error or "resume_text_not_extracted"
            except Exception as exc:
                status = "download_failed"
                processing_error = str(exc)
        else:
            status = "received_no_url"
            processing_error = "missing_remote_url"

        resume_asset_id = self.db.upsert_resume_asset(
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            source_type=source_type,
            source_id=source_id,
            provider=provider,
            provider_message_id=provider_message_id,
            file_name=file_name,
            mime_type=resolved_mime,
            file_size_bytes=resolved_size,
            remote_url=remote_url,
            storage_path=storage_path,
            content_sha256=content_sha256,
            processing_status=status,
            processing_error=processing_error,
            extracted_text=extracted_text,
            parsed_json=parsed_payload,
            observed_at=observed_at,
            asset_key=asset_key,
        )

        signal_type = "resume_parsed" if status == "processed" else "resume_received"
        signal_title = "Resume parsed and stored" if status == "processed" else "Resume received"
        signal_detail = f"status={status}; source_type={source_type}; source_id={source_id}"
        if file_name:
            signal_detail = f"{signal_detail}; file_name={file_name}"
        self.db.upsert_candidate_signal(
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            source_type="resume_asset",
            source_id=asset_key,
            signal_type=signal_type,
            signal_category="resume",
            title=signal_title,
            detail=signal_detail,
            impact_score=2.0 if status == "processed" else (1.0 if "received" in status else 0.2),
            confidence=0.9 if status == "processed" else 0.7,
            observed_at=observed_at,
            signal_meta={
                "resume_asset_id": resume_asset_id,
                "status": status,
                "provider": provider,
                "provider_message_id": provider_message_id,
                "attachment_provider_file_id": attachment_provider_file_id,
                "file_name": file_name,
                "remote_url": remote_url,
                "storage_path": storage_path,
                "content_sha256": content_sha256,
                "rank": int(rank),
                "inbound_message_id": int(inbound_message_id),
                "parse": parsed_payload,
            },
        )
        self.db.log_operation(
            operation="candidate.resume.asset",
            status="ok" if status in {"processed", "stored_unparsed", "received_no_url", "received"} else "error",
            entity_type="candidate",
            entity_id=str(candidate_id),
            details={
                "job_id": job_id,
                "conversation_id": conversation_id,
                "resume_asset_id": resume_asset_id,
                "asset_key": asset_key,
                "status": status,
                "source_type": source_type,
                "source_id": source_id,
                "attachment_provider_file_id": attachment_provider_file_id,
                "error": processing_error,
            },
        )

    def _download_provider_attachment_payload(
        self,
        *,
        provider: str,
        provider_message_id: str,
        attachment_id: str,
    ) -> tuple[bytes, str | None]:
        normalized_provider = str(provider or "").strip().lower()
        if normalized_provider not in {"unipile", "unipile_poll"}:
            raise RuntimeError("provider_attachment_download_unsupported")
        api_key = str(self.managed_unipile_api_key or "").strip()
        if not api_key:
            raise RuntimeError("managed_unipile_api_key_missing")
        base_url = str(self.managed_unipile_base_url or "https://api.unipile.com").strip().rstrip("/")
        message_id = str(provider_message_id or "").strip()
        resolved_attachment_id = str(attachment_id or "").strip()
        if not message_id or not resolved_attachment_id:
            raise RuntimeError("provider_attachment_identifiers_missing")
        url = f"{base_url}/api/v1/messages/{message_id}/attachments/{resolved_attachment_id}"
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={
                "X-API-KEY": api_key,
                "Accept": "*/*",
            },
        )
        timeout = max(5, int(self.managed_unipile_timeout_seconds or 30))
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            payload = resp.read((10 * 1024 * 1024) + 1)
            if len(payload) > 10 * 1024 * 1024:
                raise RuntimeError("provider_attachment_too_large")
            content_type = str(resp.headers.get("Content-Type") or "").strip().lower()
            if ";" in content_type:
                content_type = content_type.split(";", 1)[0].strip()
            return payload, (content_type or None)

    @staticmethod
    def _resume_asset_key(
        *,
        job_id: int,
        candidate_id: int,
        source_type: str,
        source_id: str,
        remote_url: str | None,
        file_name: str | None,
    ) -> str:
        fingerprint = str(remote_url or "").strip().lower() or str(file_name or "").strip().lower() or source_id
        digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
        return f"{int(job_id)}:{int(candidate_id)}:{str(source_type).strip().lower()}:{digest}"

    @staticmethod
    def _skip_remote_resume_fetch(remote_url: str) -> bool:
        allow_mock_fetch = str(os.environ.get("TENER_RESUME_FETCH_MOCK_URLS") or "").strip().lower() in {"1", "true", "yes", "on"}
        if allow_mock_fetch:
            return False
        lowered = str(remote_url or "").strip().lower()
        return ".example.com/" in lowered or lowered.startswith("https://example.com/") or lowered.startswith("http://example.com/")

    def _download_resume_payload(self, remote_url: str) -> tuple[bytes, str | None]:
        max_bytes_raw = os.environ.get("TENER_RESUME_MAX_BYTES", str(10 * 1024 * 1024))
        try:
            max_bytes = max(128 * 1024, min(int(max_bytes_raw), 25 * 1024 * 1024))
        except ValueError:
            max_bytes = 10 * 1024 * 1024
        req = urlrequest.Request(
            url=remote_url,
            method="GET",
            headers={"User-Agent": "TenerResumeIngest/1.0", "Accept": "*/*"},
        )
        with urlrequest.urlopen(req, timeout=20) as resp:
            payload = resp.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise RuntimeError("resume_content_too_large")
            content_type = str(resp.headers.get("Content-Type") or "").strip().lower()
            if ";" in content_type:
                content_type = content_type.split(";", 1)[0].strip()
            return payload, (content_type or None)

    def _persist_resume_payload(
        self,
        *,
        candidate_id: int,
        job_id: int,
        asset_key: str,
        file_name: str | None,
        mime_type: str | None,
        payload: bytes,
    ) -> tuple[str, str]:
        digest = hashlib.sha256(payload).hexdigest()
        root_raw = str(os.environ.get("TENER_RESUME_STORAGE_DIR") or "data/resumes").strip()
        root = Path(root_raw)
        if not root.is_absolute():
            root = Path.cwd() / root
        target_dir = root / f"job_{int(job_id)}" / f"candidate_{int(candidate_id)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        ext = self._guess_resume_extension(file_name=file_name, mime_type=mime_type)
        file_path = target_dir / f"{asset_key}{ext}"
        file_path.write_bytes(payload)
        return str(file_path), digest

    @staticmethod
    def _guess_resume_extension(file_name: str | None, mime_type: str | None) -> str:
        lower_name = str(file_name or "").strip().lower()
        if "." in lower_name:
            suffix = lower_name.rsplit(".", 1)[-1]
            if suffix:
                return f".{suffix}"
        normalized_mime = str(mime_type or "").strip().lower()
        mapping = {
            "application/pdf": ".pdf",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
            "text/plain": ".txt",
            "application/json": ".json",
        }
        return mapping.get(normalized_mime, ".bin")

    def _extract_resume_text(
        self,
        *,
        payload: bytes,
        file_name: str | None,
        mime_type: str | None,
    ) -> tuple[str | None, str, str | None]:
        lower_name = str(file_name or "").strip().lower()
        suffix = f".{lower_name.rsplit('.', 1)[-1]}" if "." in lower_name else ""
        normalized_mime = str(mime_type or "").strip().lower()

        if normalized_mime.startswith("text/") or suffix in {".txt", ".md", ".csv", ".json"}:
            text = payload.decode("utf-8", errors="ignore").strip()
            return (text[:40000] if text else None), "plain_text", None

        if suffix == ".docx" or normalized_mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            try:
                with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                    xml = archive.read("word/document.xml").decode("utf-8", errors="ignore")
                text = re.sub(r"<[^>]+>", " ", xml)
                text = re.sub(r"\s+", " ", text).strip()
                return (text[:40000] if text else None), "docx_xml", None
            except Exception as exc:
                return None, "docx_xml", f"docx_parse_failed:{exc}"

        if suffix == ".pdf" or normalized_mime == "application/pdf":
            return None, "pdf_placeholder", "pdf_text_extraction_unavailable"

        return None, "unsupported", "unsupported_resume_format"

    @staticmethod
    def _is_resume_like_descriptor(descriptor: AttachmentDescriptor) -> bool:
        name = str(descriptor.name or "").strip()
        url = str(descriptor.url or "").strip()
        mime = str(descriptor.mime_type or "").strip().lower()
        if is_resume_like_name_or_url(name) or is_resume_like_name_or_url(url):
            return True
        if "pdf" in mime or "msword" in mime or "wordprocessingml" in mime:
            return True
        return False

    @staticmethod
    def _extract_attachment_descriptors_from_inbound_meta(
        inbound_meta: Dict[str, Any] | None,
        limit: int = 12,
    ) -> List[AttachmentDescriptor]:
        if not isinstance(inbound_meta, dict):
            return []
        values: List[Any] = []
        if isinstance(inbound_meta.get("attachments"), list):
            values.append(inbound_meta.get("attachments"))
        raw = inbound_meta.get("raw")
        if isinstance(raw, dict):
            values.append(raw)
        values.append(inbound_meta)
        return extract_attachment_descriptors_from_values(values, limit=limit)

    def process_provider_inbound_message(
        self,
        external_chat_id: str,
        text: str,
        sender_provider_id: str | None = None,
        provider_payload: Dict[str, Any] | None = None,
        provider_message_id: str | None = None,
        occurred_at: str | None = None,
    ) -> Dict[str, Any]:
        conversation = self.db.get_conversation_by_external_chat_id(external_chat_id) if external_chat_id else None
        if not conversation and sender_provider_id:
            candidate = self.db.get_candidate_by_linkedin_id(sender_provider_id)
            if candidate:
                conversation = self.db.get_latest_conversation_for_candidate(int(candidate["id"]))
        if not conversation:
            return {"processed": False, "reason": "conversation_not_found"}
        result = self.process_inbound_message(
            conversation_id=int(conversation["id"]),
            text=text,
            inbound_meta={
                "type": "candidate_message",
                "provider": "unipile",
                "provider_message_id": str(provider_message_id or "").strip() or None,
                "occurred_at": str(occurred_at or "").strip() or None,
                "attachments": self._extract_attachment_descriptors_from_provider_payload(provider_payload),
                "raw": provider_payload if isinstance(provider_payload, dict) else None,
            },
        )
        pre_resume = self.db.get_pre_resume_session_by_conversation(int(conversation["id"]))
        if isinstance(pre_resume, dict) and isinstance(pre_resume.get("state_json"), dict) and isinstance(result, dict):
            result = dict(result)
            result["state"] = dict(pre_resume.get("state_json") or {})
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
        job = self.db.get_job(int(conversation["job_id"]))
        if not job:
            return {"processed": False, "reason": "job_not_found", "conversation_id": int(conversation["id"])}
        candidate = self.db.get_candidate(int(conversation["candidate_id"]))
        if not candidate:
            return {"processed": False, "reason": "candidate_not_found", "conversation_id": int(conversation["id"])}
        if str(conversation.get("status") or "") != "waiting_connection":
            return {"processed": False, "reason": "conversation_not_waiting_connection", "conversation_id": int(conversation["id"])}
        if self._job_is_paused(job):
            return {"processed": True, "reason": "job_paused", "conversation_id": int(conversation["id"])}
        self._record_outreach_account_event(
            event_key=f"conversation:{int(conversation['id'])}:connect_accepted",
            account_id=int(conversation.get("linkedin_account_id") or 0),
            event_type="connect_accepted",
            job_id=int(conversation["job_id"]),
            candidate_id=int(conversation["candidate_id"]),
            conversation_id=int(conversation["id"]),
            details={"source": "connection_event"},
        )
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
                    text = self._extract_attachment_text_from_provider_message(message)
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
                    result = self.process_inbound_message(
                        conversation_id=conversation_id,
                        text=text,
                        inbound_meta={
                            "type": "candidate_message",
                            "provider": "unipile_poll",
                            "provider_message_id": provider_message_id or None,
                            "occurred_at": occurred_at or None,
                            "attachments": message.get("attachments") if isinstance(message.get("attachments"), list) else None,
                            "raw": message.get("raw") if isinstance(message.get("raw"), dict) else message,
                        },
                    )
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

    def backfill_resume_assets_for_conversation(self, *, conversation_id: int, per_chat_limit: int = 50) -> Dict[str, Any]:
        conversation = self.db.get_conversation(int(conversation_id))
        if not conversation:
            raise ValueError("conversation not found")
        external_chat_id = str(conversation.get("external_chat_id") or "").strip()
        if not external_chat_id:
            return {"conversation_id": int(conversation_id), "processed": 0, "reason": "external_chat_id_missing"}
        fetch_fn = getattr(self.sourcing_agent, "fetch_chat_messages", None)
        if not callable(fetch_fn):
            return {"conversation_id": int(conversation_id), "processed": 0, "reason": "provider_inbound_poll_not_supported"}

        candidate = self.db.get_candidate(int(conversation.get("candidate_id") or 0))
        job = self.db.get_job(int(conversation.get("job_id") or 0))
        if not candidate or not job:
            return {"conversation_id": int(conversation_id), "processed": 0, "reason": "conversation_context_missing"}

        safe_per_chat = max(1, min(int(per_chat_limit or 50), 200))
        messages = fetch_fn(external_chat_id, limit=safe_per_chat) or []
        stored_messages = self.db.list_messages(conversation_id=int(conversation_id))
        by_provider_message_id: Dict[str, Dict[str, Any]] = {}
        for row in stored_messages:
            meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
            provider_message_id = str(meta.get("provider_message_id") or "").strip()
            if provider_message_id:
                by_provider_message_id[provider_message_id] = row

        scanned = 0
        processed = 0
        for message in messages:
            if not isinstance(message, dict):
                continue
            scanned += 1
            if not self._is_inbound_provider_message(message=message, candidate=candidate):
                continue
            attachments = message.get("attachments") if isinstance(message.get("attachments"), list) else []
            if not attachments:
                continue
            provider_message_id = str(message.get("provider_message_id") or "").strip()
            text = str(message.get("text") or "").strip()
            if not text:
                text = self._extract_attachment_text_from_provider_message(message)
            stored_row = by_provider_message_id.get(provider_message_id)
            inbound_message_id = int(stored_row.get("id") or 0) if isinstance(stored_row, dict) else 0
            self._capture_resume_assets_from_inbound(
                job=job,
                candidate=candidate,
                conversation=conversation,
                inbound_message_id=inbound_message_id,
                inbound_text=text,
                inbound_meta={
                    "type": "candidate_message",
                    "provider": "unipile_poll",
                    "provider_message_id": provider_message_id or None,
                    "occurred_at": str(message.get("created_at") or "").strip() or None,
                    "attachments": attachments,
                    "raw": message.get("raw") if isinstance(message.get("raw"), dict) else message,
                },
            )
            processed += 1

        return {
            "conversation_id": int(conversation_id),
            "external_chat_id": external_chat_id,
            "scanned": scanned,
            "processed": processed,
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
        forced_identifiers = self._load_forced_test_identifiers()
        forced_lookup_by_job: Dict[int, List[str]] = {}
        forced_mode_by_job: Dict[int, bool] = {}

        for row in due_rows:
            session_id = str(row.get("session_id") or "")
            if not session_id:
                errors += 1
                continue
            conversation_id = int(row["conversation_id"])
            job_ref = int(row["job_id"])
            candidate_id = int(row["candidate_id"])
            job_ctx = self.db.get_job(job_ref) or {"id": job_ref}
            if self._job_is_paused(job_ctx):
                skipped += 1
                items.append(
                    {
                        "session_id": session_id,
                        "conversation_id": conversation_id,
                        "status": "skipped",
                        "reason": "job_paused",
                    }
                )
                continue
            state_json = row.get("state_json") if isinstance(row.get("state_json"), dict) else {}
            if self.pre_resume_service.get_session(session_id) is None and state_json:
                self.pre_resume_service.seed_session(state_json)

            if forced_identifiers:
                forced_only = forced_mode_by_job.get(job_ref)
                if forced_only is None:
                    forced_only = self._effective_test_mode(
                        job=job_ctx,
                        test_mode=None,
                        forced_identifiers=forced_identifiers,
                    )
                    forced_mode_by_job[job_ref] = forced_only
                if forced_only:
                    lookup = forced_lookup_by_job.get(job_ref)
                    if lookup is None:
                        lookup = self._build_forced_identifier_lookup(
                            job=job_ctx,
                            forced_identifiers=forced_identifiers,
                        )
                        forced_lookup_by_job[job_ref] = lookup
                    candidate_for_filter = self.db.get_candidate(candidate_id)
                    match_for_filter = self.db.get_candidate_match(job_id=job_ref, candidate_id=candidate_id)
                    forced_identifier = None
                    if candidate_for_filter:
                        forced_identifier = self._forced_test_identifier_for_profile(candidate_for_filter, lookup)
                    if not forced_identifier:
                        forced_identifier = self._forced_test_identifier_from_match(match_for_filter, lookup)
                    if not forced_identifier:
                        skipped += 1
                        items.append(
                            {
                                "session_id": session_id,
                                "conversation_id": conversation_id,
                                "status": "skipped",
                                "reason": "test_job_forced_only",
                            }
                        )
                        self.db.log_operation(
                            operation="agent.pre_resume.followup",
                            status="skipped",
                            entity_type="conversation",
                            entity_id=str(conversation_id),
                            details={
                                "session_id": session_id,
                                "job_id": job_ref,
                                "candidate_id": candidate_id,
                                "reason": "test_job_forced_only",
                                "forced_test_ids_file": self.forced_test_ids_path,
                            },
                        )
                        continue

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
            self._sync_candidate_prescreen_from_state(
                job=job_ctx,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                state=state,
            )
            outbound = str(result.get("outbound") or "").strip()
            candidate = self.db.get_candidate(candidate_id)
            conversation = self.db.get_conversation(conversation_id)
            if result.get("sent") and outbound and candidate and conversation:
                language = resolve_conversation_language(
                    latest_message_text="",
                    previous_language=str((state or {}).get("language") or ""),
                    profile_languages=candidate.get("languages"),
                    fallback="en",
                )
                history = self._build_llm_history(self.db.list_messages(conversation_id=conversation_id), latest_inbound="")
                outbound = self._compose_linkedin_followup_message(
                    job=job_ctx,
                    candidate=candidate,
                    language=language,
                    history=history,
                    state=state,
                    conversation=conversation,
                    fallback_message=outbound,
                )
                result["outbound"] = outbound
            event_type = "followup_sent" if result.get("sent") else "followup_skipped"
            self.db.insert_pre_resume_event(
                session_id=session_id,
                conversation_id=conversation_id,
                event_type=event_type,
                intent=None,
                inbound_text=None,
                outbound_text=outbound or None,
                state_status=state.get("status") if isinstance(state, dict) else None,
                details={"reason": result.get("reason"), "source": "scheduler"},
            )

            if not result.get("sent"):
                state_status = str((state or {}).get("status") or "").strip().lower()
                if state_status == "stalled":
                    self.db.update_candidate_match_status(
                        job_id=job_ref,
                        candidate_id=candidate_id,
                        status="stalled",
                        extra_notes={"stalled_at": utc_now_iso(), "stalled_reason": str(result.get("reason") or "") or None},
                    )
                elif state_status == "not_interested":
                    self.db.update_candidate_match_status(
                        job_id=job_ref,
                        candidate_id=candidate_id,
                        status="rejected",
                        extra_notes={"rejected_at": utc_now_iso(), "rejection_reason": "candidate_not_interested"},
                    )
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

            language = resolve_conversation_language(
                latest_message_text="",
                previous_language=str((state or {}).get("language") or ""),
                profile_languages=candidate.get("languages"),
                fallback="en",
            )
            delivery = self._send_auto_reply(candidate=candidate, message=outbound, conversation=conversation)
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
                        operation="agent.pre_resume.followup.chat_binding",
                        status="partial",
                        entity_type="conversation",
                        entity_id=str(conversation_id),
                        details={"candidate_id": candidate_id, "chat_binding": chat_binding},
                    )
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
                    "external_chat_id": external_chat_id or None,
                    "chat_binding": chat_binding,
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
                self._record_communication_dialogue_assessment(
                    job_id=job_ref,
                    candidate_id=candidate_id,
                    mode="pre_resume",
                    intent="followup",
                    state=state if isinstance(state, dict) else None,
                    inbound_text=None,
                )
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
        rows = self._list_candidates_for_jobs(job_id=job_id, limit=max(safe_limit * 5, 500))
        session_index = self._build_interview_session_index(rows=rows)
        processed = 0
        updated = 0
        errors = 0
        items: List[Dict[str, Any]] = []

        for row in rows:
            if processed >= safe_limit:
                break
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            session_id = str((notes or {}).get("interview_session_id") or "").strip()
            fallback = None
            if not session_id:
                fallback = session_index.get((int(row["job_id"]), int(row["candidate_id"])))
                if isinstance(fallback, dict):
                    session_id = str(fallback.get("session_id") or "").strip()
            if not session_id:
                continue
            processed += 1
            try:
                payload = self.interview_client.refresh_session(session_id=session_id, force=force_refresh)
            except Exception:
                try:
                    payload = self.interview_client.get_session(session_id=session_id)
                except Exception as exc:
                    if isinstance(fallback, dict):
                        payload = self._session_payload_from_list_item(fallback)
                    else:
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
            if interview_status == "scored" and total_score is None:
                total_score = self._load_interview_total_score(session_id=session_id)

            changed = self._apply_interview_progress_update(
                job_id=int(row["job_id"]),
                candidate_id=int(row["candidate_id"]),
                notes=notes or {},
                interview_status=interview_status,
                session_id=session_id,
                entry_url=(
                    str((notes or {}).get("interview_entry_url") or payload.get("entry_url") or (fallback or {}).get("entry_url") or "")
                    .strip()
                    or None
                ),
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
            if self._job_is_paused(job):
                skipped += 1
                items.append(
                    {
                        "job_id": job_ref,
                        "candidate_id": candidate_id,
                        "session_id": session_id,
                        "status": "skipped",
                        "reason": "job_paused",
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
                conversation=conversation,
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

    def _send_interview_invite(
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
                "message": "",
            }

        job_id = self._safe_int(job.get("id"), self._safe_int(conversation.get("job_id"), 0))
        candidate_id = self._safe_int(candidate.get("id"), self._safe_int(conversation.get("candidate_id"), 0))
        conversation_id = self._safe_int(conversation.get("id"), 0)
        if job_id <= 0 or candidate_id <= 0 or conversation_id <= 0:
            return {"started": False, "reason": "missing_ids", "message": ""}

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
            conversation=conversation,
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
            "message": message,
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
        if bool((state or {}).get("awaiting_pre_vetting_opt_in")):
            return text
        lowered = text.lower()
        if "interview link" in lowered or "pre-vetting" in lowered or "pre vetting" in lowered:
            return text

        prompts = {
            "en": "Would you be open to a quick async pre vetting step to speed up next stage",
            "ru": "Готовы пройти короткий асинхронный pre vetting, чтобы быстрее перейти к следующему этапу",
            "es": "Te interesaria pasar un pre vetting asincrono corto para acelerar el siguiente paso",
        }
        lang = str(language or "en").strip().lower()
        prompt = prompts.get(lang, prompts["en"])
        if isinstance(state, dict):
            state["awaiting_pre_vetting_opt_in"] = True
        return f"{text} {prompt}".strip()

    @staticmethod
    def _is_pre_vetting_opt_in_message(text: str) -> bool:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return False
        markers = (
            "what is next",
            "what's next",
            "whats next",
            "next step",
            "next steps",
            "sounds interesting",
            "i am interested",
            "i'm interested",
            "interested in moving forward",
            "let's do it",
            "lets do it",
            "happy to proceed",
            "ready to proceed",
            "open to it",
        )
        return any(marker in lowered for marker in markers)

    def _compose_interview_invite_message(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        entry_url: str,
        language: str,
        conversation: Dict[str, Any] | None = None,
    ) -> str:
        title = str(job.get("title") or "this role").strip() or "this role"
        fallback_by_lang = {
            "en": (
                'Hey,\n'
                'here is your quick async pre vetting link for "{title}": {url}\n'
                "when you finish it, drop me a short reply here and I will move you forward"
            ),
            "ru": (
                'Hey,\n'
                'вот короткий async pre vetting по роли "{title}": {url}\n'
                "как закончите, просто дайте короткий ответ здесь и я двину вас дальше"
            ),
            "es": (
                'Hey,\n'
                'aqui esta tu enlace de async pre vetting para "{title}": {url}\n'
                "cuando termines, dejame una respuesta corta aqui y te muevo al siguiente paso"
            ),
        }
        lang = str(language or "en").strip().lower()
        fallback_template = fallback_by_lang.get(lang, fallback_by_lang["en"])
        fallback = fallback_template.format(title=title, url=entry_url).strip()
        instruction = self._linkedin_generation_instruction(
            kind="interview_invite",
            recruiter_name=self._linkedin_recruiter_name(conversation=conversation),
            language=lang,
        )
        generated = self._maybe_llm_reply(
            mode="linkedin_interview_invite",
            instruction=instruction,
            job={**job, "interview_entry_url": entry_url},
            candidate=candidate,
            inbound_text=f"Interview URL to include exactly: {entry_url}",
            history=[],
            fallback_reply=fallback,
            language=lang,
            state=None,
        )
        with_greeting = self._ensure_candidate_greeting(text=generated, fallback=fallback)
        return self._ensure_interview_url(text=with_greeting, fallback=fallback, entry_url=entry_url)

    def _compose_interview_followup_message(
        self,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        entry_url: str,
        language: str,
        followup_number: int,
        conversation: Dict[str, Any] | None = None,
    ) -> str:
        name = str(candidate.get("full_name") or "there").strip() or "there"
        title = str(job.get("title") or "this role").strip() or "this role"
        first = {
            "en": '{name}, quick ping on "{title}": {url}\nwant me to help with anything before you do it',
            "ru": '{name}, короткий пинг по "{title}": {url}\nесли нужна помощь перед прохождением, напишите',
            "es": '{name}, ping rapido sobre "{title}": {url}\nsi quieres, te ayudo antes de hacerlo',
        }
        second = {
            "en": '{name}, final reminder for "{title}": {url}\nif this role is still interesting, please do the quick pre vetting',
            "ru": '{name}, финальное напоминание по "{title}": {url}\nесли роль все еще актуальна, пройдите короткий pre vetting',
            "es": '{name}, ultimo recordatorio para "{title}": {url}\nsi el rol sigue siendo interesante, completa el pre vetting corto',
        }
        lang = str(language or "en").strip().lower()
        pool = first if int(followup_number) <= 1 else second
        fallback = pool.get(lang, pool["en"]).format(name=name, title=title, url=entry_url)
        instruction = self._linkedin_generation_instruction(
            kind="interview_followup",
            recruiter_name=self._linkedin_recruiter_name(conversation=conversation),
            language=lang,
        )
        generated = self._maybe_llm_reply(
            mode="linkedin_interview_followup",
            instruction=instruction,
            job={**job, "interview_entry_url": entry_url, "followup_number": followup_number},
            candidate=candidate,
            inbound_text=f"Followup number {followup_number}. Include this URL exactly: {entry_url}",
            history=[],
            fallback_reply=fallback,
            language=lang,
            state=None,
        )
        return self._ensure_interview_url(text=generated, fallback=fallback, entry_url=entry_url)

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

        mapped_status = self._match_status_for_interview(interview_status=status, total_score=total_score)
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
        base_rows = self._list_candidates_for_jobs(job_id=job_id, limit=safe_limit)
        out: List[Dict[str, Any]] = []
        for row in base_rows:
            notes = row.get("verification_notes") if isinstance(row.get("verification_notes"), dict) else {}
            if not str((notes or {}).get("interview_session_id") or "").strip():
                continue
            out.append(row)
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
    def _match_status_for_interview(interview_status: str, total_score: float | None = None) -> str | None:
        status = str(interview_status or "").strip().lower()
        if status in {"created", "invited"}:
            return "interview_invited"
        if status in {"in_progress", "completed"}:
            return "interview_in_progress"
        if status == "scored":
            if total_score is None:
                return "interview_in_progress"
            return "interview_passed" if float(total_score) >= 80.0 else "interview_failed"
        if status in {"failed", "expired", "canceled"}:
            return "interview_failed"
        return None

    @staticmethod
    def _candidate_primary_language(candidate: Dict[str, Any]) -> str:
        return resolve_outbound_language(candidate, fallback="en")

    @staticmethod
    def _safe_int(value: Any, fallback: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(fallback)

    @staticmethod
    def _safe_float(value: Any, fallback: float | None = 0.0) -> float | None:
        try:
            return float(value) if value is not None else fallback
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _safe_bool(value: Any, fallback: bool | None = False) -> bool | None:
        if value is None:
            return fallback
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        lowered = str(value or "").strip().lower()
        if not lowered:
            return fallback
        if lowered in {"1", "true", "yes", "y", "si", "sí"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
        return fallback

    def _list_candidates_for_jobs(self, job_id: int | None, limit: int = 500) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 500), 2000))
        job_ids: List[int] = []
        if job_id is not None:
            job_row = self.db.get_job(int(job_id))
            if not job_row or bool(job_row.get("is_archived")) or bool(job_row.get("is_paused")):
                return []
            job_ids = [int(job_id)]
        else:
            for job in self.db.list_jobs(limit=300):
                if bool(job.get("is_archived")) or bool(job.get("is_paused")):
                    continue
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
                enriched = dict(row)
                enriched["job_id"] = job_ref
                out.append(enriched)
                if len(out) >= safe_limit:
                    return out
        return out

    def _build_interview_session_index(self, rows: List[Dict[str, Any]]) -> Dict[tuple[int, int], Dict[str, Any]]:
        if self.interview_client is None:
            return {}
        list_fn = getattr(self.interview_client, "list_sessions", None)
        if not callable(list_fn):
            return {}

        job_ids = sorted({int(r["job_id"]) for r in rows if isinstance(r, dict) and r.get("job_id") is not None})
        out: Dict[tuple[int, int], Dict[str, Any]] = {}
        for job_ref in job_ids:
            try:
                payload = list_fn(job_id=job_ref, limit=500)
            except Exception:
                continue
            items = payload.get("items") if isinstance(payload, dict) else []
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    candidate_id = int(item.get("candidate_id"))
                except (TypeError, ValueError):
                    continue
                session_id = str(item.get("session_id") or "").strip()
                if not session_id:
                    continue
                key = (job_ref, candidate_id)
                existing = out.get(key)
                if existing is None or self._session_sort_key(item) >= self._session_sort_key(existing):
                    out[key] = item
        return out

    @staticmethod
    def _session_sort_key(item: Dict[str, Any]) -> str:
        return str(
            item.get("scored_at")
            or item.get("completed_at")
            or item.get("updated_at")
            or item.get("created_at")
            or ""
        )

    @staticmethod
    def _session_payload_from_list_item(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "session_id": item.get("session_id"),
            "status": item.get("status"),
            "entry_url": item.get("entry_url"),
            "summary": {
                "total_score": item.get("total_score"),
            },
        }

    def _load_interview_total_score(self, session_id: str) -> float | None:
        if self.interview_client is None:
            return None
        fn = getattr(self.interview_client, "get_scorecard", None)
        if not callable(fn):
            return None
        try:
            payload = fn(session_id=session_id)
        except Exception:
            return None
        scorecard = payload.get("scorecard") if isinstance(payload, dict) else None
        if not isinstance(scorecard, dict):
            return None
        raw = scorecard.get("total_score")
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

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
            "verified": "qualified",
            "needs_resume": "conditional",
            "rejected": "not_matched",
        }
        assessment_status = status_map.get(normalized_status, "review")
        score = raw_score
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
                "score_mode": "raw_match_score",
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
        quality_adjustment, quality_signals = self._communication_quality_adjustment(
            intent=normalized_intent,
            state=state if isinstance(state, dict) else None,
            inbound_text=inbound_text,
        )

        if normalized_mode == "pre_resume":
            mapping = {
                "ready_for_interview": (94.0, "cv_received", "Written prescreen complete and CV received."),
                "ready_for_screening_call": (94.0, "cv_received", "Written prescreen complete and CV received."),
                "ready_for_cv": (84.0, "written_prescreen_complete", "Written prescreen complete. Waiting for CV."),
                "cv_received_pending_answers": (80.0, "cv_received", "CV received before written prescreen was complete."),
                "resume_promised": (80.0, "resume_promised", "Candidate promised to share CV later."),
                "engaged_no_resume": (72.0, "in_dialogue", "Candidate is engaged in dialogue before CV."),
                "incomplete": (72.0, "in_dialogue", "Written prescreen is still in progress."),
                "awaiting_reply": (66.0, "awaiting_reply", "Awaiting candidate response after follow-up."),
                "delivery_blocked_identity": (32.0, "delivery_blocked", "Delivery blocked because provider identity is missing or invalid."),
                "not_interested": (35.0, "not_interested", "Candidate is not interested."),
                "stalled": (25.0, "stalled", "Dialogue stalled without response."),
                "unreachable": (15.0, "unreachable", "Candidate unreachable through current channel."),
            }
            score, status, reason = mapping.get(
                state_status,
                (66.0, "in_dialogue", f"Dialogue update captured (status: {state_status or 'unknown'})."),
            )
            if state_status in {
                "ready_for_interview",
                "ready_for_screening_call",
                "ready_for_cv",
                "cv_received_pending_answers",
                "resume_promised",
                "engaged_no_resume",
                "incomplete",
                "awaiting_reply",
            }:
                score = self._normalize_percentage(score + quality_adjustment)
        else:
            score = 68.0
            status = "in_dialogue"
            reason = f"FAQ dialogue handled (intent: {normalized_intent or 'default'})."
            score = self._normalize_percentage(score + quality_adjustment)

        if abs(quality_adjustment) >= 0.1:
            signed = f"+{quality_adjustment:.1f}" if quality_adjustment > 0 else f"{quality_adjustment:.1f}"
            reason = f"{reason} Communication quality adjustment: {signed}."

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
                "quality_adjustment": round(float(quality_adjustment), 2),
                "quality_signals": quality_signals,
            },
        )

    def _communication_quality_adjustment(
        self,
        *,
        intent: str,
        state: Dict[str, Any] | None,
        inbound_text: str | None,
    ) -> tuple[float, Dict[str, Any]]:
        state_payload = state if isinstance(state, dict) else {}
        text = str(inbound_text or "").strip()
        lowered = text.lower()
        words = [x.lower() for x in re.findall(r"[0-9A-Za-zА-Яа-яЁё]+", text)]
        word_count = len(words)
        unique_ratio = (len(set(words)) / float(word_count)) if word_count > 0 else 0.0
        followups_sent = self._safe_int(state_payload.get("followups_sent"), 0)
        turns = self._safe_int(state_payload.get("turns"), 0)
        resume_links = state_payload.get("resume_links") if isinstance(state_payload.get("resume_links"), list) else []

        adjustment = 0.0
        if word_count >= 20:
            adjustment += 8.0
        elif word_count >= 12:
            adjustment += 5.0
        elif word_count >= 6:
            adjustment += 3.0
        elif word_count >= 3:
            adjustment += 1.0
        elif text:
            adjustment -= 4.0
        else:
            adjustment -= 1.0

        if unique_ratio >= 0.72 and word_count >= 8:
            adjustment += 1.5
        if "?" in text:
            adjustment += 1.0
        if any(marker in lowered for marker in ("please", "thanks", "thank you", "gracias", "спасибо")):
            adjustment += 1.5

        filler_count = len(re.findall(r"\b(um+|uh+|hmm+|like|ну|ээ+)\b", lowered))
        if filler_count >= 3:
            adjustment -= 3.0

        if turns >= 4:
            adjustment += 2.0
        if followups_sent > 0:
            adjustment -= min(6.0, float(followups_sent) * 1.5)
        if resume_links:
            adjustment += 2.5

        intent_adjustments = {
            "resume_shared": 5.0,
            "will_send_later": 1.5,
            "not_interested": -7.0,
            "salary": 1.0,
            "stack": 1.0,
            "timeline": 1.0,
            "send_jd_first": 1.0,
        }
        adjustment += float(intent_adjustments.get(intent, 0.0))
        adjustment = max(-12.0, min(12.0, adjustment))

        signals = {
            "word_count": word_count,
            "unique_word_ratio": round(unique_ratio, 3),
            "followups_sent": followups_sent,
            "turns": turns,
            "resume_links_count": len(resume_links),
            "filler_count": filler_count,
            "intent": intent or "default",
        }
        return round(adjustment, 2), signals

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
        if self._managed_linkedin_available():
            account_id = int((conversation or {}).get("linkedin_account_id") or 0)
            if account_id > 0:
                account = self.db.get_linkedin_account(account_id)
                provider_account_id = str((account or {}).get("provider_account_id") or "").strip()
                if provider_account_id:
                    try:
                        provider = self._build_managed_provider(account_id=provider_account_id)
                        out = provider.send_message(candidate_profile=candidate, message=message)
                        if out.get("sent"):
                            self._increment_managed_account_counters(
                                account_id=account_id,
                                connect_delta=0,
                                new_threads_delta=0,
                                replies_delta=1,
                            )
                        return out
                    except Exception as exc:
                        return {"sent": False, "provider": "linkedin", "error": str(exc)}
        try:
            return self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
        except Exception as exc:
            return {"sent": False, "provider": "linkedin", "error": str(exc)}

    def dispatch_outbound_actions(
        self,
        *,
        limit: int = 100,
        action_ids: List[int] | None = None,
        job_id: int | None = None,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 100), 500))
        rows = self.db.list_pending_outbound_actions(limit=safe_limit, job_id=job_id, action_ids=action_ids)
        selection_states: Dict[int, Dict[str, Any]] = {}
        processed = 0
        sent = 0
        pending_connection = 0
        failed = 0
        deferred = 0
        items: List[Dict[str, Any]] = []

        for row in rows:
            action_id = int(row.get("id") or 0)
            if action_id <= 0:
                continue
            if not self.db.claim_outbound_action(action_id):
                continue
            processed += 1
            try:
                row_job_id = int(row.get("job_id") or 0)
                selection_state = selection_states.setdefault(
                    row_job_id,
                    self._build_linkedin_account_selection_state(job_id=row_job_id),
                )
                result = self._dispatch_single_outbound_action(row=row, selection_state=selection_state)
            except Exception as exc:
                failed += 1
                self.db.complete_outbound_action(
                    action_id=action_id,
                    status="failed",
                    result={"reason": "dispatch_single_exception"},
                    error=str(exc),
                )
                self.db.log_operation(
                    operation="agent.outreach.dispatch",
                    status="error",
                    entity_type="outbound_action",
                    entity_id=str(action_id),
                    details={"error": str(exc), "job_id": row.get("job_id"), "candidate_id": row.get("candidate_id")},
                )
                items.append(
                    {
                        "action_id": action_id,
                        "conversation_id": int(row.get("conversation_id") or 0),
                        "candidate_id": int(row.get("candidate_id") or 0),
                        "delivery_status": "failed",
                        "error": str(exc),
                    }
                )
                continue
            status = str(result.get("delivery_status") or "").strip().lower()
            if status == "sent":
                sent += 1
            elif status == "pending_connection":
                pending_connection += 1
            elif status == "deferred":
                deferred += 1
            elif status == "failed":
                failed += 1
            items.append(result)

        return {
            "processed": processed,
            "sent": sent,
            "pending_connection": pending_connection,
            "failed": failed,
            "deferred": deferred,
            "items": items,
        }

    def _queue_managed_outbound_action(
        self,
        *,
        job_id: int,
        candidate_id: int,
        conversation_id: int,
        action_type: str,
        payload: Dict[str, Any],
        priority: int = 0,
        not_before: str | None = None,
        selection_state: Dict[str, Any] | None = None,
    ) -> tuple[int, int | None]:
        assigned_account_id: int | None = None
        planned_action_kind = str(payload.get("planned_action_kind") or "").strip().lower() or "connect_request"
        if self._managed_linkedin_available():
            account, _ = self._select_linkedin_account_for_new_thread(
                job_id=job_id,
                selection_state=selection_state,
                planned_action_kind=planned_action_kind,
            )
            if account is not None:
                assigned_account_id = int(account.get("id") or 0) or None
        action_id = self.db.create_outbound_action(
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            action_type=action_type,
            payload=payload,
            account_id=assigned_account_id,
            priority=priority,
            not_before=not_before,
        )
        return int(action_id), assigned_account_id

    def _queue_recovery_outbound_action(
        self,
        *,
        row: Dict[str, Any],
        selection_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        conversation_id = int(row.get("conversation_id") or 0)
        candidate_id = int(row.get("candidate_id") or 0)
        row_job_id = int(row.get("job_id") or 0)
        pre_resume_session_id = str(row.get("pre_resume_session_id") or "").strip() or None
        message = str(row.get("last_outbound_message") or "").strip()
        language = str(row.get("last_outbound_language") or "").strip().lower() or "en"
        raw_meta = row.get("last_outbound_meta")
        last_meta: Dict[str, Any] = {}
        if isinstance(raw_meta, dict):
            last_meta = raw_meta
        elif isinstance(raw_meta, str):
            try:
                parsed = json.loads(raw_meta)
                if isinstance(parsed, dict):
                    last_meta = parsed
            except json.JSONDecodeError:
                last_meta = {}

        if not message:
            return {
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "status": "skipped",
                "reason": "no_last_outbound_message",
            }

        delivery = last_meta.get("delivery") if isinstance(last_meta.get("delivery"), dict) else {}
        connect_request = last_meta.get("connect_request") if isinstance(last_meta.get("connect_request"), dict) else {}
        if bool(delivery.get("sent")) or bool(connect_request.get("sent")):
            return {
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "status": "skipped",
                "reason": "invite_or_delivery_already_sent",
            }

        conversation = self.db.get_conversation(conversation_id)
        delivery_mode = self._determine_initial_outreach_delivery_mode(
            action_type="pre_resume_recovery",
            conversation=conversation,
        )
        planned_action_kind = self._planned_action_kind_for_delivery_mode(delivery_mode)
        action_id, assigned_account_id = self._queue_managed_outbound_action(
            job_id=row_job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            action_type="pre_resume_recovery",
            payload={
                "message": message,
                "language": language,
                "request_resume": bool(last_meta.get("request_resume")),
                "screening_status": str(last_meta.get("screening_status") or ""),
                "pre_resume_session_id": pre_resume_session_id,
                "delivery_mode": delivery_mode,
                "planned_action_kind": planned_action_kind,
            },
            priority=0,
            selection_state=selection_state,
        )
        return {
            "conversation_id": conversation_id,
            "candidate_id": candidate_id,
            "status": "queued",
            "action_id": int(action_id),
            "linkedin_account_id": assigned_account_id,
            "planned_action_kind": planned_action_kind,
        }

    def backfill_outreach_for_unassigned_conversations(
        self,
        *,
        job_id: int | None = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 500))
        rows = self.db.list_unassigned_outreach_conversations(limit=safe_limit, job_id=job_id)
        queued_action_ids: List[int] = []
        skipped = 0
        items: List[Dict[str, Any]] = []
        selection_states: Dict[int, Dict[str, Any]] = {}

        for row in rows:
            row_job_id = int(row.get("job_id") or 0)
            selection_state = selection_states.setdefault(
                row_job_id,
                self._build_linkedin_account_selection_state(job_id=row_job_id),
            )
            item = self._queue_recovery_outbound_action(row=row, selection_state=selection_state)
            if str(item.get("status") or "") == "queued":
                queued_action_ids.append(int(item.get("action_id") or 0))
            else:
                skipped += 1
            items.append(item)

        dispatch_result: Dict[str, Any] = {
            "processed": 0,
            "sent": 0,
            "pending_connection": 0,
            "failed": 0,
            "deferred": 0,
            "items": [],
        }
        if queued_action_ids:
            dispatch_result = self.dispatch_outbound_actions(
                limit=max(len(queued_action_ids), 1),
                action_ids=queued_action_ids,
                job_id=job_id,
            )

        return {
            "status": "ok",
            "job_id": int(job_id) if job_id is not None else None,
            "candidates_total": len(rows),
            "queued": len(queued_action_ids),
            "skipped": skipped,
            "dispatched": dispatch_result,
            "items": items,
        }

    def reconcile_waiting_connection_match_statuses(
        self,
        *,
        job_id: int | None = None,
        limit: int = 200,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 200), 500))
        rows = self.db.list_waiting_connection_status_drifts(limit=safe_limit, job_id=job_id)
        items: List[Dict[str, Any]] = []
        updated = 0

        for row in rows:
            row_job_id = int(row.get("job_id") or 0)
            candidate_id = int(row.get("candidate_id") or 0)
            previous_status = str(row.get("match_status") or "").strip().lower()
            item = {
                "job_id": row_job_id,
                "candidate_id": candidate_id,
                "candidate_name": row.get("candidate_name"),
                "conversation_id": int(row.get("conversation_id") or 0),
                "previous_status": previous_status,
                "target_status": "outreach_pending_connection",
                "status": "pending" if dry_run else "updated",
            }
            if not dry_run and row_job_id > 0 and candidate_id > 0:
                # job_candidates.status is the source of truth for funnel stage; this only repairs
                # stale rows where transport state already shows a sent connection request.
                self.db.update_candidate_match_status(
                    job_id=row_job_id,
                    candidate_id=candidate_id,
                    status="outreach_pending_connection",
                    extra_notes={
                        "reconciled_at": utc_now_iso(),
                        "reconciliation_reason": "waiting_connection_status_drift",
                        "reconciliation_previous_status": previous_status,
                    },
                )
                updated += 1
            items.append(item)

        return {
            "status": "ok",
            "job_id": int(job_id) if job_id is not None else None,
            "dry_run": bool(dry_run),
            "candidates_total": len(rows),
            "updated": updated,
            "items": items,
        }

    def queue_job_outreach_candidates(
        self,
        *,
        job_id: int,
        limit: int = 50,
    ) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 50), 200))
        rows = self.db.list_job_outreach_candidates(job_id=job_id, limit=max(safe_limit * 3, safe_limit))
        candidate_ids: List[int] = []
        preview_items: List[Dict[str, Any]] = []
        for row in rows:
            if str(row.get("current_status_key") or "").strip().lower() != "added":
                continue
            candidate_id = int(row.get("candidate_id") or 0)
            if candidate_id <= 0:
                continue
            candidate_ids.append(candidate_id)
            preview_items.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_name": row.get("full_name"),
                    "job_id": int(row.get("job_id") or 0),
                    "job_title": row.get("job_title"),
                    "score": float(row.get("score") or 0.0),
                }
            )
            if len(candidate_ids) >= safe_limit:
                break

        if not candidate_ids:
            return {
                "status": "ok",
                "job_id": int(job_id),
                "queued": 0,
                "candidate_ids": [],
                "action_ids": [],
                "items": [],
            }

        out = self.outreach_candidates(job_id=job_id, candidate_ids=candidate_ids)
        action_ids = [
            int(item.get("action_id") or 0)
            for item in (out.get("items") or [])
            if isinstance(item, dict) and int(item.get("action_id") or 0) > 0
        ]
        return {
            "status": "ok",
            "job_id": int(job_id),
            "queued": len(candidate_ids),
            "candidate_ids": candidate_ids,
            "action_ids": action_ids,
            "items": preview_items,
            "result": out,
        }

    def _collect_rebalance_backlog_for_job(
        self,
        *,
        job: Dict[str, Any],
        candidate_scan_limit: int,
        recovery_scan_limit: int,
    ) -> Dict[str, Any]:
        job_id = int(job.get("job_id") or job.get("id") or 0)
        if job_id <= 0:
            return {
                "job_id": 0,
                "job_title": str(job.get("job_title") or job.get("title") or "").strip() or "-",
                "connect_items": [],
                "message_items": [],
                "new_thread_backlog": 0,
                "recovery_backlog": 0,
            }
        connect_items: List[Dict[str, Any]] = []
        message_items: List[Dict[str, Any]] = []
        forced_identifiers = self._build_forced_identifier_lookup(
            job=job,
            forced_identifiers=self._load_forced_test_identifiers(),
        )
        forced_only = self._effective_test_mode(
            job=job,
            test_mode=None,
            forced_identifiers=forced_identifiers,
        )
        new_thread_rows = self.db.list_job_outreach_candidates(job_id=job_id, limit=max(1, min(candidate_scan_limit, 1000)))
        for row in new_thread_rows:
            if str(row.get("current_status_key") or "").strip().lower() != "added":
                continue
            if self._is_non_test_forced_candidate(
                candidate=row,
                match={"verification_notes": row.get("verification_notes")},
                forced_identifiers=forced_identifiers,
                forced_only=forced_only,
            ):
                continue
            candidate_id = int(row.get("candidate_id") or 0)
            if candidate_id <= 0:
                continue
            connect_items.append(
                {
                    "item_type": "new_thread",
                    "job_id": job_id,
                    "job_title": str(row.get("job_title") or job.get("job_title") or job.get("title") or "").strip() or "-",
                    "candidate_id": candidate_id,
                    "score": float(row.get("score") or 0.0),
                }
            )

        recovery_rows = self.db.list_unassigned_outreach_conversations(
            limit=max(1, min(recovery_scan_limit, 1000)),
            job_id=job_id,
        )
        for row in recovery_rows:
            candidate_id = int(row.get("candidate_id") or 0)
            candidate = self.db.get_candidate(candidate_id) if candidate_id > 0 else None
            match = self.db.get_candidate_match(job_id=job_id, candidate_id=candidate_id) if candidate_id > 0 else None
            if self._is_non_test_forced_candidate(
                candidate=candidate,
                match=match,
                forced_identifiers=forced_identifiers,
                forced_only=forced_only,
            ):
                continue
            conversation = self.db.get_conversation(int(row.get("conversation_id") or 0))
            delivery_mode = self._determine_initial_outreach_delivery_mode(
                action_type="pre_resume_recovery",
                conversation=conversation,
            )
            item = {
                "item_type": "recovery",
                "job_id": job_id,
                "job_title": str(row.get("job_title") or job.get("job_title") or job.get("title") or "").strip() or "-",
                "candidate_id": int(row.get("candidate_id") or 0),
                "conversation_id": int(row.get("conversation_id") or 0),
                "row": row,
            }
            if delivery_mode == "connect_first":
                connect_items.append(item)
            else:
                message_items.append(item)

        connect_items.sort(key=lambda item: (0 if item.get("item_type") == "recovery" else 1, -float(item.get("score") or 0.0)))
        return {
            "job_id": job_id,
            "job_title": str(job.get("job_title") or job.get("title") or "").strip() or "-",
            "connect_items": connect_items,
            "message_items": message_items,
            "new_thread_backlog": len([item for item in connect_items if item.get("item_type") == "new_thread"]),
            "recovery_backlog": len(connect_items) + len(message_items) - len([item for item in connect_items if item.get("item_type") == "new_thread"]),
        }

    @staticmethod
    def _allocate_backlog_round_robin(
        *,
        job_queues: Dict[int, List[Dict[str, Any]]],
        total_slots: int,
    ) -> List[Dict[str, Any]]:
        remaining_slots = max(0, int(total_slots or 0))
        if remaining_slots <= 0:
            return []
        queues: Dict[int, List[Dict[str, Any]]] = {
            int(job_id): list(items)
            for job_id, items in (job_queues or {}).items()
            if int(job_id) > 0 and isinstance(items, list) and items
        }
        ordered_job_ids = [job_id for job_id in sorted(queues.keys()) if queues.get(job_id)]
        out: List[Dict[str, Any]] = []
        while remaining_slots > 0 and ordered_job_ids:
            next_round: List[int] = []
            for job_id in ordered_job_ids:
                queue = queues.get(job_id) or []
                if not queue:
                    continue
                out.append(queue.pop(0))
                remaining_slots -= 1
                if queue:
                    next_round.append(job_id)
                if remaining_slots <= 0:
                    break
            ordered_job_ids = next_round
        return out

    def rebalance_outreach_capacity(
        self,
        *,
        job_limit: int = 8,
        candidates_per_job: int = 25,
        recovery_per_job: int = 25,
        jobs_scan_limit: int = 40,
    ) -> Dict[str, Any]:
        connected_accounts = self._list_dispatchable_linkedin_accounts(limit=20)
        if not connected_accounts:
            return {
                "status": "ok",
                "reason": "no_connected_accounts",
                "jobs_scanned": 0,
                "jobs_selected": 0,
                "jobs": [],
            }

        selected_jobs = self._list_recent_auto_jobs_with_open_outreach_backlog(
            limit_jobs=job_limit,
            scan_limit=jobs_scan_limit,
        )
        if not selected_jobs:
            return {
                "status": "ok",
                "reason": "no_open_backlog",
                "jobs_scanned": 0,
                "jobs_selected": 0,
                "jobs": [],
                "totals": {
                    "new_threads_queued": 0,
                    "recovery_queued": 0,
                    "sent": 0,
                    "pending_connection": 0,
                    "failed": 0,
                    "deferred": 0,
                },
            }

        shared_selection_state = self._build_linkedin_account_selection_state(job_id=int(selected_jobs[0].get("job_id") or 0))
        connect_capacity_total = sum(
            int(value or 0)
            for value in (
                shared_selection_state.get("projected_connect_remaining")
                if isinstance(shared_selection_state.get("projected_connect_remaining"), dict)
                else {}
            ).values()
        )
        message_capacity_total = 0
        projected_counts = (
            shared_selection_state.get("projected_counts") if isinstance(shared_selection_state.get("projected_counts"), dict) else {}
        )
        daily_caps = shared_selection_state.get("daily_caps") if isinstance(shared_selection_state.get("daily_caps"), dict) else {}
        for account_id, daily_cap in daily_caps.items():
            message_capacity_total += max(0, int(daily_cap or 0) - int(projected_counts.get(account_id) or 0))

        candidate_scan_limit = max(1, min(max(int(candidates_per_job or 0), 500), 1000))
        recovery_scan_limit = max(1, min(max(int(recovery_per_job or 0), 500), 1000))
        backlog_by_job: Dict[int, Dict[str, Any]] = {}
        connect_job_queues: Dict[int, List[Dict[str, Any]]] = {}
        message_job_queues: Dict[int, List[Dict[str, Any]]] = {}
        for job in selected_jobs:
            backlog = self._collect_rebalance_backlog_for_job(
                job=job,
                candidate_scan_limit=candidate_scan_limit,
                recovery_scan_limit=recovery_scan_limit,
            )
            job_id = int(backlog.get("job_id") or 0)
            if job_id <= 0:
                continue
            backlog_by_job[job_id] = backlog
            connect_job_queues[job_id] = list(backlog.get("connect_items") or [])
            message_job_queues[job_id] = list(backlog.get("message_items") or [])

        connect_plan = self._allocate_backlog_round_robin(job_queues=connect_job_queues, total_slots=connect_capacity_total)
        message_plan = self._allocate_backlog_round_robin(job_queues=message_job_queues, total_slots=message_capacity_total)
        job_results: List[Dict[str, Any]] = []
        job_results_map: Dict[int, Dict[str, Any]] = {}
        totals = {
            "new_threads_queued": 0,
            "recovery_queued": 0,
            "sent": 0,
            "pending_connection": 0,
            "failed": 0,
            "deferred": 0,
        }
        queued_action_ids: List[int] = []

        for job in selected_jobs:
            row_job_id = int(job.get("job_id") or 0)
            backlog = backlog_by_job.get(row_job_id) or {}
            job_results_map[row_job_id] = {
                "job_id": row_job_id,
                "job_title": backlog.get("job_title") or job.get("job_title"),
                "new_thread_backlog": int(backlog.get("new_thread_backlog") or 0),
                "recovery_backlog": int(backlog.get("recovery_backlog") or 0),
                "new_threads": {"queued": 0, "candidate_ids": [], "action_ids": [], "items": []},
                "recovery": {"queued": 0, "action_ids": [], "items": []},
            }

        for planned_item in connect_plan + message_plan:
            row_job_id = int(planned_item.get("job_id") or 0)
            if row_job_id <= 0:
                continue
            job_bucket = job_results_map.setdefault(
                row_job_id,
                {
                    "job_id": row_job_id,
                    "job_title": planned_item.get("job_title"),
                    "new_thread_backlog": 0,
                    "recovery_backlog": 0,
                    "new_threads": {"queued": 0, "candidate_ids": [], "action_ids": [], "items": []},
                    "recovery": {"queued": 0, "action_ids": [], "items": []},
                },
            )
            if str(planned_item.get("item_type") or "") == "new_thread":
                candidate_id = int(planned_item.get("candidate_id") or 0)
                if candidate_id <= 0:
                    continue
                out = self._outreach_candidates_managed(
                    job_id=row_job_id,
                    candidate_ids=[candidate_id],
                    selection_state_override=shared_selection_state,
                    dispatch_inline_override=False,
                )
                action_ids = [
                    int(item.get("action_id") or 0)
                    for item in (out.get("items") or [])
                    if isinstance(item, dict) and int(item.get("action_id") or 0) > 0
                ]
                queued_action_ids.extend(action_ids)
                job_bucket["new_threads"]["queued"] += len(action_ids)
                job_bucket["new_threads"]["candidate_ids"].append(candidate_id)
                job_bucket["new_threads"]["action_ids"].extend(action_ids)
                job_bucket["new_threads"]["items"].extend(out.get("items") or [])
                totals["new_threads_queued"] += len(action_ids)
            else:
                queued = self._queue_recovery_outbound_action(
                    row=planned_item.get("row") if isinstance(planned_item.get("row"), dict) else {},
                    selection_state=shared_selection_state,
                )
                if str(queued.get("status") or "") != "queued":
                    continue
                action_id = int(queued.get("action_id") or 0)
                if action_id <= 0:
                    continue
                queued_action_ids.append(action_id)
                job_bucket["recovery"]["queued"] += 1
                job_bucket["recovery"]["action_ids"].append(action_id)
                job_bucket["recovery"]["items"].append(queued)
                totals["recovery_queued"] += 1

        dispatched: Dict[str, Any] = {
            "processed": 0,
            "sent": 0,
            "pending_connection": 0,
            "failed": 0,
            "deferred": 0,
            "items": [],
        }
        if queued_action_ids:
            dispatched = self.dispatch_outbound_actions(
                limit=max(len(queued_action_ids), 1),
                action_ids=queued_action_ids,
            )
        totals["sent"] += int(dispatched.get("sent") or 0)
        totals["pending_connection"] += int(dispatched.get("pending_connection") or 0)
        totals["failed"] += int(dispatched.get("failed") or 0)
        totals["deferred"] += int(dispatched.get("deferred") or 0)
        job_results = [job_results_map[job_id] for job_id in sorted(job_results_map.keys())]

        return {
            "status": "ok",
            "reason": "planned_and_dispatched",
            "jobs_scanned": len(selected_jobs),
            "jobs_selected": len(job_results),
            "jobs": job_results,
            "dispatched": dispatched,
            "planner": {
                "connect_capacity_total": connect_capacity_total,
                "message_capacity_total": message_capacity_total,
                "connect_planned": len(connect_plan),
                "message_planned": len(message_plan),
            },
            "totals": totals,
        }

    def _list_recent_auto_jobs_with_open_outreach_backlog(
        self,
        *,
        limit_jobs: int,
        scan_limit: int,
    ) -> List[Dict[str, Any]]:
        safe_limit = max(1, min(int(limit_jobs or 1), 50))
        safe_scan = max(safe_limit, min(int(scan_limit or 40), 200))
        jobs = self.db.list_jobs(limit=safe_scan)
        out: List[Dict[str, Any]] = []
        for job in jobs:
            job_id = int(job.get("id") or 0)
            if job_id <= 0:
                continue
            if self._job_is_paused(job) or self._job_is_archived(job):
                continue
            routing_mode = str(job.get("linkedin_routing_mode") or "auto").strip().lower()
            if routing_mode != "auto":
                continue
            new_thread_candidates = [
                item
                for item in self.db.list_job_outreach_candidates(job_id=job_id, limit=50)
                if str(item.get("current_status_key") or "").strip().lower() == "added"
            ]
            recovery_candidates = self.db.list_unassigned_outreach_conversations(limit=50, job_id=job_id)
            if not new_thread_candidates and not recovery_candidates:
                continue
            out.append(
                {
                    "job_id": job_id,
                    "job_title": job.get("title"),
                    "new_thread_backlog": len(new_thread_candidates),
                    "recovery_backlog": len(recovery_candidates),
                }
            )
            if len(out) >= safe_limit:
                break
        return out

    def _dispatch_single_outbound_action(
        self,
        row: Dict[str, Any],
        *,
        selection_state: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        action_id = int(row.get("id") or 0)
        job_id = int(row.get("job_id") or 0)
        candidate_id = int(row.get("candidate_id") or 0)
        conversation_id = int(row.get("conversation_id") or 0)
        action_type = str(row.get("action_type") or "").strip().lower()
        payload = row.get("payload_json") if isinstance(row.get("payload_json"), dict) else {}
        language = str(payload.get("language") or "en").strip().lower() or "en"
        message = str(payload.get("message") or "").strip()
        request_resume = bool(payload.get("request_resume"))
        screening_status = str(payload.get("screening_status") or "")
        pre_resume_session_id = payload.get("pre_resume_session_id")

        candidate = self.db.get_candidate(candidate_id)
        conversation = self.db.get_conversation(conversation_id)
        job = self.db.get_job(job_id)
        if not candidate or not conversation or not job or not message:
            error = "action_payload_invalid_or_missing_entities"
            self.db.complete_outbound_action(
                action_id=action_id,
                status="failed",
                result={"reason": error},
                error=error,
            )
            return {
                "action_id": action_id,
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "delivery_status": "failed",
                "error": error,
            }
        if self._job_is_paused(job):
            self.db.release_outbound_action(
                action_id=action_id,
                not_before=utc_now_iso(),
                error="job_paused",
            )
            return {
                "action_id": action_id,
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "delivery_status": "deferred",
                "error": "job_paused",
            }

        delivery_mode = str(payload.get("delivery_mode") or "").strip().lower()
        if delivery_mode not in {"connect_first", "message_first"}:
            delivery_mode = self._determine_initial_outreach_delivery_mode(
                action_type=action_type,
                conversation=conversation,
            )
        planned_action_kind = str(payload.get("planned_action_kind") or "").strip().lower()
        if not planned_action_kind:
            planned_action_kind = self._planned_action_kind_for_delivery_mode(delivery_mode)
        forced_identifiers = self._build_forced_identifier_lookup(
            job=job,
            forced_identifiers=self._load_forced_test_identifiers(),
        )
        allow_forced_test_candidate = bool(payload.get("allow_forced_test_candidate"))
        match = self.db.get_candidate_match(job_id=job_id, candidate_id=candidate_id)
        if self._is_non_test_forced_candidate(
            candidate=candidate,
            match=match,
            forced_identifiers=forced_identifiers,
            forced_only=allow_forced_test_candidate,
        ):
            self._suppress_pre_resume_recovery(
                conversation_id=conversation_id,
                reason="forced_test_candidate_excluded",
            )
            self.db.complete_outbound_action(
                action_id=action_id,
                status="failed",
                result={"reason": "forced_test_candidate_excluded"},
                error="forced_test_candidate_excluded",
            )
            return {
                "action_id": action_id,
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "delivery_status": "failed",
                "error": "forced_test_candidate_excluded",
            }

        account, selection_error = self._resolve_linkedin_account_for_outbound_action(
            row=row,
            job_id=job_id,
            selection_state=selection_state,
            planned_action_kind=planned_action_kind,
        )
        if not account:
            retry_at = (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat()
            error_reason = selection_error or "no_connected_account_or_daily_budget"
            self.db.release_outbound_action(
                action_id=action_id,
                not_before=retry_at,
                error=error_reason,
            )
            return {
                "action_id": action_id,
                "conversation_id": conversation_id,
                "candidate_id": candidate_id,
                "delivery_status": "deferred",
                "error": error_reason,
            }

        provider_account_id = str(account.get("provider_account_id") or "").strip()
        account_id = int(account.get("id") or 0)
        provider = self._build_managed_provider(account_id=provider_account_id)
        planned_event_type = "connect_planned" if planned_action_kind == "connect_request" else "message_planned"
        self._record_outreach_account_event(
            event_key=f"action:{action_id}:{planned_event_type}",
            account_id=account_id,
            event_type=planned_event_type,
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details={
                "action_id": action_id,
                "action_type": action_type,
                "delivery_mode": delivery_mode,
                "planned_action_kind": planned_action_kind,
            },
        )

        connect_request = None
        delivery_status = "failed"
        if delivery_mode == "connect_first":
            delivery = {"sent": False, "provider": "unipile", "reason": "connect_first"}
        else:
            try:
                delivery = provider.send_message(candidate_profile=candidate, message=message)
            except Exception as exc:
                delivery = {"sent": False, "provider": "unipile", "error": str(exc)}

        if delivery_mode != "connect_first" and delivery.get("sent"):
            delivery_status = "sent"
            self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
            self.db.update_conversation_status(conversation_id=conversation_id, status="active")
            self.db.update_candidate_match_status(
                job_id=job_id,
                candidate_id=candidate_id,
                status="outreach_sent",
                extra_notes={"outreach_state": "sent", "linkedin_account_id": account_id},
            )
            self._increment_managed_account_counters(account_id=account_id, connect_delta=0, new_threads_delta=1, replies_delta=0)
            self._record_outreach_account_event(
                event_key=f"action:{action_id}:message_sent",
                account_id=account_id,
                event_type="message_sent",
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                details={
                    "action_id": action_id,
                    "action_type": action_type,
                    "delivery_mode": delivery_mode,
                },
            )
        elif delivery_mode == "connect_first" or self._is_connection_required_error(delivery):
            if not self._can_send_connect_request(account=account):
                retry_at = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
                self.db.release_outbound_action(
                    action_id=action_id,
                    not_before=retry_at,
                    error="connect_budget_reached",
                )
                return {
                    "action_id": action_id,
                    "conversation_id": conversation_id,
                    "candidate_id": candidate_id,
                    "delivery_status": "deferred",
                    "error": "connect_budget_reached",
                    "linkedin_account_id": account_id,
                }
            _, connect_message = self.outreach_agent.compose_connection_request(job=job, candidate=candidate)
            try:
                connect_request = provider.send_connection_request(
                    candidate_profile=candidate,
                    message=connect_message,
                )
            except Exception as exc:
                connect_request = {"sent": False, "provider": "unipile", "error": str(exc)}

            if connect_request.get("sent"):
                delivery_status = "pending_connection"
                self.db.set_conversation_linkedin_account(conversation_id=conversation_id, account_id=account_id)
                self.db.update_conversation_status(conversation_id=conversation_id, status="waiting_connection")
                self.db.update_candidate_match_status(
                    job_id=job_id,
                    candidate_id=candidate_id,
                    status="outreach_pending_connection",
                    extra_notes={
                        "outreach_state": "waiting_connection",
                        "connect_request": connect_request,
                        "linkedin_account_id": account_id,
                    },
                )
                self._increment_managed_account_counters(account_id=account_id, connect_delta=1, new_threads_delta=0, replies_delta=0)
                self._record_outreach_account_event(
                    event_key=f"action:{action_id}:connect_sent",
                    account_id=account_id,
                    event_type="connect_sent",
                    job_id=job_id,
                    candidate_id=candidate_id,
                    conversation_id=conversation_id,
                    details={
                        "action_id": action_id,
                        "action_type": action_type,
                        "delivery_mode": delivery_mode,
                    },
                )
            else:
                connect_retry_error = self._connect_request_retry_error(connect_request)
                if connect_retry_error:
                    retry_at = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
                    self.db.release_outbound_action(
                        action_id=action_id,
                        not_before=retry_at,
                        error=connect_retry_error,
                    )
                    self.db.log_operation(
                        operation="agent.outreach.connect_request",
                        status="partial",
                        entity_type="candidate",
                        entity_id=str(candidate_id),
                        details={
                            "job_id": job_id,
                            "connect_request": connect_request,
                            "delivery": delivery,
                            "retry_at": retry_at,
                            "reason": connect_retry_error,
                        },
                    )
                    return {
                        "action_id": action_id,
                        "conversation_id": conversation_id,
                        "candidate_id": candidate_id,
                        "delivery_status": "deferred",
                        "error": connect_retry_error,
                        "retry_at": retry_at,
                        "delivery_mode": delivery_mode,
                        "planned_action_kind": planned_action_kind,
                        "connect_request": connect_request,
                        "linkedin_account_id": account_id,
                    }
                terminal_connect_error = self._connect_request_terminal_error(connect_request)
                if terminal_connect_error:
                    self._suppress_pre_resume_recovery(
                        conversation_id=conversation_id,
                        reason=terminal_connect_error,
                    )
                delivery_status = "failed"
                self.db.log_operation(
                    operation="agent.outreach.connect_request",
                    status="error",
                    entity_type="candidate",
                    entity_id=str(candidate_id),
                    details={"job_id": job_id, "connect_request": connect_request, "delivery": delivery},
                )
        else:
            delivery_status = "failed"
            if self._is_provider_limit_error(delivery):
                self._exclude_linkedin_account_from_selection_state(
                    selection_state=selection_state,
                    account_id=account_id,
                )
            self.db.log_operation(
                operation="agent.outreach.delivery_error",
                status="error",
                entity_type="candidate",
                entity_id=str(candidate_id),
                details={"job_id": job_id, "delivery": delivery},
            )
            self._record_outreach_account_event(
                event_key=f"action:{action_id}:message_failed",
                account_id=account_id,
                event_type="message_failed",
                job_id=job_id,
                candidate_id=candidate_id,
                conversation_id=conversation_id,
                details={
                    "action_id": action_id,
                    "action_type": action_type,
                    "delivery_mode": delivery_mode,
                    "error": str(delivery.get("error") or delivery.get("reason") or "delivery_failed"),
                },
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
                "delivery_mode": delivery_mode,
                "planned_action_kind": planned_action_kind,
                "connect_request": connect_request,
                "pending_delivery": delivery_status == "pending_connection",
                "request_resume": request_resume,
                "screening_status": screening_status or None,
                "pre_resume_session_id": pre_resume_session_id,
                "external_chat_id": external_chat_id or None,
                "chat_binding": chat_binding,
                "linkedin_account_id": account_id,
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
                "delivery_mode": delivery_mode,
                "planned_action_kind": planned_action_kind,
                "connect_request": connect_request,
                "request_resume": request_resume,
                "screening_status": screening_status or None,
                "pre_resume_session_id": pre_resume_session_id,
                "external_chat_id": external_chat_id or None,
                "chat_binding": chat_binding,
                "linkedin_account_id": account_id,
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
        self.db.complete_outbound_action(
            action_id=action_id,
            status="completed" if delivery_status in {"sent", "pending_connection"} else "failed",
            account_id=account_id,
            result={
                "delivery_status": delivery_status,
                "delivery": delivery,
                "delivery_mode": delivery_mode,
                "planned_action_kind": planned_action_kind,
                "connect_request": connect_request,
                "external_chat_id": external_chat_id or None,
                "chat_binding": chat_binding,
                "linkedin_account_id": account_id,
            },
            error=None if delivery_status in {"sent", "pending_connection"} else "delivery_failed",
        )
        return {
            "action_id": action_id,
            "conversation_id": conversation_id,
            "candidate_id": candidate_id,
            "delivery_status": delivery_status,
            "delivery": delivery,
            "delivery_mode": delivery_mode,
            "planned_action_kind": planned_action_kind,
            "connect_request": connect_request,
            "external_chat_id": external_chat_id or None,
            "chat_binding": chat_binding,
            "linkedin_account_id": account_id,
        }

    def _managed_linkedin_available(self) -> bool:
        return self.managed_linkedin_enabled and bool(self.managed_unipile_api_key)

    def _build_managed_provider(self, account_id: str) -> UnipileLinkedInProvider:
        return UnipileLinkedInProvider(
            api_key=self.managed_unipile_api_key,
            base_url=self.managed_unipile_base_url,
            account_id=account_id,
            timeout_seconds=self.managed_unipile_timeout_seconds,
        )

    def _outreach_priority(self, match: Dict[str, Any] | None) -> int:
        raw = (match or {}).get("score")
        try:
            score = float(raw)
        except (TypeError, ValueError):
            score = 0.0
        if score <= 1.0:
            score = score * 100.0
        return int(max(0.0, min(100.0, score)) * 100)

    def _build_linkedin_account_selection_state(self, *, job_id: int) -> Dict[str, Any]:
        rows, routing_error = self._connected_linkedin_accounts_for_job(job_id=job_id)
        day = self._utc_day_key()
        week_start = self._utc_week_start_key()
        projected_counts: Dict[int, int] = {}
        projected_loads: Dict[int, int] = {}
        projected_connect_remaining: Dict[int, int] = {}
        daily_caps: Dict[int, int] = {}
        normalized_rows: List[Dict[str, Any]] = []
        account_ids: List[int] = []
        for row in rows:
            account_id = int(row.get("id") or 0)
            if account_id <= 0:
                continue
            account_ids.append(account_id)
            counters = self.db.get_linkedin_account_daily_counter(account_id=account_id, day_utc=day)
            weekly_counters = self.db.get_linkedin_account_weekly_counter(account_id=account_id, week_start_utc=week_start)
            projected_counts[account_id] = int(counters.get("new_threads_sent") or 0)
            daily_caps[account_id] = effective_daily_message_limit(row, self.linkedin_outreach_policy)
            projected_connect_remaining[account_id] = self._remaining_connect_capacity(
                account=row,
                daily_counters=counters,
                weekly_counters=weekly_counters,
            )
            normalized_rows.append(row)
        workloads = self.db.summarize_linkedin_account_workload(account_ids=account_ids)
        for account_id in account_ids:
            projected_loads[account_id] = int((workloads.get(account_id) or {}).get("total_load") or 0)
        return {
            "job_id": int(job_id),
            "routing_error": routing_error,
            "rows": normalized_rows,
            "projected_counts": projected_counts,
            "projected_loads": projected_loads,
            "projected_connect_remaining": projected_connect_remaining,
            "daily_caps": daily_caps,
            "excluded_account_ids": set(),
        }

    @staticmethod
    def _find_linkedin_account_in_selection_state(
        *,
        selection_state: Dict[str, Any] | None,
        account_id: int,
    ) -> Dict[str, Any] | None:
        if selection_state is None or int(account_id) <= 0:
            return None
        rows = selection_state.get("rows") if isinstance(selection_state.get("rows"), list) else []
        excluded_account_ids = (
            selection_state.get("excluded_account_ids")
            if isinstance(selection_state.get("excluded_account_ids"), set)
            else set()
        )
        if int(account_id) in excluded_account_ids:
            return None
        for row in rows:
            if int(row.get("id") or 0) == int(account_id):
                return row
        return None

    @staticmethod
    def _exclude_linkedin_account_from_selection_state(
        *,
        selection_state: Dict[str, Any] | None,
        account_id: int,
    ) -> None:
        if selection_state is None or int(account_id) <= 0:
            return
        excluded = selection_state.get("excluded_account_ids")
        if not isinstance(excluded, set):
            excluded = set()
            selection_state["excluded_account_ids"] = excluded
        excluded.add(int(account_id))

    def _select_linkedin_account_for_new_thread(
        self,
        *,
        job_id: int,
        selection_state: Dict[str, Any] | None = None,
        planned_action_kind: str | None = None,
    ) -> tuple[Dict[str, Any] | None, str | None]:
        if selection_state is None:
            selection_state = self._build_linkedin_account_selection_state(job_id=job_id)
        rows = selection_state.get("rows") if isinstance(selection_state.get("rows"), list) else []
        routing_error = str(selection_state.get("routing_error") or "").strip() or None
        if not rows:
            return None, routing_error or "no_connected_account"
        normalized_kind = str(planned_action_kind or "").strip().lower() or "connect_request"
        projected_counts = (
            selection_state.get("projected_counts") if isinstance(selection_state.get("projected_counts"), dict) else {}
        )
        projected_loads = (
            selection_state.get("projected_loads") if isinstance(selection_state.get("projected_loads"), dict) else {}
        )
        projected_connect_remaining = (
            selection_state.get("projected_connect_remaining")
            if isinstance(selection_state.get("projected_connect_remaining"), dict)
            else {}
        )
        daily_caps = selection_state.get("daily_caps") if isinstance(selection_state.get("daily_caps"), dict) else {}
        excluded_account_ids = (
            selection_state.get("excluded_account_ids")
            if isinstance(selection_state.get("excluded_account_ids"), set)
            else set()
        )
        eligible: List[tuple[int, int, int, Dict[str, Any]]] = []
        for row in rows:
            account_id = int(row.get("id") or 0)
            if account_id <= 0:
                continue
            if account_id in excluded_account_ids:
                continue
            sent = int(projected_counts.get(account_id) or 0)
            load = int(projected_loads.get(account_id) or 0)
            daily_cap = int(daily_caps.get(account_id) or 0)
            if normalized_kind == "connect_request":
                connect_remaining = int(projected_connect_remaining.get(account_id) or 0)
                if connect_remaining <= 0:
                    continue
            else:
                if sent >= daily_cap:
                    continue
            eligible.append((load, sent, account_id, row))
        if not eligible:
            if normalized_kind == "connect_request":
                return None, "connect_budget_reached"
            return None, "daily_new_threads_budget_reached"
        eligible.sort(key=lambda item: (item[0], item[1], item[2]))
        _, _, selected_account_id, selected_row = eligible[0]
        if normalized_kind == "connect_request":
            projected_connect_remaining[selected_account_id] = max(
                0,
                int(projected_connect_remaining.get(selected_account_id) or 0) - 1,
            )
        else:
            projected_counts[selected_account_id] = int(projected_counts.get(selected_account_id) or 0) + 1
        projected_loads[selected_account_id] = int(projected_loads.get(selected_account_id) or 0) + 1
        selection_state["projected_counts"] = projected_counts
        selection_state["projected_loads"] = projected_loads
        selection_state["projected_connect_remaining"] = projected_connect_remaining
        return selected_row, None

    def _resolve_linkedin_account_for_outbound_action(
        self,
        *,
        row: Dict[str, Any],
        job_id: int,
        selection_state: Dict[str, Any] | None = None,
        planned_action_kind: str | None = None,
    ) -> tuple[Dict[str, Any] | None, str | None]:
        assigned_account_id = int(row.get("account_id") or 0)
        if assigned_account_id > 0:
            assigned = self._find_linkedin_account_in_selection_state(
                selection_state=selection_state,
                account_id=assigned_account_id,
            )
            if assigned is not None:
                return assigned, None
        return self._select_linkedin_account_for_new_thread(
            job_id=job_id,
            selection_state=selection_state,
            planned_action_kind=planned_action_kind,
        )

    def preview_linkedin_account_sequence_for_new_threads(
        self,
        *,
        job_id: int,
        slots: int,
    ) -> Dict[str, Any]:
        safe_slots = max(0, min(int(slots or 0), 200))
        selection_state = self._build_linkedin_account_selection_state(job_id=job_id)
        rows = selection_state.get("rows") if isinstance(selection_state.get("rows"), list) else []
        routing_error = str(selection_state.get("routing_error") or "").strip() or None
        if not rows:
            return {"items": [], "reason": routing_error or "no_connected_account"}
        items: List[Dict[str, Any]] = []
        reason = "ok"
        for _ in range(safe_slots):
            row, selection_error = self._select_linkedin_account_for_new_thread(
                job_id=job_id,
                selection_state=selection_state,
            )
            if row is None:
                reason = selection_error or "daily_new_threads_budget_reached"
                break
            account_id = int(row.get("id") or 0)
            projected_counts = (
                selection_state.get("projected_counts") if isinstance(selection_state.get("projected_counts"), dict) else {}
            )
            projected_loads = (
                selection_state.get("projected_loads") if isinstance(selection_state.get("projected_loads"), dict) else {}
            )
            daily_caps = selection_state.get("daily_caps") if isinstance(selection_state.get("daily_caps"), dict) else {}
            items.append(
                {
                    "account_id": account_id,
                    "label": str(row.get("label") or "").strip() or f"Account {account_id}",
                    "provider_account_id": str(row.get("provider_account_id") or ""),
                    "daily_cap": int(daily_caps.get(account_id) or 0),
                    "projected_new_threads_sent": int(projected_counts.get(account_id) or 0),
                    "projected_load": int(projected_loads.get(account_id) or 0),
                }
            )
        return {"items": items, "reason": reason}

    def _connected_linkedin_accounts_for_job(self, *, job_id: int) -> tuple[List[Dict[str, Any]], str | None]:
        job = self.db.get_job(job_id)
        routing_mode = str((job or {}).get("linkedin_routing_mode") or "auto").strip().lower()
        if routing_mode not in {"auto", "manual"}:
            routing_mode = "auto"
        if routing_mode == "manual":
            assigned_ids = self.db.list_job_linkedin_account_ids(job_id=job_id)
            if not assigned_ids:
                return [], "manual_no_assigned_accounts"
            rows = [
                row
                for row in self.db.list_job_linkedin_accounts(job_id=job_id, status="connected")
                if self._is_operational_linkedin_account(row)
            ]
            if not rows:
                return [], "manual_assigned_accounts_not_connected"
            return rows, None
        rows = self._list_dispatchable_linkedin_accounts(limit=500)
        if not rows:
            return [], "no_connected_accounts"
        return rows, None

    def _remaining_connect_capacity(
        self,
        *,
        account: Dict[str, Any],
        daily_counters: Dict[str, Any] | None = None,
        weekly_counters: Dict[str, Any] | None = None,
    ) -> int:
        account_id = int(account.get("id") or 0)
        if account_id <= 0:
            return 0
        daily = daily_counters or self.db.get_linkedin_account_daily_counter(account_id=account_id, day_utc=self._utc_day_key())
        weekly = weekly_counters or self.db.get_linkedin_account_weekly_counter(
            account_id=account_id,
            week_start_utc=self._utc_week_start_key(),
        )
        daily_connect_sent = int((daily or {}).get("connect_sent") or 0)
        weekly_connect_sent = int((weekly or {}).get("connect_sent") or 0)
        weekly_cap = self._policy_weekly_connect_cap()
        allowed_today = effective_daily_connect_limit(account, self.linkedin_outreach_policy)
        return max(0, min(max(0, weekly_cap - weekly_connect_sent), max(0, allowed_today - daily_connect_sent)))

    def _can_send_connect_request(self, account: Dict[str, Any]) -> bool:
        return self._remaining_connect_capacity(account=account) > 0

    def _increment_managed_account_counters(
        self,
        *,
        account_id: int,
        connect_delta: int,
        new_threads_delta: int,
        replies_delta: int,
    ) -> None:
        self.db.increment_linkedin_account_counters(
            account_id=account_id,
            day_utc=self._utc_day_key(),
            week_start_utc=self._utc_week_start_key(),
            connect_delta=connect_delta,
            new_threads_delta=new_threads_delta,
            replies_delta=replies_delta,
        )

    def _policy_daily_new_threads_cap(self) -> int:
        return policy_daily_new_threads_cap(self.linkedin_outreach_policy)

    def _policy_weekly_connect_cap(self) -> int:
        return policy_weekly_connect_cap(self.linkedin_outreach_policy)

    def _policy_allowed_connects_today(self, account: Dict[str, Any]) -> int:
        return policy_allowed_connects_today(self.linkedin_outreach_policy, account)

    def _conversation_supports_direct_message(self, conversation: Dict[str, Any] | None) -> bool:
        if not isinstance(conversation, dict):
            return False
        if str(conversation.get("external_chat_id") or "").strip():
            return True
        conversation_status = str(conversation.get("status") or "").strip().lower()
        if conversation_status == "active" and int(conversation.get("linkedin_account_id") or 0) > 0:
            return True
        conversation_id = int(conversation.get("id") or 0)
        if conversation_id <= 0:
            return False
        for message in self.db.list_messages(conversation_id):
            if str(message.get("direction") or "").strip().lower() == "inbound":
                return True
            meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
            if not isinstance(meta, dict):
                continue
            delivery = meta.get("delivery") if isinstance(meta.get("delivery"), dict) else {}
            connect_request = meta.get("connect_request") if isinstance(meta.get("connect_request"), dict) else {}
            delivery_status = str(meta.get("delivery_status") or "").strip().lower()
            if bool(delivery.get("sent")) or delivery_status == "sent":
                return True
            if bool(connect_request.get("accepted")):
                return True
        return False

    def _determine_initial_outreach_delivery_mode(
        self,
        *,
        action_type: str,
        conversation: Dict[str, Any] | None,
    ) -> str:
        normalized_action_type = str(action_type or "").strip().lower()
        if not isinstance(conversation, dict):
            return "connect_first"
        if self._conversation_supports_direct_message(conversation):
            return "message_first"
        conversation_status = str(conversation.get("status") or "").strip().lower()
        if conversation_status == "waiting_connection":
            return "message_first"
        if normalized_action_type == "pre_resume_recovery":
            return "connect_first"
        return "connect_first"

    @staticmethod
    def _planned_action_kind_for_delivery_mode(delivery_mode: str) -> str:
        normalized = str(delivery_mode or "").strip().lower()
        return "connect_request" if normalized == "connect_first" else "message"

    def _record_outreach_account_event(
        self,
        *,
        event_key: str,
        account_id: int,
        event_type: str,
        job_id: int | None = None,
        candidate_id: int | None = None,
        conversation_id: int | None = None,
        details: Dict[str, Any] | None = None,
        created_at: str | None = None,
    ) -> bool:
        normalized_account_id = int(account_id or 0)
        if normalized_account_id <= 0:
            return False
        return self.db.insert_outreach_account_event(
            event_key=event_key,
            account_id=normalized_account_id,
            event_type=event_type,
            job_id=job_id,
            candidate_id=candidate_id,
            conversation_id=conversation_id,
            details=details,
            created_at=created_at,
        )

    @staticmethod
    def _normalize_pre_resume_public_status(status: Any) -> str:
        normalized = str(status or "").strip().lower()
        if normalized in {"cv_received_pending_answers", "ready_for_interview", "ready_for_screening_call"}:
            return "resume_received"
        return normalized

    @staticmethod
    def _match_status_from_prescreen_status(prescreen_status: Any) -> str | None:
        normalized = str(prescreen_status or "").strip().lower()
        if normalized == "ready_for_screening_call":
            normalized = "ready_for_interview"
        if normalized == "ready_for_cv":
            return "must_have_approved"
        if normalized == "cv_received_pending_answers":
            return "resume_received_pending_must_have"
        if normalized == "ready_for_interview":
            return "resume_received"
        return None

    @classmethod
    def _public_pre_resume_state(cls, state: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not isinstance(state, dict):
            return state
        out = dict(state)
        out["status"] = cls._normalize_pre_resume_public_status(out.get("status"))
        return out

    @classmethod
    def _public_pre_resume_session(cls, row: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not isinstance(row, dict):
            return row
        out = dict(row)
        out["status"] = cls._normalize_pre_resume_public_status(out.get("status"))
        if isinstance(out.get("state_json"), dict):
            out["state_json"] = cls._public_pre_resume_state(out.get("state_json"))
        return out

    @staticmethod
    def _utc_day_key() -> str:
        return datetime.now(timezone.utc).date().isoformat()

    @staticmethod
    def _utc_week_start_key() -> str:
        now = datetime.now(timezone.utc).date()
        monday = now - timedelta(days=now.weekday())
        return monday.isoformat()

    def _compose_linkedin_outreach_message(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        language: str,
        fallback_message: str,
        request_resume: bool,
        state: Dict[str, Any] | None,
        conversation: Dict[str, Any] | None = None,
    ) -> str:
        recruiter_name = self._linkedin_recruiter_name(conversation=conversation)
        fallback = str(fallback_message or "").strip() or self._linkedin_initial_fallback_message(
            job=job,
            recruiter_name=recruiter_name,
            request_resume=request_resume,
        )
        if not fallback:
            return ""
        instruction = self._linkedin_generation_instruction(kind="initial", recruiter_name=recruiter_name, language=language)
        generated = self._maybe_llm_reply(
            mode="linkedin_outreach",
            instruction=instruction,
            job=job,
            candidate=candidate,
            inbound_text="",
            history=[],
            fallback_reply=fallback,
            language=language,
            state=state,
        )
        return self._ensure_outreach_requirements(text=generated, fallback=fallback)

    def _compose_linkedin_followup_message(
        self,
        *,
        job: Dict[str, Any],
        candidate: Dict[str, Any],
        language: str,
        history: List[Dict[str, str]],
        state: Dict[str, Any] | None,
        fallback_message: str,
        conversation: Dict[str, Any] | None = None,
    ) -> str:
        recruiter_name = self._linkedin_recruiter_name(conversation=conversation)
        fallback = str(fallback_message or "").strip() or self._linkedin_followup_fallback_message(
            job=job,
            recruiter_name=recruiter_name,
        )
        if not fallback:
            return ""
        instruction = self._linkedin_generation_instruction(kind="followup", recruiter_name=recruiter_name, language=language)
        return self._maybe_llm_reply(
            mode="linkedin_followup",
            instruction=instruction,
            job=job,
            candidate=candidate,
            inbound_text="",
            history=history,
            fallback_reply=fallback,
            language=language,
            state=state,
        )

    def _linkedin_initial_fallback_message(self, *, job: Dict[str, Any], recruiter_name: str, request_resume: bool) -> str:
        position = str(job.get("title") or "AI Engineer").strip() or "AI Engineer"
        company = str(job.get("company") or "").strip()
        role_owner = (
            f"for a long term project with {company}, a fast moving US AI startup"
            if company
            else "for a long term project with a fast moving US AI startup"
        )
        core_profile = self.outreach_agent.matching_engine.build_core_profile(job)
        skills = core_profile.get("core_skills") if isinstance(core_profile.get("core_skills"), list) else []
        skills_text = ", ".join(str(x) for x in skills[:7] if str(x).strip())
        skills_line = f"\nMain stack in focus is {skills_text}" if skills_text else ""
        ask_line = (
            "If this sounds relevant, first we'll ask up to three written qualifying questions, then request your CV, "
            "then move to a short 10 to 15 minute screening call"
            if request_resume
            else "If this sounds relevant, send a short reply and we can share the next steps"
        )
        sign_block = (
            "Best,\n"
            f"{recruiter_name}\n"
            "Senior Talent Acquisition Manager at Tener"
            if recruiter_name
            else "Best,\nSenior Talent Acquisition Manager at Tener"
        )
        return (
            "Greetings,\n"
            f"We're Tener, and we're now looking for a {position} {role_owner}\n"
            "You'll work directly with the Founder and CTO on an autonomous coding agent, designing real agentic workflows, "
            f"RAG pipelines, LLM orchestration, and scalable ML infrastructure{skills_line}\n\n"
            f"{ask_line}\n\n"
            f"{sign_block}"
        )

    @staticmethod
    def _linkedin_followup_fallback_message(*, job: Dict[str, Any], recruiter_name: str) -> str:
        position = str(job.get("title") or "the role").strip() or "the role"
        sign_block = (
            "Warm regards,\n"
            f"{recruiter_name}\n"
            "Senior Talent Acquisition Manager at Tener"
            if recruiter_name
            else "Warm regards,\nSenior Talent Acquisition Manager at Tener"
        )
        return (
            "Hey,\n"
            f"If {position} isn't quite what you're looking for right now, maybe someone from your network could be a good fit\n"
            "Either way, I'd really appreciate a short reply, just to know where things stand\n\n"
            f"{sign_block}"
        )

    @staticmethod
    def _linkedin_generation_instruction(*, kind: str, recruiter_name: str, language: str) -> str:
        normalized_kind = str(kind or "").strip().lower()
        normalized_recruiter = str(recruiter_name or "").strip()
        recruiter_context = normalized_recruiter if normalized_recruiter else "[none]"
        style_rules = WorkflowService._linkedin_style_rules()
        if normalized_kind == "interview_invite":
            return (
                "Generate one LinkedIn message as plain text with paragraph breaks.\n"
                f"Write in language: {language}.\n"
                f"Recruiter name context: {recruiter_context}\n"
                "Goal: candidate already agreed to quick pre vetting, now send interview link in one natural message.\n"
                "Required structure:\n"
                "1) First line must be exactly: Hey,\n"
                "2) Friendly short acknowledgement\n"
                "3) Share the interview link exactly as provided in context\n"
                "4) Ask for a short reply once finished\n"
                "Do not ask the candidate to repeat consent phrase.\n"
                "Do not force corporate tone.\n"
                "Do not invent recruiter names.\n"
                "Style rules:\n"
                f"{style_rules}\n"
                "Adapt wording to context while preserving this structure and intent."
            )
        if normalized_kind == "interview_followup":
            return (
                "Generate one LinkedIn follow up message as plain text with paragraph breaks.\n"
                f"Write in language: {language}.\n"
                f"Recruiter name context: {recruiter_context}\n"
                "Goal: remind about quick pre vetting link in a casual way.\n"
                "Required structure:\n"
                "1) Very short check in\n"
                "2) Include interview link exactly as provided in context\n"
                "3) Ask if help is needed or ask for quick status\n"
                "Do not sound pushy.\n"
                "Do not invent recruiter names.\n"
                "Style rules:\n"
                f"{style_rules}\n"
                "Adapt wording to context while preserving this structure and intent."
            )
        if normalized_kind == "followup":
            signature_rule = (
                "4) Optional signature block:\n"
                "Warm regards,\n"
                f"{normalized_recruiter}\n"
                "Senior Talent Acquisition Manager at Tener\n"
                if normalized_recruiter
                else "4) Signature is optional and should not include a recruiter name when none is provided\n"
            )
            return (
                "Generate one LinkedIn follow up message as plain text with paragraph breaks.\n"
                f"Write in language: {language}.\n"
                f"Recruiter name context: {recruiter_context}\n"
                "Required structure:\n"
                "1) Start with a short greeting line like Hey,\n"
                "2) Mention the role may not be a fit and ask for referral from the network\n"
                "3) Ask for a short reply on current status\n"
                f"{signature_rule}"
                "Do not invent recruiter names.\n"
                "Style rules:\n"
                f"{style_rules}\n"
                "Adapt wording to job and candidate context while preserving this structure and intent."
            )
        signature_rule = (
            "5) Signature block exactly as:\n"
            "Best,\n"
            f"{normalized_recruiter}\n"
            "Senior Talent Acquisition Manager at Tener\n"
            if normalized_recruiter
            else "5) Signature is optional and should not include a recruiter name when none is provided\n"
        )
        return (
            "Generate one first LinkedIn outreach message as plain text with paragraph breaks.\n"
            f"Write in language: {language}.\n"
            f"Recruiter name context: {recruiter_context}\n"
            "Required structure:\n"
            "1) Start with Greetings,\n"
            "2) Mention Tener and that we are hiring for the specific position for a long term project with a fast moving US AI startup\n"
            "3) Mention direct collaboration with Founder and CTO on autonomous coding agent, agentic workflows, RAG pipelines, LLM orchestration, and scalable ML infrastructure\n"
            "4) Explain the process exactly as: first a few written qualifying questions, then CV, then a short 10 to 15 minute screening call\n"
            f"{signature_rule}"
            "Do not invent recruiter names.\n"
            "Style rules:\n"
            f"{style_rules}\n"
            "Adapt vocabulary and concrete details to the job and candidate context while preserving this structure and intent."
        )

    def _linkedin_recruiter_name(self, *, conversation: Dict[str, Any] | None = None) -> str:
        account_id = self._safe_int((conversation or {}).get("linkedin_account_id"), 0)
        if account_id > 0:
            account = self.db.get_linkedin_account(account_id)
            if account:
                label = str(account.get("label") or "").strip()
                if label:
                    return label
                metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
                profile_like = metadata.get("profile") if isinstance(metadata.get("profile"), dict) else {}
                remote_like = metadata.get("remote") if isinstance(metadata.get("remote"), dict) else {}
                for bucket in (profile_like, remote_like, metadata):
                    candidate_name = self._pick_name_from_metadata(bucket)
                    if candidate_name:
                        return candidate_name
        configured = str(os.environ.get("TENER_LINKEDIN_RECRUITER_NAME", "")).strip()
        return configured

    @staticmethod
    def _pick_name_from_metadata(payload: Dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return ""
        direct = (
            payload.get("full_name")
            or payload.get("fullName")
            or payload.get("display_name")
            or payload.get("displayName")
            or payload.get("name")
        )
        direct_name = str(direct or "").strip()
        if direct_name:
            return direct_name
        first = str(payload.get("first_name") or payload.get("firstName") or "").strip()
        last = str(payload.get("last_name") or payload.get("lastName") or "").strip()
        joined = " ".join(part for part in (first, last) if part).strip()
        return joined

    @staticmethod
    def _linkedin_style_rules() -> str:
        return (
            "Use an informal human tone.\n"
            "Do not end paragraphs with a period.\n"
            "Sometimes allow minor typos.\n"
            "Use a little slang, not too much.\n"
            "Sometimes keep it very short.\n"
            "Do not make the message perfectly structured.\n"
            "Avoid perfect logical transitions.\n"
            "Do not use template phrases like As an AI or Let me clarify.\n"
            "Vary rhythm from longer text to one sentence.\n"
            "Sometimes include reactions like hm, mmm, hard, lol, interesting.\n"
            "Sometimes ask casually for clarification like what exactly do you mean.\n"
            "Allow slight incompleteness sometimes.\n"
            "Do not provide exhaustive lists unless asked.\n"
            "Do not sound like Wikipedia.\n"
            "Sometimes acknowledge uncertainty, for example not sure or I may be wrong.\n"
            "Use conversation context, do not answer from a blank slate.\n"
            "Do not be overly polite.\n"
            "Do not use emoji in every message.\n"
            "Sometimes use dry humor or light sarcasm.\n"
            "Avoid heavy empathy in every reply.\n"
            "Do not force perfect conclusions at the end.\n"
            "Sometimes reply fast and direct.\n"
            "Sometimes show delayed logic instead of instant total understanding.\n"
            "Avoid generic universal advice.\n"
            "Sometimes be slightly subjective.\n"
            "Do not explain obvious things.\n"
            "Do not summarize every message.\n"
            "Do not try to be useful at 110 percent every time.\n"
            "Never use hyphen or dash punctuation outside URLs.\n"
            "Never use double dashes."
        )

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

        normalized_mode = str(mode or "").strip().lower()
        multiline_modes = {
            "linkedin_outreach",
            "linkedin_followup",
            "linkedin_interview_invite",
            "linkedin_interview_followup",
        }
        if normalized_mode in multiline_modes:
            final_text = self._sanitize_multiline_reply_text(generated_text, limit=1400)
        else:
            final_text = self._sanitize_reply_text(generated_text)
        if normalized_mode in {"linkedin_outreach", "linkedin_followup"}:
            final_text = self._sanitize_recruiter_outbound_text(final_text)
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
        if normalized_mode == "linkedin_outreach":
            guarded = self._ensure_outreach_requirements(text=final_text, fallback=fallback)
            if guarded != final_text:
                final_text = guarded
                source = "llm_guarded" if source == "llm" else source
                reason = "outreach_requirements_enforced"
            final_text = self._sanitize_recruiter_outbound_text(final_text)

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
    def _sanitize_multiline_reply_text(text: str, limit: int = 1400) -> str:
        raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not raw:
            return ""
        normalized_lines: List[str] = []
        previous_blank = False
        for line in raw.split("\n"):
            collapsed = " ".join(line.split()).strip()
            if not collapsed:
                if previous_blank:
                    continue
                normalized_lines.append("")
                previous_blank = True
                continue
            normalized_lines.append(collapsed)
            previous_blank = False
        normalized = "\n".join(normalized_lines).strip()
        if len(normalized) <= limit:
            return normalized
        clipped = normalized[:limit].rstrip()
        if "\n" in clipped:
            clipped = clipped.rsplit("\n", 1)[0].rstrip()
        if " " in clipped:
            clipped = clipped.rsplit(" ", 1)[0].rstrip()
        return clipped

    @classmethod
    def _sanitize_recruiter_outbound_text(cls, text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        url_pattern = re.compile(r"https?://[^\s)>\"]+", flags=re.IGNORECASE)
        parts: List[str] = []
        cursor = 0
        for match in url_pattern.finditer(raw):
            parts.append(cls._sanitize_dash_text_segment(raw[cursor : match.start()]))
            parts.append(match.group(0))
            cursor = match.end()
        parts.append(cls._sanitize_dash_text_segment(raw[cursor:]))
        normalized = "".join(parts).strip()
        if not normalized:
            return ""
        normalized_lines: List[str] = []
        previous_blank = False
        for line in normalized.split("\n"):
            collapsed = " ".join(line.split()).strip()
            if not collapsed:
                if previous_blank:
                    continue
                normalized_lines.append("")
                previous_blank = True
                continue
            normalized_lines.append(collapsed)
            previous_blank = False
        return "\n".join(normalized_lines).strip()

    @staticmethod
    def _sanitize_dash_text_segment(text: str) -> str:
        segment = str(text or "")
        if not segment:
            return ""
        sanitized = re.sub(r"(\d)\s*[-\u2010-\u2015\u2212]{1,2}\s*(\d)", r"\1 to \2", segment)
        sanitized = re.sub(r"\s*[-\u2010-\u2015\u2212]{2,}\s*", " ", sanitized)
        sanitized = re.sub(r"[-\u2010-\u2015\u2212]", " ", sanitized)
        return sanitized

    @staticmethod
    def _contains_template_placeholders(text: str) -> bool:
        return bool(re.search(r"\{[a-zA-Z_][^{}]{0,40}\}", str(text or "")))

    @staticmethod
    def _should_require_resume_cta(state: Dict[str, Any] | None) -> bool:
        if not isinstance(state, dict):
            return False
        status = str(state.get("status") or "").strip().lower()
        prescreen_status = str(state.get("prescreen_status") or "").strip().lower()
        if bool(state.get("cv_received")):
            return False
        if status in TERMINAL_PRE_RESUME_STATUSES:
            return False
        return prescreen_status not in {"ready_for_interview", "ready_for_screening_call"}

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

    @staticmethod
    def _has_salary_expectation_cta(text: str) -> bool:
        lowered = str(text or "").lower()
        markers = (
            "salary expectation",
            "salary expectations",
            "compensation expectation",
            "compensation expectations",
            "salary",
            "compensation",
        )
        return any(marker in lowered for marker in markers)

    @classmethod
    def _ensure_outreach_requirements(cls, text: str, fallback: str) -> str:
        result = str(text or "").strip()
        if not result:
            result = str(fallback or "").strip()
        process_line = "First we'll ask a few written qualifying questions, then request your CV, then a short 10 to 15 minute screening call."
        if "written qualifying" not in result.lower() and "screening call" not in result.lower():
            result = f"{result}\n\n{process_line}".strip()
        return result

    @staticmethod
    def _ensure_interview_url(text: str, fallback: str, entry_url: str) -> str:
        message = str(text or "").strip()
        link = str(entry_url or "").strip()
        if not message:
            message = str(fallback or "").strip()
        if not link:
            return message
        if link in message:
            return message
        fallback_text = str(fallback or "").strip()
        if link in fallback_text:
            return fallback_text
        if message:
            return f"{message}\n{link}".strip()
        return link

    @staticmethod
    def _ensure_candidate_greeting(text: str, fallback: str) -> str:
        message = str(text or "").strip()
        fallback_text = str(fallback or "").strip()
        greeting = "Hey,"
        base = message or fallback_text
        if not base:
            return greeting
        lines = [line.strip() for line in str(base).splitlines() if line.strip()]
        if not lines:
            return greeting
        lines[0] = greeting
        return "\n".join(lines).strip()

    @staticmethod
    def _candidate_greeting_name(candidate: Dict[str, Any]) -> str:
        raw = candidate.get("raw") if isinstance(candidate.get("raw"), dict) else {}
        detail = raw.get("detail") if isinstance(raw.get("detail"), dict) else {}
        search = raw.get("search") if isinstance(raw.get("search"), dict) else {}
        first = str(
            candidate.get("first_name")
            or detail.get("first_name")
            or search.get("first_name")
            or ""
        ).strip()
        last = str(
            candidate.get("last_name")
            or detail.get("last_name")
            or search.get("last_name")
            or ""
        ).strip()
        full = f"{first} {last}".strip()
        if not full:
            full = str(
                candidate.get("full_name")
                or detail.get("full_name")
                or search.get("full_name")
                or detail.get("name")
                or search.get("name")
                or ""
            ).strip()
        # LinkedIn testing profiles may append technical ids in parentheses.
        full = re.sub(r"\s*\([^)]*\)\s*$", "", full).strip()
        return full or "there"

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

    @staticmethod
    def _is_provider_limit_error(delivery: Dict[str, Any]) -> bool:
        if not isinstance(delivery, dict):
            return False
        if delivery.get("sent"):
            return False
        reason = str(delivery.get("reason") or "").lower()
        error = str(delivery.get("error") or "").lower()
        text = f"{reason} {error}"
        needles = (
            "limit_exceeded",
            "usage limit",
            "reached the usage limit",
            "rate limit",
            "too many requests",
        )
        return any(token in text for token in needles)

    @staticmethod
    def _connect_request_retry_error(connect_request: Dict[str, Any]) -> str | None:
        if not isinstance(connect_request, dict):
            return None
        if connect_request.get("sent"):
            return None
        reason = str(connect_request.get("reason") or "").lower()
        error = str(connect_request.get("error") or "").lower()
        text = f"{reason} {error}"
        if "cannot_resend_yet" in text or "cannot resend yet" in text:
            return "cannot_resend_yet"
        return None

    @staticmethod
    def _connect_request_terminal_error(connect_request: Dict[str, Any]) -> str | None:
        if not isinstance(connect_request, dict):
            return None
        if connect_request.get("sent"):
            return None
        reason = str(connect_request.get("reason") or "").lower()
        error = str(connect_request.get("error") or "").lower()
        text = f"{reason} {error}"
        if "invalid_candidate_identity" in text or "missing_attendee_provider_id" in text:
            return "invalid_candidate_identity"
        if "user id does not match provider's expected format" in text:
            return "invalid_candidate_identity"
        return None

    def _suppress_pre_resume_recovery(self, *, conversation_id: int, reason: str) -> None:
        session = self.db.get_pre_resume_session_by_conversation(conversation_id=conversation_id)
        if not session:
            return
        session_id = str(session.get("session_id") or "").strip()
        if not session_id:
            return
        try:
            state = json.loads(str(session.get("state_json") or "{}"))
            if not isinstance(state, dict):
                state = {}
        except json.JSONDecodeError:
            state = {}
        state["status"] = "delivery_blocked_identity"
        state["next_followup_at"] = None
        state["updated_at"] = utc_now_iso()
        state["last_error"] = reason
        self.db.upsert_pre_resume_session(
            session_id=session_id,
            conversation_id=conversation_id,
            job_id=int(session.get("job_id") or 0),
            candidate_id=int(session.get("candidate_id") or 0),
            state=state,
            instruction=str(session.get("instruction") or ""),
        )
        self.db.insert_pre_resume_event(
            session_id=session_id,
            conversation_id=conversation_id,
            event_type="system_delivery_blocked",
            intent=None,
            inbound_text=None,
            outbound_text=None,
            state_status="delivery_blocked_identity",
            details={"reason": reason},
        )

    def _deliver_pending_outreach_message(self, conversation_id: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        conversation = self.db.get_conversation(conversation_id)
        job = self.db.get_job(int((conversation or {}).get("job_id") or 0)) if conversation else None
        if job and self._job_is_paused(job):
            return {"sent": False, "reason": "job_paused"}
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
            conversation = self.db.get_conversation(conversation_id)
            if self._managed_linkedin_available() and conversation:
                account_id = int(conversation.get("linkedin_account_id") or 0)
                account = self.db.get_linkedin_account(account_id) if account_id > 0 else None
                provider_account_id = str((account or {}).get("provider_account_id") or "").strip()
                if provider_account_id:
                    provider = self._build_managed_provider(account_id=provider_account_id)
                    delivery = provider.send_message(candidate_profile=candidate, message=message)
                    if delivery.get("sent"):
                        self._increment_managed_account_counters(
                            account_id=account_id,
                            connect_delta=0,
                            new_threads_delta=0,
                            replies_delta=1,
                        )
                else:
                    delivery = self.sourcing_agent.send_outreach(candidate_profile=candidate, message=message)
            else:
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
                self._record_outreach_account_event(
                    event_key=f"message:{int(pending.get('id') or 0)}:message_sent",
                    account_id=int(conversation.get("linkedin_account_id") or 0),
                    event_type="message_sent",
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    conversation_id=conversation_id,
                    details={"source": "send_after_connection", "trigger": "connection_accepted"},
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
                candidate_language=resolve_conversation_language(
                    latest_message_text="",
                    previous_language=str(pending.get("candidate_language") or ""),
                    profile_languages=candidate.get("languages"),
                    fallback="en",
                ),
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
            conversation = self.db.get_conversation(conversation_id)
            if conversation:
                # job_candidates.status is the source of truth for funnel stage;
                # keep it aligned whenever the transport layer falls back to a
                # waiting-for-connection state.
                self.db.update_candidate_match_status(
                    job_id=int(conversation["job_id"]),
                    candidate_id=int(conversation["candidate_id"]),
                    status="outreach_pending_connection",
                    extra_notes={"outreach_state": "waiting_connection_retry"},
                )
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
        self._record_outreach_account_event(
            event_key=f"message:{int(pending.get('id') or 0)}:message_failed",
            account_id=int((conversation or {}).get("linkedin_account_id") or 0),
            event_type="message_failed",
            job_id=int((conversation or {}).get("job_id") or 0) or None,
            candidate_id=int((conversation or {}).get("candidate_id") or 0) or None,
            conversation_id=conversation_id,
            details={
                "source": "send_after_connection",
                "error": str(delivery.get("error") or delivery.get("reason") or "delivery_failed"),
            },
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

    def _exclude_forced_test_profiles(
        self,
        *,
        profiles: List[Dict[str, Any]],
        forced_identifiers: List[str],
    ) -> List[Dict[str, Any]]:
        if not forced_identifiers:
            return list(profiles)
        out: List[Dict[str, Any]] = []
        for profile in profiles:
            if not isinstance(profile, dict):
                continue
            if self._candidate_is_forced_test(
                candidate=profile,
                match=None,
                forced_identifiers=forced_identifiers,
            ):
                continue
            out.append(profile)
        return out

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

    def _candidate_is_forced_test(
        self,
        *,
        candidate: Dict[str, Any] | None,
        match: Dict[str, Any] | None,
        forced_identifiers: List[str],
    ) -> bool:
        if candidate and self._forced_test_identifier_for_profile(candidate, forced_identifiers):
            return True
        if match and self._forced_test_identifier_from_match(match, forced_identifiers):
            return True
        return False

    def _is_non_test_forced_candidate(
        self,
        *,
        candidate: Dict[str, Any] | None,
        match: Dict[str, Any] | None,
        forced_identifiers: List[str],
        forced_only: bool,
    ) -> bool:
        if forced_only:
            return False
        return self._candidate_is_forced_test(
            candidate=candidate,
            match=match,
            forced_identifiers=forced_identifiers,
        )

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
        company = str(job.get("company") or "").strip().lower()
        text = f"{title}\n{company}"
        return any(keyword in text for keyword in self.test_job_keywords)

    @staticmethod
    def _extract_attachment_text_from_provider_message(message: Dict[str, Any], limit: int = 8) -> str:
        payload = message.get("raw") if isinstance(message.get("raw"), dict) else message
        descriptors = extract_attachment_descriptors_from_values([payload], limit=limit)
        return descriptors_to_text(descriptors, limit=limit)

    @staticmethod
    def _extract_attachment_descriptors_from_provider_payload(payload: Any, limit: int = 12) -> List[Dict[str, Any]]:
        descriptors = extract_attachment_descriptors_from_values([payload], limit=limit)
        return [entry.to_dict() for entry in descriptors]

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

    @staticmethod
    def _job_state(job: Dict[str, Any] | None) -> str:
        if not isinstance(job, dict):
            return "active"
        state = str(job.get("job_state") or "").strip().lower()
        if state in {"active", "paused", "archived"}:
            return state
        if bool(job.get("is_archived")):
            return "archived"
        if bool(job.get("is_paused")):
            return "paused"
        return "active"

    def _job_is_paused(self, job: Dict[str, Any] | None) -> bool:
        return self._job_state(job) == "paused"

    def _job_is_archived(self, job: Dict[str, Any] | None) -> bool:
        return self._job_state(job) == "archived"

    def _assert_job_automation_allowed(self, job: Dict[str, Any], *, operation: str) -> None:
        state = self._job_state(job)
        if state == "paused":
            raise JobOperationBlockedError(
                f"Job {int(job.get('id') or 0)} is paused; {operation} is blocked"
            )
        if state == "archived":
            raise JobOperationBlockedError(
                f"Job {int(job.get('id') or 0)} is archived; {operation} is blocked"
            )
