from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .db import InterviewDatabase, utc_now_iso
from .providers.base import InterviewProviderAdapter
from .question_generation import InterviewQuestionGenerator
from .scoring import InterviewScoringEngine
from .transcription_scoring import TranscriptionScoringEngine
from .token_service import InterviewTokenService, InvalidTokenError

UTC = timezone.utc


class InterviewService:
    def __init__(
        self,
        db: InterviewDatabase,
        provider: InterviewProviderAdapter,
        token_service: InterviewTokenService,
        scoring_engine: InterviewScoringEngine,
        transcription_scoring_engine: Optional[TranscriptionScoringEngine] = None,
        source_catalog: Optional[Any] = None,
        question_generator: Optional[InterviewQuestionGenerator] = None,
        default_ttl_hours: int = 72,
        public_base_url: str = "",
        system_name: str = "Tener",
    ) -> None:
        self.db = db
        self.provider = provider
        self.token_service = token_service
        self.scoring_engine = scoring_engine
        self.transcription_scoring_engine = transcription_scoring_engine
        self.source_catalog = source_catalog
        self.question_generator = question_generator
        self.default_ttl_hours = max(1, int(default_ttl_hours))
        self.public_base_url = public_base_url.rstrip("/")
        self.system_name = str(system_name or "Tener").strip() or "Tener"

    def start_session(
        self,
        job_id: int,
        candidate_id: int,
        candidate_name: Optional[str] = None,
        candidate_email: Optional[str] = None,
        conversation_id: Optional[int] = None,
        language: Optional[str] = None,
        ttl_hours: Optional[int] = None,
        request_base_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = datetime.now(UTC)
        expires_at = now + timedelta(hours=max(1, int(ttl_hours or self.default_ttl_hours)))
        session_id = f"iv_{uuid4().hex}"
        assessment_ctx: Dict[str, Any] = {}
        assessment_error: Optional[str] = None
        try:
            assessment_ctx = self._resolve_assessment_for_job(job_id=job_id, language=language)
        except Exception as exc:
            assessment_error = str(exc)
            assessment_ctx = {}
        entry_context = self._build_entry_context(
            job_id=int(job_id),
            candidate_id=int(candidate_id),
            candidate_name=candidate_name,
            language=language,
            assessment_ctx=assessment_ctx,
        )

        invite_payload = {
            "job_id": job_id,
            "candidate_id": candidate_id,
            "candidate_name": candidate_name,
            "candidate_email": candidate_email,
            "language": language,
        }
        if assessment_ctx.get("provider_assessment_id"):
            invite_payload["position_id"] = assessment_ctx["provider_assessment_id"]

        invitation = self.provider.create_invitation(invite_payload)
        provider_assessment_id = str(
            invitation.get("assessment_id") or assessment_ctx.get("provider_assessment_id") or ""
        ).strip() or None

        if provider_assessment_id and assessment_ctx.get("generation_hash"):
            self.db.upsert_job_assessment(
                job_id=int(job_id),
                provider=self.provider.name,
                provider_assessment_id=provider_assessment_id,
                assessment_name=assessment_ctx.get("assessment_name"),
                generation_hash=assessment_ctx.get("generation_hash"),
                generated_questions=assessment_ctx.get("questions"),
                meta=assessment_ctx.get("meta"),
            )

        payload = {
            "sid": session_id,
            "jid": int(job_id),
            "cid": int(candidate_id),
            "exp": int(expires_at.timestamp()),
        }
        token = self.token_service.generate(payload)
        token_hash = self.token_service.token_hash(token)

        self.db.insert_session(
            {
                "session_id": session_id,
                "job_id": int(job_id),
                "candidate_id": int(candidate_id),
                "candidate_name": candidate_name,
                "conversation_id": int(conversation_id) if conversation_id is not None else None,
                "provider": self.provider.name,
                "provider_assessment_id": provider_assessment_id,
                "provider_invitation_id": invitation.get("invitation_id"),
                "provider_candidate_id": invitation.get("candidate_id"),
                "status": "invited",
                "language": language,
                "entry_token_hash": token_hash,
                "entry_token_expires_at": expires_at.isoformat(),
                "entry_context_json": entry_context,
                "provider_interview_url": invitation.get("interview_url"),
                "started_at": None,
                "completed_at": None,
                "scored_at": None,
                "last_sync_at": None,
                "last_error_code": None,
                "last_error_message": None,
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        )
        self.db.insert_event(session_id=session_id, event_type="invited", source="system", payload={"provider": self.provider.name})
        if assessment_error:
            self.db.insert_event(
                session_id=session_id,
                event_type="assessment_generation_failed",
                source="system",
                payload={"error": assessment_error},
            )
        self.db.upsert_candidate_summary(
            job_id=int(job_id),
            candidate_id=int(candidate_id),
            candidate_name=candidate_name,
            session_id=session_id,
            interview_status="not_started",
            technical_score=None,
            soft_skills_score=None,
            culture_fit_score=None,
            total_score=None,
            score_confidence=None,
        )

        entry_url = self._build_entry_url(token=token, request_base_url=request_base_url)
        return {
            "session_id": session_id,
            "status": "invited",
            "entry_url": entry_url,
            "expires_at": expires_at.isoformat(),
            "provider": {
                "name": self.provider.name,
                "invitation_id": invitation.get("invitation_id"),
                "assessment_id": provider_assessment_id,
                "assessment_generation_error": assessment_error,
            },
        }

    def get_entry_landing(self, token: str) -> Dict[str, Any]:
        session = self._session_for_token(token, mark_started=False)
        self.db.insert_event(session_id=session["session_id"], event_type="landing_viewed", source="candidate")
        entry_context = session.get("entry_context_json") if isinstance(session.get("entry_context_json"), dict) else {}
        return {
            "session_id": session["session_id"],
            "status": session.get("status"),
            "candidate_id": session.get("candidate_id"),
            "candidate_name": session.get("candidate_name"),
            "language": session.get("language"),
            "provider": session.get("provider"),
            "expires_at": session.get("entry_token_expires_at"),
            "landing": entry_context,
        }

    def resolve_entry_token(self, token: str) -> Dict[str, Any]:
        updated = self._session_for_token(token, mark_started=True)
        self.db.insert_event(session_id=updated["session_id"], event_type="link_opened", source="candidate")
        self.db.insert_event(session_id=updated["session_id"], event_type="interview_started", source="candidate")
        return {
            "session_id": updated["session_id"],
            "provider_url": updated.get("provider_interview_url"),
            "status": updated.get("status"),
        }

    def get_session_view(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = self.db.get_session(session_id)
        if not session:
            return None
        result = self.db.get_latest_result(session_id)
        return {
            "session_id": session["session_id"],
            "job_id": session["job_id"],
            "candidate_id": session["candidate_id"],
            "candidate_name": session.get("candidate_name"),
            "status": session.get("status"),
            "provider": session.get("provider"),
            "started_at": session.get("started_at"),
            "completed_at": session.get("completed_at"),
            "last_sync_at": session.get("last_sync_at"),
            "summary": {
                "technical_score": (result or {}).get("technical_score"),
                "soft_skills_score": (result or {}).get("soft_skills_score"),
                "culture_fit_score": (result or {}).get("culture_fit_score"),
                "total_score": (result or {}).get("total_score"),
            },
        }

    def get_session_scorecard(self, session_id: str) -> Optional[Dict[str, Any]]:
        session = self.db.get_session(session_id)
        if not session:
            return None
        result = self.db.get_latest_result(session_id)
        if not result:
            return {
                "session_id": session_id,
                "status": session.get("status"),
                "scorecard": None,
            }
        normalized_json = result.get("normalized_json") if isinstance(result.get("normalized_json"), dict) else {}
        transcription_scoring = (
            normalized_json.get("transcription_scoring")
            if isinstance(normalized_json.get("transcription_scoring"), dict)
            else {}
        )
        return {
            "session_id": session_id,
            "status": session.get("status"),
            "scorecard": {
                "technical_score": result.get("technical_score"),
                "soft_skills_score": result.get("soft_skills_score"),
                "culture_fit_score": result.get("culture_fit_score"),
                "total_score": result.get("total_score"),
                "score_confidence": result.get("score_confidence"),
                "pass_recommendation": result.get("pass_recommendation"),
                "transcription_scoring": transcription_scoring,
            },
        }

    def refresh_session(self, session_id: str, force: bool = False) -> Dict[str, Any]:
        session = self.db.get_session(session_id)
        if not session:
            raise LookupError("session not found")

        invitation_id = str(session.get("provider_invitation_id") or "")
        if not invitation_id:
            self.db.update_session(
                session_id,
                {
                    "status": "failed",
                    "last_error_code": "MISSING_PROVIDER_INVITATION_ID",
                    "last_error_message": "Session has no provider invitation id",
                    "updated_at": utc_now_iso(),
                },
            )
            raise ValueError("provider invitation id is missing")

        status_payload = self.provider.get_interview_status(
            invitation_id=invitation_id,
            assessment_id=str(session.get("provider_assessment_id") or "") or None,
            candidate_id=str(session.get("provider_candidate_id") or "") or None,
            force=force,
        )
        provider_status = str(status_payload.get("status") or "failed")
        now_iso = utc_now_iso()

        if provider_status == "failed":
            err_code = str(status_payload.get("error_code") or "PROVIDER_STATUS_FAILED")
            err_message = str(status_payload.get("error_message") or "provider returned failed status")
            self.db.update_session(
                session_id,
                {
                    "status": "failed",
                    "last_sync_at": now_iso,
                    "last_error_code": err_code,
                    "last_error_message": err_message,
                    "updated_at": now_iso,
                },
            )
            self.db.insert_event(session_id=session_id, event_type="sync_failed", source="provider", payload=status_payload)
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session_id,
                interview_status="failed",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            return {
                "session_id": session_id,
                "status": "failed",
                "updated": True,
                "error": {"code": err_code, "message": err_message},
            }

        if provider_status == "in_progress":
            self.db.update_session(
                session_id,
                {
                    "status": "in_progress",
                    "started_at": session.get("started_at") or now_iso,
                    "last_sync_at": now_iso,
                    "updated_at": now_iso,
                },
            )
            self.db.insert_event(session_id=session_id, event_type="provider_in_progress", source="provider")
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session_id,
                interview_status="in_progress",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            return {
                "session_id": session_id,
                "status": "in_progress",
                "updated": True,
                "result": None,
            }

        if provider_status == "invited":
            self.db.update_session(
                session_id,
                {
                    "status": "invited",
                    "last_sync_at": now_iso,
                    "updated_at": now_iso,
                },
            )
            self.db.insert_event(session_id=session_id, event_type="provider_invited", source="provider")
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session_id,
                interview_status="not_started",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            return {
                "session_id": session_id,
                "status": "invited",
                "updated": True,
                "result": None,
            }

        self.db.update_session(
            session_id,
            {
                "status": "completed",
                "completed_at": session.get("completed_at") or now_iso,
                "last_sync_at": now_iso,
                "updated_at": now_iso,
            },
        )
        self.db.insert_event(session_id=session_id, event_type="provider_completed", source="provider")

        if session.get("status") == "scored" and not force:
            latest = self.db.get_latest_result(session_id)
            return {
                "session_id": session_id,
                "status": "scored",
                "updated": False,
                "result": self._format_result(latest),
            }

        raw = self.provider.get_interview_result(
            invitation_id=invitation_id,
            assessment_id=str(session.get("provider_assessment_id") or "") or None,
            candidate_id=str(session.get("provider_candidate_id") or "") or None,
        )
        if str(raw.get("status") or "") != "ok":
            err_code = str(raw.get("error_code") or "PROVIDER_RESULT_FAILED")
            err_message = str(raw.get("error_message") or "provider returned invalid result")
            self.db.update_session(
                session_id,
                {
                    "status": "failed",
                    "last_error_code": err_code,
                    "last_error_message": err_message,
                    "updated_at": now_iso,
                },
            )
            self.db.insert_event(session_id=session_id, event_type="result_failed", source="provider", payload=raw)
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session_id,
                interview_status="failed",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            return {
                "session_id": session_id,
                "status": "failed",
                "updated": True,
                "error": {"code": err_code, "message": err_message},
            }

        transcription_scoring: Dict[str, Any] = {}
        if self.transcription_scoring_engine is not None:
            try:
                transcription_scoring = self.transcription_scoring_engine.score_provider_payload(raw)
            except Exception as exc:
                transcription_scoring = {
                    "applied": False,
                    "reason": f"transcription_scoring_failed: {exc}",
                    "scores": {},
                    "question_scores": [],
                }

        if transcription_scoring.get("applied") and isinstance(transcription_scoring.get("scores"), dict):
            raw = dict(raw)
            raw["scores"] = dict(transcription_scoring.get("scores") or {})
            raw["transcription_scoring"] = transcription_scoring

        normalized = self.scoring_engine.normalize_provider_result(raw)
        if transcription_scoring:
            normalized_json = normalized.get("normalized_json") if isinstance(normalized.get("normalized_json"), dict) else {}
            normalized_json["transcription_scoring"] = transcription_scoring
            normalized["normalized_json"] = normalized_json
        self.db.insert_result(
            session_id=session_id,
            provider_result_id=raw.get("result_id"),
            scores=normalized,
            normalized=normalized.get("normalized_json") or {},
            raw_payload=normalized.get("raw_payload") or raw,
        )

        self.db.update_session(
            session_id,
            {
                "status": "scored",
                "scored_at": now_iso,
                "last_sync_at": now_iso,
                "updated_at": now_iso,
            },
        )
        self.db.insert_event(session_id=session_id, event_type="scored", source="system", payload=normalized)
        self.db.upsert_candidate_summary(
            job_id=int(session["job_id"]),
            candidate_id=int(session["candidate_id"]),
            candidate_name=session.get("candidate_name"),
            session_id=session_id,
            interview_status="scored",
            technical_score=normalized.get("technical_score"),
            soft_skills_score=normalized.get("soft_skills_score"),
            culture_fit_score=normalized.get("culture_fit_score"),
            total_score=normalized.get("total_score"),
            score_confidence=normalized.get("score_confidence"),
        )

        return {
            "session_id": session_id,
            "status": "scored",
            "updated": True,
            "result": {
                "technical_score": normalized.get("technical_score"),
                "soft_skills_score": normalized.get("soft_skills_score"),
                "culture_fit_score": normalized.get("culture_fit_score"),
                "total_score": normalized.get("total_score"),
                "score_confidence": normalized.get("score_confidence"),
                "question_scores": (
                    transcription_scoring.get("question_scores")
                    if isinstance(transcription_scoring.get("question_scores"), list)
                    else []
                ),
            },
        }

    def list_sessions(
        self,
        limit: int = 100,
        status: Optional[str] = None,
        job_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {"items": self.db.list_sessions(limit=limit, status=status, job_id=job_id)}

    def get_leaderboard(self, job_id: int, limit: int = 50) -> Dict[str, Any]:
        rows = self.db.list_leaderboard(job_id=job_id, limit=limit)
        items = [
            {
                "candidate_id": x["candidate_id"],
                "candidate_name": x.get("candidate_name"),
                "interview_status": x.get("interview_status"),
                "technical_score": x.get("technical_score"),
                "soft_skills_score": x.get("soft_skills_score"),
                "culture_fit_score": x.get("culture_fit_score"),
                "total_score": x.get("total_score"),
                "score_confidence": x.get("score_confidence"),
                "session_id": x.get("session_id"),
                "updated_at": x.get("updated_at"),
            }
            for x in rows
        ]
        return {"job_id": job_id, "items": items}

    def prepare_job_assessment(self, job_id: int, language: Optional[str] = None) -> Dict[str, Any]:
        job_int = int(job_id)
        existing = self.db.get_job_assessment(job_int)
        existing_id = str((existing or {}).get("provider_assessment_id") or "").strip()

        ctx = self._resolve_assessment_for_job(job_id=job_int, language=language)
        assessment_id = str(ctx.get("provider_assessment_id") or "").strip()
        if not assessment_id:
            raise ValueError("assessment generation is unavailable for this job")

        return {
            "job_id": job_int,
            "provider": self.provider.name,
            "assessment_id": assessment_id,
            "assessment_name": ctx.get("assessment_name"),
            "generation_hash": ctx.get("generation_hash"),
            "created_now": existing_id != assessment_id,
        }

    def run_interview_step(
        self,
        job_id: int,
        candidate_ids: List[int],
        mode: str,
        request_base_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        started = 0
        in_progress = 0
        scored = 0
        failed = 0
        items: List[Dict[str, Any]] = []

        for raw_candidate_id in candidate_ids:
            try:
                candidate_id = int(raw_candidate_id)
            except (TypeError, ValueError):
                failed += 1
                continue

            existing = self.db.get_latest_session_for_candidate(job_id=job_id, candidate_id=candidate_id)
            candidate_name = existing.get("candidate_name") if existing else None

            try:
                if mode == "start_or_refresh" and existing is None:
                    started_item = self.start_session(
                        job_id=job_id,
                        candidate_id=candidate_id,
                        candidate_name=candidate_name,
                        request_base_url=request_base_url,
                    )
                    started += 1
                    items.append({"candidate_id": candidate_id, "action": "started", **started_item})
                    continue

                if existing is None:
                    failed += 1
                    items.append({"candidate_id": candidate_id, "action": "missing_session", "status": "failed"})
                    continue

                refreshed = self.refresh_session(existing["session_id"], force=False)
                status = str(refreshed.get("status") or "")
                if status == "scored":
                    scored += 1
                elif status in {"in_progress", "completed", "invited", "created"}:
                    in_progress += 1
                else:
                    failed += 1
                items.append({"candidate_id": candidate_id, "action": "refreshed", **refreshed})
            except Exception as exc:
                failed += 1
                items.append(
                    {
                        "candidate_id": candidate_id,
                        "action": "error",
                        "status": "failed",
                        "error": str(exc),
                    }
                )

        output = {
            "job_id": job_id,
            "started": started,
            "in_progress": in_progress,
            "scored": scored,
            "failed": failed,
            "items": items,
        }
        status = "error" if failed > 0 and started == 0 and in_progress == 0 and scored == 0 else "success"
        self.db.upsert_job_step_progress(job_id=job_id, status=status, output=output)
        return output

    def _resolve_assessment_for_job(self, *, job_id: int, language: Optional[str]) -> Dict[str, Any]:
        if self.question_generator is None or self.source_catalog is None:
            return {}
        get_job = getattr(self.source_catalog, "get_job", None)
        if not callable(get_job):
            return {}

        job = get_job(int(job_id))
        if not isinstance(job, dict) or not job:
            return {}
        if "id" not in job:
            job = dict(job)
            job["id"] = int(job_id)

        generated = self.question_generator.generate_for_job(job)
        generation_hash = str(generated.get("generation_hash") or "").strip()
        if not generation_hash:
            return {}

        cached = self.db.get_job_assessment(int(job_id))
        if (
            isinstance(cached, dict)
            and str(cached.get("provider") or "") == self.provider.name
            and str(cached.get("generation_hash") or "") == generation_hash
            and str(cached.get("provider_assessment_id") or "").strip()
        ):
            return {
                "provider_assessment_id": str(cached["provider_assessment_id"]),
                "assessment_name": cached.get("assessment_name"),
                "generation_hash": generation_hash,
                "questions": generated.get("questions"),
                "meta": generated.get("meta"),
            }

        create_assessment = getattr(self.provider, "create_assessment", None)
        if not callable(create_assessment):
            return {}

        created = create_assessment(
            {
                "assessment_name": generated.get("assessment_name"),
                "questions": generated.get("questions"),
                "language": language,
                "job_id": int(job_id),
            }
        )
        provider_assessment_id = str(created.get("assessment_id") or "").strip()
        if not provider_assessment_id:
            return {}

        self.db.upsert_job_assessment(
            job_id=int(job_id),
            provider=self.provider.name,
            provider_assessment_id=provider_assessment_id,
            assessment_name=str(created.get("assessment_name") or generated.get("assessment_name") or "").strip()
            or None,
            generation_hash=generation_hash,
            generated_questions=generated.get("questions"),
            meta=generated.get("meta"),
        )
        return {
            "provider_assessment_id": provider_assessment_id,
            "assessment_name": str(created.get("assessment_name") or generated.get("assessment_name") or "").strip() or None,
            "generation_hash": generation_hash,
            "questions": generated.get("questions"),
            "meta": generated.get("meta"),
        }

    def _build_entry_url(self, token: str, request_base_url: Optional[str]) -> str:
        base = (self.public_base_url or request_base_url or "").rstrip("/")
        if not base:
            base = "http://127.0.0.1:8090"
        return f"{base}/i/{token}"

    def _session_for_token(self, token: str, *, mark_started: bool) -> Dict[str, Any]:
        try:
            self.token_service.parse_and_validate(token)
        except InvalidTokenError as exc:
            raise ValueError(str(exc)) from exc

        token_hash = self.token_service.token_hash(token)
        session = self.db.get_session_by_token_hash(token_hash)
        if not session:
            raise LookupError("session not found")

        now = datetime.now(UTC)
        expires_at = self._parse_iso(session.get("entry_token_expires_at"))
        status = str(session.get("status") or "").strip().lower()
        if expires_at and now > expires_at and status not in {"completed", "scored", "expired", "canceled", "failed"}:
            self.db.update_session(
                session["session_id"],
                {
                    "status": "expired",
                    "updated_at": utc_now_iso(),
                },
            )
            self.db.insert_event(session_id=session["session_id"], event_type="expired", source="system")
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session["session_id"],
                interview_status="failed",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            raise ValueError("token expired")

        if mark_started and status in {"created", "invited"}:
            self.db.update_session(
                session["session_id"],
                {
                    "status": "in_progress",
                    "started_at": session.get("started_at") or utc_now_iso(),
                    "updated_at": utc_now_iso(),
                },
            )
            self.db.upsert_candidate_summary(
                job_id=int(session["job_id"]),
                candidate_id=int(session["candidate_id"]),
                candidate_name=session.get("candidate_name"),
                session_id=session["session_id"],
                interview_status="in_progress",
                technical_score=None,
                soft_skills_score=None,
                culture_fit_score=None,
                total_score=None,
                score_confidence=None,
            )
            session = self.db.get_session(session["session_id"]) or session
        return session

    def _build_entry_context(
        self,
        *,
        job_id: int,
        candidate_id: int,
        candidate_name: Optional[str],
        language: Optional[str],
        assessment_ctx: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        job: Dict[str, Any] = {}
        get_job = getattr(self.source_catalog, "get_job", None)
        if callable(get_job):
            try:
                loaded = get_job(int(job_id))
            except Exception:
                loaded = {}
            if isinstance(loaded, dict):
                job = dict(loaded)

        title = str(job.get("title") or "Interview").strip() or "Interview"
        company = str(job.get("company") or self._company_name_from_profile(job) or self._company_name_from_job(job) or "").strip()
        language_code = str(language or "").strip().lower() or "en"
        questions = self._entry_questions(job_id=job_id, assessment_ctx=assessment_ctx)
        meta = self._entry_meta(job_id=job_id, assessment_ctx=assessment_ctx)
        process = self._build_entry_process(questions=questions, meta=meta)
        salary_text = self._salary_text(job)

        return {
            "candidate": {
                "id": int(candidate_id),
                "name": str(candidate_name or "").strip() or None,
            },
            "job": {
                "id": int(job_id),
                "title": title,
                "company": company or None,
                "company_tagline": self._company_tagline(job),
                "location": str(job.get("location") or "").strip() or None,
                "seniority": str(job.get("seniority") or "").strip() or None,
                "salary_text": salary_text,
                "summary": self._job_summary(job),
                "highlights": self._job_highlights(job, meta=meta),
                "skills": self._job_skills(job, meta=meta),
                "preferred_languages": self._to_str_list(job.get("preferred_languages")),
            },
            "interview": {
                "provider": str(self.provider.name or "").strip() or None,
                "estimated_minutes": process.get("estimated_minutes"),
                "steps": process.get("steps") or [],
                "cta_label": "Start Interview",
                "support_text": self._support_text(language_code, self.system_name),
                "privacy_note": self._privacy_note(language_code),
            },
        }

    def _entry_questions(self, *, job_id: int, assessment_ctx: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if isinstance(assessment_ctx, dict) and isinstance(assessment_ctx.get("questions"), list):
            return [item for item in assessment_ctx.get("questions") or [] if isinstance(item, dict)]
        stored = self.db.get_job_assessment(int(job_id))
        questions = stored.get("generated_questions_json") if isinstance(stored, dict) else None
        return [item for item in questions or [] if isinstance(item, dict)]

    def _entry_meta(self, *, job_id: int, assessment_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(assessment_ctx, dict) and isinstance(assessment_ctx.get("meta"), dict):
            return dict(assessment_ctx.get("meta") or {})
        stored = self.db.get_job_assessment(int(job_id))
        meta = stored.get("meta_json") if isinstance(stored, dict) else None
        return dict(meta or {}) if isinstance(meta, dict) else {}

    def _build_entry_process(self, *, questions: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
        category_counts = meta.get("categories") if isinstance(meta.get("categories"), dict) else {}
        steps: List[Dict[str, Any]] = []
        for category, label, description in (
            ("cultural_fit", "Culture And Values", "Expect questions about team fit, ownership, and working style."),
            ("hard_skills", "Technical Depth", "Role-specific scenarios focused on the stack and problem solving."),
            ("soft_skills", "Communication", "Clear communication, prioritization, and collaboration signals."),
        ):
            count = int(category_counts.get(category) or 0)
            if count <= 0:
                continue
            steps.append(
                {
                    "label": label,
                    "description": description,
                    "duration_minutes": max(3, count * 2),
                }
            )
        if not steps:
            steps = [
                {
                    "label": "Async Interview",
                    "description": "A structured async interview focused on fit, technical depth, and communication.",
                    "duration_minutes": max(12, len(questions) * 2),
                }
            ]
        estimated_from_questions = self._estimate_question_minutes(questions)
        estimated_minutes = max(sum(int(item.get("duration_minutes") or 0) for item in steps), estimated_from_questions or 0)
        return {
            "estimated_minutes": estimated_minutes or 15,
            "steps": steps[:3],
        }

    @staticmethod
    def _estimate_question_minutes(questions: List[Dict[str, Any]]) -> int:
        total_seconds = 0
        for question in questions:
            try:
                total_seconds += int(question.get("timeToAnswer") or 0)
            except (TypeError, ValueError):
                pass
            try:
                total_seconds += int(question.get("timeToThink") or 0)
            except (TypeError, ValueError):
                pass
        if total_seconds <= 0:
            return max(0, len(questions) * 2)
        return max(1, int(round(total_seconds / 60.0)))

    def _job_summary(self, job: Dict[str, Any]) -> str:
        jd_text = str(job.get("jd_text") or "").strip()
        if not jd_text:
            return "This async interview is the next step in the process."
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", jd_text) if part.strip()]
        if paragraphs:
            return paragraphs[0][:420].strip()
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", jd_text) if part.strip()]
        return " ".join(sentences[:2])[:420].strip()

    def _job_highlights(self, job: Dict[str, Any], *, meta: Dict[str, Any]) -> List[str]:
        highlights = [str(item).strip() for item in (meta.get("jd_highlights") or []) if str(item).strip()]
        if highlights:
            return highlights[:5]
        jd_text = str(job.get("jd_text") or "").strip()
        bullets = []
        for line in jd_text.splitlines():
            normalized = line.strip().lstrip("-*0123456789. ").strip()
            if normalized and normalized not in bullets:
                bullets.append(normalized)
            if len(bullets) >= 5:
                break
        return bullets[:5]

    def _job_skills(self, job: Dict[str, Any], *, meta: Dict[str, Any]) -> List[str]:
        skills: List[str] = []
        for raw in (
            self._to_str_list(job.get("must_have_skills")),
            self._to_str_list(job.get("nice_to_have_skills")),
            [str(item).strip() for item in (meta.get("skills_detected") or []) if str(item).strip()],
        ):
            for item in raw:
                if item and item not in skills:
                    skills.append(item)
        return skills[:10]

    @staticmethod
    def _to_str_list(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    @staticmethod
    def _salary_text(job: Dict[str, Any]) -> Optional[str]:
        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        currency = str(job.get("salary_currency") or "").strip().upper()
        symbol = {"USD": "$", "EUR": "EUR ", "GBP": "GBP "}.get(currency, f"{currency} " if currency else "")
        try:
            min_value = float(salary_min) if salary_min is not None else None
        except (TypeError, ValueError):
            min_value = None
        try:
            max_value = float(salary_max) if salary_max is not None else None
        except (TypeError, ValueError):
            max_value = None
        if min_value is None and max_value is None:
            return None
        if min_value is not None and max_value is not None:
            return f"{symbol}{min_value:,.0f} - {symbol}{max_value:,.0f}"
        if min_value is not None:
            return f"{symbol}{min_value:,.0f}+"
        return f"Up to {symbol}{max_value:,.0f}"

    @staticmethod
    def _company_name_from_profile(job: Dict[str, Any]) -> str:
        profile = job.get("company_culture_profile") if isinstance(job.get("company_culture_profile"), dict) else {}
        return str(profile.get("company_name") or "").strip()

    @staticmethod
    def _company_name_from_job(job: Dict[str, Any]) -> str:
        return str(job.get("company_name") or job.get("company") or "").strip()

    def _company_tagline(self, job: Dict[str, Any]) -> Optional[str]:
        parts: List[str] = []
        seniority = str(job.get("seniority") or "").strip()
        location = str(job.get("location") or "").strip()
        languages = self._to_str_list(job.get("preferred_languages"))
        if seniority:
            parts.append(seniority.title())
        if location:
            parts.append(location)
        if languages:
            parts.append("/".join(lang.upper() for lang in languages[:3]))
        return " • ".join(parts) if parts else None

    @staticmethod
    def _support_text(language_code: str, system_name: str = "Tener") -> str:
        safe_system_name = str(system_name or "Tener").strip() or "Tener"
        copy = {
            "ru": f"Интервью можно пройти в удобное время. После завершения ответ просто появится в системе {safe_system_name}.",
            "es": f"Puedes completarlo cuando te convenga. Cuando termines, tu respuesta quedara registrada en {safe_system_name}.",
        }
        return copy.get(
            language_code,
            f"You can complete it when convenient. Once finished, your interview will be recorded in {safe_system_name}.",
        )

    @staticmethod
    def _privacy_note(language_code: str) -> str:
        copy = {
            "ru": "Ссылка персональная и привязана к этой заявке.",
            "es": "Este enlace es personal y esta vinculado a esta candidatura.",
        }
        return copy.get(language_code, "This link is personal and tied to this application.")

    @staticmethod
    def _parse_iso(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed

    @staticmethod
    def _format_result(result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not result:
            return None
        normalized_json = result.get("normalized_json") if isinstance(result.get("normalized_json"), dict) else {}
        transcription_scoring = (
            normalized_json.get("transcription_scoring")
            if isinstance(normalized_json.get("transcription_scoring"), dict)
            else {}
        )
        return {
            "technical_score": result.get("technical_score"),
            "soft_skills_score": result.get("soft_skills_score"),
            "culture_fit_score": result.get("culture_fit_score"),
            "total_score": result.get("total_score"),
            "score_confidence": result.get("score_confidence"),
            "question_scores": (
                transcription_scoring.get("question_scores")
                if isinstance(transcription_scoring.get("question_scores"), list)
                else []
            ),
        }
