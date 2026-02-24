from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .db import InterviewDatabase, utc_now_iso
from .providers.base import InterviewProviderAdapter
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
        default_ttl_hours: int = 72,
        public_base_url: str = "",
    ) -> None:
        self.db = db
        self.provider = provider
        self.token_service = token_service
        self.scoring_engine = scoring_engine
        self.transcription_scoring_engine = transcription_scoring_engine
        self.default_ttl_hours = max(1, int(default_ttl_hours))
        self.public_base_url = public_base_url.rstrip("/")

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

        invitation = self.provider.create_invitation(
            {
                "job_id": job_id,
                "candidate_id": candidate_id,
                "candidate_name": candidate_name,
                "candidate_email": candidate_email,
                "language": language,
            }
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
                "provider_assessment_id": invitation.get("assessment_id"),
                "provider_invitation_id": invitation.get("invitation_id"),
                "provider_candidate_id": invitation.get("candidate_id"),
                "status": "invited",
                "language": language,
                "entry_token_hash": token_hash,
                "entry_token_expires_at": expires_at.isoformat(),
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
            },
        }

    def resolve_entry_token(self, token: str) -> Dict[str, Any]:
        try:
            self.token_service.parse_and_validate(token)
        except InvalidTokenError as exc:
            raise ValueError(str(exc)) from exc

        token_hash = self.token_service.token_hash(token)
        session = self.db.get_session_by_token_hash(token_hash)
        if not session:
            raise LookupError("session not found")

        now = datetime.now(UTC)
        expires_at = self._parse_iso(session["entry_token_expires_at"])
        if expires_at and now > expires_at and session.get("status") not in {"completed", "scored", "expired", "canceled", "failed"}:
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

        if session.get("status") in {"created", "invited"}:
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

        self.db.insert_event(session_id=session["session_id"], event_type="link_opened", source="candidate")
        updated = self.db.get_session(session["session_id"]) or session
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

    def _build_entry_url(self, token: str, request_base_url: Optional[str]) -> str:
        base = (self.public_base_url or request_base_url or "").rstrip("/")
        if not base:
            base = "http://127.0.0.1:8090"
        return f"{base}/i/{token}"

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
