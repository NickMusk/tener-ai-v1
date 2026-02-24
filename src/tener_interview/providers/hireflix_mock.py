from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import uuid4


@dataclass
class _MockInterviewState:
    interview_id: str
    position_id: str
    candidate_email: str
    interview_url: str
    sync_count: int = 0
    completed: bool = False
    result_id: str = field(default_factory=lambda: f"hflx_res_{uuid4().hex[:12]}")


class HireflixMockAdapter:
    name = "hireflix"

    def __init__(self) -> None:
        self._sessions: Dict[str, _MockInterviewState] = {}

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        interview_id = f"hflx_iv_{uuid4().hex[:12]}"
        position_id = str(payload.get("position_id") or "hflx_position_default")
        candidate_email = str(payload.get("candidate_email") or f"candidate-{uuid4().hex[:8]}@interview.local")
        interview_hash = interview_id.split("_")[-1]
        state = _MockInterviewState(
            interview_id=interview_id,
            position_id=position_id,
            candidate_email=candidate_email,
            interview_url=f"https://app.hireflix.com/{interview_hash}",
        )
        self._sessions[interview_id] = state
        return {
            "invitation_id": state.interview_id,
            "assessment_id": state.position_id,
            "candidate_id": state.candidate_email,
            "interview_url": state.interview_url,
        }

    def get_interview_status(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        _ = assessment_id, candidate_id
        state = self._sessions.get(invitation_id)
        if not state:
            return {"status": "failed", "error_code": "INTERVIEW_NOT_FOUND", "error_message": "Unknown interview"}

        state.sync_count += 1
        if force or state.sync_count >= 2:
            state.completed = True

        if state.completed:
            return {"status": "completed"}
        return {"status": "in_progress"}

    def get_interview_result(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        _ = assessment_id, candidate_id
        state = self._sessions.get(invitation_id)
        if not state:
            return {"status": "failed", "error_code": "INTERVIEW_NOT_FOUND", "error_message": "Unknown interview"}

        seed = sum(ord(ch) for ch in invitation_id) % 10
        technical = 74.0 + float(seed)
        soft = 70.0 + float(seed % 7)
        culture = 72.0 + float(seed % 8)

        return {
            "status": "ok",
            "result_id": state.result_id,
            "scores": {
                "technical": technical,
                "soft_skills": soft,
                "culture_fit": culture,
            },
            "raw": {
                "provider": "hireflix_mock",
                "interview_id": invitation_id,
                "sync_count": state.sync_count,
                "completed": state.completed,
            },
        }
