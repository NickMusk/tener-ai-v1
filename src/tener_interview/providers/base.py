from __future__ import annotations

from typing import Any, Dict, Optional, Protocol


class InterviewProviderAdapter(Protocol):
    name: str

    def create_invitation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...

    def get_interview_status(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        ...

    def get_interview_result(
        self,
        invitation_id: str,
        *,
        assessment_id: Optional[str] = None,
        candidate_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...
