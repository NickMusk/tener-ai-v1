from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class AuthPrincipal:
    org_id: str
    user_id: str
    role: str
    scopes: List[str] = field(default_factory=list)
    token_type: str = "api_key"
    token_id: str = ""
    email: Optional[str] = None
    full_name: Optional[str] = None


@dataclass
class AuthDecision:
    allowed: bool
    status_code: int
    error: Optional[str] = None
    principal: Optional[AuthPrincipal] = None

