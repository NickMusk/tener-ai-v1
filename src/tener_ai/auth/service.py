from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence

from .contracts import AuthDecision, AuthPrincipal
from .repository import AuthRepository


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class AuthService:
    enabled: bool
    repository: Optional[AuthRepository] = None
    legacy_admin_token: str = ""

    @classmethod
    def from_env(cls, *, root: Path) -> "AuthService":
        enabled = env_bool("TENER_AUTH_ENABLED", False)
        legacy_admin_token = str(os.environ.get("TENER_ADMIN_API_TOKEN", "") or "").strip()
        if not enabled:
            return cls(enabled=False, repository=None, legacy_admin_token=legacy_admin_token)

        backend = str(
            os.environ.get("TENER_AUTH_DB_BACKEND") or os.environ.get("TENER_DB_BACKEND") or "sqlite"
        ).strip().lower()
        if backend not in {"sqlite", "postgres"}:
            backend = "sqlite"

        sqlite_path = str(
            os.environ.get("TENER_AUTH_DB_PATH") or (root / "runtime" / "tener_auth.sqlite3")
        )
        postgres_dsn = str(
            os.environ.get("TENER_AUTH_DB_DSN") or os.environ.get("TENER_DB_DSN") or ""
        ).strip() or None

        repo = AuthRepository(
            backend=backend,
            sqlite_path=sqlite_path,
            postgres_dsn=postgres_dsn,
        )
        repo.init_schema()

        bootstrap_token = str(os.environ.get("TENER_AUTH_BOOTSTRAP_TOKEN", "") or "").strip()
        if bootstrap_token:
            bootstrap_email = str(os.environ.get("TENER_AUTH_BOOTSTRAP_EMAIL", "admin@tener.local") or "").strip().lower()
            bootstrap_name = str(os.environ.get("TENER_AUTH_BOOTSTRAP_NAME", "Bootstrap Admin") or "").strip()
            bootstrap_org = str(os.environ.get("TENER_AUTH_BOOTSTRAP_ORG", "Tener") or "").strip()
            repo.ensure_bootstrap_api_key(
                org_name=bootstrap_org,
                email=bootstrap_email,
                full_name=bootstrap_name,
                plain_token=bootstrap_token,
                scopes=["api:*", "admin:*"],
                role="owner",
            )

        return cls(enabled=True, repository=repo, legacy_admin_token=legacy_admin_token)

    def authorize_request(
        self,
        *,
        authorization_header: str,
        required_scopes: Optional[Sequence[str]] = None,
        require_admin: bool = False,
    ) -> AuthDecision:
        if not self.enabled:
            return AuthDecision(allowed=True, status_code=200, principal=None)

        token = self._extract_bearer_token(authorization_header)
        if not token:
            return AuthDecision(allowed=False, status_code=401, error="auth_required")

        if self.legacy_admin_token and token == self.legacy_admin_token:
            principal = AuthPrincipal(
                org_id="legacy",
                user_id="legacy_admin",
                role="owner",
                scopes=["api:*", "admin:*"],
                token_type="legacy",
                token_id="legacy_admin_token",
            )
            return AuthDecision(allowed=True, status_code=200, principal=principal)

        if self.repository is None:
            return AuthDecision(allowed=False, status_code=503, error="auth_repository_unavailable")

        data = self.repository.get_principal_by_bearer_token(token)
        if not isinstance(data, dict):
            return AuthDecision(allowed=False, status_code=401, error="invalid_auth_token")

        principal = AuthPrincipal(
            org_id=str(data.get("org_id") or ""),
            user_id=str(data.get("user_id") or ""),
            role=str(data.get("role") or "member").strip().lower() or "member",
            scopes=list(data.get("scopes") or []),
            token_type=str(data.get("token_type") or "api_key"),
            token_id=str(data.get("token_id") or ""),
            email=data.get("email"),
            full_name=data.get("full_name"),
        )
        if require_admin and not self._is_admin(principal):
            return AuthDecision(allowed=False, status_code=403, error="admin_scope_required", principal=principal)

        requested = [str(x).strip().lower() for x in (required_scopes or []) if str(x).strip()]
        if requested and not self._has_any_scope(principal.scopes, requested):
            return AuthDecision(allowed=False, status_code=403, error="scope_forbidden", principal=principal)
        return AuthDecision(allowed=True, status_code=200, principal=principal)

    @staticmethod
    def _extract_bearer_token(header: str) -> str:
        text = str(header or "").strip()
        if not text.lower().startswith("bearer "):
            return ""
        return text[7:].strip()

    @staticmethod
    def _is_admin(principal: AuthPrincipal) -> bool:
        if principal.role in {"owner", "admin"}:
            return True
        return AuthService._has_any_scope(principal.scopes, ["admin:*", "*"])

    @staticmethod
    def _has_any_scope(granted: List[str], required: Sequence[str]) -> bool:
        granted_norm = [str(x).strip().lower() for x in granted if str(x).strip()]
        if "*" in granted_norm:
            return True
        for need in required:
            wanted = str(need).strip().lower()
            if not wanted:
                continue
            if wanted in granted_norm:
                return True
            if ":" in wanted:
                prefix = wanted.split(":", 1)[0]
                if f"{prefix}:*" in granted_norm:
                    return True
        return False

