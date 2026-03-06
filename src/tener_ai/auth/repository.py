from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class AuthRepository:
    def __init__(
        self,
        *,
        backend: str = "sqlite",
        sqlite_path: Optional[str] = None,
        postgres_dsn: Optional[str] = None,
    ) -> None:
        normalized = str(backend or "sqlite").strip().lower()
        if normalized not in {"sqlite", "postgres"}:
            normalized = "sqlite"
        self.backend = normalized
        self.sqlite_path = sqlite_path
        self.postgres_dsn = postgres_dsn
        self._lock = threading.Lock()
        self._sqlite: Optional[sqlite3.Connection] = None

        if self.backend == "sqlite":
            if not self.sqlite_path:
                raise ValueError("sqlite_path is required for sqlite auth repository")
            Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            self._sqlite = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            self._sqlite.row_factory = sqlite3.Row
        else:
            if not self.postgres_dsn:
                raise ValueError("postgres_dsn is required for postgres auth repository")
            self._require_psycopg()

    def init_schema(self) -> None:
        with self._transaction() as conn:
            conn.executescript(self._sqlite_schema()) if self.backend == "sqlite" else self._run_postgres_schema(conn)
        self._seed_default_roles()

    def create_organization(self, *, name: str) -> str:
        org_id = f"org_{uuid.uuid4().hex}"
        with self._transaction() as conn:
            self._execute(
                conn,
                """
                INSERT INTO organizations (id, name, created_at)
                VALUES ({p}, {p}, {p})
                """,
                (org_id, str(name or "").strip() or "Default Org", utc_now_iso()),
            )
        return org_id

    def create_user(self, *, email: str, full_name: Optional[str] = None, is_active: bool = True) -> str:
        user_id = f"user_{uuid.uuid4().hex}"
        with self._transaction() as conn:
            self._execute(
                conn,
                """
                INSERT INTO users (id, email, full_name, is_active, created_at)
                VALUES ({p}, {p}, {p}, {p}, {p})
                """,
                (
                    user_id,
                    str(email or "").strip().lower(),
                    str(full_name or "").strip() or None,
                    self._bool_db(is_active),
                    utc_now_iso(),
                ),
            )
        return user_id

    def upsert_membership(
        self,
        *,
        org_id: str,
        user_id: str,
        role: str = "member",
        is_active: bool = True,
    ) -> None:
        with self._transaction() as conn:
            self._execute(
                conn,
                """
                INSERT INTO memberships (org_id, user_id, role, is_active, created_at)
                VALUES ({p}, {p}, {p}, {p}, {p})
                ON CONFLICT(org_id, user_id) DO UPDATE SET
                    role = EXCLUDED.role,
                    is_active = EXCLUDED.is_active
                """,
                (
                    str(org_id),
                    str(user_id),
                    str(role or "member").strip().lower() or "member",
                    self._bool_db(is_active),
                    utc_now_iso(),
                ),
            )

    def create_api_key(
        self,
        *,
        org_id: str,
        user_id: str,
        name: str,
        scopes: Optional[List[str]] = None,
        ttl_days: Optional[int] = None,
        plain_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        token = str(plain_token or "").strip() or f"tk_{secrets.token_urlsafe(32)}"
        token_id = f"key_{uuid.uuid4().hex}"
        expires_at = None
        if ttl_days is not None:
            expires_at = (datetime.now(UTC) + timedelta(days=max(1, int(ttl_days)))).isoformat()
        cleaned_scopes = self._normalize_scopes(scopes)
        with self._transaction() as conn:
            self._execute(
                conn,
                """
                INSERT INTO api_keys (
                    id, org_id, user_id, name, key_hash, prefix, scopes_json,
                    is_active, expires_at, last_used_at, created_at
                )
                VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, NULL, {p})
                """,
                (
                    token_id,
                    str(org_id),
                    str(user_id),
                    str(name or "").strip() or "API Key",
                    self.hash_token(token),
                    token[:12],
                    self._json_db(cleaned_scopes),
                    self._bool_db(True),
                    expires_at,
                    utc_now_iso(),
                ),
            )
        return {
            "id": token_id,
            "token": token,
            "prefix": token[:12],
            "scopes": cleaned_scopes,
            "expires_at": expires_at,
        }

    def get_principal_by_bearer_token(self, token: str) -> Optional[Dict[str, Any]]:
        token_hash = self.hash_token(token)
        with self._transaction() as conn:
            row = self._fetchone(
                conn,
                """
                SELECT
                    ak.id AS token_id,
                    'api_key' AS token_type,
                    ak.org_id AS org_id,
                    ak.user_id AS user_id,
                    COALESCE(m.role, 'member') AS role,
                    ak.scopes_json AS scopes_json,
                    ak.is_active AS token_active,
                    ak.expires_at AS expires_at,
                    u.is_active AS user_active,
                    COALESCE(m.is_active, {true_value}) AS membership_active,
                    u.email AS email,
                    u.full_name AS full_name
                FROM api_keys ak
                JOIN users u ON u.id = ak.user_id
                LEFT JOIN memberships m ON m.org_id = ak.org_id AND m.user_id = ak.user_id
                WHERE ak.key_hash = {p}
                LIMIT 1
                """,
                (token_hash,),
            )
            if row is not None:
                if not self._is_accessible(row):
                    return None
                self._execute(
                    conn,
                    "UPDATE api_keys SET last_used_at = {p} WHERE id = {p}",
                    (utc_now_iso(), str(row.get("token_id") or "")),
                )
                return self._build_principal_row(row)

            row = self._fetchone(
                conn,
                """
                SELECT
                    s.id AS token_id,
                    'session' AS token_type,
                    s.org_id AS org_id,
                    s.user_id AS user_id,
                    COALESCE(m.role, 'member') AS role,
                    s.scopes_json AS scopes_json,
                    s.is_active AS token_active,
                    s.expires_at AS expires_at,
                    u.is_active AS user_active,
                    COALESCE(m.is_active, {true_value}) AS membership_active,
                    u.email AS email,
                    u.full_name AS full_name
                FROM sessions s
                JOIN users u ON u.id = s.user_id
                LEFT JOIN memberships m ON m.org_id = s.org_id AND m.user_id = s.user_id
                WHERE s.token_hash = {p}
                LIMIT 1
                """,
                (token_hash,),
            )
            if row is None:
                return None
            if not self._is_accessible(row):
                return None
            self._execute(
                conn,
                "UPDATE sessions SET last_used_at = {p} WHERE id = {p}",
                (utc_now_iso(), str(row.get("token_id") or "")),
            )
            return self._build_principal_row(row)

    def ensure_bootstrap_api_key(
        self,
        *,
        org_name: str,
        email: str,
        full_name: str,
        plain_token: str,
        scopes: Optional[List[str]] = None,
        role: str = "owner",
    ) -> Dict[str, Any]:
        existing = self.get_principal_by_bearer_token(plain_token)
        if existing:
            return {
                "status": "exists",
                "org_id": existing.get("org_id"),
                "user_id": existing.get("user_id"),
                "token_id": existing.get("token_id"),
            }

        with self._transaction() as conn:
            org_row = self._fetchone(
                conn,
                "SELECT id FROM organizations ORDER BY created_at ASC LIMIT 1",
                (),
            )
            if org_row is None:
                org_id = f"org_{uuid.uuid4().hex}"
                self._execute(
                    conn,
                    "INSERT INTO organizations (id, name, created_at) VALUES ({p}, {p}, {p})",
                    (org_id, str(org_name or "Default Org").strip() or "Default Org", utc_now_iso()),
                )
            else:
                org_id = str(org_row.get("id") or "")

            user_row = self._fetchone(
                conn,
                "SELECT id FROM users WHERE email = {p} LIMIT 1",
                (str(email or "").strip().lower(),),
            )
            if user_row is None:
                user_id = f"user_{uuid.uuid4().hex}"
                self._execute(
                    conn,
                    """
                    INSERT INTO users (id, email, full_name, is_active, created_at)
                    VALUES ({p}, {p}, {p}, {p}, {p})
                    """,
                    (
                        user_id,
                        str(email or "").strip().lower(),
                        str(full_name or "").strip() or None,
                        self._bool_db(True),
                        utc_now_iso(),
                    ),
                )
            else:
                user_id = str(user_row.get("id") or "")

            self._execute(
                conn,
                """
                INSERT INTO memberships (org_id, user_id, role, is_active, created_at)
                VALUES ({p}, {p}, {p}, {p}, {p})
                ON CONFLICT(org_id, user_id) DO UPDATE SET
                    role = EXCLUDED.role,
                    is_active = EXCLUDED.is_active
                """,
                (org_id, user_id, str(role or "owner").strip().lower(), self._bool_db(True), utc_now_iso()),
            )

            token_id = f"key_{uuid.uuid4().hex}"
            self._execute(
                conn,
                """
                INSERT INTO api_keys (
                    id, org_id, user_id, name, key_hash, prefix, scopes_json,
                    is_active, expires_at, last_used_at, created_at
                )
                VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, NULL, NULL, {p})
                ON CONFLICT(key_hash) DO NOTHING
                """,
                (
                    token_id,
                    org_id,
                    user_id,
                    "Bootstrap Admin Key",
                    self.hash_token(plain_token),
                    plain_token[:12],
                    self._json_db(self._normalize_scopes(scopes or ["api:*", "admin:*"])),
                    self._bool_db(True),
                    utc_now_iso(),
                ),
            )

        principal = self.get_principal_by_bearer_token(plain_token) or {}
        return {
            "status": "created" if principal else "unknown",
            "org_id": principal.get("org_id"),
            "user_id": principal.get("user_id"),
            "token_id": principal.get("token_id"),
        }

    @staticmethod
    def hash_token(token: str) -> str:
        return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()

    def _seed_default_roles(self) -> None:
        roles = [
            ("owner", "Owner", ["api:*", "admin:*"]),
            ("admin", "Admin", ["api:*", "admin:*"]),
            ("member", "Member", ["api:read", "api:write"]),
            ("viewer", "Viewer", ["api:read"]),
        ]
        with self._transaction() as conn:
            for role_key, label, permissions in roles:
                self._execute(
                    conn,
                    """
                    INSERT INTO roles (key, label, permissions_json, created_at)
                    VALUES ({p}, {p}, {p}, {p})
                    ON CONFLICT(key) DO UPDATE SET
                        label = EXCLUDED.label,
                        permissions_json = EXCLUDED.permissions_json
                    """,
                    (role_key, label, self._json_db(permissions), utc_now_iso()),
                )

    @staticmethod
    def _normalize_scopes(scopes: Optional[List[str]]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for raw in scopes or []:
            value = str(raw or "").strip().lower()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out or ["api:read"]

    @staticmethod
    def _parse_scopes(raw: Any) -> List[str]:
        if isinstance(raw, list):
            return [str(x).strip().lower() for x in raw if str(x).strip()]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return []
            if isinstance(parsed, list):
                return [str(x).strip().lower() for x in parsed if str(x).strip()]
        return []

    def _is_accessible(self, row: Dict[str, Any]) -> bool:
        if not self._bool_value(row.get("token_active")):
            return False
        if not self._bool_value(row.get("user_active")):
            return False
        if not self._bool_value(row.get("membership_active")):
            return False
        expires_at = str(row.get("expires_at") or "").strip()
        if not expires_at:
            return True
        try:
            dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            return True
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt > datetime.now(UTC)

    def _build_principal_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        scopes = self._parse_scopes(row.get("scopes_json"))
        return {
            "token_id": str(row.get("token_id") or ""),
            "token_type": str(row.get("token_type") or "api_key"),
            "org_id": str(row.get("org_id") or ""),
            "user_id": str(row.get("user_id") or ""),
            "role": str(row.get("role") or "member").strip().lower() or "member",
            "scopes": scopes,
            "email": str(row.get("email") or "").strip() or None,
            "full_name": str(row.get("full_name") or "").strip() or None,
        }

    @staticmethod
    def _bool_value(raw: Any) -> bool:
        if isinstance(raw, bool):
            return raw
        if raw is None:
            return False
        if isinstance(raw, (int, float)):
            return int(raw) != 0
        text = str(raw).strip().lower()
        return text in {"1", "true", "yes", "on", "t"}

    def _bool_db(self, value: bool) -> Any:
        if self.backend == "postgres":
            return bool(value)
        return 1 if value else 0

    def _json_db(self, value: Any) -> Any:
        if self.backend == "sqlite":
            return json.dumps(value)
        psycopg = self._require_psycopg()
        return psycopg.types.json.Json(value)

    def _true_value_sql(self) -> str:
        return "TRUE" if self.backend == "postgres" else "1"

    @contextmanager
    def _transaction(self) -> Generator[Any, None, None]:
        if self.backend == "sqlite":
            assert self._sqlite is not None
            with self._lock:
                try:
                    yield self._sqlite
                    self._sqlite.commit()
                except Exception:
                    self._sqlite.rollback()
                    raise
            return

        psycopg = self._require_psycopg()
        with self._lock:
            with psycopg.connect(self.postgres_dsn) as conn:
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

    def _execute(self, conn: Any, template_sql: str, params: tuple[Any, ...]) -> None:
        sql = self._format_sql(template_sql)
        if self.backend == "sqlite":
            conn.execute(sql, params)
            return
        with conn.cursor() as cur:
            cur.execute(sql, params)

    def _fetchone(self, conn: Any, template_sql: str, params: tuple[Any, ...]) -> Optional[Dict[str, Any]]:
        sql = self._format_sql(template_sql)
        if self.backend == "sqlite":
            row = conn.execute(sql, params).fetchone()
            return self._row_to_dict(row) if row else None
        psycopg = self._require_psycopg()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return dict(row) if row else None

    def _format_sql(self, template_sql: str) -> str:
        placeholder = "%s" if self.backend == "postgres" else "?"
        sql = template_sql.replace("{p}", placeholder)
        sql = sql.replace("{true_value}", self._true_value_sql())
        return sql

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    @staticmethod
    def _sqlite_schema() -> str:
        return """
        CREATE TABLE IF NOT EXISTS organizations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memberships (
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            role TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            PRIMARY KEY(org_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS roles (
            key TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            permissions_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            scopes_json TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            expires_at TEXT,
            last_used_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_api_keys_org_user
            ON api_keys(org_id, user_id);

        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            scopes_json TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            expires_at TEXT,
            last_used_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_org_user
            ON sessions(org_id, user_id);

        CREATE TABLE IF NOT EXISTS auth_audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id TEXT,
            user_id TEXT,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL
        );
        """

    def _run_postgres_schema(self, conn: Any) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS organizations (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memberships (
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            role TEXT NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(org_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS roles (
            key TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            permissions_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            scopes_json JSONB NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            expires_at TIMESTAMPTZ,
            last_used_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_api_keys_org_user
            ON api_keys(org_id, user_id);

        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            org_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            scopes_json JSONB NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            expires_at TIMESTAMPTZ,
            last_used_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_org_user
            ON sessions(org_id, user_id);

        CREATE TABLE IF NOT EXISTS auth_audit_events (
            id BIGSERIAL PRIMARY KEY,
            org_id TEXT,
            user_id TEXT,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            details_json JSONB,
            created_at TIMESTAMPTZ NOT NULL
        );
        """
        with conn.cursor() as cur:
            cur.execute(sql)

    @staticmethod
    def _require_psycopg() -> Any:
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for postgres auth repository") from exc
        return psycopg
