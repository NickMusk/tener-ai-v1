from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Dependency-safe insertion order.
TABLE_ORDER: List[str] = [
    "organizations",
    "users",
    "roles",
    "memberships",
    "api_keys",
    "sessions",
    "auth_audit_events",
    "jobs",
    "job_culture_profiles",
    "candidates",
    "candidate_job_matches",
    "candidate_signals",
    "conversations",
    "messages",
    "job_step_progress",
    "candidate_agent_assessments",
    "linkedin_accounts",
    "linkedin_onboarding_sessions",
    "job_linkedin_account_assignments",
    "pre_resume_sessions",
    "pre_resume_events",
    "outbound_actions",
    "linkedin_account_daily_counters",
    "linkedin_account_weekly_counters",
    "webhook_events",
    "operation_logs",
]

JSON_COLUMN_NAMES = {
    "preferred_languages",
    "languages",
    "skills",
    "verification_notes",
    "meta",
    "details",
    "payload",
    "output_json",
    "profile_json",
    "sources_json",
    "warnings_json",
    "search_queries_json",
    "permissions_json",
    "scopes_json",
    "details_json",
    "metadata",
    "payload_json",
    "result_json",
    "resume_links",
    "state_json",
    "signal_meta",
}

BOOL_COLUMN_NAMES = {
    "is_active",
}


@dataclass
class TableBackfillStats:
    table: str
    copied: int = 0
    failed: int = 0
    skipped: int = 0


@dataclass
class BackfillResult:
    sqlite_path: str
    tables: List[TableBackfillStats]

    @property
    def copied_total(self) -> int:
        return sum(item.copied for item in self.tables)

    @property
    def failed_total(self) -> int:
        return sum(item.failed for item in self.tables)

    @property
    def skipped_total(self) -> int:
        return sum(item.skipped for item in self.tables)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sqlite_path": self.sqlite_path,
            "copied_total": self.copied_total,
            "failed_total": self.failed_total,
            "skipped_total": self.skipped_total,
            "tables": [
                {
                    "table": item.table,
                    "copied": item.copied,
                    "failed": item.failed,
                    "skipped": item.skipped,
                }
                for item in self.tables
            ],
        }


def _require_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
    except Exception as exc:
        raise RuntimeError("psycopg is required for sqlite->postgres backfill") from exc
    return psycopg


def _validate_identifier(name: str) -> str:
    token = str(name or "").strip().lower()
    if not IDENTIFIER_RE.match(token):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return token


def _table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _table_exists_postgres(conn: Any, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        row = cur.fetchone()
    return bool(row and row[0])


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({_validate_identifier(table)})").fetchall()
    return [str(row[1]) for row in rows]


def _postgres_columns(conn: Any, table: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position ASC
            """,
            (table,),
        )
        rows = cur.fetchall()
    for name, data_type, udt_name, column_default in rows:
        out[str(name)] = {
            "data_type": data_type,
            "udt_name": udt_name,
            "column_default": column_default,
        }
    return out


def _coerce_value(column: str, value: Any, pg_meta: Dict[str, Any], psycopg: Any) -> Any:
    if value is None:
        return None

    col = str(column or "")
    data_type = str(pg_meta.get("data_type") or "").lower()
    udt_name = str(pg_meta.get("udt_name") or "").lower()

    if col in BOOL_COLUMN_NAMES or data_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return int(value) != 0
        return str(value).strip().lower() in {"1", "true", "yes", "on", "t"}

    if col in JSON_COLUMN_NAMES or data_type == "jsonb" or udt_name == "jsonb":
        if isinstance(value, (dict, list)):
            return psycopg.types.json.Json(value)
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return psycopg.types.json.Json({})
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = raw
            return psycopg.types.json.Json(parsed)
        return psycopg.types.json.Json(value)

    return value


def _build_common_columns(sqlite_cols: Sequence[str], pg_cols: Dict[str, Dict[str, Any]]) -> List[str]:
    return [col for col in sqlite_cols if col in pg_cols]


def _reset_pg_sequence_if_needed(conn: Any, table: str, pg_cols: Dict[str, Dict[str, Any]]) -> None:
    if "id" not in pg_cols:
        return
    default = str(pg_cols.get("id", {}).get("column_default") or "")
    if "nextval(" not in default:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE(MAX(id), 1), COALESCE(MAX(id), 0) > 0) FROM {table}",
            (table,),
        )


def _copy_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn: Any,
    table: str,
    *,
    batch_size: int,
    truncate_first: bool,
) -> TableBackfillStats:
    psycopg = _require_psycopg()
    stats = TableBackfillStats(table=table)
    safe_table = _validate_identifier(table)

    if not _table_exists_sqlite(sqlite_conn, safe_table):
        stats.skipped += 1
        return stats
    if not _table_exists_postgres(pg_conn, safe_table):
        stats.skipped += 1
        return stats

    sqlite_cols = _sqlite_columns(sqlite_conn, safe_table)
    pg_cols = _postgres_columns(pg_conn, safe_table)
    common_cols = _build_common_columns(sqlite_cols, pg_cols)
    if not common_cols:
        stats.skipped += 1
        return stats

    columns_sql = ", ".join(common_cols)
    placeholders = ", ".join(["%s"] * len(common_cols))
    insert_sql = f"INSERT INTO {safe_table} ({columns_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    with pg_conn.cursor() as cur:
        if truncate_first:
            cur.execute(f"TRUNCATE TABLE {safe_table} RESTART IDENTITY CASCADE")

        sqlite_cursor = sqlite_conn.execute(f"SELECT {columns_sql} FROM {safe_table}")
        while True:
            rows = sqlite_cursor.fetchmany(max(1, int(batch_size)))
            if not rows:
                break

            converted_batch: List[Tuple[Any, ...]] = []
            for row in rows:
                converted_row: List[Any] = []
                for index, col in enumerate(common_cols):
                    value = row[index]
                    converted_row.append(_coerce_value(col, value, pg_cols[col], psycopg))
                converted_batch.append(tuple(converted_row))

            try:
                cur.executemany(insert_sql, converted_batch)
                stats.copied += int(cur.rowcount or 0)
            except Exception:
                stats.failed += len(converted_batch)

    _reset_pg_sequence_if_needed(pg_conn, safe_table, pg_cols)
    return stats


def backfill_sqlite_to_postgres(
    *,
    sqlite_path: str,
    postgres_dsn: str,
    batch_size: int = 500,
    truncate_first: bool = False,
    tables: Optional[Sequence[str]] = None,
) -> BackfillResult:
    sqlite_file = str(sqlite_path or "").strip()
    dsn = str(postgres_dsn or "").strip()
    if not sqlite_file:
        raise ValueError("sqlite_path is required")
    if not dsn:
        raise ValueError("postgres_dsn is required")
    if not Path(sqlite_file).exists():
        raise FileNotFoundError(f"sqlite db not found: {sqlite_file}")

    selected_tables = [
        _validate_identifier(item)
        for item in (tables or TABLE_ORDER)
    ]

    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_conn.row_factory = sqlite3.Row

    psycopg = _require_psycopg()
    try:
        with psycopg.connect(dsn) as pg_conn:
            table_stats: List[TableBackfillStats] = []
            for table in selected_tables:
                table_stats.append(
                    _copy_table(
                        sqlite_conn,
                        pg_conn,
                        table,
                        batch_size=batch_size,
                        truncate_first=truncate_first,
                    )
                )
            pg_conn.commit()
    finally:
        sqlite_conn.close()

    return BackfillResult(sqlite_path=sqlite_file, tables=table_stats)


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Tener SQLite data into Postgres")
    parser.add_argument(
        "--sqlite-path",
        default=str(os.environ.get("TENER_DB_PATH", "")),
        help="Path to SQLite database (defaults to TENER_DB_PATH)",
    )
    parser.add_argument(
        "--postgres-dsn",
        default=str(os.environ.get("TENER_DB_DSN", "")),
        help="Postgres DSN (defaults to TENER_DB_DSN)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per batch",
    )
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Truncate each destination table before copy",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table allowlist (defaults to built-in ordered list)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    selected_tables = [x.strip() for x in str(args.tables or "").split(",") if x.strip()]
    result = backfill_sqlite_to_postgres(
        sqlite_path=str(args.sqlite_path or "").strip(),
        postgres_dsn=str(args.postgres_dsn or "").strip(),
        batch_size=max(1, int(args.batch_size or 500)),
        truncate_first=bool(args.truncate_first),
        tables=selected_tables or None,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=True, indent=2))
    return 0 if result.failed_total == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
