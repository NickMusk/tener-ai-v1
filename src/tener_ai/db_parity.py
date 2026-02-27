from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


TABLE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

DEFAULT_PARITY_TABLES: List[str] = [
    "jobs",
    "candidates",
    "candidate_job_matches",
    "conversations",
    "messages",
    "operation_logs",
    "pre_resume_sessions",
    "pre_resume_events",
    "job_step_progress",
    "candidate_agent_assessments",
    "webhook_events",
    "outbound_actions",
]

DEFAULT_KEYSET_TABLES: Dict[str, Tuple[str, ...]] = {
    "jobs": ("id",),
    "candidates": ("id",),
    "candidate_job_matches": ("job_id", "candidate_id"),
    "conversations": ("id",),
    "messages": ("id",),
    "pre_resume_sessions": ("session_id",),
    "pre_resume_events": ("id",),
    "job_step_progress": ("job_id", "step"),
    "candidate_agent_assessments": ("job_id", "candidate_id", "agent_key", "stage_key"),
    "webhook_events": ("event_key",),
    "outbound_actions": ("id",),
}


def _require_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
    except Exception as exc:
        raise RuntimeError("psycopg is required for postgres parity") from exc
    return psycopg


def validate_table_name(name: str) -> str:
    token = str(name or "").strip().lower()
    if not TABLE_NAME_RE.match(token):
        raise ValueError(f"invalid table name: {name}")
    return token


def normalize_tables(tables: Optional[Sequence[str]]) -> List[str]:
    raw = list(tables or DEFAULT_PARITY_TABLES)
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        table = validate_table_name(item)
        if table in seen:
            continue
        seen.add(table)
        out.append(table)
    return out


def normalize_keyset_tables(keyset_tables: Optional[Dict[str, Sequence[str]]]) -> Dict[str, Tuple[str, ...]]:
    raw = dict(keyset_tables or DEFAULT_KEYSET_TABLES)
    out: Dict[str, Tuple[str, ...]] = {}
    for table_name, columns in raw.items():
        safe_table = validate_table_name(table_name)
        if not columns:
            raise ValueError(f"keyset columns cannot be empty for table: {table_name}")
        safe_columns: List[str] = []
        seen: set[str] = set()
        for column in columns:
            safe_col = validate_table_name(column)
            if safe_col in seen:
                continue
            seen.add(safe_col)
            safe_columns.append(safe_col)
        if not safe_columns:
            raise ValueError(f"keyset columns cannot be empty for table: {table_name}")
        out[safe_table] = tuple(safe_columns)
    return out


def collect_sqlite_table_counts(*, sqlite_path: str, tables: Sequence[str]) -> Dict[str, Optional[int]]:
    safe_tables = normalize_tables(tables)
    out: Dict[str, Optional[int]] = {}
    with sqlite3.connect(str(sqlite_path)) as conn:
        for table in safe_tables:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
                (table,),
            ).fetchone()
            if exists is None:
                out[table] = None
                continue
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            out[table] = int(row[0] if row else 0)
    return out


def collect_postgres_table_counts(*, postgres_dsn: str, tables: Sequence[str]) -> Dict[str, Optional[int]]:
    psycopg = _require_psycopg()
    safe_tables = normalize_tables(tables)
    out: Dict[str, Optional[int]] = {}
    with psycopg.connect(str(postgres_dsn)) as conn:
        with conn.cursor() as cur:
            for table in safe_tables:
                cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                reg = cur.fetchone()
                if not reg or reg[0] is None:
                    out[table] = None
                    continue
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = cur.fetchone()
                out[table] = int(row[0] if row else 0)
    return out


def compare_counts(
    *,
    tables: Sequence[str],
    sqlite_counts: Dict[str, Optional[int]],
    postgres_counts: Dict[str, Optional[int]],
) -> List[Dict[str, Any]]:
    safe_tables = normalize_tables(tables)
    mismatches: List[Dict[str, Any]] = []
    for table in safe_tables:
        sqlite_count = sqlite_counts.get(table)
        postgres_count = postgres_counts.get(table)
        if sqlite_count == postgres_count:
            continue
        mismatches.append(
            {
                "table": table,
                "sqlite_count": sqlite_count,
                "postgres_count": postgres_count,
                "delta": None if sqlite_count is None or postgres_count is None else int(postgres_count) - int(sqlite_count),
            }
        )
    return mismatches


def _sqlite_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]).strip().lower() for row in rows}


def _collect_sqlite_keyset(
    conn: sqlite3.Connection,
    *,
    table: str,
    key_columns: Sequence[str],
) -> Optional[set[Tuple[Any, ...]]]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    if exists is None:
        return None
    available_columns = _sqlite_table_columns(conn, table)
    if any(col not in available_columns for col in key_columns):
        return None
    select_cols = ", ".join(key_columns)
    rows = conn.execute(f"SELECT {select_cols} FROM {table}").fetchall()
    return {tuple(row[index] for index in range(len(key_columns))) for row in rows}


def _collect_postgres_keyset(
    conn: Any,
    *,
    table: str,
    key_columns: Sequence[str],
) -> Optional[set[Tuple[Any, ...]]]:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        reg = cur.fetchone()
        if not reg or reg[0] is None:
            return None

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table,),
        )
        available_columns = {str(row[0]).strip().lower() for row in cur.fetchall()}
        if any(col not in available_columns for col in key_columns):
            return None

        select_cols = ", ".join(key_columns)
        cur.execute(f"SELECT {select_cols} FROM {table}")
        rows = cur.fetchall()
    return {tuple(row[index] for index in range(len(key_columns))) for row in rows}


def _keyset_sample(*, key_columns: Sequence[str], keys: set[Tuple[Any, ...]], sample_limit: int) -> List[Dict[str, Any]]:
    limit = max(1, int(sample_limit or 20))
    sample: List[Dict[str, Any]] = []
    for key_tuple in sorted(keys, key=lambda item: repr(item))[:limit]:
        sample.append({key_columns[idx]: key_tuple[idx] for idx in range(len(key_columns))})
    return sample


def compare_keysets(
    *,
    table: str,
    key_columns: Sequence[str],
    sqlite_keys: Optional[set[Tuple[Any, ...]]],
    postgres_keys: Optional[set[Tuple[Any, ...]]],
    sample_limit: int = 20,
) -> Dict[str, Any]:
    if sqlite_keys is None or postgres_keys is None:
        return {
            "table": table,
            "key_columns": list(key_columns),
            "status": "skipped",
            "reason": "missing_table_or_columns",
            "sqlite_key_count": None if sqlite_keys is None else len(sqlite_keys),
            "postgres_key_count": None if postgres_keys is None else len(postgres_keys),
            "missing_in_postgres_count": None,
            "missing_in_sqlite_count": None,
            "missing_in_postgres_sample": [],
            "missing_in_sqlite_sample": [],
        }

    missing_in_postgres = sqlite_keys - postgres_keys
    missing_in_sqlite = postgres_keys - sqlite_keys
    status = "ok" if not missing_in_postgres and not missing_in_sqlite else "mismatch"
    return {
        "table": table,
        "key_columns": list(key_columns),
        "status": status,
        "reason": None,
        "sqlite_key_count": len(sqlite_keys),
        "postgres_key_count": len(postgres_keys),
        "missing_in_postgres_count": len(missing_in_postgres),
        "missing_in_sqlite_count": len(missing_in_sqlite),
        "missing_in_postgres_sample": _keyset_sample(
            key_columns=key_columns,
            keys=missing_in_postgres,
            sample_limit=sample_limit,
        ),
        "missing_in_sqlite_sample": _keyset_sample(
            key_columns=key_columns,
            keys=missing_in_sqlite,
            sample_limit=sample_limit,
        ),
    }


def build_deep_keyset_report(
    *,
    sqlite_path: str,
    postgres_dsn: str,
    keyset_tables: Optional[Dict[str, Sequence[str]]] = None,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    psycopg = _require_psycopg()
    safe_map = normalize_keyset_tables(keyset_tables)
    safe_sample_limit = max(1, min(int(sample_limit or 20), 200))

    checks: List[Dict[str, Any]] = []
    with sqlite3.connect(str(sqlite_path)) as sqlite_conn, psycopg.connect(str(postgres_dsn)) as pg_conn:
        for table, key_columns in safe_map.items():
            sqlite_keys = _collect_sqlite_keyset(
                sqlite_conn,
                table=table,
                key_columns=key_columns,
            )
            postgres_keys = _collect_postgres_keyset(
                pg_conn,
                table=table,
                key_columns=key_columns,
            )
            checks.append(
                compare_keysets(
                    table=table,
                    key_columns=key_columns,
                    sqlite_keys=sqlite_keys,
                    postgres_keys=postgres_keys,
                    sample_limit=safe_sample_limit,
                )
            )

    mismatches = [item for item in checks if str(item.get("status") or "") == "mismatch"]
    skipped = [item for item in checks if str(item.get("status") or "") == "skipped"]
    return {
        "enabled": True,
        "status": "ok" if not mismatches else "mismatch",
        "sample_limit": safe_sample_limit,
        "checks": checks,
        "mismatch_count": len(mismatches),
        "skipped_count": len(skipped),
    }


def build_parity_report(
    *,
    sqlite_path: str,
    postgres_dsn: str,
    tables: Optional[Sequence[str]] = None,
    deep: bool = False,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    safe_tables = normalize_tables(tables)
    sqlite_counts = collect_sqlite_table_counts(sqlite_path=sqlite_path, tables=safe_tables)
    postgres_counts = collect_postgres_table_counts(postgres_dsn=postgres_dsn, tables=safe_tables)
    mismatches = compare_counts(tables=safe_tables, sqlite_counts=sqlite_counts, postgres_counts=postgres_counts)

    report: Dict[str, Any] = {
        "status": "ok" if not mismatches else "mismatch",
        "sqlite_path": str(sqlite_path),
        "tables_checked": safe_tables,
        "sqlite_counts": sqlite_counts,
        "postgres_counts": postgres_counts,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }

    if deep:
        deep_report = build_deep_keyset_report(
            sqlite_path=sqlite_path,
            postgres_dsn=postgres_dsn,
            sample_limit=sample_limit,
        )
        report["deep"] = deep_report
        if str(deep_report.get("status") or "") != "ok":
            report["status"] = "mismatch"
    else:
        report["deep"] = {
            "enabled": False,
            "status": "disabled",
            "sample_limit": max(1, min(int(sample_limit or 20), 200)),
            "checks": [],
            "mismatch_count": 0,
            "skipped_count": 0,
        }

    return report


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare SQLite and Postgres table counts")
    parser.add_argument("--sqlite-path", required=True, help="Path to SQLite database")
    parser.add_argument("--postgres-dsn", required=True, help="Postgres DSN")
    parser.add_argument("--tables", default="", help="Comma-separated table list")
    parser.add_argument("--deep", action="store_true", help="Enable deep key-set parity checks")
    parser.add_argument("--sample-limit", type=int, default=20, help="Deep parity mismatch sample size per table")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    sqlite_path = str(args.sqlite_path or "").strip()
    postgres_dsn = str(args.postgres_dsn or "").strip()
    if not sqlite_path:
        raise ValueError("sqlite_path is required")
    if not postgres_dsn:
        raise ValueError("postgres_dsn is required")
    if not Path(sqlite_path).exists():
        raise FileNotFoundError(f"sqlite db not found: {sqlite_path}")

    tables = [x.strip() for x in str(args.tables or "").split(",") if x.strip()]
    report = build_parity_report(
        sqlite_path=sqlite_path,
        postgres_dsn=postgres_dsn,
        tables=tables or None,
        deep=bool(args.deep),
        sample_limit=max(1, min(int(args.sample_limit or 20), 200)),
    )
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0 if str(report.get("status") or "") == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
