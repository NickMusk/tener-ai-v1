from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


def _require_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
    except Exception as exc:
        raise RuntimeError("psycopg is required for postgres migrations") from exc
    return psycopg


@dataclass
class PostgresMigrationRunner:
    dsn: str
    migrations_dir: str

    def apply_all(self) -> Dict[str, Any]:
        psycopg = _require_psycopg()
        dir_path = Path(self.migrations_dir)
        if not dir_path.exists():
            raise RuntimeError(f"migrations dir not found: {dir_path}")
        files = sorted([path for path in dir_path.glob("*.sql") if path.is_file()])
        applied: List[str] = []

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )

            for path in files:
                version = path.name
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM schema_migrations WHERE version = %s LIMIT 1", (version,))
                    exists = cur.fetchone() is not None
                if exists:
                    continue

                sql = path.read_text(encoding="utf-8")
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
                applied.append(version)
            conn.commit()

        return {
            "status": "ok",
            "migrations_dir": str(dir_path),
            "migrations_total": len(files),
            "applied_now": applied,
        }

