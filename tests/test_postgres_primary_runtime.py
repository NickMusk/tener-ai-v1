from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

# Prevent import-time default service bootstrap from writing inside repository runtime dir.
os.environ.setdefault("TENER_DB_PATH", str(Path(gettempdir()) / "tener_postgres_primary_runtime_bootstrap.sqlite3"))

from tener_ai import main as api_main


class _FakePostgresRuntimeDB:
    def __init__(self, dsn: str) -> None:
        self.dsn = str(dsn)

    def log_operation(
        self,
        operation: str,
        status: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        return None


class PostgresPrimaryRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_services = api_main.SERVICES

    def tearDown(self) -> None:
        api_main.SERVICES = self._previous_services

    def test_build_services_uses_postgres_runtime_primary_without_sqlite(self) -> None:
        fake_runner = MagicMock()
        fake_runner.apply_all.return_value = {"status": "ok"}

        with patch.dict(
            os.environ,
            {
                "TENER_DB_BACKEND": "postgres",
                "TENER_DB_DSN": "postgres://user:pass@localhost:5432/tener",
            },
            clear=False,
        ), patch("tener_ai.main.PostgresMigrationRunner", return_value=fake_runner), patch(
            "tener_ai.main.PostgresRuntimeDatabase", _FakePostgresRuntimeDB
        ), patch("tener_ai.main.Database", side_effect=AssertionError("sqlite database should not be initialized")):
            services = api_main.build_services()

        self.assertEqual(services["db_runtime_mode"], "postgres_primary")
        self.assertEqual(services["db_backend"], "postgres")
        self.assertIsInstance(services["db"], _FakePostgresRuntimeDB)
        self.assertIs(services["read_db"], services["db"])
        self.assertEqual(str((services.get("db_read_status") or {}).get("source") or ""), "postgres")
        self.assertEqual(str(services.get("db_primary_path") or ""), "")
        self.assertEqual(str((services.get("postgres_migration_status") or {}).get("status") or ""), "ok")

    def test_switch_read_source_sqlite_is_skipped_in_postgres_primary(self) -> None:
        runtime_db = _FakePostgresRuntimeDB("postgres://example")
        api_main.SERVICES = {
            "db": runtime_db,
            "read_db": runtime_db,
            "db_runtime_mode": "postgres_primary",
            "db_read_status": {"status": "ok", "source": "postgres"},
            "postgres_dsn": "postgres://example",
        }
        out = api_main.TenerRequestHandler._switch_read_source(source="sqlite", reason="test")
        self.assertEqual(str(out.get("status") or ""), "skipped")
        self.assertEqual(str(out.get("source") or ""), "postgres")
        self.assertIs(api_main.SERVICES.get("read_db"), runtime_db)
        self.assertEqual(str((api_main.SERVICES.get("db_read_status") or {}).get("source") or ""), "postgres")

    def test_switch_read_source_postgres_keeps_runtime_db_in_postgres_primary(self) -> None:
        runtime_db = _FakePostgresRuntimeDB("postgres://example")
        api_main.SERVICES = {
            "db": runtime_db,
            "read_db": None,
            "db_runtime_mode": "postgres_primary",
            "db_read_status": {"status": "ok", "source": "postgres"},
            "postgres_dsn": "",
        }
        out = api_main.TenerRequestHandler._switch_read_source(
            source="postgres",
            postgres_dsn="postgres://example",
            reason="manual_test",
        )
        self.assertEqual(str(out.get("status") or ""), "ok")
        self.assertEqual(str(out.get("source") or ""), "postgres")
        self.assertIs(api_main.SERVICES.get("read_db"), runtime_db)


if __name__ == "__main__":
    unittest.main()
