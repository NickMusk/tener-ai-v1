from __future__ import annotations

import re
import unittest
from pathlib import Path

from tener_ai import db_backfill


class BackfillHelpersTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_validate_identifier(self) -> None:
        self.assertEqual(db_backfill._validate_identifier("jobs"), "jobs")
        with self.assertRaises(ValueError):
            db_backfill._validate_identifier("jobs;drop table")

    def test_build_common_columns_preserves_sqlite_order(self) -> None:
        sqlite_cols = ["id", "name", "payload"]
        pg_cols = {
            "payload": {},
            "name": {},
            "id": {},
        }
        self.assertEqual(db_backfill._build_common_columns(sqlite_cols, pg_cols), ["id", "name", "payload"])

    def test_coerce_json_boolean_and_passthrough(self) -> None:
        class _FakeJson:
            def __init__(self, value):
                self.value = value

        class _FakeTypes:
            class json:
                Json = _FakeJson

        class _FakePsycopg:
            types = _FakeTypes

        json_value = db_backfill._coerce_value(
            "details",
            '{"k":1}',
            {"data_type": "jsonb", "udt_name": "jsonb"},
            _FakePsycopg,
        )
        self.assertIsInstance(json_value, _FakeJson)
        self.assertEqual(json_value.value.get("k"), 1)

        bool_value = db_backfill._coerce_value(
            "is_active",
            1,
            {"data_type": "boolean", "udt_name": "bool"},
            _FakePsycopg,
        )
        self.assertIs(bool_value, True)

        plain_value = db_backfill._coerce_value(
            "title",
            "Backend Engineer",
            {"data_type": "text", "udt_name": "text"},
            _FakePsycopg,
        )
        self.assertEqual(plain_value, "Backend Engineer")

    def test_table_order_covers_all_sqlite_runtime_tables(self) -> None:
        db_source = (self.root / "src" / "tener_ai" / "db.py").read_text(encoding="utf-8")
        runtime_tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)", db_source))
        missing = sorted(runtime_tables - set(db_backfill.TABLE_ORDER))
        self.assertFalse(missing, f"Missing sqlite runtime tables in backfill order: {missing}")

    def test_outreach_account_events_is_backfilled_after_dependencies(self) -> None:
        order = db_backfill.TABLE_ORDER
        event_index = order.index("outreach_account_events")
        for dependency in ("linkedin_accounts", "jobs", "candidates", "conversations"):
            self.assertLess(order.index(dependency), event_index)


if __name__ == "__main__":
    unittest.main()
