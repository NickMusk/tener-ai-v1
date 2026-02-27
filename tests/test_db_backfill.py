from __future__ import annotations

import unittest

from tener_ai import db_backfill


class BackfillHelpersTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
