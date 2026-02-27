from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tener_ai import db_parity


class DbParityTests(unittest.TestCase):
    def test_validate_and_normalize_tables(self) -> None:
        self.assertEqual(db_parity.validate_table_name("Jobs"), "jobs")
        self.assertEqual(db_parity.normalize_tables(["jobs", "jobs", "candidates"]), ["jobs", "candidates"])
        with self.assertRaises(ValueError):
            db_parity.validate_table_name("jobs;drop table")

    def test_collect_sqlite_table_counts(self) -> None:
        with TemporaryDirectory() as td:
            path = Path(td) / "parity.sqlite3"
            conn = sqlite3.connect(str(path))
            conn.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY, title TEXT)")
            conn.execute("INSERT INTO jobs (title) VALUES ('A')")
            conn.execute("INSERT INTO jobs (title) VALUES ('B')")
            conn.commit()
            conn.close()

            counts = db_parity.collect_sqlite_table_counts(
                sqlite_path=str(path),
                tables=["jobs", "missing_table"],
            )
            self.assertEqual(counts.get("jobs"), 2)
            self.assertIsNone(counts.get("missing_table"))

    def test_compare_counts(self) -> None:
        mismatches = db_parity.compare_counts(
            tables=["jobs", "candidates"],
            sqlite_counts={"jobs": 10, "candidates": 3},
            postgres_counts={"jobs": 12, "candidates": 3},
        )
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0]["table"], "jobs")
        self.assertEqual(mismatches[0]["delta"], 2)

    def test_compare_keysets_detects_bidirectional_missing(self) -> None:
        out = db_parity.compare_keysets(
            table="jobs",
            key_columns=("id",),
            sqlite_keys={(1,), (2,)},
            postgres_keys={(2,), (3,)},
            sample_limit=10,
        )
        self.assertEqual(out["status"], "mismatch")
        self.assertEqual(out["missing_in_postgres_count"], 1)
        self.assertEqual(out["missing_in_sqlite_count"], 1)
        self.assertEqual(out["missing_in_postgres_sample"][0]["id"], 1)
        self.assertEqual(out["missing_in_sqlite_sample"][0]["id"], 3)

    def test_build_parity_report_marks_mismatch_when_deep_mismatch(self) -> None:
        with patch.object(db_parity, "collect_sqlite_table_counts", return_value={"jobs": 1}), patch.object(
            db_parity, "collect_postgres_table_counts", return_value={"jobs": 1}
        ), patch.object(
            db_parity,
            "build_deep_keyset_report",
            return_value={
                "enabled": True,
                "status": "mismatch",
                "sample_limit": 20,
                "checks": [{"table": "jobs", "status": "mismatch"}],
                "mismatch_count": 1,
                "skipped_count": 0,
            },
        ):
            report = db_parity.build_parity_report(
                sqlite_path="/tmp/x.sqlite3",
                postgres_dsn="postgres://example",
                tables=["jobs"],
                deep=True,
                sample_limit=20,
            )
        self.assertEqual(report["status"], "mismatch")
        self.assertEqual((report.get("deep") or {}).get("mismatch_count"), 1)

    def test_build_parity_report_deep_disabled_shape(self) -> None:
        with patch.object(db_parity, "collect_sqlite_table_counts", return_value={"jobs": 1}), patch.object(
            db_parity, "collect_postgres_table_counts", return_value={"jobs": 1}
        ):
            report = db_parity.build_parity_report(
                sqlite_path="/tmp/x.sqlite3",
                postgres_dsn="postgres://example",
                tables=["jobs"],
                deep=False,
                sample_limit=5,
            )
        self.assertEqual(report["status"], "ok")
        self.assertEqual((report.get("deep") or {}).get("status"), "disabled")
        self.assertEqual((report.get("deep") or {}).get("sample_limit"), 5)


if __name__ == "__main__":
    unittest.main()
