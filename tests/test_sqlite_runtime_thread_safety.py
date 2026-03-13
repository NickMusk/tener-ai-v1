from __future__ import annotations

import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import Database, utc_now_iso


class SqliteRuntimeThreadSafetyTests(unittest.TestCase):
    def test_shared_sqlite_connection_serializes_cross_thread_access(self) -> None:
        with TemporaryDirectory() as td:
            db = Database(str(Path(td) / "thread_safety.sqlite3"))
            db.init_schema()
            account_id = db.upsert_linkedin_account(
                provider="unipile",
                provider_account_id="acc-thread-1",
                status="connected",
                connected_at=utc_now_iso(),
            )

            tx_started = threading.Event()
            release_tx = threading.Event()
            reader_done = threading.Event()
            reader_result: dict[str, object] = {}
            errors: list[BaseException] = []

            def writer() -> None:
                try:
                    with db.transaction():
                        tx_started.set()
                        release_tx.wait(timeout=2)
                except BaseException as exc:  # pragma: no cover
                    errors.append(exc)

            def reader() -> None:
                try:
                    tx_started.wait(timeout=2)
                    started_at = time.monotonic()
                    items = db.list_linkedin_accounts(limit=10)
                    reader_result["elapsed"] = time.monotonic() - started_at
                    reader_result["count"] = len(items)
                    reader_result["first_id"] = int(items[0]["id"]) if items else None
                except BaseException as exc:  # pragma: no cover
                    errors.append(exc)
                finally:
                    reader_done.set()

            writer_thread = threading.Thread(target=writer)
            reader_thread = threading.Thread(target=reader)
            writer_thread.start()
            self.assertTrue(tx_started.wait(timeout=2))
            reader_thread.start()

            time.sleep(0.05)
            self.assertFalse(reader_done.is_set())

            release_tx.set()
            writer_thread.join(timeout=2)
            reader_thread.join(timeout=2)

            self.assertFalse(errors)
            self.assertTrue(reader_done.is_set())
            self.assertEqual(reader_result.get("count"), 1)
            self.assertEqual(reader_result.get("first_id"), account_id)
            self.assertGreaterEqual(float(reader_result.get("elapsed") or 0.0), 0.05)


if __name__ == "__main__":
    unittest.main()
