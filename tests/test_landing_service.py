from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tener_ai.db import Database
from tener_ai.landing import LandingService, LandingValidationError


class LandingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.db = Database(str(tmp / "landing.sqlite3"))
        self.db.init_schema()
        self.service = LandingService(self.db)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_newsletter_deduplicates_email_and_keeps_single_row(self) -> None:
        first = self.service.submit_newsletter(
            {"email": "Founder@Example.com", "full_name": "Jane Doe"},
            source_path="/landing",
            ip_address="127.0.0.1",
            user_agent="tests",
        )
        second = self.service.submit_newsletter(
            {"email": " founder@example.com ", "company_name": "Acme"},
            source_path="/landing",
            ip_address="127.0.0.1",
            user_agent="tests",
        )

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])

        rows = self.db.list_newsletter_subscriptions(limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "founder@example.com")
        self.assertEqual(rows[0]["full_name"], "Jane Doe")
        self.assertEqual(rows[0]["company_name"], "Acme")

    def test_contact_request_requires_core_fields(self) -> None:
        with self.assertRaises(LandingValidationError) as ctx:
            self.service.submit_contact_request(
                {"work_email": "bad-email"},
                source_path="/landing",
                ip_address="127.0.0.1",
                user_agent="tests",
            )

        self.assertIn("full_name", ctx.exception.field_errors)
        self.assertIn("company_name", ctx.exception.field_errors)
        self.assertIn("hiring_need", ctx.exception.field_errors)
        self.assertIn("work_email", ctx.exception.field_errors)


if __name__ == "__main__":
    unittest.main()
