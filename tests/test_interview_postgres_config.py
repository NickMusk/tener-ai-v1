from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from tener_interview.config import InterviewModuleConfig
from tener_interview import http_api


class InterviewPostgresConfigTests(unittest.TestCase):
    def test_config_reads_postgres_backend_and_dsn(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TENER_INTERVIEW_DB_BACKEND": "postgres",
                "TENER_INTERVIEW_DB_DSN": "postgres://user:pass@localhost:5432/interview",
            },
            clear=False,
        ):
            cfg = InterviewModuleConfig.from_env()
        self.assertEqual(cfg.db_backend, "postgres")
        self.assertEqual(cfg.db_dsn, "postgres://user:pass@localhost:5432/interview")

    def test_build_services_uses_postgres_database_when_enabled(self) -> None:
        fake_db = MagicMock()
        fake_db.init_schema.return_value = None

        with patch.dict(
            os.environ,
            {
                "TENER_INTERVIEW_DB_BACKEND": "postgres",
                "TENER_INTERVIEW_DB_DSN": "postgres://user:pass@localhost:5432/interview",
                "TENER_INTERVIEW_PROVIDER": "hireflix_mock",
            },
            clear=False,
        ), patch.object(http_api, "InterviewPostgresDatabase", return_value=fake_db) as pg_db_cls:
            services = http_api.build_services()

        pg_db_cls.assert_called_once_with(dsn="postgres://user:pass@localhost:5432/interview")
        fake_db.init_schema.assert_called_once()
        self.assertIs(services["db"], fake_db)
        self.assertEqual(services["config"].db_backend, "postgres")


if __name__ == "__main__":
    unittest.main()

