from __future__ import annotations

import os
import unittest
from pathlib import Path

from tener_ai.auth import AuthRepository
from tener_ai.db_pg import PostgresMigrationRunner


class PostgresSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]
        cls.dsn = str(os.environ.get("TENER_TEST_POSTGRES_DSN", "") or "").strip()
        if not cls.dsn:
            raise unittest.SkipTest("TENER_TEST_POSTGRES_DSN is not set")
        try:
            import psycopg  # noqa: F401
        except Exception as exc:  # pragma: no cover
            raise unittest.SkipTest(f"psycopg is unavailable: {exc}")

    def test_migrations_and_auth_repository(self) -> None:
        runner = PostgresMigrationRunner(
            dsn=self.dsn,
            migrations_dir=str(self.root / "migrations"),
        )
        out = runner.apply_all()
        self.assertEqual(out.get("status"), "ok")

        repo = AuthRepository(
            backend="postgres",
            postgres_dsn=self.dsn,
        )
        repo.init_schema()
        org_id = repo.create_organization(name="PG Smoke Org")
        user_id = repo.create_user(email="pg-smoke-user@tener.local", full_name="PG Smoke")
        repo.upsert_membership(org_id=org_id, user_id=user_id, role="admin", is_active=True)
        created = repo.create_api_key(
            org_id=org_id,
            user_id=user_id,
            name="PG Smoke Key",
            scopes=["api:*", "admin:*"],
        )
        principal = repo.get_principal_by_bearer_token(str(created.get("token") or ""))
        self.assertIsInstance(principal, dict)
        self.assertEqual(principal.get("org_id"), org_id)
        self.assertEqual(principal.get("user_id"), user_id)


if __name__ == "__main__":
    unittest.main()

