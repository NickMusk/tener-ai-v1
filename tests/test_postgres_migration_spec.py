from __future__ import annotations

import re
import unittest
from pathlib import Path


class PostgresMigrationSpecTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]

    def test_runtime_schema_tables_are_present_in_migrations(self) -> None:
        migrations_sql = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.root / "migrations").glob("*.sql"))
        )
        tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)", migrations_sql))

        expected_runtime_tables = {
            "jobs",
            "job_culture_profiles",
            "candidates",
            "candidate_job_matches",
            "candidate_signals",
            "conversations",
            "messages",
            "operation_logs",
            "pre_resume_sessions",
            "pre_resume_events",
            "webhook_events",
            "job_step_progress",
            "candidate_agent_assessments",
            "linkedin_accounts",
            "linkedin_onboarding_sessions",
            "outbound_actions",
            "linkedin_account_daily_counters",
            "linkedin_account_weekly_counters",
            "job_linkedin_account_assignments",
            "interview_sessions",
            "interview_results",
            "interview_events",
            "candidate_interview_summary",
            "idempotency_keys",
            "job_interview_assessments",
        }
        missing = sorted(expected_runtime_tables - tables)
        self.assertFalse(missing, f"Missing runtime tables in postgres migrations: {missing}")

    def test_auth_schema_tables_are_present_in_migrations(self) -> None:
        migrations_sql = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.root / "migrations").glob("*.sql"))
        )
        tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)", migrations_sql))

        expected_auth_tables = {
            "organizations",
            "users",
            "memberships",
            "roles",
            "api_keys",
            "sessions",
            "auth_audit_events",
        }
        missing = sorted(expected_auth_tables - tables)
        self.assertFalse(missing, f"Missing auth tables in postgres migrations: {missing}")


if __name__ == "__main__":
    unittest.main()
