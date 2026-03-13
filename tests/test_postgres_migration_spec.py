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
            "newsletter_subscriptions",
            "contact_requests",
            "outreach_account_events",
            "pre_resume_sessions",
            "pre_resume_events",
            "candidate_prescreens",
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

    def test_job_budget_and_candidate_prescreen_columns_are_present(self) -> None:
        migrations_sql = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.root / "migrations").glob("*.sql"))
        )
        normalized = migrations_sql.lower()
        for column in ("salary_min", "salary_max", "salary_currency", "work_authorization_required"):
            self.assertIn(column, normalized)
        for column in (
            "must_have_answers_json",
            "salary_expectation_min",
            "salary_expectation_max",
            "salary_expectation_currency",
            "location_confirmed",
            "work_authorization_confirmed",
            "cv_received",
        ):
            self.assertIn(column, normalized)

    def test_job_pause_columns_are_present(self) -> None:
        migrations_sql = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((self.root / "migrations").glob("*.sql"))
        )
        normalized = migrations_sql.lower()
        for column in ("job_state", "paused_at", "pause_reason"):
            self.assertIn(column, normalized)


if __name__ == "__main__":
    unittest.main()
