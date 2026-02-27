from __future__ import annotations

import json
import hashlib
import ipaddress
import os
import re
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urlerror, request as urlrequest
from urllib.parse import parse_qs, unquote, urlparse

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .auth import AuthService
from .candidate_profile import CandidateProfileService
from .candidate_scoring import CandidateScoringPolicy
from .company_culture_profile import (
    BraveHtmlSearchProvider,
    CompanyCultureProfileService,
    GoogleCSESearchProvider,
    HeuristicCompanyProfileSynthesizer,
    OpenAICompanyProfileSynthesizer,
    SeedSearchProvider,
    SimpleHtmlTextExtractor,
    UrllibPageFetcher,
    canonicalize_url,
)
from .db import Database
from .db_backfill import backfill_sqlite_to_postgres
from .db_parity import DEFAULT_PARITY_TABLES, build_parity_report
from .db_dual import DualWriteDatabase, PostgresMirrorWriter
from .db_pg import PostgresMigrationRunner
from .db_read_pg import PostgresReadDatabase
from .emulator import EmulatorProjectStore
from .instructions import AgentEvaluationPlaybook, AgentInstructions
from .interview_client import InterviewAPIClient
from .linkedin_accounts import LinkedInAccountService
from .llm_responder import CandidateLLMResponder
from .linkedin_provider import build_linkedin_provider
from .matching import MatchingEngine
from .outreach_policy import LinkedInOutreachPolicy
from .pre_resume_service import PreResumeCommunicationService
from .workflow import WorkflowService


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def default_interview_api_base() -> str:
    configured = str(os.environ.get("TENER_INTERVIEW_API_BASE", "")).strip()
    if configured:
        return configured.rstrip("/")
    if os.environ.get("RENDER"):
        return "https://tener-interview-dashboard.onrender.com"
    return ""


def apply_agent_instructions(services: Dict[str, Any]) -> None:
    instructions: AgentInstructions = services["instructions"]
    workflow: WorkflowService = services["workflow"]

    workflow.sourcing_agent.instruction = instructions.get("sourcing")
    workflow.verification_agent.instruction = instructions.get("verification")
    workflow.outreach_agent.instruction = instructions.get("outreach")
    workflow.faq_agent.instruction = instructions.get("faq")
    workflow.stage_instructions = {
        "sourcing": instructions.get("sourcing"),
        "enrich": instructions.get("enrich"),
        "verification": instructions.get("verification"),
        "add": instructions.get("add"),
        "outreach": instructions.get("outreach"),
        "faq": instructions.get("faq"),
        "pre_resume": instructions.get("pre_resume"),
        "interview_invite": instructions.get("interview_invite"),
    }
    services["pre_resume"].instruction = instructions.get("pre_resume")


def build_services() -> Dict[str, Any]:
    root = project_root()
    db_backend = str(os.environ.get("TENER_DB_BACKEND", "sqlite") or "sqlite").strip().lower()
    local_db_path = str(root / "runtime" / "tener_v1.sqlite3")
    default_db_path = "/var/data/tener_v1.sqlite3" if os.environ.get("RENDER") else local_db_path
    configured_db_path = os.environ.get("TENER_DB_PATH", default_db_path)
    db_path = configured_db_path
    postgres_dsn = str(os.environ.get("TENER_DB_DSN", "") or "").strip()
    postgres_migration_status: Dict[str, Any] = {"status": "disabled", "reason": "sqlite_backend"}
    db_runtime_mode = "sqlite_primary"
    if db_backend in {"postgres", "dual"}:
        if not postgres_dsn:
            raise RuntimeError("TENER_DB_DSN is required when TENER_DB_BACKEND is set to postgres/dual")
        runner = PostgresMigrationRunner(
            dsn=postgres_dsn,
            migrations_dir=str(root / "migrations"),
        )
        postgres_migration_status = runner.apply_all()
    try:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        db_path = local_db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    rules_path = os.environ.get("TENER_MATCHING_RULES_PATH", str(root / "config" / "matching_rules.json"))
    templates_path = os.environ.get("TENER_TEMPLATES_PATH", str(root / "config" / "outreach_templates.json"))
    instructions_path = os.environ.get("TENER_AGENT_INSTRUCTIONS_PATH", str(root / "config" / "agent_instructions.json"))
    evaluation_playbook_path = os.environ.get(
        "TENER_AGENT_EVAL_INSTRUCTIONS_PATH",
        str(root / "config" / "agent_evaluation_instructions.json"),
    )
    scoring_formula_path = os.environ.get(
        "TENER_SCORING_FORMULA_PATH",
        str(root / "config" / "candidate_scoring_formula.json"),
    )
    outreach_policy_path = os.environ.get(
        "TENER_OUTREACH_POLICY_PATH",
        str(root / "config" / "linkedin_outreach_policy.json"),
    )
    mock_profiles_path = os.environ.get("TENER_MOCK_LINKEDIN_DATA_PATH", str(root / "data" / "mock_linkedin_profiles.json"))
    forced_test_ids_path = os.environ.get(
        "TENER_FORCED_TEST_IDS_PATH",
        str(root / "config" / "forced_test_linkedin_ids.txt"),
    )
    emulator_projects_dir = os.environ.get(
        "TENER_EMULATOR_PROJECTS_DIR",
        str(root / "config" / "emulator" / "projects"),
    )
    emulator_company_profiles_path = os.environ.get(
        "TENER_EMULATOR_COMPANY_PROFILES_PATH",
        str(root / "config" / "emulator" / "company_profiles.json"),
    )
    forced_test_score_raw = os.environ.get("TENER_FORCED_TEST_SCORE", "0.99")
    try:
        forced_test_score = float(forced_test_score_raw)
    except ValueError:
        forced_test_score = 0.99

    try:
        sqlite_db = Database(db_path=db_path)
    except Exception:
        if db_path != local_db_path:
            sqlite_db = Database(db_path=local_db_path)
        else:
            raise
    sqlite_db.init_schema()
    db: Any = sqlite_db
    if db_backend in {"postgres", "dual"}:
        dual_strict = env_bool("TENER_DB_DUAL_STRICT", False)
        db = DualWriteDatabase(
            primary=sqlite_db,
            mirror=PostgresMirrorWriter(postgres_dsn),
            strict=dual_strict,
        )
        db_runtime_mode = "sqlite_primary_postgres_mirror"

    db_read_source_raw = str(os.environ.get("TENER_DB_READ_SOURCE", "auto") or "auto").strip().lower()
    if db_read_source_raw not in {"sqlite", "postgres", "auto"}:
        db_read_source_raw = "auto"
    db_read_source = db_read_source_raw
    if db_read_source == "auto":
        db_read_source = "postgres" if db_backend in {"postgres", "dual"} else "sqlite"
    db_read_status: Dict[str, Any] = {
        "status": "ok",
        "source": db_read_source,
        "requested_source": db_read_source_raw,
        "reason": "default",
    }
    read_db: Any = db
    if db_read_source == "postgres":
        if not postgres_dsn:
            db_read_status = {
                "status": "degraded",
                "source": "sqlite",
                "requested_source": db_read_source_raw,
                "reason": "postgres_read_requested_without_dsn",
            }
        else:
            try:
                read_db = PostgresReadDatabase(postgres_dsn)
                db_read_status = {
                    "status": "ok",
                    "source": "postgres",
                    "requested_source": db_read_source_raw,
                    "reason": "postgres_read_enabled",
                }
            except Exception as exc:
                if env_bool("TENER_DB_READ_STRICT", False):
                    raise
                db_read_status = {
                    "status": "degraded",
                    "source": "sqlite",
                    "requested_source": db_read_source_raw,
                    "reason": f"postgres_read_init_failed:{exc}",
                }
    else:
        db_read_status = {
            "status": "ok",
            "source": "sqlite",
            "requested_source": db_read_source_raw,
            "reason": "sqlite_read_enabled",
        }

    instructions = AgentInstructions(path=instructions_path)
    evaluation_playbook = AgentEvaluationPlaybook(path=evaluation_playbook_path)
    scoring_formula = CandidateScoringPolicy(path=scoring_formula_path)
    outreach_policy = LinkedInOutreachPolicy(path=outreach_policy_path)
    matching_engine = MatchingEngine(rules_path=rules_path)
    linkedin_provider = build_linkedin_provider(mock_dataset_path=mock_profiles_path)
    unipile_timeout_raw = os.environ.get("UNIPILE_TIMEOUT_SECONDS", "30")
    connect_ttl_raw = os.environ.get("TENER_LINKEDIN_CONNECT_STATE_TTL_SECONDS", "900")
    try:
        unipile_timeout = int(unipile_timeout_raw)
    except ValueError:
        unipile_timeout = 30
    try:
        connect_ttl = int(connect_ttl_raw)
    except ValueError:
        connect_ttl = 900

    linkedin_account_service = LinkedInAccountService(
        db=db,
        provider="unipile",
        api_key=os.environ.get("UNIPILE_API_KEY", ""),
        base_url=os.environ.get("UNIPILE_BASE_URL", "https://api.unipile.com"),
        timeout_seconds=unipile_timeout,
        state_secret=os.environ.get("TENER_LINKEDIN_CONNECT_STATE_SECRET", ""),
        state_ttl_seconds=connect_ttl,
        connect_url_template=os.environ.get("TENER_UNIPILE_CONNECT_URL_TEMPLATE", ""),
        callback_url=os.environ.get("TENER_LINKEDIN_CONNECT_CALLBACK_URL", ""),
        accounts_path=os.environ.get("UNIPILE_ACCOUNTS_PATH", "/api/v1/accounts"),
        hosted_connect_path=os.environ.get("UNIPILE_HOSTED_LINKEDIN_CONNECT_PATH", "/api/v1/hosted/accounts/linkedin"),
        disconnect_path_template=os.environ.get("UNIPILE_DISCONNECT_PATH_TEMPLATE", "/api/v1/accounts/{account_id}"),
    )

    sourcing_agent = SourcingAgent(
        linkedin_provider=linkedin_provider,
        instruction=instructions.get("sourcing"),
    )
    verification_agent = VerificationAgent(
        matching_engine=matching_engine,
        instruction=instructions.get("verification"),
    )
    outreach_agent = OutreachAgent(
        templates_path=templates_path,
        matching_engine=matching_engine,
        instruction=instructions.get("outreach"),
    )
    faq_agent = FAQAgent(
        templates_path=templates_path,
        matching_engine=matching_engine,
        instruction=instructions.get("faq"),
    )
    followup_delays_raw = os.environ.get("TENER_FOLLOWUP_DELAYS_HOURS", "48,72,72")
    followup_delays: list[float] = []
    for token in followup_delays_raw.split(","):
        part = token.strip()
        if not part:
            continue
        try:
            followup_delays.append(float(part))
        except ValueError:
            continue
    try:
        max_followups = int(os.environ.get("TENER_MAX_FOLLOWUPS", "3"))
    except ValueError:
        max_followups = 3
    interview_followup_delays_raw = os.environ.get("TENER_INTERVIEW_FOLLOWUP_DELAYS_HOURS", "24,48")
    interview_followup_delays: list[float] = []
    for token in interview_followup_delays_raw.split(","):
        part = token.strip()
        if not part:
            continue
        try:
            interview_followup_delays.append(float(part))
        except ValueError:
            continue
    try:
        interview_max_followups = int(os.environ.get("TENER_INTERVIEW_MAX_FOLLOWUPS", "2"))
    except ValueError:
        interview_max_followups = 2
    try:
        interview_invite_ttl_hours = int(os.environ.get("TENER_INTERVIEW_INVITE_TTL_HOURS", "72"))
    except ValueError:
        interview_invite_ttl_hours = 72
    pre_resume_service = PreResumeCommunicationService(
        templates_path=templates_path,
        max_followups=max_followups,
        followup_delays_hours=followup_delays or None,
        instruction=instructions.get("pre_resume"),
    )
    interview_api_base = default_interview_api_base()
    try:
        interview_api_timeout = int(os.environ.get("TENER_INTERVIEW_API_TIMEOUT_SECONDS", "20"))
    except ValueError:
        interview_api_timeout = 20
    interview_client = InterviewAPIClient(interview_api_base, timeout_seconds=interview_api_timeout) if interview_api_base else None
    llm_responder = None
    llm_enabled = env_bool("TENER_LLM_ENABLED", True)
    llm_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    llm_timeout_raw = os.environ.get("TENER_LLM_TIMEOUT_SECONDS", "30")
    try:
        llm_timeout = int(llm_timeout_raw)
    except ValueError:
        llm_timeout = 30
    if llm_enabled and llm_api_key:
        llm_responder = CandidateLLMResponder(
            api_key=llm_api_key,
            model=os.environ.get("TENER_LLM_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini")),
            base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            timeout_seconds=llm_timeout,
        )

    culture_search_mode = str(os.environ.get("TENER_COMPANY_CULTURE_SEARCH_PROVIDER", "brave") or "brave").strip().lower()
    culture_search_timeout_raw = os.environ.get("TENER_COMPANY_CULTURE_SEARCH_TIMEOUT_SECONDS", "20")
    culture_fetch_timeout_raw = os.environ.get("TENER_COMPANY_CULTURE_FETCH_TIMEOUT_SECONDS", "15")
    culture_max_links_raw = os.environ.get("TENER_COMPANY_CULTURE_MAX_LINKS", "10")
    culture_per_query_raw = os.environ.get("TENER_COMPANY_CULTURE_PER_QUERY_LIMIT", "10")
    culture_min_job_board_raw = os.environ.get("TENER_COMPANY_CULTURE_MIN_JOB_BOARD_LINKS", "2")
    try:
        culture_search_timeout = int(culture_search_timeout_raw)
    except ValueError:
        culture_search_timeout = 20
    try:
        culture_fetch_timeout = int(culture_fetch_timeout_raw)
    except ValueError:
        culture_fetch_timeout = 15
    try:
        culture_max_links = int(culture_max_links_raw)
    except ValueError:
        culture_max_links = 10
    try:
        culture_per_query = int(culture_per_query_raw)
    except ValueError:
        culture_per_query = 10
    try:
        culture_min_job_board = int(culture_min_job_board_raw)
    except ValueError:
        culture_min_job_board = 2

    google_cse_api_key = str(os.environ.get("TENER_COMPANY_CULTURE_GOOGLE_CSE_API_KEY", "")).strip()
    google_cse_cx = str(os.environ.get("TENER_COMPANY_CULTURE_GOOGLE_CSE_CX", "")).strip()
    if culture_search_mode == "google" and google_cse_api_key and google_cse_cx:
        culture_search_provider = GoogleCSESearchProvider(
            api_key=google_cse_api_key,
            cx=google_cse_cx,
            timeout_seconds=culture_search_timeout,
        )
    elif culture_search_mode == "seed":
        culture_search_provider = SeedSearchProvider(company_name="Tener", website_url="https://tener.ai")
    else:
        culture_search_provider = BraveHtmlSearchProvider(timeout_seconds=culture_search_timeout)

    use_culture_llm = env_bool("TENER_COMPANY_CULTURE_USE_LLM", True)
    if use_culture_llm and llm_api_key:
        culture_synthesizer = OpenAICompanyProfileSynthesizer(
            api_key=llm_api_key,
            model=os.environ.get("TENER_COMPANY_CULTURE_LLM_MODEL", os.environ.get("TENER_LLM_MODEL", "gpt-4o-mini")),
            base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
            timeout_seconds=max(10, llm_timeout),
            analysis_rules_path=os.environ.get("TENER_COMPANY_CULTURE_ANALYSIS_RULES_PATH", ""),
        )
    else:
        culture_synthesizer = HeuristicCompanyProfileSynthesizer()

    company_culture_service = CompanyCultureProfileService(
        search_provider=culture_search_provider,
        page_fetcher=UrllibPageFetcher(),
        content_extractor=SimpleHtmlTextExtractor(),
        synthesizer=culture_synthesizer,
        max_links=culture_max_links,
        per_query_limit=culture_per_query,
        min_job_board_links=culture_min_job_board,
        fetch_timeout_seconds=culture_fetch_timeout,
    )

    workflow = WorkflowService(
        db=db,
        sourcing_agent=sourcing_agent,
        verification_agent=verification_agent,
        outreach_agent=outreach_agent,
        faq_agent=faq_agent,
        pre_resume_service=pre_resume_service,
        llm_responder=llm_responder,
        interview_client=interview_client,
        agent_evaluation_playbook=evaluation_playbook,
        contact_all_mode=env_bool("TENER_CONTACT_ALL_MODE", True),
        require_resume_before_final_verify=env_bool("TENER_REQUIRE_RESUME_BEFORE_FINAL_VERIFY", True),
        forced_test_ids_path=forced_test_ids_path,
        forced_test_score=forced_test_score,
        interview_invite_ttl_hours=interview_invite_ttl_hours,
        interview_max_followups=interview_max_followups,
        interview_followup_delays_hours=interview_followup_delays or None,
        linkedin_outreach_policy=outreach_policy.to_dict(),
        managed_linkedin_enabled=env_bool("TENER_MANAGED_LINKEDIN_ENABLED", True),
        managed_linkedin_dispatch_inline=env_bool("TENER_MANAGED_LINKEDIN_DISPATCH_INLINE", True),
        managed_unipile_api_key=os.environ.get("UNIPILE_API_KEY", ""),
        managed_unipile_base_url=os.environ.get("UNIPILE_BASE_URL", "https://api.unipile.com"),
        managed_unipile_timeout_seconds=unipile_timeout,
        stage_instructions={
            "sourcing": instructions.get("sourcing"),
            "enrich": instructions.get("enrich"),
            "verification": instructions.get("verification"),
            "add": instructions.get("add"),
            "outreach": instructions.get("outreach"),
            "faq": instructions.get("faq"),
            "pre_resume": instructions.get("pre_resume"),
            "interview_invite": instructions.get("interview_invite"),
        },
    )
    candidate_profile = CandidateProfileService(
        db=db,
        matching_engine=matching_engine,
        scoring_policy=scoring_formula,
        llm_responder=llm_responder,
    )
    emulator_store = EmulatorProjectStore(
        projects_dir=emulator_projects_dir,
        company_profiles_path=emulator_company_profiles_path,
    )
    auth_service = AuthService.from_env(root=root)

    services = {
        "db": db,
        "read_db": read_db,
        "db_primary_path": sqlite_db.db_path,
        "postgres_dsn": postgres_dsn,
        "db_backend": db_backend,
        "db_runtime_mode": db_runtime_mode,
        "db_read_status": db_read_status,
        "db_cutover_state": {
            "status": "idle",
            "executed_at": None,
            "details": {},
        },
        "db_cutover_lock": threading.Lock(),
        "postgres_migration_status": postgres_migration_status,
        "instructions": instructions,
        "evaluation_playbook": evaluation_playbook,
        "scoring_formula": scoring_formula,
        "outreach_policy": outreach_policy,
        "auth": auth_service,
        "linkedin_accounts": linkedin_account_service,
        "matching_engine": matching_engine,
        "pre_resume": pre_resume_service,
        "candidate_profile": candidate_profile,
        "emulator_store": emulator_store,
        "company_culture": company_culture_service,
        "workflow": workflow,
        "interview_api_base": default_interview_api_base(),
    }
    apply_agent_instructions(services)
    return services


SERVICES = build_services()


class TenerRequestHandler(BaseHTTPRequestHandler):
    server_version = "TenerAIV1/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if not self._require_request_auth(method="GET", path=parsed.path):
            return

        if parsed.path == "/dashboard/emulator":
            dashboard = project_root() / "src" / "tener_ai" / "static" / "emulator_dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "emulator dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        if parsed.path in {"/", "/dashboard"}:
            dashboard = project_root() / "src" / "tener_ai" / "static" / "dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        candidate_page_match = re.match(r"^/candidate/(\d+)$", parsed.path)
        if candidate_page_match:
            candidate_page = project_root() / "src" / "tener_ai" / "static" / "candidate_profile.html"
            if not candidate_page.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "candidate profile page not found"})
                return
            self._html_response(HTTPStatus.OK, candidate_page.read_text(encoding="utf-8"))
            return

        if parsed.path == "/api":
            self._json_response(
                HTTPStatus.OK,
                {
                    "service": "Tener AI V1 API",
                    "status": "ok",
                    "endpoints": {
                        "health": "GET /health",
                        "create_job": "POST /api/jobs",
                        "list_jobs": "GET /api/jobs",
                        "get_job": "GET /api/jobs/{job_id}",
                        "job_progress": "GET /api/jobs/{job_id}/progress",
                        "list_job_candidates": "GET /api/jobs/{job_id}/candidates",
                        "candidate_profile": "GET /api/candidates/{candidate_id}/profile?job_id=...&audit=0|1",
                        "candidate_resume_preview": "GET /api/candidates/{candidate_id}/resume-preview?job_id=...&url=...",
                        "candidate_resume_content": "GET /api/candidates/{candidate_id}/resume-preview/content?url=...",
                        "candidate_demo_profile": "POST /api/candidates/demo-profile",
                        "emulator_status": "GET /api/emulator",
                        "emulator_projects": "GET /api/emulator/projects",
                        "emulator_project": "GET /api/emulator/projects/{project_id}",
                        "emulator_company_profiles": "GET /api/emulator/company-profiles",
                        "emulator_company_profile": "GET /api/emulator/company-profiles/{company_key}",
                        "emulator_reload": "POST /api/emulator/reload",
                        "emulator_dashboard": "GET /dashboard/emulator",
                        "job_linkedin_routing": "GET /api/jobs/{job_id}/linkedin-routing",
                        "update_job_jd": "POST /api/jobs/{job_id}/jd",
                        "update_job_linkedin_routing": "POST /api/jobs/{job_id}/linkedin-routing",
                        "run_workflow": "POST /api/workflows/execute",
                        "source_step": "POST /api/steps/source",
                        "enrich_step": "POST /api/steps/enrich",
                        "verify_step": "POST /api/steps/verify",
                        "add_step": "POST /api/steps/add",
                        "outreach_step": "POST /api/steps/outreach",
                        "outreach_dispatch_run": "POST /api/outreach/dispatch/run",
                        "outreach_poll_connections": "POST /api/outreach/poll-connections",
                        "inbound_poll": "POST /api/inbound/poll",
                        "instructions": "GET /api/instructions",
                        "outreach_policy": "GET /api/outreach-policy",
                        "agent_system": "GET /api/agent-system",
                        "reload_instructions": "POST /api/instructions/reload",
                        "reload_outreach_policy": "POST /api/outreach-policy/reload",
                        "pre_resume_start": "POST /api/pre-resume/sessions/start",
                        "pre_resume_list": "GET /api/pre-resume/sessions?limit=100&status=awaiting_reply",
                        "pre_resume_get": "GET /api/pre-resume/sessions/{session_id}",
                        "pre_resume_events": "GET /api/pre-resume/events?limit=200",
                        "pre_resume_inbound": "POST /api/pre-resume/sessions/{session_id}/inbound",
                        "pre_resume_followup": "POST /api/pre-resume/sessions/{session_id}/followup",
                        "pre_resume_followups_run": "POST /api/pre-resume/followups/run",
                        "pre_resume_unreachable": "POST /api/pre-resume/sessions/{session_id}/unreachable",
                        "interview_sync": "POST /api/interviews/sync",
                        "interview_followups_run": "POST /api/interviews/followups/run",
                        "conversation_messages": "GET /api/conversations/{conversation_id}/messages",
                        "chats_overview": "GET /api/chats/overview?limit=200",
                        "linkedin_accounts_list": "GET /api/linkedin/accounts?limit=200&status=connected",
                        "linkedin_connect_callback": "GET /api/linkedin/accounts/connect/callback?state=...",
                        "linkedin_connect_start": "POST /api/linkedin/accounts/connect/start",
                        "linkedin_account_sync": "POST /api/linkedin/accounts/{account_id}/sync",
                        "linkedin_account_disconnect": "POST /api/linkedin/accounts/{account_id}/disconnect",
                        "add_manual_account": "POST /api/agent/accounts/manual",
                        "unipile_webhook": "POST /api/webhooks/unipile",
                        "conversation_inbound": "POST /api/conversations/{conversation_id}/inbound",
                        "logs": "GET /api/logs?limit=100",
                        "db_parity": "GET /api/db/parity?deep=0|1&sample_limit=20",
                        "db_backfill_run": "POST /api/db/backfill/run",
                        "db_read_source_set": "POST /api/db/read-source",
                        "db_cutover_status": "GET /api/db/cutover/status",
                        "db_cutover_preflight": "GET /api/db/cutover/preflight",
                        "db_cutover_run": "POST /api/db/cutover/run",
                        "db_cutover_rollback": "POST /api/db/cutover/rollback",
                        "db_dual_write_strict": "POST /api/db/dual-write/strict",
                        "reload_rules": "POST /api/rules/reload",
                    },
                },
            )
            return

        if parsed.path == "/api/emulator":
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            self._json_response(HTTPStatus.OK, emulator_store.health())
            return

        if parsed.path == "/api/emulator/projects":
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            self._json_response(HTTPStatus.OK, {"items": emulator_store.list_projects()})
            return

        if parsed.path.startswith("/api/emulator/projects/"):
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            match = re.match(r"^/api/emulator/projects/([^/]+)$", parsed.path)
            if not match:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid project id"})
                return
            project_id = unquote(match.group(1))
            project = emulator_store.get_project(project_id=project_id)
            if project is None:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "emulator project not found"})
                return
            self._json_response(HTTPStatus.OK, project)
            return

        if parsed.path == "/api/emulator/company-profiles":
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            self._json_response(HTTPStatus.OK, {"items": emulator_store.list_company_profiles()})
            return

        if parsed.path.startswith("/api/emulator/company-profiles/"):
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            match = re.match(r"^/api/emulator/company-profiles/([^/]+)$", parsed.path)
            if not match:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid company key"})
                return
            company_key = unquote(match.group(1))
            profile = emulator_store.get_company_profile(company_key=company_key)
            if profile is None:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "company profile not found"})
                return
            self._json_response(HTTPStatus.OK, profile)
            return

        if parsed.path.startswith("/api/candidates/") and parsed.path.endswith("/profile"):
            candidate_id = self._extract_id(parsed.path, pattern=r"^/api/candidates/(\d+)/profile$")
            if candidate_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid candidate id"})
                return
            params = parse_qs(parsed.query or "")
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            include_audit = bool(self._safe_bool((params.get("audit") or [None])[0], False))
            explain_raw = self._safe_bool((params.get("explain") or [None])[0], True)
            include_explanation = True if explain_raw is None else bool(explain_raw)
            try:
                payload = SERVICES["candidate_profile"].build_candidate_profile(
                    candidate_id=int(candidate_id),
                    selected_job_id=job_id,
                    include_audit=include_audit,
                    include_explanation=include_explanation,
                )
            except ValueError:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "candidate not found"})
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "candidate profile failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, payload)
            return

        if parsed.path.startswith("/api/candidates/") and parsed.path.endswith("/resume-preview"):
            candidate_id = self._extract_id(parsed.path, pattern=r"^/api/candidates/(\d+)/resume-preview$")
            if candidate_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid candidate id"})
                return
            candidate = SERVICES["db"].get_candidate(candidate_id)
            if not candidate:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "candidate not found"})
                return
            params = parse_qs(parsed.query or "")
            requested_url = str((params.get("url") or [""])[0] or "").strip()
            links = SERVICES["candidate_profile"].list_candidate_resume_links(candidate_id=int(candidate_id))
            allowed = set(links)
            selected_url = requested_url or (links[0] if links else "")
            if selected_url and selected_url not in allowed:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "resume url is not linked to candidate"})
                return
            if selected_url and not (selected_url.startswith("https://") or selected_url.startswith("http://")):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "unsupported resume url scheme"})
                return
            self._json_response(
                HTTPStatus.OK,
                {
                    "candidate_id": int(candidate_id),
                    "candidate_name": candidate.get("full_name"),
                    "available": bool(selected_url),
                    "url": selected_url or None,
                    "links": links,
                },
            )
            return

        if parsed.path.startswith("/api/candidates/") and parsed.path.endswith("/resume-preview/content"):
            candidate_id = self._extract_id(parsed.path, pattern=r"^/api/candidates/(\d+)/resume-preview/content$")
            if candidate_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid candidate id"})
                return
            candidate = SERVICES["db"].get_candidate(candidate_id)
            if not candidate:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "candidate not found"})
                return
            params = parse_qs(parsed.query or "")
            selected_url = str((params.get("url") or [""])[0] or "").strip()
            if not selected_url:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "url is required"})
                return
            links = SERVICES["candidate_profile"].list_candidate_resume_links(candidate_id=int(candidate_id))
            if selected_url not in set(links):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "resume url is not linked to candidate"})
                return
            if not (selected_url.startswith("https://") or selected_url.startswith("http://")):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "unsupported resume url scheme"})
                return
            req = urlrequest.Request(
                url=selected_url,
                method="GET",
                headers={"User-Agent": "TenerResumePreview/1.0", "Accept": "application/pdf,*/*"},
            )
            try:
                with urlrequest.urlopen(req, timeout=20) as resp:
                    data = resp.read((10 * 1024 * 1024) + 1)
                    if len(data) > 10 * 1024 * 1024:
                        self._json_response(HTTPStatus.BAD_REQUEST, {"error": "resume content too large"})
                        return
                    content_type = str(resp.headers.get("Content-Type") or "").strip().lower()
                    if "pdf" not in content_type:
                        content_type = "application/pdf"
                    else:
                        content_type = content_type.split(";")[0].strip() or "application/pdf"
                    self._binary_response(
                        status=HTTPStatus.OK,
                        content_type=content_type,
                        payload=data,
                        extra_headers={
                            "Content-Disposition": "inline; filename=\"resume.pdf\"",
                            "Cache-Control": "no-store",
                        },
                    )
                    return
            except urlerror.HTTPError as exc:
                self._json_response(
                    HTTPStatus.BAD_GATEWAY,
                    {"error": "resume fetch failed", "details": f"upstream_http_{int(exc.code)}"},
                )
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.BAD_GATEWAY,
                    {"error": "resume fetch failed", "details": str(exc)},
                )
                return

        if parsed.path == "/health":
            cutover_state = SERVICES.get("db_cutover_state") if isinstance(SERVICES.get("db_cutover_state"), dict) else {}
            payload: Dict[str, Any] = {
                "status": "ok",
                "db_backend": SERVICES.get("db_backend"),
                "db_runtime_mode": SERVICES.get("db_runtime_mode"),
                "db_read_status": SERVICES.get("db_read_status"),
                "db_cutover": {
                    "status": cutover_state.get("status"),
                    "executed_at": cutover_state.get("executed_at"),
                },
                "postgres_migration_status": SERVICES.get("postgres_migration_status"),
            }
            db_obj = SERVICES.get("db")
            dual_status = getattr(db_obj, "dual_write_status", None)
            if isinstance(dual_status, dict):
                payload["dual_write"] = dual_status
            self._json_response(HTTPStatus.OK, payload)
            return

        if parsed.path == "/api/instructions":
            self._json_response(HTTPStatus.OK, SERVICES["instructions"].to_dict())
            return

        if parsed.path == "/api/outreach-policy":
            self._json_response(HTTPStatus.OK, SERVICES["outreach_policy"].to_dict())
            return

        if parsed.path == "/api/db/parity":
            params = parse_qs(parsed.query or "")
            tables = list(DEFAULT_PARITY_TABLES)
            deep = bool(self._safe_bool((params.get("deep") or ["0"])[0], False))
            sample_limit_raw = self._safe_int((params.get("sample_limit") or ["20"])[0], 20)
            sample_limit = max(1, min(int(sample_limit_raw or 20), 200))
            sqlite_path = str(SERVICES.get("db_primary_path") or "").strip()
            postgres_dsn = str(SERVICES.get("postgres_dsn") or os.environ.get("TENER_DB_DSN", "") or "").strip()
            if not sqlite_path:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "error", "reason": "sqlite_primary_path_missing"},
                )
                return
            if not postgres_dsn:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "status": "error",
                        "reason": "postgres_dsn_missing",
                        "sqlite_path": sqlite_path,
                        "tables": tables,
                    },
                )
                return
            try:
                report = build_parity_report(
                    sqlite_path=sqlite_path,
                    postgres_dsn=postgres_dsn,
                    tables=tables,
                    deep=deep,
                    sample_limit=sample_limit,
                )
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"status": "error", "reason": "parity_check_failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, report)
            return

        if parsed.path == "/api/db/cutover/status":
            if not self._require_admin_access():
                return
            cutover_lock = SERVICES.get("db_cutover_lock")
            in_progress = bool(getattr(cutover_lock, "locked", lambda: False)())
            payload = {
                "status": "ok",
                "cutover": SERVICES.get("db_cutover_state") or {"status": "idle", "executed_at": None, "details": {}},
                "in_progress": in_progress,
                "db_read_status": SERVICES.get("db_read_status"),
                "db_backend": SERVICES.get("db_backend"),
                "db_runtime_mode": SERVICES.get("db_runtime_mode"),
            }
            db_obj = SERVICES.get("db")
            dual_status = getattr(db_obj, "dual_write_status", None)
            if isinstance(dual_status, dict):
                payload["dual_write"] = dual_status
            self._json_response(HTTPStatus.OK, payload)
            return

        if parsed.path == "/api/db/cutover/preflight":
            if not self._require_admin_access():
                return
            sqlite_path = str(SERVICES.get("db_primary_path") or "").strip()
            postgres_dsn = str(SERVICES.get("postgres_dsn") or os.environ.get("TENER_DB_DSN", "") or "").strip()
            if not sqlite_path:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "error", "reason": "sqlite_primary_path_missing"},
                )
                return
            if not postgres_dsn:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "error", "reason": "postgres_dsn_missing"},
                )
                return
            try:
                report = self._build_cutover_preflight_report(
                    sqlite_path=sqlite_path,
                    postgres_dsn=postgres_dsn,
                )
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"status": "error", "reason": "cutover_preflight_failed", "details": str(exc)},
                )
                return
            code = HTTPStatus.OK if str(report.get("status") or "") == "ok" else HTTPStatus.CONFLICT
            self._json_response(code, report)
            return

        if parsed.path == "/api/linkedin/accounts":
            if not self._require_admin_access():
                return
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["200"])[0], 200) or 200
            status = str((params.get("status") or [""])[0] or "").strip().lower() or None
            out = SERVICES["linkedin_accounts"].list_accounts(status=status, limit=limit)
            self._json_response(HTTPStatus.OK, out)
            return

        if parsed.path == "/api/linkedin/accounts/connect/callback":
            params = parse_qs(parsed.query or "")
            out = SERVICES["linkedin_accounts"].complete_connect_callback(query=params)
            status = str(out.get("status") or "").strip().lower()
            SERVICES["db"].log_operation(
                operation="linkedin.connect.callback",
                status="ok" if status in {"connected", "already_completed"} else "error",
                entity_type="linkedin_onboarding",
                entity_id=str(out.get("session_id") or ""),
                details={"result": out},
            )
            if status in {"connected", "already_completed"}:
                self._html_response(
                    HTTPStatus.OK,
                    """
                    <html><body style="font-family:Arial,sans-serif;padding:24px;">
                    <h2>LinkedIn account connected</h2>
                    <p>You can close this tab and return to Tener dashboard.</p>
                    </body></html>
                    """.strip(),
                )
                return
            self._html_response(
                HTTPStatus.BAD_REQUEST,
                f"""
                <html><body style="font-family:Arial,sans-serif;padding:24px;">
                <h2>LinkedIn connect failed</h2>
                <p>{self._escape_html(str(out.get("reason") or "unknown_error"))}</p>
                </body></html>
                """.strip(),
            )
            return

        if parsed.path == "/api/agent-system":
            self._json_response(
                HTTPStatus.OK,
                {
                    "agents": {
                        "culture_analyst": {
                            "name": SERVICES["workflow"]._agent_name("culture_analyst"),
                            "stages": ["target_profile", "culture_fit_brief"],
                            "active": False,
                        },
                        "job_architect": {
                            "name": SERVICES["workflow"]._agent_name("job_architect"),
                            "stages": ["jd_structuring", "core_profile_definition"],
                            "active": False,
                        },
                        "sourcing_vetting": {
                            "name": SERVICES["workflow"]._agent_name("sourcing_vetting"),
                            "stages": ["source", "enrich", "verify", "add", "vetting"],
                            "active": True,
                        },
                        "communication": {
                            "name": SERVICES["workflow"]._agent_name("communication"),
                            "stages": ["outreach", "faq", "pre_resume", "interview_invite", "dialogue"],
                            "active": True,
                        },
                        "interview_evaluation": {
                            "name": SERVICES["workflow"]._agent_name("interview_evaluation"),
                            "stages": ["interview_results"],
                            "active": True,
                        },
                    },
                    "evaluation_playbook": SERVICES["evaluation_playbook"].to_dict(),
                    "scoring_formula": SERVICES["scoring_formula"].to_dict(),
                    "outreach_policy": SERVICES["outreach_policy"].to_dict(),
                },
            )
            return

        if parsed.path == "/api/pre-resume/sessions":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100)
            status = (params.get("status") or [None])[0]
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            items = SERVICES["db"].list_pre_resume_sessions(limit=limit or 100, status=status, job_id=job_id)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path == "/api/pre-resume/events":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["200"])[0], 200)
            session_id = (params.get("session_id") or [None])[0]
            items = SERVICES["db"].list_pre_resume_events(limit=limit or 200, session_id=session_id)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path.startswith("/api/pre-resume/sessions/"):
            match = re.match(r"^/api/pre-resume/sessions/([^/]+)$", parsed.path)
            if match:
                session_id = match.group(1)
                session = SERVICES["pre_resume"].get_session(session_id)
                if not session:
                    db_row = SERVICES["db"].get_pre_resume_session(session_id)
                    if db_row and isinstance(db_row.get("state_json"), dict):
                        SERVICES["pre_resume"].seed_session(db_row["state_json"])
                        session = SERVICES["pre_resume"].get_session(session_id)
                if not session:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "session not found"})
                    return
                self._json_response(HTTPStatus.OK, session)
                return

        if parsed.path == "/api/jobs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100)
            items = self._read_db().list_jobs(limit=limit or 100)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/candidates"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/candidates$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            rows = self._read_db().list_candidates_for_job(job_id)
            scoring_formula = SERVICES.get("scoring_formula")
            if scoring_formula is not None:
                rows = [scoring_formula.decorate_candidate_row(row) for row in rows]
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "items": rows})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/linkedin-routing"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/linkedin-routing$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            job = SERVICES["db"].get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            routing_mode = str(job.get("linkedin_routing_mode") or "auto").strip().lower()
            if routing_mode not in {"auto", "manual"}:
                routing_mode = "auto"
            account_ids = SERVICES["db"].list_job_linkedin_account_ids(job_id=job_id)
            assigned_accounts = SERVICES["db"].list_job_linkedin_accounts(job_id=job_id)
            available_accounts = SERVICES["db"].list_linkedin_accounts(limit=500, status="connected")
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": job_id,
                    "routing_mode": routing_mode,
                    "account_ids": account_ids,
                    "assigned_accounts": assigned_accounts,
                    "available_accounts": available_accounts,
                },
            )
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/progress"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/progress$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            read_db = self._read_db()
            job = read_db.get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            steps = read_db.list_job_step_progress(job_id=job_id)
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "items": steps})
            return

        if parsed.path.startswith("/api/jobs/"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            job = self._read_db().get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            self._json_response(HTTPStatus.OK, job)
            return

        if parsed.path.startswith("/api/conversations/") and parsed.path.endswith("/messages"):
            conversation_id = self._extract_id(parsed.path, pattern=r"^/api/conversations/(\d+)/messages$")
            if conversation_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid conversation id"})
                return
            conversation = SERVICES["db"].get_conversation(conversation_id)
            if not conversation:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "conversation not found"})
                return
            items = SERVICES["db"].list_messages(conversation_id)
            self._json_response(HTTPStatus.OK, {"conversation": conversation, "items": items})
            return

        if parsed.path == "/api/chats/overview":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["200"])[0], 200)
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            items = self._read_db().list_conversations_overview(limit=limit or 200, job_id=job_id)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path == "/api/logs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100)
            items = self._read_db().list_logs(limit=limit)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not self._require_request_auth(method="POST", path=parsed.path):
            return
        payload = self._read_json_body()
        if isinstance(payload, dict) and payload.get("_error"):
            self._json_response(HTTPStatus.BAD_REQUEST, payload)
            return

        if parsed.path == "/api/emulator/reload":
            emulator_store = SERVICES.get("emulator_store")
            if emulator_store is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "emulator store unavailable"})
                return
            self._json_response(HTTPStatus.OK, emulator_store.reload())
            return

        if parsed.path == "/api/db/backfill/run":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            sqlite_path = str(body.get("sqlite_path") or SERVICES.get("db_primary_path") or "").strip()
            postgres_dsn = str(
                body.get("postgres_dsn")
                or SERVICES.get("postgres_dsn")
                or os.environ.get("TENER_DB_DSN", "")
                or ""
            ).strip()
            if not sqlite_path:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "error", "reason": "sqlite_primary_path_missing"},
                )
                return
            if not postgres_dsn:
                self._json_response(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "status": "error",
                        "reason": "postgres_dsn_missing",
                        "sqlite_path": sqlite_path,
                    },
                )
                return
            batch_size_raw = self._safe_int(body.get("batch_size"), 500)
            batch_size = max(1, min(int(batch_size_raw or 500), 5000))
            truncate_first = bool(self._safe_bool(body.get("truncate_first"), False))
            tables_raw = body.get("tables")
            tables: Optional[List[str]] = None
            if tables_raw is not None:
                if not isinstance(tables_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "tables must be an array"})
                    return
                normalized = [str(item).strip() for item in tables_raw if str(item).strip()]
                tables = normalized or None
            try:
                result = backfill_sqlite_to_postgres(
                    sqlite_path=sqlite_path,
                    postgres_dsn=postgres_dsn,
                    batch_size=batch_size,
                    truncate_first=truncate_first,
                    tables=tables,
                )
                output = result.to_dict()
                output["status"] = "ok" if int(output.get("failed_total") or 0) == 0 else "partial"
                output["batch_size"] = batch_size
                output["truncate_first"] = truncate_first
                output["tables_requested"] = tables or []
                SERVICES["db"].log_operation(
                    operation="db.backfill.run",
                    status=str(output.get("status") or "unknown"),
                    entity_type="database",
                    entity_id="sqlite_to_postgres",
                    details=output,
                )
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="db.backfill.run",
                    status="error",
                    entity_type="database",
                    entity_id="sqlite_to_postgres",
                    details={
                        "sqlite_path": sqlite_path,
                        "batch_size": batch_size,
                        "truncate_first": truncate_first,
                        "tables_requested": tables or [],
                        "error": str(exc),
                    },
                )
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"status": "error", "reason": "backfill_failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, output)
            return

        if parsed.path == "/api/db/read-source":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            source = str(body.get("source") or "").strip().lower()
            if source not in {"sqlite", "postgres"}:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "source must be sqlite or postgres"})
                return
            postgres_dsn = str(
                body.get("postgres_dsn")
                or SERVICES.get("postgres_dsn")
                or os.environ.get("TENER_DB_DSN", "")
                or ""
            ).strip()
            try:
                out = self._switch_read_source(source=source, postgres_dsn=postgres_dsn, reason="manual_switch")
                SERVICES["db"].log_operation(
                    operation="db.read_source.set",
                    status="ok",
                    entity_type="database",
                    entity_id="read_source",
                    details={"source": source},
                )
                self._json_response(HTTPStatus.OK, out)
            except RuntimeError as exc:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "error", "reason": str(exc)})
                return
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="db.read_source.set",
                    status="error",
                    entity_type="database",
                    entity_id="read_source",
                    details={"source": source, "error": str(exc)},
                )
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"status": "error", "reason": "read_source_switch_failed", "details": str(exc)},
                )
                return
            return

        if parsed.path == "/api/db/cutover/run":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            cutover_lock = SERVICES.get("db_cutover_lock")
            acquired = bool(getattr(cutover_lock, "acquire", lambda blocking=False: True)(False))
            if not acquired:
                self._json_response(
                    HTTPStatus.CONFLICT,
                    {"status": "error", "reason": "cutover_in_progress"},
                )
                return
            sqlite_path = str(body.get("sqlite_path") or SERVICES.get("db_primary_path") or "").strip()
            postgres_dsn = str(
                body.get("postgres_dsn")
                or SERVICES.get("postgres_dsn")
                or os.environ.get("TENER_DB_DSN", "")
                or ""
            ).strip()
            if not sqlite_path:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "error", "reason": "sqlite_primary_path_missing"})
                if acquired:
                    getattr(cutover_lock, "release", lambda: None)()
                return
            if not postgres_dsn:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"status": "error", "reason": "postgres_dsn_missing"})
                if acquired:
                    getattr(cutover_lock, "release", lambda: None)()
                return

            execute_backfill = bool(self._safe_bool(body.get("execute_backfill"), False))
            truncate_first = bool(self._safe_bool(body.get("truncate_first"), False))
            strict_parity = bool(self._safe_bool(body.get("strict_parity"), True))
            auto_switch_read_source = bool(self._safe_bool(body.get("auto_switch_read_source"), True))
            set_dual_strict_on_success = bool(self._safe_bool(body.get("set_dual_strict_on_success"), True))
            deep = bool(self._safe_bool(body.get("deep"), True))
            batch_size_raw = self._safe_int(body.get("batch_size"), 500)
            batch_size = max(1, min(int(batch_size_raw or 500), 5000))
            sample_limit_raw = self._safe_int(body.get("sample_limit"), 20)
            sample_limit = max(1, min(int(sample_limit_raw or 20), 200))
            tables_raw = body.get("tables")
            tables: Optional[List[str]] = None
            if tables_raw is not None:
                if not isinstance(tables_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "tables must be an array"})
                    return
                normalized = [str(item).strip() for item in tables_raw if str(item).strip()]
                tables = normalized or None

            result: Dict[str, Any] = {
                "status": "ok",
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "config": {
                    "execute_backfill": execute_backfill,
                    "truncate_first": truncate_first,
                    "strict_parity": strict_parity,
                    "auto_switch_read_source": auto_switch_read_source,
                    "set_dual_strict_on_success": set_dual_strict_on_success,
                    "deep": deep,
                    "batch_size": batch_size,
                    "sample_limit": sample_limit,
                    "tables_requested": tables or [],
                },
                "sqlite_path": sqlite_path,
                "backfill": None,
                "parity": None,
                "switch_read_source": None,
                "dual_write_strict": None,
            }
            try:
                try:
                    if execute_backfill:
                        backfill_result = backfill_sqlite_to_postgres(
                            sqlite_path=sqlite_path,
                            postgres_dsn=postgres_dsn,
                            batch_size=batch_size,
                            truncate_first=truncate_first,
                            tables=tables,
                        )
                        backfill_out = backfill_result.to_dict()
                        backfill_out["status"] = "ok" if int(backfill_out.get("failed_total") or 0) == 0 else "partial"
                        result["backfill"] = backfill_out

                    parity_report = build_parity_report(
                        sqlite_path=sqlite_path,
                        postgres_dsn=postgres_dsn,
                        tables=tables or DEFAULT_PARITY_TABLES,
                        deep=deep,
                        sample_limit=sample_limit,
                    )
                    result["parity"] = parity_report
                    parity_ok = str(parity_report.get("status") or "") == "ok"
                    if not parity_ok and strict_parity:
                        result["status"] = "blocked"
                        result["reason"] = "parity_mismatch"
                    else:
                        result["status"] = "ok" if parity_ok else "warning"
                        if not parity_ok:
                            result["reason"] = "parity_mismatch_but_not_strict"

                    if auto_switch_read_source and (parity_ok or not strict_parity):
                        switch_out = self._switch_read_source(
                            source="postgres",
                            postgres_dsn=postgres_dsn,
                            reason="cutover_run",
                        )
                        result["switch_read_source"] = switch_out

                    if set_dual_strict_on_success and str(result.get("status") or "") in {"ok", "warning"}:
                        dual_out = self._set_dual_write_strict_mode(True)
                        result["dual_write_strict"] = dual_out
                except Exception as exc:
                    result["status"] = "error"
                    result["reason"] = "cutover_failed"
                    result["details"] = str(exc)

                SERVICES["db_cutover_state"] = result
                SERVICES["db"].log_operation(
                    operation="db.cutover.run",
                    status=str(result.get("status") or "unknown"),
                    entity_type="database",
                    entity_id="cutover",
                    details=result,
                )
                http_status = HTTPStatus.OK if str(result.get("status") or "") in {"ok", "warning"} else HTTPStatus.CONFLICT
                if str(result.get("status") or "") == "error":
                    http_status = HTTPStatus.INTERNAL_SERVER_ERROR
                self._json_response(http_status, result)
            finally:
                if acquired:
                    getattr(cutover_lock, "release", lambda: None)()
            return

        if parsed.path == "/api/db/cutover/rollback":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                body = {}
            cutover_lock = SERVICES.get("db_cutover_lock")
            acquired = bool(getattr(cutover_lock, "acquire", lambda blocking=False: True)(False))
            if not acquired:
                self._json_response(
                    HTTPStatus.CONFLICT,
                    {"status": "error", "reason": "cutover_in_progress"},
                )
                return
            force_disable_strict = bool(self._safe_bool(body.get("disable_dual_strict"), True))
            result: Dict[str, Any] = {
                "status": "ok",
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "switch_read_source": None,
                "dual_write_strict": None,
            }
            try:
                try:
                    result["switch_read_source"] = self._switch_read_source(source="sqlite", reason="cutover_rollback")
                    if force_disable_strict:
                        result["dual_write_strict"] = self._set_dual_write_strict_mode(False)
                except Exception as exc:
                    result["status"] = "error"
                    result["reason"] = "rollback_failed"
                    result["details"] = str(exc)
                SERVICES["db_cutover_state"] = {
                    "status": "rolled_back" if str(result.get("status") or "") == "ok" else "rollback_error",
                    "executed_at": result.get("executed_at"),
                    "details": result,
                }
                SERVICES["db"].log_operation(
                    operation="db.cutover.rollback",
                    status=str(result.get("status") or "unknown"),
                    entity_type="database",
                    entity_id="cutover",
                    details=result,
                )
                status = HTTPStatus.OK if str(result.get("status") or "") == "ok" else HTTPStatus.INTERNAL_SERVER_ERROR
                self._json_response(status, result)
            finally:
                if acquired:
                    getattr(cutover_lock, "release", lambda: None)()
            return

        if parsed.path == "/api/db/dual-write/strict":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            strict_value = self._safe_bool(body.get("strict"), None)
            if strict_value is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "strict is required and must be boolean"})
                return
            result = self._set_dual_write_strict_mode(bool(strict_value))
            SERVICES["db"].log_operation(
                operation="db.dual_write.strict.set",
                status=str(result.get("status") or "unknown"),
                entity_type="database",
                entity_id="dual_write",
                details=result,
            )
            code = HTTPStatus.OK if str(result.get("status") or "") == "ok" else HTTPStatus.BAD_REQUEST
            self._json_response(code, result)
            return

        if parsed.path == "/api/linkedin/accounts/connect/start":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            label = str(body.get("label") or "").strip()
            callback_url = str(
                body.get("callback_url")
                or SERVICES["linkedin_accounts"].callback_url
                or f"{self._public_base_url()}/api/linkedin/accounts/connect/callback"
            ).strip()
            if not callback_url:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "callback_url is required"})
                return
            try:
                out = SERVICES["linkedin_accounts"].start_connect(callback_url=callback_url, label=label)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "failed_to_start_linkedin_connect", "details": str(exc)},
                )
                return
            SERVICES["db"].log_operation(
                operation="linkedin.connect.start",
                status="ok",
                entity_type="linkedin_onboarding",
                entity_id=str(out.get("session_id") or ""),
                details={"label": label, "callback_url": callback_url},
            )
            self._json_response(HTTPStatus.OK, out)
            return

        if parsed.path == "/api/candidates/demo-profile":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            try:
                out = SERVICES["candidate_profile"].create_demo_profile(job_id=job_id)
            except ValueError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "candidate demo profile failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.CREATED, out)
            return

        match_sync = re.match(r"^/api/linkedin/accounts/(\d+)/sync$", parsed.path)
        if match_sync:
            if not self._require_admin_access():
                return
            account_id = self._safe_int(match_sync.group(1), None)
            if account_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid account id"})
                return
            try:
                out = SERVICES["linkedin_accounts"].sync_accounts(account_id=account_id)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "linkedin_account_sync_failed", "details": str(exc)},
                )
                return
            status = str(out.get("status") or "").lower()
            SERVICES["db"].log_operation(
                operation="linkedin.account.sync",
                status="ok" if status == "ok" else "error",
                entity_type="linkedin_account",
                entity_id=str(account_id),
                details=out,
            )
            http_status = HTTPStatus.OK if status == "ok" else HTTPStatus.BAD_REQUEST
            self._json_response(http_status, out)
            return

        match_disconnect = re.match(r"^/api/linkedin/accounts/(\d+)/disconnect$", parsed.path)
        if match_disconnect:
            if not self._require_admin_access():
                return
            account_id = self._safe_int(match_disconnect.group(1), None)
            if account_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid account id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                body = {}
            remote_disable = bool(body.get("remote_disable"))
            out = SERVICES["linkedin_accounts"].disconnect_account(account_id=account_id, remote_disable=remote_disable)
            status = str(out.get("status") or "").lower()
            SERVICES["db"].log_operation(
                operation="linkedin.account.disconnect",
                status="ok" if status == "ok" else "error",
                entity_type="linkedin_account",
                entity_id=str(account_id),
                details=out,
            )
            http_status = HTTPStatus.OK if status == "ok" else HTTPStatus.NOT_FOUND
            self._json_response(http_status, out)
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/jd"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/jd$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            jd_text = str(body.get("jd_text") or "").strip()
            if not jd_text:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "jd_text is required"})
                return
            updated = SERVICES["db"].update_job_jd_text(job_id=job_id, jd_text=jd_text)
            if not updated:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            SERVICES["db"].log_operation(
                operation="job.jd.updated",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"length": len(jd_text)},
            )
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "jd_text": jd_text})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/linkedin-routing"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/linkedin-routing$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            if not SERVICES["db"].get_job(job_id):
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            routing_mode_raw = body.get("routing_mode")
            if routing_mode_raw is not None:
                updated = SERVICES["db"].update_job_linkedin_routing_mode(
                    job_id=job_id,
                    routing_mode=str(routing_mode_raw),
                )
                if not updated:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return

            account_ids_raw = body.get("account_ids")
            if account_ids_raw is not None:
                if not isinstance(account_ids_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "account_ids must be an array"})
                    return
                account_ids = SERVICES["db"].replace_job_linkedin_account_assignments(
                    job_id=job_id,
                    account_ids=account_ids_raw,
                )
            else:
                account_ids = SERVICES["db"].list_job_linkedin_account_ids(job_id=job_id)

            job = SERVICES["db"].get_job(job_id) or {}
            routing_mode = str(job.get("linkedin_routing_mode") or "auto").strip().lower()
            if routing_mode not in {"auto", "manual"}:
                routing_mode = "auto"
            assigned_accounts = SERVICES["db"].list_job_linkedin_accounts(job_id=job_id)
            SERVICES["db"].log_operation(
                operation="job.linkedin_routing.updated",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"routing_mode": routing_mode, "account_ids": account_ids},
            )
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": job_id,
                    "routing_mode": routing_mode,
                    "account_ids": account_ids,
                    "assigned_accounts": assigned_accounts,
                },
            )
            return

        if parsed.path == "/api/agent/accounts/manual":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            full_name = str(body.get("full_name") or "").strip()
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not full_name:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "full_name is required"})
                return
            try:
                result = SERVICES["workflow"].add_manual_test_account(
                    job_id=job_id,
                    full_name=full_name,
                    language=str(body.get("language") or "en"),
                    linkedin_id=(str(body.get("linkedin_id")).strip() if body.get("linkedin_id") else None),
                    location=(str(body.get("location")).strip() if body.get("location") else None),
                    headline=(str(body.get("headline")).strip() if body.get("headline") else None),
                    external_chat_id=(str(body.get("external_chat_id")).strip() if body.get("external_chat_id") else None),
                    scope_summary=(str(body.get("scope_summary")).strip() if body.get("scope_summary") else None),
                )
            except ValueError as exc:
                text = str(exc)
                status = HTTPStatus.NOT_FOUND if "not found" in text.lower() else HTTPStatus.BAD_REQUEST
                self._json_response(status, {"error": text})
                return
            self._json_response(HTTPStatus.CREATED, result)
            return

        if parsed.path == "/api/webhooks/unipile":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return

            secret = os.environ.get("UNIPILE_WEBHOOK_SECRET")
            if secret:
                incoming = self.headers.get("X-Webhook-Secret", "")
                if incoming != secret:
                    self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "invalid webhook secret"})
                    return

            event_id = self._pick_str(body, "event_id", "id", "message_id", "event.id", "message.id", "data.id", "data.event_id")
            event_type = self._pick_str(body, "type", "event", "event.type", "data.type", "data.event").lower()
            external_chat_id = self._pick_str(
                body,
                "chat_id",
                "chat.id",
                "conversation_id",
                "data.chat_id",
                "data.chat.id",
                "data.conversation_id",
                "message.chat_id",
                "message.chat.id",
            )
            text = self._pick_text(
                body,
                "text",
                "message",
                "content",
                "message.text",
                "message.content",
                "message.body",
                "data.text",
                "data.message",
                "data.message.text",
                "data.message.content",
                "data.message.body",
            )
            attachment_text = self._pick_attachment_text(
                body,
                "attachments",
                "files",
                "documents",
                "media",
                "message.attachments",
                "message.files",
                "message.documents",
                "message.media",
                "data.attachments",
                "data.files",
                "data.documents",
                "data.media",
                "data.message.attachments",
                "data.message.files",
                "data.message.documents",
                "data.message.media",
            )
            inbound_text = self._merge_inbound_text(text=text, attachment_text=attachment_text)
            direction = self._pick_str(body, "direction", "message.direction", "data.direction", "data.message.direction").lower()
            sender_provider_id = self._pick_str(
                body,
                "sender.provider_id",
                "sender_id",
                "from.provider_id",
                "from.id",
                "attendee_provider_id",
                "sender.id",
                "data.sender.provider_id",
                "data.sender.id",
                "data.from.provider_id",
                "data.from.id",
            )
            occurred_at = self._pick_str(body, "created_at", "timestamp", "occurred_at", "message.created_at")

            event_key = event_id or hashlib.sha256(
                f"{event_type}|{external_chat_id}|{sender_provider_id}|{inbound_text}|{occurred_at}".encode("utf-8")
            ).hexdigest()

            if direction in {"outbound", "sent", "from_me", "self"}:
                SERVICES["db"].log_operation(
                    operation="webhook.unipile.ignored",
                    status="ignored",
                    entity_type="webhook",
                    entity_id=event_key,
                    details={"reason": "outbound_event", "event_type": event_type},
                )
                self._json_response(HTTPStatus.OK, {"status": "ignored", "reason": "outbound_event"})
                return
            connection_event = ("connect" in event_type or "invitation" in event_type) and (
                "accept" in event_type or "connected" in event_type
            )
            if not inbound_text and not connection_event:
                SERVICES["db"].log_operation(
                    operation="webhook.unipile.ignored",
                    status="ignored",
                    entity_type="webhook",
                    entity_id=event_key,
                    details={
                        "reason": "empty_text",
                        "event_type": event_type,
                        "external_chat_id": external_chat_id,
                        "sender_provider_id": sender_provider_id,
                    },
                )
                self._json_response(HTTPStatus.OK, {"status": "ignored", "reason": "empty_text"})
                return

            is_new = SERVICES["db"].record_webhook_event(event_key=event_key, source="unipile", payload=body)
            if not is_new:
                SERVICES["db"].log_operation(
                    operation="webhook.unipile.duplicate",
                    status="ignored",
                    entity_type="webhook",
                    entity_id=event_key,
                    details={"event_type": event_type},
                )
                self._json_response(HTTPStatus.OK, {"status": "duplicate", "event_key": event_key})
                return

            if connection_event:
                try:
                    result = SERVICES["workflow"].process_connection_event(
                        sender_provider_id=sender_provider_id or None,
                        external_chat_id=external_chat_id or None,
                    )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="webhook.unipile.connection_error",
                        status="error",
                        entity_type="webhook",
                        entity_id=event_key,
                        details={"error": str(exc), "event_type": event_type},
                    )
                    self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "webhook processing failed", "details": str(exc)})
                    return
                SERVICES["db"].log_operation(
                    operation="webhook.unipile.connection_event",
                    status="ok" if result.get("processed") else "ignored",
                    entity_type="webhook",
                    entity_id=event_key,
                    details={
                        "event_type": event_type,
                        "external_chat_id": external_chat_id,
                        "sender_provider_id": sender_provider_id,
                        "processed": bool(result.get("processed")),
                        "reason": result.get("reason"),
                    },
                )
                self._json_response(HTTPStatus.OK, {"status": "ok", "event_key": event_key, "result": result})
                return

            try:
                result = SERVICES["workflow"].process_provider_inbound_message(
                    external_chat_id=external_chat_id,
                    text=inbound_text,
                    sender_provider_id=sender_provider_id or None,
                )
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="webhook.unipile.error",
                    status="error",
                    entity_type="webhook",
                    entity_id=event_key,
                    details={"error": str(exc)},
                )
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "webhook processing failed", "details": str(exc)})
                return

            SERVICES["db"].log_operation(
                operation="webhook.unipile.inbound",
                status="ok" if result.get("processed") else "ignored",
                entity_type="webhook",
                entity_id=event_key,
                details={
                    "external_chat_id": external_chat_id,
                    "sender_provider_id": sender_provider_id,
                    "processed": bool(result.get("processed")),
                    "reason": result.get("reason"),
                },
            )
            self._json_response(HTTPStatus.OK, {"status": "ok", "event_key": event_key, "result": result})
            return

        if parsed.path == "/api/jobs":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            title = str(body.get("title") or "").strip()
            jd_text = str(body.get("jd_text") or "").strip()
            company = str(body.get("company") or "").strip() or None
            company_website_raw = str(body.get("company_website") or "").strip()
            company_website = None
            if company_website_raw:
                company_website = self._validate_company_website(company_website_raw)
                if not company_website:
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "company_website must be a valid public http/https URL"})
                    return
            if not title or not jd_text:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "title and jd_text are required"})
                return

            preferred_languages = body.get("preferred_languages") or []
            if not isinstance(preferred_languages, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "preferred_languages must be an array"})
                return

            job_id = SERVICES["db"].insert_job(
                title=title,
                jd_text=jd_text,
                location=body.get("location"),
                preferred_languages=[str(x).lower() for x in preferred_languages if str(x).strip()],
                seniority=(str(body.get("seniority")).lower() if body.get("seniority") else None),
                company=company,
                company_website=company_website,
            )
            SERVICES["db"].log_operation(
                operation="job.created",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"title": title, "company": company, "company_website": company_website},
            )
            culture_profile = self._start_company_culture_profile_pipeline(job_id=job_id)
            if str(culture_profile.get("status") or "").strip().lower() in {"pending", "running"}:
                interview_assessment = {"status": "pending", "reason": "waiting_for_company_culture_profile"}
            else:
                interview_assessment = self._prepare_job_interview_assessment(job_id=job_id)
                self._persist_job_step_progress(
                    job_id=job_id,
                    step="interview_assessment",
                    status="success" if str(interview_assessment.get("status") or "") == "ok" else "error",
                    output=interview_assessment,
                )
                SERVICES["db"].log_operation(
                    operation="job.interview_assessment.prepare",
                    status=str(interview_assessment.get("status") or "unknown"),
                    entity_type="job",
                    entity_id=str(job_id),
                    details=interview_assessment,
                )
            self._json_response(
                HTTPStatus.CREATED,
                {
                    "job_id": job_id,
                    "company_culture_profile": culture_profile,
                    "interview_assessment": interview_assessment,
                },
            )
            return

        if parsed.path == "/api/workflows/execute":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return

            limit = self._safe_int(body.get("limit"), 30)
            test_mode = self._safe_bool(body.get("test_mode"), None)
            try:
                summary = SERVICES["workflow"].execute_job_workflow(job_id=job_id, limit=limit, test_mode=test_mode)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="workflow", status="error", output={"error": str(exc)})
                SERVICES["db"].log_operation(
                    operation="workflow.execute.error",
                    status="error",
                    entity_type="job",
                    entity_id=str(job_id),
                    details={"error": str(exc)},
                )
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "workflow failed", "details": str(exc)})
                return

            outreach_status = (
                "error"
                if summary.outreach_failed > 0 and summary.outreach_sent == 0 and summary.outreach_pending_connection == 0
                else "success"
            )
            workflow_payload = {
                "job_id": summary.job_id,
                "searched": summary.searched,
                "verified": summary.verified,
                "needs_resume": summary.needs_resume,
                "rejected": summary.rejected,
                "outreached": summary.outreached,
                "outreach_sent": summary.outreach_sent,
                "outreach_pending_connection": summary.outreach_pending_connection,
                "outreach_failed": summary.outreach_failed,
                "conversation_ids": summary.conversation_ids,
                "test_mode_requested": test_mode,
            }
            self._persist_job_step_progress(job_id=job_id, step="source", status="success", output={"total": summary.searched})
            self._persist_job_step_progress(
                job_id=job_id,
                step="enrich",
                status="success",
                output={"total": summary.searched, "failed": 0},
            )
            self._persist_job_step_progress(
                job_id=job_id,
                step="verify",
                status="success",
                output={
                    "verified": summary.verified,
                    "needs_resume": summary.needs_resume,
                    "rejected": summary.rejected,
                    "enriched_total": summary.searched,
                    "enrich_failed": 0,
                },
            )
            self._persist_job_step_progress(
                job_id=job_id,
                step="add",
                status="success",
                output={"total": summary.verified + summary.needs_resume},
            )
            self._persist_job_step_progress(
                job_id=job_id,
                step="outreach",
                status=outreach_status,
                output={
                    "total": summary.outreached,
                    "sent": summary.outreach_sent,
                    "pending_connection": summary.outreach_pending_connection,
                    "failed": summary.outreach_failed,
                    "conversation_ids": summary.conversation_ids,
                },
            )
            self._persist_job_step_progress(job_id=job_id, step="workflow", status="success", output=workflow_payload)

            self._json_response(
                HTTPStatus.OK,
                workflow_payload,
            )
            return

        if parsed.path == "/api/steps/source":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            limit = self._safe_int(body.get("limit"), 30)
            test_mode = self._safe_bool(body.get("test_mode"), None)
            try:
                result = SERVICES["workflow"].source_candidates(job_id=job_id, limit=limit or 30, test_mode=test_mode)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="source", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "source step failed", "details": str(exc)})
                return
            self._persist_job_step_progress(job_id=job_id, step="source", status="success", output=result)
            for step in ("enrich", "verify", "add", "outreach", "workflow"):
                self._persist_job_step_progress(job_id=job_id, step=step, status="idle", output={})
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/steps/verify":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            profiles = body.get("profiles")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not isinstance(profiles, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "profiles must be an array"})
                return
            try:
                result = SERVICES["workflow"].verify_profiles(job_id=job_id, profiles=profiles)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="verify", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "verify step failed", "details": str(exc)})
                return
            self._persist_job_step_progress(job_id=job_id, step="verify", status="success", output=result)
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/steps/enrich":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            profiles = body.get("profiles")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not isinstance(profiles, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "profiles must be an array"})
                return
            try:
                result = SERVICES["workflow"].enrich_profiles(job_id=job_id, profiles=profiles)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="enrich", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "enrich step failed", "details": str(exc)})
                return
            self._persist_job_step_progress(job_id=job_id, step="enrich", status="success", output=result)
            for step in ("verify", "add", "outreach", "workflow"):
                self._persist_job_step_progress(job_id=job_id, step=step, status="idle", output={})
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/steps/add":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            items = body.get("verified_items")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not isinstance(items, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "verified_items must be an array"})
                return
            try:
                result = SERVICES["workflow"].add_verified_candidates(job_id=job_id, verified_items=items)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="add", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "add step failed", "details": str(exc)})
                return
            self._persist_job_step_progress(job_id=job_id, step="add", status="success", output=result)
            for step in ("outreach", "workflow"):
                self._persist_job_step_progress(job_id=job_id, step=step, status="idle", output={})
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/steps/outreach":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            candidate_ids = body.get("candidate_ids")
            test_mode = self._safe_bool(body.get("test_mode"), None)
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not isinstance(candidate_ids, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "candidate_ids must be an array"})
                return
            try:
                result = SERVICES["workflow"].outreach_candidates(
                    job_id=job_id,
                    candidate_ids=candidate_ids,
                    test_mode=test_mode,
                )
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._persist_job_step_progress(job_id=job_id, step="outreach", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "outreach step failed", "details": str(exc)})
                return
            outreach_status = (
                "error"
                if (result.get("failed") or 0) > 0 and (result.get("sent") or 0) == 0 and (result.get("pending_connection") or 0) == 0
                else "success"
            )
            self._persist_job_step_progress(job_id=job_id, step="outreach", status=outreach_status, output=result)
            self._persist_job_step_progress(job_id=job_id, step="workflow", status="idle", output={})
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/outreach/dispatch/run":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 100) or 100
            job_id_raw = body.get("job_id")
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            try:
                result = SERVICES["workflow"].dispatch_outbound_actions(limit=limit, job_id=job_id)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "outreach dispatch run failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/outreach/poll-connections":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            limit = self._safe_int(body.get("limit"), 200) or 200
            try:
                result = SERVICES["workflow"].poll_pending_connections(job_id=job_id, limit=limit)
            except Exception as exc:
                if job_id is not None:
                    self._persist_job_step_progress(job_id=job_id, step="outreach", status="error", output={"error": str(exc)})
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "poll pending connections failed", "details": str(exc)},
                )
                return
            if job_id is not None:
                outreach_status = (
                    "error"
                    if (result.get("failed") or 0) > 0 and (result.get("sent") or 0) == 0
                    else "success"
                )
                self._persist_job_step_progress(job_id=job_id, step="outreach", status=outreach_status, output=result)
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/inbound/poll":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            limit = self._safe_int(body.get("limit"), 100) or 100
            per_chat_limit = self._safe_int(body.get("per_chat_limit"), 20) or 20
            try:
                result = SERVICES["workflow"].poll_provider_inbound_messages(
                    job_id=job_id,
                    limit=limit,
                    per_chat_limit=per_chat_limit,
                )
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "inbound poll failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/pre-resume/sessions/start":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            session_id = str(body.get("session_id") or "").strip()
            candidate_name = str(body.get("candidate_name") or "").strip()
            job_title = str(body.get("job_title") or "").strip()
            scope_summary = str(body.get("scope_summary") or "").strip()
            core_profile_summary = str(body.get("core_profile_summary") or "").strip()
            language = str(body.get("language") or "").strip() or None
            conversation_id = self._safe_int(body.get("conversation_id"), None)
            job_id = self._safe_int(body.get("job_id"), None)
            candidate_id = self._safe_int(body.get("candidate_id"), None)
            if not session_id:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "session_id is required"})
                return
            if not candidate_name:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "candidate_name is required"})
                return
            if not job_title:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_title is required"})
                return
            if not scope_summary:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "scope_summary is required"})
                return
            try:
                result = SERVICES["pre_resume"].start_session(
                    session_id=session_id,
                    candidate_name=candidate_name,
                    job_title=job_title,
                    scope_summary=scope_summary,
                    core_profile_summary=core_profile_summary or None,
                    language=language,
                )
            except ValueError as exc:
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
                return
            state = result.get("state") if isinstance(result, dict) else None
            if isinstance(state, dict) and conversation_id is not None and job_id is not None and candidate_id is not None:
                SERVICES["db"].upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    job_id=job_id,
                    candidate_id=candidate_id,
                    state=state,
                    instruction=SERVICES["instructions"].get("pre_resume"),
                )
                SERVICES["db"].insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=conversation_id,
                    event_type="session_started",
                    intent="started",
                    inbound_text=None,
                    outbound_text=result.get("outbound"),
                    state_status=state.get("status"),
                    details={"job_id": job_id, "candidate_id": candidate_id, "source": "api"},
                )
            self._json_response(HTTPStatus.CREATED, result)
            return

        if parsed.path.startswith("/api/pre-resume/sessions/") and parsed.path.endswith("/inbound"):
            match = re.match(r"^/api/pre-resume/sessions/([^/]+)/inbound$", parsed.path)
            if not match:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid session id"})
                return
            session_id = match.group(1)
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            text = str(body.get("message") or "").strip()
            if not text:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "message is required"})
                return
            if SERVICES["pre_resume"].get_session(session_id) is None:
                row = SERVICES["db"].get_pre_resume_session(session_id)
                if row and isinstance(row.get("state_json"), dict):
                    SERVICES["pre_resume"].seed_session(row["state_json"])
            try:
                result = SERVICES["pre_resume"].handle_inbound(session_id=session_id, text=text)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            row = SERVICES["db"].get_pre_resume_session(session_id)
            state = result.get("state") if isinstance(result.get("state"), dict) else None
            if row and isinstance(state, dict):
                SERVICES["db"].upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    job_id=int(row["job_id"]),
                    candidate_id=int(row["candidate_id"]),
                    state=state,
                    instruction=SERVICES["instructions"].get("pre_resume"),
                )
                SERVICES["db"].insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    event_type="inbound_processed",
                    intent=result.get("intent"),
                    inbound_text=text,
                    outbound_text=result.get("outbound"),
                    state_status=state.get("status"),
                    details={"source": "api"},
                )
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/pre-resume/followups/run":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 100) or 100
            job_id = self._safe_int(body.get("job_id"), None)
            try:
                result = SERVICES["workflow"].run_due_pre_resume_followups(job_id=job_id, limit=limit)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "pre-resume followup run failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/interviews/sync":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 100) or 100
            job_id = self._safe_int(body.get("job_id"), None)
            force_refresh = bool(body.get("force_refresh"))
            try:
                result = SERVICES["workflow"].sync_interview_progress(
                    job_id=job_id,
                    limit=limit,
                    force_refresh=force_refresh,
                )
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "interview sync failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/interviews/followups/run":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 100) or 100
            job_id = self._safe_int(body.get("job_id"), None)
            try:
                result = SERVICES["workflow"].run_due_interview_followups(job_id=job_id, limit=limit)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "interview followup run failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path.startswith("/api/pre-resume/sessions/") and parsed.path.endswith("/followup"):
            match = re.match(r"^/api/pre-resume/sessions/([^/]+)/followup$", parsed.path)
            if not match:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid session id"})
                return
            session_id = match.group(1)
            if SERVICES["pre_resume"].get_session(session_id) is None:
                row = SERVICES["db"].get_pre_resume_session(session_id)
                if row and isinstance(row.get("state_json"), dict):
                    SERVICES["pre_resume"].seed_session(row["state_json"])
            try:
                result = SERVICES["pre_resume"].build_followup(session_id=session_id)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            row = SERVICES["db"].get_pre_resume_session(session_id)
            state = result.get("state") if isinstance(result.get("state"), dict) else None
            if row and isinstance(state, dict):
                SERVICES["db"].upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    job_id=int(row["job_id"]),
                    candidate_id=int(row["candidate_id"]),
                    state=state,
                    instruction=SERVICES["instructions"].get("pre_resume"),
                )
                SERVICES["db"].insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    event_type="followup_sent" if result.get("sent") else "followup_skipped",
                    intent=None,
                    inbound_text=None,
                    outbound_text=result.get("outbound"),
                    state_status=state.get("status"),
                    details={"reason": result.get("reason"), "source": "api"},
                )
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path.startswith("/api/pre-resume/sessions/") and parsed.path.endswith("/unreachable"):
            match = re.match(r"^/api/pre-resume/sessions/([^/]+)/unreachable$", parsed.path)
            if not match:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid session id"})
                return
            session_id = match.group(1)
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            error = str(body.get("error") or "delivery_failed")
            if SERVICES["pre_resume"].get_session(session_id) is None:
                row = SERVICES["db"].get_pre_resume_session(session_id)
                if row and isinstance(row.get("state_json"), dict):
                    SERVICES["pre_resume"].seed_session(row["state_json"])
            try:
                result = SERVICES["pre_resume"].mark_unreachable(session_id=session_id, error=error)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            row = SERVICES["db"].get_pre_resume_session(session_id)
            state = result.get("state") if isinstance(result.get("state"), dict) else None
            if row and isinstance(state, dict):
                SERVICES["db"].upsert_pre_resume_session(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    job_id=int(row["job_id"]),
                    candidate_id=int(row["candidate_id"]),
                    state=state,
                    instruction=SERVICES["instructions"].get("pre_resume"),
                )
                SERVICES["db"].insert_pre_resume_event(
                    session_id=session_id,
                    conversation_id=int(row["conversation_id"]),
                    event_type="session_unreachable",
                    intent=None,
                    inbound_text=None,
                    outbound_text=None,
                    state_status=state.get("status"),
                    details={"error": error, "source": "api"},
                )
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path.startswith("/api/conversations/") and parsed.path.endswith("/inbound"):
            conversation_id = self._extract_id(parsed.path, pattern=r"^/api/conversations/(\d+)/inbound$")
            if conversation_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid conversation id"})
                return

            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            text = str(body.get("message") or "").strip()
            if not text:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "message is required"})
                return

            try:
                reply = SERVICES["workflow"].process_inbound_message(conversation_id=conversation_id, text=text)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="conversation.inbound.error",
                    status="error",
                    entity_type="conversation",
                    entity_id=str(conversation_id),
                    details={"error": str(exc)},
                )
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "inbound processing failed", "details": str(exc)})
                return

            self._json_response(HTTPStatus.OK, reply)
            return

        if parsed.path == "/api/rules/reload":
            SERVICES["matching_engine"].reload()
            SERVICES["db"].log_operation(
                operation="matching.rules.reload",
                status="ok",
                entity_type="system",
                entity_id="rules",
            )
            self._json_response(HTTPStatus.OK, {"status": "reloaded"})
            return

        if parsed.path == "/api/instructions/reload":
            SERVICES["instructions"].reload()
            SERVICES["evaluation_playbook"].reload()
            SERVICES["scoring_formula"].reload()
            apply_agent_instructions(SERVICES)
            self._json_response(
                HTTPStatus.OK,
                {
                    "instructions": SERVICES["instructions"].to_dict(),
                    "evaluation_playbook": SERVICES["evaluation_playbook"].to_dict(),
                    "scoring_formula": SERVICES["scoring_formula"].to_dict(),
                },
            )
            return

        if parsed.path == "/api/outreach-policy/reload":
            SERVICES["outreach_policy"].reload()
            self._json_response(
                HTTPStatus.OK,
                {
                    "outreach_policy": SERVICES["outreach_policy"].to_dict(),
                },
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def _persist_job_step_progress(self, job_id: int, step: str, status: str, output: Dict[str, Any] | None = None) -> None:
        try:
            SERVICES["db"].upsert_job_step_progress(
                job_id=job_id,
                step=step,
                status=status,
                output=output or {},
            )
        except Exception:
            # Progress persistence must not break primary API response path.
            return

    @staticmethod
    def _validate_company_website(raw_url: str) -> str | None:
        normalized = canonicalize_url(str(raw_url or "").strip())
        if not normalized:
            return None
        parsed = urlparse(normalized)
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            return None
        if host in {"localhost", "127.0.0.1", "::1"}:
            return None
        if host.endswith(".local"):
            return None
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            ip = None
        if ip is not None and (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast):
            return None
        return normalized

    def _start_company_culture_profile_pipeline(self, *, job_id: int) -> Dict[str, Any]:
        job = SERVICES["db"].get_job(int(job_id))
        if not job:
            return {"status": "error", "reason": "job_not_found"}
        website = str(job.get("company_website") or "").strip()
        company_name = str(job.get("company") or job.get("title") or "").strip()
        if not website:
            skipped = {
                "status": "skipped",
                "reason": "company_website_missing",
                "job_id": int(job_id),
            }
            SERVICES["db"].upsert_job_culture_profile(
                job_id=int(job_id),
                status="skipped",
                company_name=company_name or None,
                company_website=None,
                profile=None,
                sources=None,
                warnings=None,
                search_queries=None,
                error="company_website_missing",
                generated_at=None,
            )
            self._persist_job_step_progress(job_id=int(job_id), step="culture_profile", status="skipped", output=skipped)
            return skipped

        pending = {
            "status": "pending",
            "job_id": int(job_id),
            "company_name": company_name or None,
            "company_website": website,
        }
        SERVICES["db"].upsert_job_culture_profile(
            job_id=int(job_id),
            status="pending",
            company_name=company_name or None,
            company_website=website,
            profile=None,
            sources=None,
            warnings=None,
            search_queries=None,
            error=None,
            generated_at=None,
        )
        self._persist_job_step_progress(job_id=int(job_id), step="culture_profile", status="pending", output=pending)

        def _run() -> None:
            profile_result = self._generate_company_culture_profile(job_id=int(job_id))
            status = str(profile_result.get("status") or "").strip().lower()
            progress_status = "success" if status == "ok" else ("skipped" if status == "skipped" else "error")
            self._persist_job_step_progress(
                job_id=int(job_id),
                step="culture_profile",
                status=progress_status,
                output=profile_result,
            )
            SERVICES["db"].log_operation(
                operation="job.company_culture_profile.generate",
                status="ok" if status == "ok" else "error",
                entity_type="job",
                entity_id=str(job_id),
                details=profile_result,
            )
            interview_assessment = self._prepare_job_interview_assessment(job_id=int(job_id))
            interview_status = "success" if str(interview_assessment.get("status") or "") == "ok" else "error"
            self._persist_job_step_progress(
                job_id=int(job_id),
                step="interview_assessment",
                status=interview_status,
                output=interview_assessment,
            )
            SERVICES["db"].log_operation(
                operation="job.interview_assessment.prepare",
                status=str(interview_assessment.get("status") or "unknown"),
                entity_type="job",
                entity_id=str(job_id),
                details=interview_assessment,
            )

        threading.Thread(
            target=_run,
            daemon=True,
            name=f"job-culture-profile-{int(job_id)}",
        ).start()
        return pending

    def _generate_company_culture_profile(self, *, job_id: int) -> Dict[str, Any]:
        job = SERVICES["db"].get_job(int(job_id))
        if not job:
            return {"status": "error", "reason": "job_not_found", "job_id": int(job_id)}
        company_name = str(job.get("company") or job.get("title") or "").strip()
        website = str(job.get("company_website") or "").strip()
        if not company_name or not website:
            return {"status": "skipped", "reason": "company_name_or_website_missing", "job_id": int(job_id)}

        service: CompanyCultureProfileService | None = SERVICES.get("company_culture")
        if service is None:
            return {"status": "skipped", "reason": "company_culture_service_not_configured", "job_id": int(job_id)}

        try:
            generated = service.generate(company_name=company_name, website_url=website)
            profile = generated.get("profile") if isinstance(generated.get("profile"), dict) else {}
            sources = generated.get("sources") if isinstance(generated.get("sources"), list) else []
            warnings = generated.get("warnings") if isinstance(generated.get("warnings"), list) else []
            search_queries = generated.get("search_queries") if isinstance(generated.get("search_queries"), list) else []
            SERVICES["db"].upsert_job_culture_profile(
                job_id=int(job_id),
                status="ready",
                company_name=company_name,
                company_website=website,
                profile=profile,
                sources=sources,
                warnings=[str(x) for x in warnings if str(x).strip()],
                search_queries=[str(x) for x in search_queries if str(x).strip()],
                error=None,
                generated_at=generated.get("generated_at") if isinstance(generated, dict) else None,
            )
            return {
                "status": "ok",
                "job_id": int(job_id),
                "company_name": company_name,
                "company_website": website,
                "sources_total": len(sources),
                "warnings_total": len(warnings),
            }
        except Exception as exc:
            SERVICES["db"].upsert_job_culture_profile(
                job_id=int(job_id),
                status="error",
                company_name=company_name,
                company_website=website,
                profile=None,
                sources=None,
                warnings=None,
                search_queries=None,
                error=str(exc),
                generated_at=None,
            )
            return {
                "status": "error",
                "job_id": int(job_id),
                "company_name": company_name,
                "company_website": website,
                "reason": "culture_profile_generation_failed",
                "error": str(exc),
            }

    def _prepare_job_interview_assessment(self, job_id: int) -> Dict[str, Any]:
        base = str(SERVICES.get("interview_api_base") or "").strip().rstrip("/")
        if not base:
            return {"status": "skipped", "reason": "interview_api_not_configured"}

        url = f"{base}/api/admin/jobs/{int(job_id)}/assessment/prepare"
        req = urlrequest.Request(
            url=url,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=b"{}",
        )
        try:
            with urlrequest.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8")
                parsed = json.loads(raw) if raw else {}
                if not isinstance(parsed, dict):
                    return {
                        "status": "error",
                        "reason": "invalid_interview_response",
                        "http_status": int(resp.status),
                    }
                return {
                    "status": "ok",
                    "http_status": int(resp.status),
                    "details": parsed,
                }
        except urlerror.HTTPError as exc:
            body_raw = exc.read().decode("utf-8") if exc.fp else ""
            details: Dict[str, Any] = {}
            if body_raw:
                try:
                    parsed = json.loads(body_raw)
                    if isinstance(parsed, dict):
                        details = parsed
                except json.JSONDecodeError:
                    details = {"raw": body_raw}
            return {
                "status": "error",
                "reason": "interview_api_http_error",
                "http_status": int(exc.code),
                "details": details or {"message": str(exc.reason or "http error")},
            }
        except urlerror.URLError as exc:
            return {
                "status": "error",
                "reason": "interview_api_network_error",
                "details": {"message": str(exc.reason)},
            }
        except Exception as exc:
            return {
                "status": "error",
                "reason": "interview_api_request_failed",
                "details": {"message": str(exc)},
            }

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _read_json_body(self) -> Any:
        length_raw = self.headers.get("Content-Length", "0")
        length = self._safe_int(length_raw, 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {"_error": "invalid json"}

    def _html_response(self, status: HTTPStatus, content: str) -> None:
        encoded = content.encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _json_response(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _binary_response(
        self,
        *,
        status: HTTPStatus,
        content_type: str,
        payload: bytes,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(str(key), str(value))
        self.end_headers()
        self.wfile.write(payload)

    @staticmethod
    def _extract_id(path: str, pattern: str) -> Optional[int]:
        match = re.match(pattern, path)
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any, default: Optional[int]) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_bool(value: Any, default: Optional[bool]) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if not text:
            return default
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _read_db() -> Any:
        db = SERVICES.get("read_db")
        if db is None:
            db = SERVICES["db"]
        return db

    @staticmethod
    def _switch_read_source(*, source: str, postgres_dsn: str = "", reason: str = "manual_switch") -> Dict[str, Any]:
        normalized = str(source or "").strip().lower()
        if normalized == "sqlite":
            SERVICES["read_db"] = SERVICES["db"]
            SERVICES["db_read_status"] = {
                "status": "ok",
                "source": "sqlite",
                "requested_source": "runtime",
                "reason": reason,
            }
            return {
                "status": "ok",
                "source": "sqlite",
                "db_read_status": SERVICES.get("db_read_status"),
            }
        if normalized != "postgres":
            raise ValueError("source must be sqlite or postgres")
        dsn = str(postgres_dsn or SERVICES.get("postgres_dsn") or os.environ.get("TENER_DB_DSN", "") or "").strip()
        if not dsn:
            raise RuntimeError("postgres_dsn_missing")
        SERVICES["read_db"] = PostgresReadDatabase(dsn)
        SERVICES["postgres_dsn"] = dsn
        SERVICES["db_read_status"] = {
            "status": "ok",
            "source": "postgres",
            "requested_source": "runtime",
            "reason": reason,
        }
        return {
            "status": "ok",
            "source": "postgres",
            "db_read_status": SERVICES.get("db_read_status"),
        }

    @staticmethod
    def _set_dual_write_strict_mode(strict: bool) -> Dict[str, Any]:
        db = SERVICES.get("db")
        if db is None:
            return {"status": "skipped", "reason": "db_unavailable"}
        setter = getattr(db, "set_strict_mode", None)
        if not callable(setter):
            return {"status": "skipped", "reason": "db_is_not_dual_write"}
        out = setter(bool(strict))
        return {
            "status": "ok",
            "strict": bool(strict),
            "dual_write": out if isinstance(out, dict) else {},
        }

    @staticmethod
    def _build_cutover_preflight_report(*, sqlite_path: str, postgres_dsn: str) -> Dict[str, Any]:
        sqlite_exists = Path(sqlite_path).exists()
        migration_files = sorted([path.name for path in (project_root() / "migrations").glob("*.sql") if path.is_file()])
        checks: Dict[str, Any] = {
            "sqlite_exists": sqlite_exists,
            "migrations_total": len(migration_files),
            "migrations_files": migration_files,
            "postgres_connected": False,
            "postgres_migrations_applied": [],
            "postgres_migrations_missing": [],
            "read_source": str((SERVICES.get("db_read_status") or {}).get("source") or ""),
            "db_backend": SERVICES.get("db_backend"),
            "db_runtime_mode": SERVICES.get("db_runtime_mode"),
            "db_cutover_state": SERVICES.get("db_cutover_state"),
        }

        db_obj = SERVICES.get("db")
        dual_status = getattr(db_obj, "dual_write_status", None)
        if isinstance(dual_status, dict):
            checks["dual_write"] = dual_status

        try:
            import psycopg  # type: ignore
        except Exception as exc:
            checks["postgres_error"] = f"psycopg_missing:{exc}"
            return {
                "status": "warning" if sqlite_exists else "error",
                "checks": checks,
            }

        try:
            with psycopg.connect(postgres_dsn) as conn:
                checks["postgres_connected"] = True
                with conn.cursor() as cur:
                    cur.execute("SELECT to_regclass('public.schema_migrations')")
                    reg = cur.fetchone()
                    if reg and reg[0] is not None:
                        cur.execute("SELECT version FROM schema_migrations ORDER BY version ASC")
                        checks["postgres_migrations_applied"] = [str(row[0]) for row in cur.fetchall()]
                    else:
                        checks["postgres_migrations_applied"] = []
        except Exception as exc:
            checks["postgres_error"] = str(exc)
            return {
                "status": "warning" if sqlite_exists else "error",
                "checks": checks,
            }

        applied_set = set(str(x) for x in (checks.get("postgres_migrations_applied") or []))
        checks["postgres_migrations_missing"] = [name for name in migration_files if name not in applied_set]
        ready = (
            sqlite_exists
            and bool(checks.get("postgres_connected"))
            and len(checks.get("postgres_migrations_missing") or []) == 0
        )
        return {
            "status": "ok" if ready else "warning",
            "checks": checks,
        }

    def _require_request_auth(self, *, method: str, path: str) -> bool:
        auth_service = SERVICES.get("auth")
        if auth_service is None or not bool(getattr(auth_service, "enabled", False)):
            return True
        if self._is_public_path(method=method, path=path):
            return True
        required_scopes = self._required_scopes_for_path(method=method, path=path)
        decision = auth_service.authorize_request(
            authorization_header=str(self.headers.get("Authorization", "") or ""),
            required_scopes=required_scopes,
            require_admin=False,
        )
        if decision.allowed:
            return True
        status = HTTPStatus.UNAUTHORIZED if int(decision.status_code) == 401 else HTTPStatus.FORBIDDEN
        self._json_response(
            status,
            {
                "error": str(decision.error or "auth_forbidden"),
                "required_scopes": required_scopes,
            },
        )
        return False

    @staticmethod
    def _is_public_path(*, method: str, path: str) -> bool:
        normalized = str(path or "").strip()
        if normalized in {"/", "/dashboard", "/dashboard/emulator", "/health", "/api"}:
            return True
        if normalized.startswith("/candidate/"):
            return True
        if method.upper() == "POST" and normalized == "/api/webhooks/unipile":
            return True
        if normalized == "/api/linkedin/accounts/connect/callback":
            return True
        return False

    @staticmethod
    def _required_scopes_for_path(*, method: str, path: str) -> List[str]:
        if not str(path or "").startswith("/api/"):
            return []
        if method.upper() == "GET":
            return ["api:read"]
        return ["api:write"]

    def _require_admin_access(self) -> bool:
        auth_service = SERVICES.get("auth")
        if auth_service is not None and bool(getattr(auth_service, "enabled", False)):
            decision = auth_service.authorize_request(
                authorization_header=str(self.headers.get("Authorization", "") or ""),
                required_scopes=["admin:*"],
                require_admin=True,
            )
            if decision.allowed:
                return True
            status = HTTPStatus.UNAUTHORIZED if int(decision.status_code) == 401 else HTTPStatus.FORBIDDEN
            self._json_response(status, {"error": str(decision.error or "admin_auth_required")})
            return False
        expected = str(os.environ.get("TENER_ADMIN_API_TOKEN", "") or "").strip()
        if not expected:
            return True
        auth = str(self.headers.get("Authorization", "") or "").strip()
        incoming = ""
        if auth.lower().startswith("bearer "):
            incoming = auth[7:].strip()
        if incoming and incoming == expected:
            return True
        self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "admin auth required"})
        return False

    def _public_base_url(self) -> str:
        configured = str(os.environ.get("TENER_PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
        if configured:
            return configured
        host = str(self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or "").strip()
        proto = str(self.headers.get("X-Forwarded-Proto") or "").strip().lower()
        if not proto:
            proto = "https" if os.environ.get("RENDER") else "http"
        if host:
            return f"{proto}://{host}"
        return "http://localhost:8080"

    @staticmethod
    def _escape_html(text: str) -> str:
        return (
            str(text or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;")
        )

    @staticmethod
    def _pick_str(payload: Dict[str, Any], *paths: str) -> str:
        for path in paths:
            value = TenerRequestHandler._get_nested(payload, path)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _pick_text(payload: Dict[str, Any], *paths: str) -> str:
        for path in paths:
            value = TenerRequestHandler._get_nested(payload, path)
            text = TenerRequestHandler._coerce_text(value)
            if text:
                return text
        return ""

    @staticmethod
    def _pick_attachment_text(payload: Dict[str, Any], *paths: str) -> str:
        fragments: List[str] = []
        seen: set[str] = set()
        for path in paths:
            value = TenerRequestHandler._get_nested(payload, path)
            TenerRequestHandler._collect_attachment_fragments(value, fragments, seen, limit=12)
            if len(fragments) >= 12:
                break
        return "\n".join(fragments[:12]).strip()

    @staticmethod
    def _collect_attachment_fragments(value: Any, fragments: List[str], seen: set[str], limit: int = 12) -> None:
        if len(fragments) >= limit:
            return
        if isinstance(value, dict):
            name_keys = ("name", "filename", "file_name", "title")
            url_keys = (
                "url",
                "link",
                "href",
                "download_url",
                "downloadUrl",
                "signed_url",
                "signedUrl",
                "public_url",
                "publicUrl",
                "file_url",
                "fileUrl",
            )
            names: List[str] = []
            urls: List[str] = []
            for key in name_keys:
                raw = value.get(key)
                if isinstance(raw, str):
                    cleaned = raw.strip()
                    if cleaned:
                        names.append(cleaned)
            for key in url_keys:
                raw = value.get(key)
                if isinstance(raw, str):
                    cleaned = raw.strip()
                    if cleaned.startswith("http://") or cleaned.startswith("https://"):
                        urls.append(cleaned)

            for url in urls:
                if len(fragments) >= limit:
                    return
                text = f"attached file {names[0]} {url}".strip() if names else f"attached file {url}"
                token = text.lower()
                if token in seen:
                    continue
                seen.add(token)
                fragments.append(text)

            for nested in value.values():
                TenerRequestHandler._collect_attachment_fragments(nested, fragments, seen, limit=limit)
                if len(fragments) >= limit:
                    return
            return

        if isinstance(value, list):
            for item in value:
                TenerRequestHandler._collect_attachment_fragments(item, fragments, seen, limit=limit)
                if len(fragments) >= limit:
                    return

    @staticmethod
    def _merge_inbound_text(text: str, attachment_text: str) -> str:
        head = str(text or "").strip()
        tail = str(attachment_text or "").strip()
        if head and tail:
            return f"{head}\n{tail}".strip()
        return head or tail

    @staticmethod
    def _coerce_text(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            for key in ("text", "content", "body", "message"):
                nested = value.get(key)
                text = TenerRequestHandler._coerce_text(nested)
                if text:
                    return text
        if isinstance(value, list):
            for item in value:
                text = TenerRequestHandler._coerce_text(item)
                if text:
                    return text
        return ""

    @staticmethod
    def _get_nested(payload: Dict[str, Any], dotted_path: str) -> Any:
        current: Any = payload
        for part in dotted_path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current


def run() -> None:
    # Cloud runtimes usually inject PORT; prefer it when TENER_PORT is not set.
    host = os.environ.get("TENER_HOST", "0.0.0.0")
    port = int(os.environ.get("TENER_PORT", os.environ.get("PORT", "8080")))
    scheduler_stop: threading.Event | None = None

    if env_bool("TENER_FOLLOWUP_SCHEDULER_ENABLED", True):
        interval_seconds = max(30, int(os.environ.get("TENER_FOLLOWUP_INTERVAL_SECONDS", "120")))
        scheduler_limit = max(1, int(os.environ.get("TENER_FOLLOWUP_BATCH_LIMIT", "100")))
        scheduler_stop = threading.Event()

        def _scheduler_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    result = SERVICES["workflow"].run_due_pre_resume_followups(limit=scheduler_limit)
                    if int(result.get("processed") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.pre_resume.followups",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="pre_resume",
                            details={
                                "processed": int(result.get("processed") or 0),
                                "sent": int(result.get("sent") or 0),
                                "skipped": int(result.get("skipped") or 0),
                                "errors": int(result.get("errors") or 0),
                            },
                        )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.pre_resume.followups",
                        status="error",
                        entity_type="scheduler",
                        entity_id="pre_resume",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(interval_seconds)

        threading.Thread(target=_scheduler_loop, daemon=True, name="pre-resume-followup-scheduler").start()
        print(f"Pre-resume followup scheduler enabled: every {interval_seconds}s")

    if env_bool("TENER_INBOUND_POLL_SCHEDULER_ENABLED", True):
        poll_interval_seconds = max(15, int(os.environ.get("TENER_INBOUND_POLL_INTERVAL_SECONDS", "45")))
        poll_limit = max(1, int(os.environ.get("TENER_INBOUND_POLL_LIMIT", "100")))
        poll_per_chat_limit = max(1, int(os.environ.get("TENER_INBOUND_POLL_PER_CHAT_LIMIT", "20")))
        if scheduler_stop is None:
            scheduler_stop = threading.Event()

        def _inbound_poll_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    result = SERVICES["workflow"].poll_provider_inbound_messages(
                        limit=poll_limit,
                        per_chat_limit=poll_per_chat_limit,
                    )
                    if int(result.get("processed") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.inbound.poll",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="unipile_inbound",
                            details={
                                "conversations_checked": int(result.get("conversations_checked") or 0),
                                "messages_scanned": int(result.get("messages_scanned") or 0),
                                "processed": int(result.get("processed") or 0),
                                "duplicates": int(result.get("duplicates") or 0),
                                "ignored": int(result.get("ignored") or 0),
                                "errors": int(result.get("errors") or 0),
                            },
                        )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.inbound.poll",
                        status="error",
                        entity_type="scheduler",
                        entity_id="unipile_inbound",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(poll_interval_seconds)

        threading.Thread(target=_inbound_poll_loop, daemon=True, name="unipile-inbound-poller").start()
        print(f"Unipile inbound poll scheduler enabled: every {poll_interval_seconds}s")

    if env_bool("TENER_OUTBOUND_DISPATCH_SCHEDULER_ENABLED", True):
        dispatch_interval_seconds = max(15, int(os.environ.get("TENER_OUTBOUND_DISPATCH_INTERVAL_SECONDS", "30")))
        dispatch_limit = max(1, int(os.environ.get("TENER_OUTBOUND_DISPATCH_BATCH_LIMIT", "100")))
        if scheduler_stop is None:
            scheduler_stop = threading.Event()

        def _outbound_dispatch_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    result = SERVICES["workflow"].dispatch_outbound_actions(limit=dispatch_limit)
                    if int(result.get("processed") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.outreach.dispatch",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="outbound_dispatch",
                            details={
                                "processed": int(result.get("processed") or 0),
                                "sent": int(result.get("sent") or 0),
                                "pending_connection": int(result.get("pending_connection") or 0),
                                "deferred": int(result.get("deferred") or 0),
                                "failed": int(result.get("failed") or 0),
                            },
                        )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.outreach.dispatch",
                        status="error",
                        entity_type="scheduler",
                        entity_id="outbound_dispatch",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(dispatch_interval_seconds)

        threading.Thread(target=_outbound_dispatch_loop, daemon=True, name="outbound-dispatch-scheduler").start()
        print(f"Outbound dispatch scheduler enabled: every {dispatch_interval_seconds}s")

    if env_bool("TENER_INTERVIEW_SCHEDULER_ENABLED", True):
        interview_interval_seconds = max(30, int(os.environ.get("TENER_INTERVIEW_SCHEDULER_INTERVAL_SECONDS", "180")))
        interview_sync_limit = max(1, int(os.environ.get("TENER_INTERVIEW_SYNC_LIMIT", "100")))
        interview_followup_limit = max(1, int(os.environ.get("TENER_INTERVIEW_FOLLOWUP_LIMIT", "100")))
        if scheduler_stop is None:
            scheduler_stop = threading.Event()

        def _interview_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    sync_result = SERVICES["workflow"].sync_interview_progress(
                        limit=interview_sync_limit,
                        force_refresh=False,
                    )
                    if int(sync_result.get("processed") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.interview.sync",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="interview_sync",
                            details={
                                "processed": int(sync_result.get("processed") or 0),
                                "updated": int(sync_result.get("updated") or 0),
                                "errors": int(sync_result.get("errors") or 0),
                            },
                        )

                    followup_result = SERVICES["workflow"].run_due_interview_followups(limit=interview_followup_limit)
                    if int(followup_result.get("processed") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.interview.followups",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="interview_followups",
                            details={
                                "processed": int(followup_result.get("processed") or 0),
                                "sent": int(followup_result.get("sent") or 0),
                                "skipped": int(followup_result.get("skipped") or 0),
                                "errors": int(followup_result.get("errors") or 0),
                            },
                        )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.interview",
                        status="error",
                        entity_type="scheduler",
                        entity_id="interview",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(interview_interval_seconds)

        threading.Thread(target=_interview_loop, daemon=True, name="interview-scheduler").start()
        print(f"Interview scheduler enabled: every {interview_interval_seconds}s")

    server = ThreadingHTTPServer((host, port), TenerRequestHandler)
    print(f"Tener AI V1 API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        if scheduler_stop is not None:
            scheduler_stop.set()


if __name__ == "__main__":
    run()
