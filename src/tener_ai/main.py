from __future__ import annotations

import json
import hashlib
import ipaddress
import mimetypes
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urlerror, request as urlrequest
from urllib.parse import parse_qs, unquote, urlparse

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .attachments import descriptors_to_text, extract_attachment_descriptors_from_values
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
try:
    from .demo_jobs import seed_full_demo_job
except ModuleNotFoundError:  # Optional demo tooling is not part of every deploy branch.
    seed_full_demo_job = None  # type: ignore[assignment]
from .db import Database
from .db_backfill import TABLE_ORDER, backfill_sqlite_to_postgres
from .db_parity import DEFAULT_PARITY_TABLES, build_parity_report
from .db_dual import DualWriteDatabase, PostgresMirrorWriter
from .db_pg import PostgresMigrationRunner
from .db_read_pg import PostgresReadDatabase
from .db_runtime_pg import PostgresRuntimeDatabase
from .emulator import EmulatorProjectStore
from .instructions import AgentEvaluationPlaybook, AgentInstructions
from .interview_client import InterviewAPIClient
from .landing import LandingService, LandingValidationError
from .linkedin_accounts import LinkedInAccountService
from .linkedin_limits import resolve_account_limit_snapshot, validate_account_limits_payload
from .llm_responder import CandidateLLMResponder
from .linkedin_provider import build_linkedin_provider
from .matching import MatchingEngine
from .outreach_policy import LinkedInOutreachPolicy
from .pre_resume_service import PreResumeCommunicationService
from .signal_rules import SignalRulesEngine
from .signals import JobSignalsLiveViewService, MonitoringService, SignalIngestionService
from .workflow import JobOperationBlockedError, WorkflowService


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def list_sqlite_user_tables(sqlite_path: str) -> List[str]:
    db_path = str(sqlite_path or "").strip()
    if not db_path or not Path(db_path).exists():
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name ASC
            """
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows if row and str(row[0] or "").strip()]


def default_interview_api_base() -> str:
    configured = str(os.environ.get("TENER_INTERVIEW_API_BASE", "")).strip()
    if configured:
        return configured.rstrip("/")
    if os.environ.get("RENDER"):
        return "https://tener-interview-dashboard.onrender.com"
    return ""


OUTREACH_STALE_REPLY_DAYS = 7
OUTREACH_STALE_REPLY_MINUTES = OUTREACH_STALE_REPLY_DAYS * 24 * 60


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


def _run_outreach_connection_poll_scheduler_tick(*, poll_limit: int) -> Dict[str, Any]:
    result = SERVICES["workflow"].poll_pending_connections(limit=poll_limit)
    if int(result.get("checked") or 0) > 0:
        SERVICES["db"].log_operation(
            operation="scheduler.outreach.poll_connections",
            status="ok",
            entity_type="scheduler",
            entity_id="outreach_connection_poll",
            details={
                "checked": int(result.get("checked") or 0),
                "connected": int(result.get("connected") or 0),
                "sent": int(result.get("sent") or 0),
                "still_waiting": int(result.get("still_waiting") or 0),
                "failed": int(result.get("failed") or 0),
            },
        )
    return result


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
    if db_backend != "postgres":
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

    sqlite_db: Any = None
    db_primary_path = ""
    if db_backend == "postgres":
        db = PostgresRuntimeDatabase(postgres_dsn)
        db_runtime_mode = "postgres_primary"
    else:
        try:
            sqlite_db = Database(db_path=db_path)
        except Exception:
            if db_path != local_db_path:
                sqlite_db = Database(db_path=local_db_path)
            else:
                raise
        sqlite_db.init_schema()
        db = sqlite_db
        db_primary_path = str(sqlite_db.db_path or "")
    if db_backend == "dual":
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
    if db_runtime_mode == "postgres_primary":
        read_db = db
        db_read_status = {
            "status": "ok",
            "source": "postgres",
            "requested_source": db_read_source_raw,
            "reason": "postgres_runtime_primary",
        }
    elif db_read_source == "postgres":
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
        matching_engine=matching_engine,
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
    signal_rules = SignalRulesEngine()
    signals_ingestion = SignalIngestionService(db=db, rules_engine=signal_rules)
    signals_live = JobSignalsLiveViewService(db=db, rules_engine=signal_rules)
    monitoring = MonitoringService(db=db)
    landing = LandingService(db=db)

    services = {
        "db": db,
        "read_db": read_db,
        "db_primary_path": db_primary_path,
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
        "signals_ingestion": signals_ingestion,
        "signals_live": signals_live,
        "monitoring": monitoring,
        "landing": landing,
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

        if parsed.path == "/zalando":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/zalando/")
            return

        if parsed.path.startswith("/zalando/"):
            if self._serve_static_directory(prefix="/zalando/", directory=project_root() / "Zalando-prototype", path=parsed.path):
                return

        if parsed.path == "/liveramp":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/liveramp/")
            return

        if parsed.path.startswith("/liveramp/"):
            if self._serve_static_directory(prefix="/liveramp/", directory=project_root() / "LiveRamp-prototype", path=parsed.path):
                return

        if parsed.path == "/fiverr":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/fiverr/")
            return

        if parsed.path.startswith("/fiverr/"):
            if self._serve_static_directory(prefix="/fiverr/", directory=project_root() / "Fiverr-prototype", path=parsed.path):
                return

        if parsed.path == "/skilled-trades":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/skilled-trades/")
            return

        if parsed.path.startswith("/skilled-trades/"):
            if self._serve_static_directory(
                prefix="/skilled-trades/",
                directory=project_root() / "SkilledTrades-prototype",
                path=parsed.path,
            ):
                return

        if parsed.path == "/agents-office":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/agents-office/")
            return

        if parsed.path.startswith("/agents-office/"):
            if self._serve_static_directory(
                prefix="/agents-office/",
                directory=project_root() / "AgentsOffice-prototype",
                path=parsed.path,
            ):
                return

        if parsed.path == "/toptal":
            self._redirect_response(HTTPStatus.MOVED_PERMANENTLY, "/toptal/")
            return

        if parsed.path.startswith("/toptal/"):
            if self._serve_static_directory(prefix="/toptal/", directory=project_root() / "Toptal-prototype", path=parsed.path):
                return

        if parsed.path == "/dashboard/emulator":
            dashboard = project_root() / "src" / "tener_ai" / "static" / "emulator_dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "emulator dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        if parsed.path == "/dashboard/signals-live":
            dashboard = project_root() / "src" / "tener_ai" / "static" / "signals_live_dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "signals live dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        if parsed.path in {"/", "/landing", "/landing/"}:
            landing_page = project_root() / "src" / "tener_ai" / "static" / "landing.html"
            if not landing_page.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "landing file not found"})
                return
            self._html_response(HTTPStatus.OK, landing_page.read_text(encoding="utf-8"))
            return

        if parsed.path in {"/favicon.ico", "/favicon.png"}:
            favicon = project_root() / "src" / "tener_ai" / "static" / "favicon.png"
            if not favicon.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "favicon file not found"})
                return
            self._binary_response(
                status=HTTPStatus.OK,
                content_type="image/png",
                payload=favicon.read_bytes(),
                extra_headers={"Cache-Control": "public, max-age=3600"},
            )
            return

        if parsed.path == "/dashboard":
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
                        "landing": "GET /landing",
                        "create_job": "POST /api/jobs",
                        "admin_seed_full_demo_job": "POST /api/admin/seeds/full-demo-job",
                        "list_jobs": "GET /api/jobs",
                        "get_job": "GET /api/jobs/{job_id}",
                        "archive_jobs_bulk": "POST /api/jobs/archive-bulk",
                        "pause_job": "POST /api/jobs/{job_id}/pause",
                        "resume_job": "POST /api/jobs/{job_id}/resume",
                        "job_progress": "GET /api/jobs/{job_id}/progress",
                        "list_job_candidates": "GET /api/jobs/{job_id}/candidates",
                        "job_source_filters": "GET /api/jobs/{job_id}/source-filters",
                        "job_source_top_up": "POST /api/jobs/{job_id}/source-top-up",
                        "candidate_profile": "GET /api/candidates/{candidate_id}/profile?job_id=...&audit=0|1",
                        "candidate_resume_preview": "GET /api/candidates/{candidate_id}/resume-preview?job_id=...&url=...",
                        "candidate_resume_content": "GET /api/candidates/{candidate_id}/resume-preview/content?url=...",
                        "candidate_demo_profile": "POST /api/candidates/demo-profile",
                        "job_signals_live": "GET /api/jobs/{job_id}/signals/live?refresh=1&limit=200&signals_limit=5000",
                        "job_signals_ingest": "POST /api/jobs/{job_id}/signals/ingest",
                        "monitoring_status": "GET /api/monitoring/status?limit_jobs=20",
                        "emulator_status": "GET /api/emulator",
                        "emulator_projects": "GET /api/emulator/projects",
                        "emulator_project": "GET /api/emulator/projects/{project_id}",
                        "emulator_company_profiles": "GET /api/emulator/company-profiles",
                        "emulator_company_profile": "GET /api/emulator/company-profiles/{company_key}",
                        "emulator_reload": "POST /api/emulator/reload",
                        "emulator_dashboard": "GET /dashboard/emulator",
                        "job_linkedin_routing": "GET /api/jobs/{job_id}/linkedin-routing",
                        "update_job_jd": "POST /api/jobs/{job_id}/jd",
                        "update_job_requirements": "POST /api/jobs/{job_id}/requirements",
                        "update_job_linkedin_routing": "POST /api/jobs/{job_id}/linkedin-routing",
                        "run_workflow": "POST /api/workflows/execute",
                        "source_step": "POST /api/steps/source",
                        "enrich_step": "POST /api/steps/enrich",
                        "verify_step": "POST /api/steps/verify",
                        "add_step": "POST /api/steps/add",
                        "outreach_step": "POST /api/steps/outreach",
                        "outreach_dispatch_run": "POST /api/outreach/dispatch/run",
                        "outreach_backfill_unassigned": "POST /api/outreach/backfill-unassigned",
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
                        "conversation_resume_backfill": "POST /api/conversations/{conversation_id}/resume-backfill",
                        "chats_overview": "GET /api/chats/overview?limit=200",
                        "outreach_ops": "GET /api/outreach/ops?job_id=...",
                        "outreach_ats_board": "GET /api/outreach/ats-board?job_id=...&limit=600",
                        "linkedin_accounts_list": "GET /api/linkedin/accounts?limit=200&status=connected",
                        "linkedin_connect_callback": "GET /api/linkedin/accounts/connect/callback?state=...",
                        "linkedin_connect_start": "POST /api/linkedin/accounts/connect/start",
                        "linkedin_accounts_sync_all": "POST /api/linkedin/accounts/sync-all",
                        "linkedin_account_limits_update": "POST /api/linkedin/accounts/{account_id}/limits",
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
                        "landing_newsletter": "POST /api/landing/newsletter",
                        "landing_contact": "POST /api/landing/contact",
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
            has_local_asset = self._has_local_candidate_resume_asset(candidate_id=int(candidate_id), selected_url=selected_url)
            if selected_url and not has_local_asset and not (selected_url.startswith("https://") or selected_url.startswith("http://")):
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
            if self._serve_local_candidate_resume_asset(candidate_id=int(candidate_id), selected_url=selected_url):
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

        if parsed.path == "/api/monitoring/status":
            if not self._require_admin_access():
                return
            monitoring = SERVICES.get("monitoring")
            if monitoring is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "monitoring service unavailable"})
                return
            params = parse_qs(parsed.query or "")
            limit_jobs = self._safe_int((params.get("limit_jobs") or ["20"])[0], 20) or 20
            report = monitoring.build_status(limit_jobs=limit_jobs)
            status_code = HTTPStatus.OK if str(report.get("status") or "ok") == "ok" else HTTPStatus.MULTI_STATUS
            self._json_response(status_code, report)
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
            policy = SERVICES["outreach_policy"].to_dict()
            items = out.get("items") if isinstance(out.get("items"), list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                item.update(resolve_account_limit_snapshot(item, policy))
            self._json_response(HTTPStatus.OK, out)
            return

        if parsed.path == "/api/linkedin/accounts/connect/callback":
            params = parse_qs(parsed.query or "")
            out = SERVICES["linkedin_accounts"].complete_connect_callback(query=params)
            status = str(out.get("status") or "").strip().lower()
            auto_rebalance: Dict[str, Any] = {}
            if status in {"connected", "already_completed"}:
                auto_rebalance = self._run_outreach_capacity_rebalance(trigger="linkedin_connect_callback")
            SERVICES["db"].log_operation(
                operation="linkedin.connect.callback",
                status="ok" if status in {"connected", "already_completed"} else "error",
                entity_type="linkedin_onboarding",
                entity_id=str(out.get("session_id") or ""),
                details={"result": out, "outreach_rebalance": auto_rebalance},
            )
            if status in {"connected", "already_completed"}:
                totals = auto_rebalance.get("totals") if isinstance(auto_rebalance.get("totals"), dict) else {}
                queued = int((totals.get("new_threads_queued") or 0) + (totals.get("recovery_queued") or 0))
                dispatched_sent = int(totals.get("sent") or 0)
                dispatched_pending = int(totals.get("pending_connection") or 0)
                self._html_response(
                    HTTPStatus.OK,
                    f"""
                    <html><body style="font-family:Arial,sans-serif;padding:24px;">
                    <h2>LinkedIn account connected</h2>
                    <p>You can close this tab and return to Tener dashboard.</p>
                    <p>Auto rebalance queued: {queued}, sent: {dispatched_sent}, pending connection: {dispatched_pending}</p>
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
            workflow = SERVICES.get("workflow")
            session_public = getattr(workflow, "_public_pre_resume_session", None)
            if callable(session_public):
                items = [session_public(item) for item in items]
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
                workflow = SERVICES.get("workflow")
                state_public = getattr(workflow, "_public_pre_resume_state", None)
                if callable(state_public):
                    session = state_public(session)
                self._json_response(HTTPStatus.OK, session)
                return

        if parsed.path == "/api/jobs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100)
            items = self._read_db().list_jobs(limit=limit or 100)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path == "/api/demo/agents-office/jobs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["8"])[0], 8)
            payload = self._build_agents_office_demo_jobs(limit=limit or 8)
            self._json_response(HTTPStatus.OK, payload)
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

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/source-filters"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/source-filters$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            job = self._read_db().get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            workflow = SERVICES.get("workflow")
            sourcing_agent = getattr(workflow, "sourcing_agent", None) if workflow is not None else None
            build_preview = getattr(sourcing_agent, "build_search_preview", None)
            if not callable(build_preview):
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "sourcing preview unavailable"})
                return
            filters = build_preview(job)
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": int(job_id),
                    "job_title": str(job.get("title") or "").strip() or None,
                    "job_company": str(job.get("company") or "").strip() or None,
                    "filters": filters,
                },
            )
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/signals/live"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/signals/live$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            ingestion_service = SERVICES.get("signals_ingestion")
            live_service = SERVICES.get("signals_live")
            if ingestion_service is None or live_service is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "signals services unavailable"})
                return
            params = parse_qs(parsed.query or "")
            refresh = bool(self._safe_bool((params.get("refresh") or ["1"])[0], True))
            limit = self._safe_int((params.get("limit") or ["200"])[0], 200) or 200
            signals_limit = self._safe_int((params.get("signals_limit") or ["5000"])[0], 5000) or 5000
            ingest_result = None
            if refresh:
                try:
                    ingest_result = ingestion_service.ingest_job(
                        job_id=job_id,
                        limit_candidates=limit,
                    )
                except ValueError:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                    return
                except Exception as exc:
                    self._json_response(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {"error": "signals ingestion failed", "details": str(exc)},
                    )
                    return
            try:
                view = live_service.build_job_view(
                    job_id=job_id,
                    limit_candidates=limit,
                    limit_signals=signals_limit,
                )
            except ValueError:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "signals live view failed", "details": str(exc)},
                )
                return
            if ingest_result is not None:
                view["ingestion"] = ingest_result
            self._json_response(HTTPStatus.OK, view)
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
            started_only_raw = str((params.get("started_only") or ["0"])[0] or "").strip().lower()
            started_only = started_only_raw in {"1", "true", "yes", "y", "on"}
            dialogue_bucket = str((params.get("dialogue_bucket") or [""])[0] or "").strip().lower() or None
            items = self._read_db().list_conversations_overview(
                limit=limit or 200,
                job_id=job_id,
                started_only=started_only,
                dialogue_bucket=dialogue_bucket,
            )
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path == "/api/outreach/ops":
            params = parse_qs(parsed.query or "")
            logs_limit = self._safe_int((params.get("limit_logs") or ["800"])[0], 800)
            chats_limit = self._safe_int((params.get("limit_chats") or ["600"])[0], 600)
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            if logs_limit is None:
                logs_limit = 800
            if chats_limit is None:
                chats_limit = 600
            report = self._build_outreach_ops_report(
                db=SERVICES["db"],
                job_id=job_id,
                logs_limit=max(100, min(int(logs_limit), 2000)),
                chats_limit=max(100, min(int(chats_limit), 2000)),
            )
            self._json_response(HTTPStatus.OK, report)
            return

        if parsed.path == "/api/outreach/ats-board":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["600"])[0], 600)
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            report = self._build_outreach_ats_board(
                db=SERVICES["db"],
                job_id=job_id,
                limit=max(50, min(int(limit or 600), 2000)),
            )
            self._json_response(HTTPStatus.OK, report)
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

        if parsed.path == "/api/landing/newsletter":
            if not isinstance(payload, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            try:
                result = SERVICES["landing"].submit_newsletter(
                    payload,
                    source_path="/landing",
                    ip_address=self._request_ip_address(),
                    user_agent=str(self.headers.get("User-Agent") or ""),
                )
            except LandingValidationError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "validation_failed", "field_errors": exc.field_errors})
                return
            status = HTTPStatus.CREATED if bool(result.get("created")) else HTTPStatus.OK
            self._json_response(status, result)
            return

        if parsed.path == "/api/landing/contact":
            if not isinstance(payload, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            try:
                result = SERVICES["landing"].submit_contact_request(
                    payload,
                    source_path="/landing",
                    ip_address=self._request_ip_address(),
                    user_agent=str(self.headers.get("User-Agent") or ""),
                )
            except LandingValidationError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "validation_failed", "field_errors": exc.field_errors})
                return
            self._json_response(HTTPStatus.CREATED, result)
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

        if parsed.path == "/api/linkedin/accounts/sync-all":
            if not self._require_admin_access():
                return
            try:
                out = SERVICES["linkedin_accounts"].sync_accounts(account_id=None)
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "linkedin_accounts_sync_all_failed", "details": str(exc)},
                )
                return
            status = str(out.get("status") or "").lower()
            SERVICES["db"].log_operation(
                operation="linkedin.accounts.sync_all",
                status="ok" if status == "ok" else "error",
                entity_type="linkedin_account",
                entity_id="all",
                details=out,
            )
            if status == "ok":
                out["outreach_rebalance"] = self._run_outreach_capacity_rebalance(trigger="linkedin_accounts_sync")
            http_status = HTTPStatus.OK if status == "ok" else HTTPStatus.BAD_REQUEST
            self._json_response(http_status, out)
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

        if parsed.path == "/api/admin/seeds/full-demo-job":
            if not self._require_admin_access():
                return
            if seed_full_demo_job is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "demo job seed unavailable"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            force_reseed = bool(body.get("force_reseed") is True)
            try:
                out = seed_full_demo_job(
                    db=SERVICES["db"],
                    pre_resume_service=SERVICES.get("pre_resume"),
                    interview_assessment_preparer=self._prepare_job_interview_assessment,
                    force_reseed=force_reseed,
                    postgres_dsn=str(SERVICES.get("postgres_dsn") or os.environ.get("TENER_DB_DSN", "") or "").strip(),
                )
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="admin.seed.full_demo_job",
                    status="error",
                    entity_type="job",
                    entity_id="seed_full_demo_job",
                    details={"error": str(exc), "force_reseed": force_reseed},
                )
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "full demo job seed failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, out)
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

        match_limits = re.match(r"^/api/linkedin/accounts/(\d+)/limits$", parsed.path)
        if match_limits:
            if not self._require_admin_access():
                return
            account_id = self._safe_int(match_limits.group(1), None)
            if account_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid account id"})
                return
            body = payload or {}
            try:
                parsed_limits = validate_account_limits_payload(body)
            except ValueError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            account = SERVICES["db"].update_linkedin_account_limits(
                account_id=account_id,
                has_daily_message_limit=bool(parsed_limits.get("has_daily_message_limit")),
                daily_message_limit=parsed_limits.get("daily_message_limit"),
                has_daily_connect_limit=bool(parsed_limits.get("has_daily_connect_limit")),
                daily_connect_limit=parsed_limits.get("daily_connect_limit"),
            )
            if not isinstance(account, dict):
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "account_not_found"})
                return
            account.update(resolve_account_limit_snapshot(account, SERVICES["outreach_policy"].to_dict()))
            SERVICES["db"].log_operation(
                operation="linkedin.account.limits.updated",
                status="ok",
                entity_type="linkedin_account",
                entity_id=str(account_id),
                details={
                    "daily_message_limit": account.get("daily_message_limit"),
                    "daily_connect_limit": account.get("daily_connect_limit"),
                },
            )
            self._json_response(HTTPStatus.OK, {"status": "ok", "account": account})
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
            refreshed_job = SERVICES["db"].get_job(job_id)
            requirements = self._compute_job_requirements(refreshed_job or {"jd_text": jd_text})
            SERVICES["db"].update_job_requirements(
                job_id=job_id,
                must_have_skills=requirements.get("must_have_skills"),
                nice_to_have_skills=requirements.get("nice_to_have_skills"),
                questionable_skills=requirements.get("questionable_skills"),
            )
            SERVICES["db"].log_operation(
                operation="job.jd.updated",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"length": len(jd_text), **requirements},
            )
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "jd_text": jd_text, **requirements})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/requirements"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/requirements$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job = SERVICES["db"].get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            salary_min = self._safe_float(body.get("salary_min"), None) if "salary_min" in body else None
            salary_max = self._safe_float(body.get("salary_max"), None) if "salary_max" in body else None
            if "salary_min" in body and salary_min is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_min must be numeric"})
                return
            if "salary_max" in body and salary_max is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_max must be numeric"})
                return
            if salary_min is not None and salary_max is not None and salary_min > salary_max:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_min must be less than or equal to salary_max"})
                return
            salary_currency = str(body.get("salary_currency") or "").strip().upper() or None if "salary_currency" in body else None
            work_authorization_required = (
                self._safe_bool(body.get("work_authorization_required"), None)
                if "work_authorization_required" in body
                else None
            )
            location = str(body.get("location") or "").strip() or None if "location" in body else None
            seniority = str(body.get("seniority") or "").strip().lower() or None if "seniority" in body else None
            if any(
                key in body
                for key in ("location", "seniority", "salary_min", "salary_max", "salary_currency", "work_authorization_required")
            ):
                SERVICES["db"].update_job_details(
                    job_id=job_id,
                    location=location,
                    seniority=seniority,
                    salary_min=salary_min,
                    salary_max=salary_max,
                    salary_currency=salary_currency,
                    work_authorization_required=work_authorization_required,
                )
                job = SERVICES["db"].get_job(job_id) or job
            manual_override = any(
                key in body for key in ("must_have_skills", "nice_to_have_skills", "questionable_skills")
            )
            if manual_override:
                must_have_raw = body.get("must_have_skills") or []
                if not isinstance(must_have_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "must_have_skills must be an array"})
                    return
                nice_to_have_raw = body.get("nice_to_have_skills") or []
                if not isinstance(nice_to_have_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "nice_to_have_skills must be an array"})
                    return
                questionable_raw = body.get("questionable_skills") or []
                if not isinstance(questionable_raw, list):
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": "questionable_skills must be an array"})
                    return
                must_have_skills = self._normalize_text_list(must_have_raw)
                nice_to_have_skills = self._normalize_text_list(nice_to_have_raw)
                questionable_skills = self._normalize_text_list(questionable_raw)
            else:
                requirements = self._compute_job_requirements(job)
                must_have_skills = requirements.get("must_have_skills") or []
                nice_to_have_skills = requirements.get("nice_to_have_skills") or []
                questionable_skills = requirements.get("questionable_skills") or []
            updated = SERVICES["db"].update_job_requirements(
                job_id=job_id,
                must_have_skills=must_have_skills,
                nice_to_have_skills=nice_to_have_skills,
                questionable_skills=questionable_skills,
            )
            if not updated:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            SERVICES["db"].log_operation(
                operation="job.requirements.updated",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={
                    "must_have_skills": must_have_skills,
                    "nice_to_have_skills": nice_to_have_skills,
                    "questionable_skills": questionable_skills,
                    "mode": "manual_override" if manual_override else "auto_extract",
                    "location": location if "location" in body else None,
                    "seniority": seniority if "seniority" in body else None,
                    "salary_min": salary_min if "salary_min" in body else None,
                    "salary_max": salary_max if "salary_max" in body else None,
                    "salary_currency": salary_currency if "salary_currency" in body else None,
                    "work_authorization_required": work_authorization_required,
                },
            )
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": job_id,
                    "location": (job or {}).get("location"),
                    "seniority": (job or {}).get("seniority"),
                    "salary_min": (job or {}).get("salary_min"),
                    "salary_max": (job or {}).get("salary_max"),
                    "salary_currency": (job or {}).get("salary_currency"),
                    "work_authorization_required": (job or {}).get("work_authorization_required"),
                    "must_have_skills": must_have_skills,
                    "nice_to_have_skills": nice_to_have_skills,
                    "questionable_skills": questionable_skills,
                },
            )
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
                    provider_payload=body,
                    provider_message_id=event_id or None,
                    occurred_at=occurred_at or None,
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

        if parsed.path == "/api/jobs/archive-bulk":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_ids_raw = body.get("job_ids")
            exclude_titles_raw = body.get("exclude_titles")
            exclude_job_ids_raw = body.get("exclude_job_ids")
            if job_ids_raw is not None and not isinstance(job_ids_raw, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_ids must be an array"})
                return
            if exclude_titles_raw is not None and not isinstance(exclude_titles_raw, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "exclude_titles must be an array"})
                return
            if exclude_job_ids_raw is not None and not isinstance(exclude_job_ids_raw, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "exclude_job_ids must be an array"})
                return
            normalized_job_ids = [int(item) for item in (job_ids_raw or []) if self._safe_int(item, None) is not None]
            result = SERVICES["db"].archive_jobs(
                job_ids=normalized_job_ids,
                exclude_titles=[str(item or "").strip() for item in (exclude_titles_raw or []) if str(item or "").strip()],
                exclude_job_ids=exclude_job_ids_raw or [],
            )
            SERVICES["db"].log_operation(
                operation="job.archive.bulk",
                status="ok",
                entity_type="job",
                entity_id="bulk",
                details={
                    "updated": int(result.get("updated") or 0),
                    "job_ids": normalized_job_ids,
                    "exclude_titles": [str(item or "").strip() for item in (exclude_titles_raw or []) if str(item or "").strip()],
                    "exclude_job_ids": [int(item) for item in (exclude_job_ids_raw or []) if self._safe_int(item, None) is not None],
                },
            )
            self._json_response(HTTPStatus.OK, {"status": "ok", **result})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/pause"):
            if not self._require_admin_access():
                return
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/pause$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                body = {}
            result = SERVICES["db"].pause_job(job_id=job_id, reason=str(body.get("reason") or "").strip() or None)
            reason = str(result.get("reason") or "").strip().lower()
            if reason == "job_not_found":
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            SERVICES["db"].log_operation(
                operation="job.pause",
                status="ok" if int(result.get("updated") or 0) > 0 else "skipped",
                entity_type="job",
                entity_id=str(job_id),
                details={"reason": body.get("reason"), "result": result},
            )
            self._json_response(HTTPStatus.OK, {"status": "ok", **result})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/resume"):
            if not self._require_admin_access():
                return
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/resume$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            result = SERVICES["db"].resume_job(job_id=job_id)
            reason = str(result.get("reason") or "").strip().lower()
            if reason == "job_not_found":
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            SERVICES["db"].log_operation(
                operation="job.resume",
                status="ok" if int(result.get("updated") or 0) > 0 else "skipped",
                entity_type="job",
                entity_id=str(job_id),
                details={"result": result},
            )
            self._json_response(HTTPStatus.OK, {"status": "ok", **result})
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
            normalized_languages = [str(x).lower() for x in preferred_languages if str(x).strip()]
            salary_min = self._safe_float(body.get("salary_min"), None)
            salary_max = self._safe_float(body.get("salary_max"), None)
            if body.get("salary_min") is not None and salary_min is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_min must be numeric"})
                return
            if body.get("salary_max") is not None and salary_max is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_max must be numeric"})
                return
            if salary_min is not None and salary_max is not None and salary_min > salary_max:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "salary_min must be less than or equal to salary_max"})
                return
            salary_currency = str(body.get("salary_currency") or "").strip().upper() or None
            work_authorization_required = bool(self._safe_bool(body.get("work_authorization_required"), False))
            requirements = self._compute_job_requirements(
                {
                    "title": title,
                    "jd_text": jd_text,
                    "company": company,
                    "company_website": company_website,
                    "location": body.get("location"),
                    "preferred_languages": normalized_languages,
                    "seniority": (str(body.get("seniority")).lower() if body.get("seniority") else None),
                }
            )

            job_id = SERVICES["db"].insert_job(
                title=title,
                jd_text=jd_text,
                location=body.get("location"),
                preferred_languages=normalized_languages,
                must_have_skills=requirements.get("must_have_skills"),
                nice_to_have_skills=requirements.get("nice_to_have_skills"),
                questionable_skills=requirements.get("questionable_skills"),
                seniority=(str(body.get("seniority")).lower() if body.get("seniority") else None),
                company=company,
                company_website=company_website,
                salary_min=salary_min,
                salary_max=salary_max,
                salary_currency=salary_currency,
                work_authorization_required=work_authorization_required,
            )
            SERVICES["db"].log_operation(
                operation="job.created",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={
                    "title": title,
                    "company": company,
                    "company_website": company_website,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                    "salary_currency": salary_currency,
                    "work_authorization_required": work_authorization_required,
                },
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
                    "requirements": requirements,
                    "company_culture_profile": culture_profile,
                    "interview_assessment": interview_assessment,
                },
            )
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/signals/ingest"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/signals/ingest$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            ingestion_service = SERVICES.get("signals_ingestion")
            if ingestion_service is None:
                self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "signals ingestion service unavailable"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                body = {}
            limit_candidates = self._safe_int(body.get("limit_candidates"), 500) or 500
            try:
                out = ingestion_service.ingest_job(
                    job_id=job_id,
                    limit_candidates=max(1, min(limit_candidates, 5000)),
                )
            except ValueError:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "signals ingestion failed", "details": str(exc)},
                )
                return
            self._json_response(HTTPStatus.OK, out)
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
            except JobOperationBlockedError as exc:
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
                return
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="workflow.execute.error",
                    status="error",
                    entity_type="job",
                    entity_id=str(job_id),
                    details={"error": str(exc)},
                )
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "workflow failed", "details": str(exc)})
                return

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

            self._json_response(
                HTTPStatus.OK,
                workflow_payload,
            )
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/source-top-up"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/source-top-up$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                body = {}
            limit = self._safe_int(body.get("limit"), 30)
            test_mode = self._safe_bool(body.get("test_mode"), None)
            try:
                out = SERVICES["workflow"].top_up_job_candidates(job_id=job_id, limit=limit, test_mode=test_mode)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except JobOperationBlockedError as exc:
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
                return
            except Exception as exc:
                SERVICES["db"].log_operation(
                    operation="workflow.source_top_up.error",
                    status="error",
                    entity_type="job",
                    entity_id=str(job_id),
                    details={"error": str(exc), "limit": limit},
                )
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "source top up failed", "details": str(exc)})
                return
            self._json_response(HTTPStatus.OK, out)
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
            except JobOperationBlockedError as exc:
                self._persist_job_step_progress(job_id=job_id, step="source", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
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
            except JobOperationBlockedError as exc:
                self._persist_job_step_progress(job_id=job_id, step="verify", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
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
            except JobOperationBlockedError as exc:
                self._persist_job_step_progress(job_id=job_id, step="enrich", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
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
            except JobOperationBlockedError as exc:
                self._persist_job_step_progress(job_id=job_id, step="add", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
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
            except JobOperationBlockedError as exc:
                self._persist_job_step_progress(job_id=job_id, step="outreach", status="error", output={"error": str(exc)})
                self._json_response(HTTPStatus.CONFLICT, {"error": str(exc)})
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

        if parsed.path == "/api/outreach/backfill-unassigned":
            if not self._require_admin_access():
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 300) or 300
            job_id_raw = body.get("job_id")
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            try:
                result = SERVICES["workflow"].backfill_outreach_for_unassigned_conversations(
                    limit=max(1, min(int(limit), 500)),
                    job_id=job_id,
                )
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "outreach_backfill_unassigned_failed", "details": str(exc)},
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
            workflow = SERVICES.get("workflow")
            state_public = getattr(workflow, "_public_pre_resume_state", None)
            if callable(state_public) and isinstance(result.get("state"), dict):
                result = dict(result)
                result["state"] = state_public(result.get("state"))
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

        if parsed.path.startswith("/api/conversations/") and parsed.path.endswith("/resume-backfill"):
            conversation_id = self._extract_id(parsed.path, pattern=r"^/api/conversations/(\d+)/resume-backfill$")
            if conversation_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid conversation id"})
                return
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            limit = self._safe_int(body.get("limit"), 50) or 50
            try:
                result = SERVICES["workflow"].backfill_resume_assets_for_conversation(
                    conversation_id=int(conversation_id),
                    per_chat_limit=limit,
                )
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "resume backfill failed", "details": str(exc)})
                return
            self._json_response(HTTPStatus.OK, result)
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

    @staticmethod
    def _normalize_text_list(values: Any) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in values or []:
            value = str(raw or "").strip().lower()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    def _compute_job_requirements(self, job_like: Dict[str, Any]) -> Dict[str, Any]:
        matching_engine = SERVICES.get("matching_engine")
        builder = getattr(matching_engine, "build_job_requirements", None)
        if not callable(builder):
            return {
                "must_have_skills": [],
                "nice_to_have_skills": [],
                "questionable_skills": [],
            }
        try:
            requirements = builder(job_like)
        except Exception:
            return {
                "must_have_skills": [],
                "nice_to_have_skills": [],
                "questionable_skills": [],
            }
        if not isinstance(requirements, dict):
            return {
                "must_have_skills": [],
                "nice_to_have_skills": [],
                "questionable_skills": [],
            }
        return {
            "must_have_skills": self._normalize_text_list(requirements.get("must_have_skills") or []),
            "nice_to_have_skills": self._normalize_text_list(requirements.get("nice_to_have_skills") or []),
            "questionable_skills": self._normalize_text_list(requirements.get("questionable_skills") or []),
        }

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

    def _redirect_response(self, status: HTTPStatus, location: str) -> None:
        self.send_response(status.value)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

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

    def _serve_static_directory(self, *, prefix: str, directory: Path, path: str) -> bool:
        relative_path = str(path or "")[len(prefix):]
        if not relative_path:
            relative_path = "index.html"
        root = directory.resolve()
        requested = (root / relative_path).resolve()
        if requested != root and root not in requested.parents:
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "static asset not found"})
            return True
        if requested.is_dir():
            requested = requested / "index.html"
        if not requested.is_file():
            self._json_response(HTTPStatus.NOT_FOUND, {"error": "static asset not found"})
            return True
        content_type, _ = mimetypes.guess_type(str(requested))
        self._binary_response(
            status=HTTPStatus.OK,
            content_type=content_type or "application/octet-stream",
            payload=requested.read_bytes(),
        )
        return True

    def _has_local_candidate_resume_asset(self, *, candidate_id: int, selected_url: str) -> bool:
        return self._candidate_local_resume_asset_path(candidate_id=int(candidate_id), selected_url=selected_url) is not None

    def _serve_local_candidate_resume_asset(self, *, candidate_id: int, selected_url: str) -> bool:
        match = self._candidate_local_resume_asset_path(candidate_id=int(candidate_id), selected_url=selected_url)
        if match is not None:
            storage_path, mime_type = match
            try:
                payload = storage_path.read_bytes()
            except OSError:
                return False
            content_type = self._resume_content_type(storage_path=storage_path, mime_type=mime_type)
            self._binary_response(
                status=HTTPStatus.OK,
                content_type=content_type,
                payload=payload,
                extra_headers={
                    "Content-Disposition": f"inline; filename=\"{storage_path.name}\"",
                    "Cache-Control": "no-store",
                },
            )
            return True
        return False

    def _candidate_local_resume_asset_path(self, *, candidate_id: int, selected_url: str) -> Optional[tuple[Path, Any]]:
        db = SERVICES.get("db")
        if db is None:
            return None
        try:
            rows = db.list_resume_assets_for_candidate(candidate_id=int(candidate_id), limit=500)
        except Exception:
            return None
        matching = [
            row
            for row in rows
            if str(row.get("remote_url") or "").strip() == str(selected_url or "").strip()
            and str(row.get("storage_path") or "").strip()
        ]
        matching.sort(
            key=lambda row: self._parse_iso_datetime(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        for row in matching:
            storage_path = self._validated_resume_storage_path(row.get("storage_path"))
            if storage_path is None:
                continue
            return storage_path, row.get("mime_type")
        return None

    @classmethod
    def _validated_resume_storage_path(cls, raw_path: Any) -> Optional[Path]:
        text = str(raw_path or "").strip()
        if not text:
            return None
        candidate = Path(text)
        try:
            resolved = candidate.resolve()
        except OSError:
            return None
        root_raw = str(os.environ.get("TENER_RESUME_STORAGE_DIR") or "data/resumes").strip()
        root = Path(root_raw)
        if not root.is_absolute():
            root = Path.cwd() / root
        try:
            allowed_root = root.resolve()
        except OSError:
            return None
        if resolved != allowed_root and allowed_root not in resolved.parents:
            return None
        if not resolved.is_file():
            return None
        return resolved

    @staticmethod
    def _resume_content_type(*, storage_path: Path, mime_type: Any) -> str:
        normalized = str(mime_type or "").strip().lower()
        if normalized:
            return normalized.split(";", 1)[0].strip() or "application/octet-stream"
        suffix = storage_path.suffix.lower()
        mapping = {
            ".pdf": "application/pdf",
            ".doc": "application/msword",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".txt": "text/plain; charset=utf-8",
            ".json": "application/json; charset=utf-8",
        }
        return mapping.get(suffix, "application/octet-stream")

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
    def _safe_float(value: Any, default: Optional[float]) -> Optional[float]:
        try:
            return float(value)
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

    def _request_ip_address(self) -> str:
        forwarded = str(self.headers.get("X-Forwarded-For") or "").strip()
        if forwarded:
            return forwarded.split(",", 1)[0].strip()
        if isinstance(self.client_address, tuple) and self.client_address:
            return str(self.client_address[0] or "").strip()
        return ""

    @staticmethod
    def _parse_iso_datetime(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @classmethod
    def _run_outreach_capacity_rebalance(
        cls,
        *,
        trigger: str,
        job_limit: int = 8,
        candidates_per_job: int = 25,
        recovery_per_job: int = 25,
        jobs_scan_limit: int = 40,
    ) -> Dict[str, Any]:
        workflow = SERVICES.get("workflow")
        if workflow is None:
            return {"status": "error", "reason": "workflow_unavailable", "trigger": trigger}
        try:
            result = workflow.rebalance_outreach_capacity(
                job_limit=job_limit,
                candidates_per_job=candidates_per_job,
                recovery_per_job=recovery_per_job,
                jobs_scan_limit=jobs_scan_limit,
            )
        except Exception as exc:
            return {"status": "error", "reason": "outreach_rebalance_failed", "details": str(exc), "trigger": trigger}
        if "trigger" not in result:
            result["trigger"] = trigger
        return result

    @classmethod
    def _build_outreach_ats_board(
        cls,
        *,
        db: Any,
        job_id: Optional[int],
        limit: int,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        safe_limit = max(50, min(int(limit or 600), 2000))
        scan_limit = None if job_id is not None else max(safe_limit, 200)
        items = db.list_outreach_ats_candidates(job_id=job_id, limit=scan_limit)
        if items:
            filtered_items: List[Dict[str, Any]] = []
            for item in items:
                verification_notes = item.get("verification_notes")
                include_item = not (
                    isinstance(verification_notes, dict)
                    and bool(verification_notes.get("forced_test_candidate"))
                )
                if include_item:
                    filtered_items.append(item)
            items = filtered_items
        total_items = len(items)
        display_items = items[:safe_limit]
        column_defs = [
            ("queued", "Queued"),
            ("connect_sent", "Connect Sent"),
            ("queued_delivery", "Queued for Delivery"),
            ("dialogue", "Dialogue"),
            ("cv_received", "CV Received"),
            ("interview_pending", "Interview Pending"),
            ("delivery_blocked", "Delivery Blocked"),
            ("completed", "Completed"),
        ]
        columns: List[Dict[str, Any]] = []
        summary = {
            "total_candidates": total_items,
            "queued": 0,
            "connect_sent": 0,
            "queued_delivery": 0,
            "dialogue": 0,
            "cv_received": 0,
            "interview_pending": 0,
            "delivery_blocked": 0,
            "completed": 0,
        }
        items_by_stage: Dict[str, List[Dict[str, Any]]] = {key: [] for key, _ in column_defs}
        stage_counts: Dict[str, int] = {key: 0 for key, _ in column_defs}

        for item in items:
            raw_stage_key = str(item.get("ats_stage_key") or "").strip().lower()
            stage_key = raw_stage_key
            if raw_stage_key in {"interview_passed", "interview_failed", "closed"}:
                stage_key = "completed"
            if stage_key not in items_by_stage:
                continue
            stage_counts[stage_key] += 1
            summary[stage_key] += 1

        for item in display_items:
            raw_stage_key = str(item.get("ats_stage_key") or "").strip().lower()
            stage_key = raw_stage_key
            if raw_stage_key in {"interview_passed", "interview_failed", "closed"}:
                stage_key = "completed"
            if stage_key not in items_by_stage:
                continue
            completion_label = None
            if raw_stage_key == "interview_passed":
                completion_label = "Passed"
            elif raw_stage_key == "interview_failed":
                completion_label = "Failed"
            elif raw_stage_key == "closed":
                completion_label = "Closed"
            card = {
                "candidate_id": int(item.get("candidate_id") or 0) or None,
                "candidate_name": str(item.get("full_name") or item.get("candidate_name") or "").strip()
                or f"Candidate {int(item.get('candidate_id') or 0)}",
                "job_id": int(item.get("job_id") or 0) or None,
                "job_title": str(item.get("job_title") or "").strip() or "-",
                "conversation_id": int(item.get("conversation_id") or 0) or None,
                "score": float(item.get("score") or 0.0) if item.get("score") is not None else None,
                "ats_stage_key": stage_key,
                "ats_stage_label": "Completed" if stage_key == "completed" else (
                    str(item.get("ats_stage_label") or "").strip() or stage_key.replace("_", " ").title()
                ),
                "stage_detail": str(item.get("ats_stage_detail") or "").strip() or None,
                "assigned_account_id": int(item.get("assigned_account_id") or 0) or None,
                "assigned_account_label": str(item.get("assigned_account_label") or "").strip() or None,
                "next_action_kind": str(item.get("next_action_kind") or "").strip() or None,
                "next_action_at": item.get("next_action_at"),
                "last_activity_at": item.get("last_activity_at"),
                "current_status_key": str(item.get("current_status_key") or "").strip() or None,
                "current_status_label": str(item.get("current_status_label") or "").strip() or None,
                "candidate_lifecycle_key": str(item.get("candidate_lifecycle_key") or "").strip() or None,
                "candidate_lifecycle_label": str(item.get("candidate_lifecycle_label") or "").strip() or None,
                "completion_label": completion_label,
                "raw_stage_key": raw_stage_key or None,
                "raw_stage_label": str(item.get("ats_stage_label") or "").strip() or None,
            }
            items_by_stage[stage_key].append(card)

        for stage_key, stage_label in column_defs:
            stage_items = list(items_by_stage.get(stage_key) or [])
            stage_items.sort(
                key=lambda item: (
                    str(item.get("last_activity_at") or item.get("next_action_at") or ""),
                    str(item.get("candidate_name") or ""),
                ),
                reverse=True,
            )
            columns.append(
                {
                    "key": stage_key,
                    "label": stage_label,
                    "count": int(stage_counts.get(stage_key) or 0),
                    "items": stage_items,
                }
            )

        return {
            "status": "ok",
            "generated_at": now.isoformat(),
            "job_id": int(job_id) if job_id else None,
            "displayed_candidates": len(display_items),
            "limited": len(display_items) < total_items,
            "summary": summary,
            "columns": columns,
        }

    @classmethod
    def _build_outreach_ops_report(
        cls,
        *,
        db: Any,
        job_id: Optional[int],
        logs_limit: int,
        chats_limit: int,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        last_hour = now - timedelta(hours=1)
        last_day = now - timedelta(hours=24)
        flow_window = now - timedelta(minutes=30)
        stale_cutoff = now - timedelta(minutes=OUTREACH_STALE_REPLY_MINUTES)

        accounts = db.list_linkedin_accounts(limit=500)
        conversations = db.list_conversations_overview(limit=chats_limit, job_id=job_id)
        logs = db.list_logs(limit=logs_limit)

        account_map: Dict[int, Dict[str, Any]] = {}
        for row in accounts or []:
            account_id = int(row.get("id") or 0)
            if account_id <= 0:
                continue
            account_map[account_id] = {
                "account_id": account_id,
                "label": str(row.get("label") or "").strip() or f"Account {account_id}",
                "provider_account_id": str(row.get("provider_account_id") or ""),
                "status": str(row.get("status") or "unknown"),
                "active_conversations": 0,
                "waiting_connection": 0,
                "awaiting_reply": 0,
                "stuck_threads": 0,
                "stuck_candidates": [],
                "sent_1h": 0,
                "sent_24h": 0,
                "failed_1h": 0,
                "failed_24h": 0,
                "last_send_at": None,
                "last_error_at": None,
                "last_error": "",
                "_last_send_dt": None,
                "_last_error_dt": None,
            }

        def _ensure_account(account_id: int) -> Dict[str, Any]:
            existing = account_map.get(account_id)
            if existing is not None:
                return existing
            fallback = {
                "account_id": account_id,
                "label": f"Account {account_id}",
                "provider_account_id": "",
                "status": "unknown",
                "active_conversations": 0,
                "waiting_connection": 0,
                "awaiting_reply": 0,
                "stuck_threads": 0,
                "stuck_candidates": [],
                "sent_1h": 0,
                "sent_24h": 0,
                "failed_1h": 0,
                "failed_24h": 0,
                "last_send_at": None,
                "last_error_at": None,
                "last_error": "",
                "_last_send_dt": None,
                "_last_error_dt": None,
            }
            account_map[account_id] = fallback
            return fallback

        flow_detected_recently = False
        recent_events: List[Dict[str, Any]] = []
        max_recent_events = 80
        conversation_account_map: Dict[int, int] = {}
        backlog_stuck_items: List[Dict[str, Any]] = []
        backlog_waiting_connection_items: List[Dict[str, Any]] = []
        seen_stuck_people: set[tuple[int, int, str]] = set()
        seen_stuck_people_by_account: Dict[int, set[tuple[int, int, str]]] = {}

        def _stuck_person_key(*, account_id: int, candidate_id: int, job_id: int, candidate_name: str) -> tuple[int, int, str]:
            if candidate_id > 0:
                return (account_id, job_id, f"candidate:{candidate_id}")
            normalized_name = re.sub(r"\s+", " ", str(candidate_name or "").strip().lower())
            return (account_id, job_id, f"name:{normalized_name or '-'}")

        def _classify_account_issue(error_text: str) -> str | None:
            lowered = str(error_text or "").strip().lower()
            if not lowered:
                return None
            if "subscription_required" in lowered:
                return "blocked_subscription"
            if "account not found" in lowered:
                return "removed"
            if "limit_exceeded" in lowered or "rate limit" in lowered or "usage limit" in lowered:
                return "rate_limited"
            return None

        def _queue_reason_label(reason: str) -> str:
            mapping = {
                "new_thread": "New outreach",
                "recovery": "Recovery backlog",
                "waiting_connection": "Connect sent",
                "stuck_reply": "Stuck reply",
            }
            return mapping.get(str(reason or "").strip().lower(), str(reason or "").replace("_", " ").strip().title() or "Unknown")

        for item in logs or []:
            operation = str(item.get("operation") or "").strip().lower()
            status = str(item.get("status") or "").strip().lower()
            created_at = str(item.get("created_at") or "")
            created_dt = cls._parse_iso_datetime(created_at)
            details = item.get("details") if isinstance(item.get("details"), dict) else {}
            account_id = int(details.get("linkedin_account_id") or 0)
            if account_id <= 0 and str(item.get("entity_type") or "") == "linkedin_account":
                account_id = cls._safe_int(item.get("entity_id"), 0) or 0
            entry = _ensure_account(account_id) if account_id > 0 else None

            if operation == "scheduler.outreach.dispatch" and status == "ok" and created_dt and created_dt >= flow_window:
                flow_detected_recently = True
            if (
                operation == "scheduler.outreach.poll_connections"
                and status == "ok"
                and created_dt
                and created_dt >= flow_window
                and (int(details.get("connected") or 0) > 0 or int(details.get("sent") or 0) > 0)
            ):
                flow_detected_recently = True

            if operation == "agent.outreach.send":
                delivery_status = str(details.get("delivery_status") or "").strip().lower()
                is_send_ok = status == "ok" and delivery_status in {"sent", "pending_connection"}
                is_send_failed = not is_send_ok
                if str(item.get("entity_type") or "") == "conversation":
                    conversation_id = cls._safe_int(item.get("entity_id"), 0) or 0
                    if conversation_id > 0 and account_id > 0:
                        conversation_account_map[int(conversation_id)] = int(account_id)
                if is_send_ok and created_dt and created_dt >= flow_window:
                    flow_detected_recently = True
                if entry is not None and created_dt:
                    if created_dt >= last_hour:
                        if is_send_ok:
                            entry["sent_1h"] += 1
                        elif is_send_failed:
                            entry["failed_1h"] += 1
                    if created_dt >= last_day:
                        if is_send_ok:
                            entry["sent_24h"] += 1
                        elif is_send_failed:
                            entry["failed_24h"] += 1
                    if is_send_ok:
                        current_dt = entry.get("_last_send_dt")
                        if current_dt is None or created_dt > current_dt:
                            entry["_last_send_dt"] = created_dt
                            entry["last_send_at"] = created_dt.isoformat()
                    if is_send_failed:
                        err_text = (
                            str((details.get("delivery") or {}).get("error") or "").strip()
                            or str((details.get("connect_request") or {}).get("error") or "").strip()
                            or str(details.get("error") or "").strip()
                            or str(details.get("delivery_status") or "").strip()
                            or "delivery_failed"
                        )
                        current_dt = entry.get("_last_error_dt")
                        if current_dt is None or created_dt > current_dt:
                            entry["_last_error_dt"] = created_dt
                            entry["last_error_at"] = created_dt.isoformat()
                            entry["last_error"] = err_text
                            entry["dispatch_issue"] = _classify_account_issue(err_text)

            if operation in {
                "agent.outreach.send",
                "agent.outreach.delivery_error",
                "agent.outreach.dispatch",
                "scheduler.outreach.dispatch",
                "scheduler.outreach.poll_connections",
                "scheduler.outreach.rebalance",
            } and len(recent_events) < max_recent_events:
                recent_events.append(
                    {
                        "id": int(item.get("id") or 0),
                        "created_at": created_at,
                        "operation": operation,
                        "status": status,
                        "account_id": account_id or None,
                        "entity_type": item.get("entity_type"),
                        "entity_id": item.get("entity_id"),
                        "delivery_status": details.get("delivery_status"),
                        "error": (
                            str((details.get("delivery") or {}).get("error") or "").strip()
                            or str(details.get("error") or "").strip()
                            or None
                        ),
                    }
                )

        for row in conversations or []:
            conversation_id = int(row.get("conversation_id") or 0)
            account_id = int(row.get("linkedin_account_id") or 0)
            if account_id <= 0 and conversation_id > 0:
                account_id = int(conversation_account_map.get(conversation_id) or 0)
            conversation_status = str(row.get("conversation_status") or "").strip().lower()
            pre_resume_status = str(row.get("pre_resume_status") or "").strip().lower()
            last_message_dt = cls._parse_iso_datetime(row.get("last_message_at"))
            candidate_name = str(row.get("candidate_name") or "").strip() or f"Candidate {int(row.get('candidate_id') or 0)}"
            queue_base = {
                "conversation_id": conversation_id,
                "candidate_id": int(row.get("candidate_id") or 0),
                "candidate_name": candidate_name,
                "job_id": int(row.get("job_id") or 0),
                "job_title": str(row.get("job_title") or "").strip() or "-",
                "linkedin_account_id": account_id or None,
                "assigned_account_id": account_id or None,
                "assigned_account_label": _ensure_account(account_id).get("label") if account_id > 0 else None,
                "last_message_at": row.get("last_message_at"),
            }
            if account_id > 0:
                entry = _ensure_account(account_id)
                entry["active_conversations"] += 1
            else:
                entry = None
            if conversation_status == "waiting_connection":
                if entry is not None:
                    entry["waiting_connection"] += 1
                backlog_waiting_connection_items.append(
                    {
                        **queue_base,
                        "queue_type": "waiting_connection",
                        "queue_reason": "waiting_connection",
                        "queue_reason_label": _queue_reason_label("waiting_connection"),
                        "planned_action_kind": "message",
                        "planned_action_label": "Message after acceptance",
                        "status": "waiting_connection",
                    }
                )
            if pre_resume_status == "awaiting_reply":
                if entry is not None:
                    entry["awaiting_reply"] += 1
                if last_message_dt and last_message_dt <= stale_cutoff:
                    stuck_key = _stuck_person_key(
                        account_id=account_id,
                        candidate_id=int(row.get("candidate_id") or 0),
                        job_id=int(row.get("job_id") or 0),
                        candidate_name=candidate_name,
                    )
                    stuck_item = {
                        **queue_base,
                        "queue_type": "stuck_reply",
                        "queue_reason": "stuck_reply",
                        "queue_reason_label": _queue_reason_label("stuck_reply"),
                        "planned_action_kind": "message",
                        "planned_action_label": "Follow up message",
                        "status": "awaiting_reply",
                    }
                    if stuck_key not in seen_stuck_people:
                        backlog_stuck_items.append(stuck_item)
                        seen_stuck_people.add(stuck_key)
                    if entry is not None:
                        account_seen = seen_stuck_people_by_account.setdefault(account_id, set())
                        if stuck_key in account_seen:
                            continue
                        account_seen.add(stuck_key)
                        entry["stuck_threads"] += 1
                        if len(entry["stuck_candidates"]) < 10:
                            entry["stuck_candidates"].append(stuck_item)

        severity = {"ok": 0, "warning": 1, "critical": 2, "paused": 0}
        rows: List[Dict[str, Any]] = []
        for row in account_map.values():
            account_status = str(row.get("status") or "").strip().lower()
            delivery_health = "ok"
            backlog_health = "ok"
            dispatch_issue = str(row.get("dispatch_issue") or "").strip().lower()
            dispatch_state = "ready"
            dispatch_state_label = "Ready"
            if account_status == "removed" or dispatch_issue == "removed":
                dispatch_state = "removed"
                dispatch_state_label = "Removed from provider"
                delivery_health = "critical"
                backlog_health = "paused"
            elif account_status not in {"connected", "active"}:
                dispatch_state = "inactive"
                dispatch_state_label = "Inactive"
                delivery_health = "paused"
                backlog_health = "paused"
            elif dispatch_issue == "blocked_subscription":
                dispatch_state = "blocked_subscription"
                dispatch_state_label = "Blocked by subscription"
                delivery_health = "critical"
            elif dispatch_issue == "rate_limited":
                dispatch_state = "rate_limited"
                dispatch_state_label = "Rate limited"
                delivery_health = "warning"
            elif (
                int(row.get("active_conversations") or 0) == 0
                and int(row.get("sent_24h") or 0) == 0
                and int(row.get("failed_24h") or 0) == 0
                and not row.get("last_send_at")
            ):
                dispatch_state = "idle_unverified"
                dispatch_state_label = "Connected, unverified"
                delivery_health = "warning"
            elif int(row.get("failed_1h") or 0) >= 2:
                delivery_health = "critical"
            elif int(row.get("failed_24h") or 0) > 0:
                delivery_health = "warning"
            elif int(row.get("active_conversations") or 0) > 0 and not row.get("last_send_at"):
                delivery_health = "warning"
            if account_status in {"connected", "active"} and int(row.get("stuck_threads") or 0) > 0:
                backlog_health = "warning"
            health = delivery_health
            if delivery_health != "critical" and backlog_health == "warning":
                health = "warning"
            row["health"] = health
            row["delivery_health"] = delivery_health
            row["backlog_health"] = backlog_health
            row["connection_status"] = account_status or "unknown"
            row["dispatch_state"] = dispatch_state
            row["dispatch_state_label"] = dispatch_state_label
            row.pop("_last_send_dt", None)
            row.pop("_last_error_dt", None)
            if dispatch_state != "removed":
                rows.append(row)

        rows.sort(
            key=lambda item: (
                -severity.get(str(item.get("health") or "ok"), 0),
                -int(item.get("stuck_threads") or 0),
                -int(item.get("failed_1h") or 0),
                -int(item.get("sent_24h") or 0),
                int(item.get("account_id") or 0),
            )
        )

        funnel_by_account = db.summarize_outreach_account_funnel(
            account_ids=[int(item.get("account_id") or 0) for item in rows],
            recent_limit=5,
        )
        for row in rows:
            funnel = funnel_by_account.get(int(row.get("account_id") or 0)) or {}
            row["funnel"] = {
                "connects_planned": int(funnel.get("connects_planned") or 0),
                "connects_sent": int(funnel.get("connects_sent") or 0),
                "connects_accepted": int(funnel.get("connects_accepted") or 0),
                "messages_planned": int(funnel.get("messages_planned") or 0),
                "messages_sent": int(funnel.get("messages_sent") or 0),
                "replies_received": int(funnel.get("replies_received") or 0),
                "resumes_received": int(funnel.get("resumes_received") or 0),
            }
            row["recent_funnel_candidates"] = funnel.get("recent_candidates") or []

        sent_1h_total = sum(int(item.get("sent_1h") or 0) for item in rows)
        sent_24h_total = sum(int(item.get("sent_24h") or 0) for item in rows)
        failed_1h_total = sum(int(item.get("failed_1h") or 0) for item in rows)
        failed_24h_total = sum(int(item.get("failed_24h") or 0) for item in rows)
        stuck_threads_total = sum(int(item.get("stuck_threads") or 0) for item in rows)
        waiting_connection_total = sum(int(item.get("waiting_connection") or 0) for item in rows)
        awaiting_reply_total = sum(int(item.get("awaiting_reply") or 0) for item in rows)
        active_conversations_total = sum(int(item.get("active_conversations") or 0) for item in rows)
        active_accounts_total = sum(1 for item in rows if int(item.get("active_conversations") or 0) > 0)
        connected_accounts_total = sum(
            1 for item in rows if str(item.get("status") or "").strip().lower() in {"connected", "active"}
        )
        last_send_candidates = [str(item.get("last_send_at") or "") for item in rows if item.get("last_send_at")]
        last_send_at = max(last_send_candidates) if last_send_candidates else None

        delivery_health = "ok"
        delivery_issues: List[str] = []
        if failed_1h_total >= 5:
            delivery_health = "critical"
            delivery_issues.append(f"{failed_1h_total} failed send(s) in last hour")
        elif failed_24h_total > 0:
            delivery_health = "warning"
            delivery_issues.append(f"{failed_24h_total} failed send(s) in last 24h")
        if active_conversations_total > 0 and not flow_detected_recently:
            if delivery_health == "ok":
                delivery_health = "warning"
            delivery_issues.append("no successful outreach activity in last 30 minutes")

        backlog_health = "ok"
        backlog_issues: List[str] = []
        if stuck_threads_total > 0:
            backlog_health = "warning"
            backlog_issues.append(f"{stuck_threads_total} stuck thread(s)")

        overall_health = "critical" if delivery_health == "critical" else "warning" if (
            delivery_health == "warning" or backlog_health == "warning"
        ) else "ok"

        backlog_rows: List[Dict[str, Any]] = []
        backlog_summary = {
            "new_threads": 0,
            "unassigned_recovery": 0,
            "waiting_connection": len(backlog_waiting_connection_items),
            "stuck_replies": len(backlog_stuck_items),
            "selected_jobs": 0,
        }
        backlog_jobs: List[Dict[str, Any]] = []
        backlog_job_limit = 8
        backlog_job_scan_limit = 40
        backlog_items_per_job = 25
        selected_jobs_payload: List[Dict[str, Any]] = []
        if job_id:
            job = db.get_job(int(job_id))
            if job and not bool(job.get("is_archived")):
                selected_jobs_payload = [job]
        else:
            selected_jobs_payload = db.list_jobs(limit=backlog_job_scan_limit)

        workflow = SERVICES.get("workflow")
        for job in selected_jobs_payload:
            row_job_id = int(job.get("id") or 0)
            if row_job_id <= 0:
                continue
            routing_mode = str(job.get("linkedin_routing_mode") or "auto").strip().lower()
            if job_id is None and routing_mode != "auto":
                continue
            new_thread_candidates = [
                item
                for item in db.list_job_outreach_candidates(job_id=row_job_id, limit=backlog_items_per_job * 2)
                if str(item.get("current_status_key") or "").strip().lower() == "added"
            ]
            recovery_candidates = db.list_unassigned_outreach_conversations(
                limit=backlog_items_per_job * 2,
                job_id=row_job_id,
            )
            if job_id is None and not new_thread_candidates and not recovery_candidates:
                continue
            backlog_jobs.append(
                {
                    "job_id": row_job_id,
                    "job_title": str(job.get("title") or "").strip() or "-",
                    "new_thread_backlog": len(new_thread_candidates),
                    "recovery_backlog": len(recovery_candidates),
                }
            )
            job_backlog_rows: List[Dict[str, Any]] = []
            for item in new_thread_candidates[:backlog_items_per_job]:
                job_backlog_rows.append(
                    {
                        "queue_type": "new_thread",
                        "queue_reason": "new_thread",
                        "queue_reason_label": _queue_reason_label("new_thread"),
                        "job_id": row_job_id,
                        "job_title": str(item.get("job_title") or "").strip() or "-",
                        "candidate_id": int(item.get("candidate_id") or 0),
                        "candidate_name": str(item.get("full_name") or "").strip() or f"Candidate {int(item.get('candidate_id') or 0)}",
                        "conversation_id": int(item.get("conversation_id") or 0) or None,
                        "status": "queued",
                        "score": float(item.get("score") or 0.0),
                        "last_message_at": item.get("last_message_created_at") or item.get("last_message_at"),
                        "linkedin_account_id": int(item.get("linkedin_account_id") or 0) or None,
                        "assigned_account_id": None,
                        "assigned_account_label": None,
                        "planned_action_kind": str(item.get("planned_action_kind") or "connect_request"),
                        "planned_action_label": str(item.get("planned_action_label") or "Connect planned"),
                    }
                )
            for item in recovery_candidates[:backlog_items_per_job]:
                job_backlog_rows.append(
                    {
                        "queue_type": "recovery",
                        "queue_reason": "recovery",
                        "queue_reason_label": _queue_reason_label("recovery"),
                        "job_id": row_job_id,
                        "job_title": str(item.get("job_title") or "").strip() or "-",
                        "candidate_id": int(item.get("candidate_id") or 0),
                        "candidate_name": str(item.get("candidate_name") or "").strip()
                        or f"Candidate {int(item.get('candidate_id') or 0)}",
                        "conversation_id": int(item.get("conversation_id") or 0) or None,
                        "status": "recovery",
                        "score": None,
                        "last_message_at": item.get("last_message_at"),
                        "linkedin_account_id": None,
                        "assigned_account_id": None,
                        "assigned_account_label": None,
                        "planned_action_kind": "message",
                        "planned_action_label": "Recovery message planned",
                    }
                )
            for item in job_backlog_rows:
                backlog_rows.append(item)
            backlog_summary["new_threads"] += len(new_thread_candidates)
            backlog_summary["unassigned_recovery"] += len(recovery_candidates)
            if len(backlog_jobs) >= backlog_job_limit:
                break

        backlog_summary["selected_jobs"] = len(backlog_jobs)
        backlog_rows.extend(backlog_waiting_connection_items[:50])
        backlog_rows.extend(backlog_stuck_items[:50])

        return {
            "status": "ok",
            "generated_at": now.isoformat(),
            "job_id": int(job_id) if job_id else None,
            "thresholds": {
                "stale_minutes": OUTREACH_STALE_REPLY_MINUTES,
                "flow_window_minutes": 30,
                "logs_limit": logs_limit,
                "chats_limit": chats_limit,
            },
            "summary": {
                "health": overall_health,
                "delivery_health": delivery_health,
                "backlog_health": backlog_health,
                "issues": delivery_issues + backlog_issues,
                "delivery_issues": delivery_issues,
                "backlog_issues": backlog_issues,
                "accounts_total": len(rows),
                "connected_accounts": connected_accounts_total,
                "active_accounts": active_accounts_total,
                "active_conversations": active_conversations_total,
                "sent_1h": sent_1h_total,
                "sent_24h": sent_24h_total,
                "failed_1h": failed_1h_total,
                "failed_24h": failed_24h_total,
                "stuck_threads": stuck_threads_total,
                "waiting_connection": waiting_connection_total,
                "awaiting_reply": awaiting_reply_total,
                "flow_detected_recently": flow_detected_recently,
                "last_successful_send_at": last_send_at,
            },
            "accounts": rows,
            "backlog": {
                "summary": backlog_summary,
                "jobs": backlog_jobs,
                "items": backlog_rows[:200],
            },
            "events": recent_events,
        }

    @staticmethod
    def _read_db() -> Any:
        db = SERVICES.get("read_db")
        if db is None:
            db = SERVICES["db"]
        return db

    @staticmethod
    def _switch_read_source(*, source: str, postgres_dsn: str = "", reason: str = "manual_switch") -> Dict[str, Any]:
        normalized = str(source or "").strip().lower()
        runtime_mode = str(SERVICES.get("db_runtime_mode") or "").strip().lower()
        if runtime_mode == "postgres_primary":
            if normalized == "sqlite":
                SERVICES["db_read_status"] = {
                    "status": "ok",
                    "source": "postgres",
                    "requested_source": "runtime",
                    "reason": "postgres_runtime_primary",
                }
                SERVICES["read_db"] = SERVICES.get("db")
                return {
                    "status": "skipped",
                    "source": "postgres",
                    "reason": "postgres_runtime_primary",
                    "db_read_status": SERVICES.get("db_read_status"),
                }
            if normalized == "postgres":
                SERVICES["read_db"] = SERVICES.get("db")
                dsn = str(postgres_dsn or SERVICES.get("postgres_dsn") or os.environ.get("TENER_DB_DSN", "") or "").strip()
                if dsn:
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
        sqlite_tables = list_sqlite_user_tables(sqlite_path) if sqlite_exists else []
        backfill_tables = list(TABLE_ORDER)
        backfill_table_set = set(backfill_tables)
        missing_backfill_tables = [name for name in sqlite_tables if name not in backfill_table_set]
        checks: Dict[str, Any] = {
            "sqlite_exists": sqlite_exists,
            "sqlite_tables_total": len(sqlite_tables),
            "sqlite_tables": sqlite_tables,
            "backfill_tables_total": len(backfill_tables),
            "backfill_tables": backfill_tables,
            "backfill_missing_tables": missing_backfill_tables,
            "backfill_complete": len(missing_backfill_tables) == 0,
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
            and bool(checks.get("backfill_complete"))
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
    def _agents_office_fixture_jobs() -> List[Dict[str, Any]]:
        path = project_root() / "AgentsOffice-prototype" / "demo-jobs.json"
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        items = payload.get("items") if isinstance(payload, dict) else []
        return items if isinstance(items, list) else []

    @staticmethod
    def _agents_office_phase_label(
        *,
        steps: List[Dict[str, Any]],
        market_leads: int,
        live_threads: int,
        buyer_finalists: int,
        fallback_label: Optional[str],
    ) -> str:
        labels = {
            "source": "Scanning the market and stacking aligned leads.",
            "enrich": "Enriching raw profiles before they hit the board.",
            "verify": "Filtering the strongest market signal for the buyer.",
            "add": "Promoting aligned candidates into the buyer board.",
            "outreach": "Opening live candidate threads and warming replies.",
            "faq": "Handling candidate questions while outreach keeps moving.",
            "pre_resume": "Keeping active conversations moving toward resumes.",
            "interview_invite": "Packaging interview-ready finalists for review.",
        }
        if steps:
            active = next(
                (
                    row for row in steps
                    if str(row.get("status") or "").strip().lower() not in {"success", "complete", "completed", "idle"}
                ),
                steps[0],
            )
            step = str(active.get("step") or "").strip().lower()
            status = str(active.get("status") or "").strip().lower()
            if status in {"error", "failed"} and step:
                return f"Recovering from a {step} blocker while the office reroutes."
            if step in labels:
                return labels[step]
        if buyer_finalists > 0 and live_threads > 0:
            return "Keeping finalist conversations warm while the buyer board stays fresh."
        if live_threads > 0:
            return "Opening and maintaining live candidate threads."
        if market_leads > 0:
            return "Scanning the market and shaping the first qualified signals."
        return str(fallback_label or "").strip() or "The office is actively moving this search."

    def _build_agents_office_demo_jobs(self, *, limit: int) -> Dict[str, Any]:
        safe_limit = max(1, min(int(limit or 8), 12))
        fixture_items = self._agents_office_fixture_jobs()
        try:
            read_db = self._read_db()
        except Exception:
            read_db = None
        if read_db is None:
            return {"items": fixture_items[:safe_limit], "source": "fixture"}
        try:
            jobs = read_db.list_jobs(limit=safe_limit)
        except Exception:
            jobs = []
        if not jobs:
            return {"items": fixture_items[:safe_limit], "source": "fixture"}

        shortlist_statuses = {
            "cv_received",
            "interview_invited",
            "interview_in_progress",
            "interview_completed",
            "interview_scored",
            "interview_passed",
        }

        items: List[Dict[str, Any]] = []
        for index, job in enumerate(jobs[:safe_limit]):
            job_id = int(job.get("id") or 0)
            if job_id <= 0:
                continue
            fallback = fixture_items[index % len(fixture_items)] if fixture_items else {}
            try:
                candidate_rows = read_db.list_candidates_for_job(job_id)
            except Exception:
                candidate_rows = []
            try:
                steps = read_db.list_job_step_progress(job_id=job_id)
            except Exception:
                steps = []

            market_leads = len(candidate_rows)
            live_threads = sum(1 for row in candidate_rows if int(row.get("conversation_id") or 0) > 0)

            sorted_rows = sorted(
                candidate_rows,
                key=lambda row: float(row.get("score") or 0.0),
                reverse=True,
            )
            candidate_signals: List[Dict[str, Any]] = []
            for row in sorted_rows[:3]:
                headline = str(row.get("headline") or "").strip() or "Qualified market signal"
                location = str(row.get("location") or job.get("location") or fallback.get("location") or "").strip()
                score = max(1, min(int(round(float(row.get("score") or 0.0))) if row.get("score") is not None else 0, 99))
                note = str(row.get("current_status_label") or "").strip() or "Strong market signal"
                candidate_signals.append(
                    {
                        "headline": headline,
                        "location": location,
                        "score": score or 90,
                        "note": note,
                    }
                )

            buyer_finalists = sum(
                1
                for row in candidate_rows
                if str(row.get("current_status_key") or "").strip().lower() in shortlist_statuses
            )
            if candidate_signals:
                buyer_finalists = max(1 if buyer_finalists <= 0 else buyer_finalists, min(len(candidate_signals), 3))

            if market_leads <= 0:
                market_leads = int(fallback.get("market_leads") or 0)
            if live_threads <= 0:
                live_threads = int(fallback.get("live_threads") or 0)
            if buyer_finalists <= 0:
                buyer_finalists = int(fallback.get("buyer_finalists") or 0)
            if not candidate_signals and isinstance(fallback, dict):
                fallback_signals = fallback.get("candidate_signals")
                if isinstance(fallback_signals, list):
                    candidate_signals = fallback_signals[:3]

            title = str(job.get("title") or fallback.get("title") or "Field Role").strip() or "Field Role"
            location = str(job.get("location") or fallback.get("location") or "").strip()
            company = str(job.get("company") or fallback.get("company") or "Tener Buyer").strip() or "Tener Buyer"
            market = str(fallback.get("market") or "Talent search").strip() or "Talent search"
            summary = str(fallback.get("summary") or "").strip()
            if not summary:
                location_tail = f" in {location}" if location else ""
                summary = (
                    f"Buyer search for {title}{location_tail}, with sourcing, fit review and outreach moving in parallel."
                )

            items.append(
                {
                    "id": str(job_id),
                    "company": company,
                    "title": title,
                    "location": location or str(fallback.get("location") or "").strip(),
                    "market": market,
                    "summary": summary,
                    "phase_label": self._agents_office_phase_label(
                        steps=steps,
                        market_leads=market_leads,
                        live_threads=live_threads,
                        buyer_finalists=buyer_finalists,
                        fallback_label=str(fallback.get("phase_label") or "").strip() or None,
                    ),
                    "channels": list(fallback.get("channels") or []),
                    "market_leads": market_leads,
                    "live_threads": live_threads,
                    "buyer_finalists": min(max(buyer_finalists, 0), 3),
                    "candidate_signals": candidate_signals[:3],
                }
            )

        if items:
            return {"items": items, "source": "db"}
        return {"items": fixture_items[:safe_limit], "source": "fixture"}

    @staticmethod
    def _is_public_path(*, method: str, path: str) -> bool:
        normalized = str(path or "").strip()
        if normalized in {
            "/",
            "/dashboard",
            "/dashboard/emulator",
            "/dashboard/signals-live",
            "/health",
            "/api",
            "/landing",
            "/landing/",
            "/favicon.ico",
            "/favicon.png",
        }:
            return True
        if normalized == "/zalando" or normalized.startswith("/zalando/"):
            return True
        if normalized == "/liveramp" or normalized.startswith("/liveramp/"):
            return True
        if normalized == "/skilled-trades" or normalized.startswith("/skilled-trades/"):
            return True
        if normalized == "/agents-office" or normalized.startswith("/agents-office/"):
            return True
        if normalized == "/api/demo/agents-office/jobs":
            return True
        if normalized.startswith("/candidate/"):
            return True
        if method.upper() == "POST" and normalized == "/api/webhooks/unipile":
            return True
        if method.upper() == "POST" and normalized in {"/api/landing/newsletter", "/api/landing/contact"}:
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
        values: List[Any] = []
        for path in paths:
            value = TenerRequestHandler._get_nested(payload, path)
            values.append(value)
        descriptors = extract_attachment_descriptors_from_values(values, limit=12)
        return descriptors_to_text(descriptors, limit=12)

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

    if env_bool("TENER_OUTREACH_CONNECTION_POLL_SCHEDULER_ENABLED", True):
        connection_poll_interval_seconds = max(
            15,
            int(os.environ.get("TENER_OUTREACH_CONNECTION_POLL_INTERVAL_SECONDS", "45")),
        )
        connection_poll_limit = max(
            1,
            int(os.environ.get("TENER_OUTREACH_CONNECTION_POLL_LIMIT", "200")),
        )
        if scheduler_stop is None:
            scheduler_stop = threading.Event()

        def _outreach_connection_poll_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    _run_outreach_connection_poll_scheduler_tick(poll_limit=connection_poll_limit)
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.outreach.poll_connections",
                        status="error",
                        entity_type="scheduler",
                        entity_id="outreach_connection_poll",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(connection_poll_interval_seconds)

        threading.Thread(
            target=_outreach_connection_poll_loop,
            daemon=True,
            name="outreach-connection-poll-scheduler",
        ).start()
        print(f"Outreach connection poll scheduler enabled: every {connection_poll_interval_seconds}s")

    if env_bool("TENER_OUTREACH_REBALANCE_SCHEDULER_ENABLED", True):
        rebalance_interval_seconds = max(30, int(os.environ.get("TENER_OUTREACH_REBALANCE_INTERVAL_SECONDS", "90")))
        rebalance_job_limit = max(1, int(os.environ.get("TENER_OUTREACH_REBALANCE_JOB_LIMIT", "8")))
        rebalance_candidates_per_job = max(1, int(os.environ.get("TENER_OUTREACH_REBALANCE_CANDIDATES_PER_JOB", "25")))
        rebalance_recovery_per_job = max(1, int(os.environ.get("TENER_OUTREACH_REBALANCE_RECOVERY_PER_JOB", "25")))
        rebalance_scan_limit = max(rebalance_job_limit, int(os.environ.get("TENER_OUTREACH_REBALANCE_SCAN_LIMIT", "40")))
        if scheduler_stop is None:
            scheduler_stop = threading.Event()

        def _outreach_rebalance_loop() -> None:
            while not scheduler_stop.is_set():
                try:
                    result = TenerRequestHandler._run_outreach_capacity_rebalance(
                        trigger="scheduler_outreach_rebalance",
                        job_limit=rebalance_job_limit,
                        candidates_per_job=rebalance_candidates_per_job,
                        recovery_per_job=rebalance_recovery_per_job,
                        jobs_scan_limit=rebalance_scan_limit,
                    )
                    totals = result.get("totals") if isinstance(result.get("totals"), dict) else {}
                    if int(totals.get("new_threads_queued") or 0) > 0 or int(totals.get("recovery_queued") or 0) > 0:
                        SERVICES["db"].log_operation(
                            operation="scheduler.outreach.rebalance",
                            status="ok",
                            entity_type="scheduler",
                            entity_id="outreach_rebalance",
                            details=result,
                        )
                except Exception as exc:
                    SERVICES["db"].log_operation(
                        operation="scheduler.outreach.rebalance",
                        status="error",
                        entity_type="scheduler",
                        entity_id="outreach_rebalance",
                        details={"error": str(exc)},
                    )
                scheduler_stop.wait(rebalance_interval_seconds)

        threading.Thread(target=_outreach_rebalance_loop, daemon=True, name="outreach-rebalance-scheduler").start()
        print(f"Outreach rebalance scheduler enabled: every {rebalance_interval_seconds}s")

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
