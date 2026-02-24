from __future__ import annotations

import json
import hashlib
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database
from .instructions import AgentEvaluationPlaybook, AgentInstructions
from .llm_responder import CandidateLLMResponder
from .linkedin_provider import build_linkedin_provider
from .matching import MatchingEngine
from .pre_resume_service import PreResumeCommunicationService
from .workflow import WorkflowService


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


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
    }
    services["pre_resume"].instruction = instructions.get("pre_resume")


def build_services() -> Dict[str, Any]:
    root = project_root()
    local_db_path = str(root / "runtime" / "tener_v1.sqlite3")
    default_db_path = "/var/data/tener_v1.sqlite3" if os.environ.get("RENDER") else local_db_path
    configured_db_path = os.environ.get("TENER_DB_PATH", default_db_path)
    db_path = configured_db_path
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
    mock_profiles_path = os.environ.get("TENER_MOCK_LINKEDIN_DATA_PATH", str(root / "data" / "mock_linkedin_profiles.json"))
    forced_test_ids_path = os.environ.get(
        "TENER_FORCED_TEST_IDS_PATH",
        str(root / "config" / "forced_test_linkedin_ids.txt"),
    )
    forced_test_score_raw = os.environ.get("TENER_FORCED_TEST_SCORE", "0.99")
    try:
        forced_test_score = float(forced_test_score_raw)
    except ValueError:
        forced_test_score = 0.99

    try:
        db = Database(db_path=db_path)
    except Exception:
        if db_path != local_db_path:
            db = Database(db_path=local_db_path)
        else:
            raise
    db.init_schema()

    instructions = AgentInstructions(path=instructions_path)
    evaluation_playbook = AgentEvaluationPlaybook(path=evaluation_playbook_path)
    matching_engine = MatchingEngine(rules_path=rules_path)
    linkedin_provider = build_linkedin_provider(mock_dataset_path=mock_profiles_path)

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
    pre_resume_service = PreResumeCommunicationService(
        templates_path=templates_path,
        max_followups=max_followups,
        followup_delays_hours=followup_delays or None,
        instruction=instructions.get("pre_resume"),
    )
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

    workflow = WorkflowService(
        db=db,
        sourcing_agent=sourcing_agent,
        verification_agent=verification_agent,
        outreach_agent=outreach_agent,
        faq_agent=faq_agent,
        pre_resume_service=pre_resume_service,
        llm_responder=llm_responder,
        agent_evaluation_playbook=evaluation_playbook,
        contact_all_mode=env_bool("TENER_CONTACT_ALL_MODE", True),
        require_resume_before_final_verify=env_bool("TENER_REQUIRE_RESUME_BEFORE_FINAL_VERIFY", True),
        forced_test_ids_path=forced_test_ids_path,
        forced_test_score=forced_test_score,
        stage_instructions={
            "sourcing": instructions.get("sourcing"),
            "enrich": instructions.get("enrich"),
            "verification": instructions.get("verification"),
            "add": instructions.get("add"),
            "outreach": instructions.get("outreach"),
            "faq": instructions.get("faq"),
            "pre_resume": instructions.get("pre_resume"),
        },
    )

    services = {
        "db": db,
        "instructions": instructions,
        "evaluation_playbook": evaluation_playbook,
        "matching_engine": matching_engine,
        "pre_resume": pre_resume_service,
        "workflow": workflow,
    }
    apply_agent_instructions(services)
    return services


SERVICES = build_services()


class TenerRequestHandler(BaseHTTPRequestHandler):
    server_version = "TenerAIV1/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path in {"/", "/dashboard"}:
            dashboard = project_root() / "src" / "tener_ai" / "static" / "dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
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
                        "update_job_jd": "POST /api/jobs/{job_id}/jd",
                        "run_workflow": "POST /api/workflows/execute",
                        "source_step": "POST /api/steps/source",
                        "enrich_step": "POST /api/steps/enrich",
                        "verify_step": "POST /api/steps/verify",
                        "add_step": "POST /api/steps/add",
                        "outreach_step": "POST /api/steps/outreach",
                        "outreach_poll_connections": "POST /api/outreach/poll-connections",
                        "inbound_poll": "POST /api/inbound/poll",
                        "instructions": "GET /api/instructions",
                        "agent_system": "GET /api/agent-system",
                        "reload_instructions": "POST /api/instructions/reload",
                        "pre_resume_start": "POST /api/pre-resume/sessions/start",
                        "pre_resume_list": "GET /api/pre-resume/sessions?limit=100&status=awaiting_reply",
                        "pre_resume_get": "GET /api/pre-resume/sessions/{session_id}",
                        "pre_resume_events": "GET /api/pre-resume/events?limit=200",
                        "pre_resume_inbound": "POST /api/pre-resume/sessions/{session_id}/inbound",
                        "pre_resume_followup": "POST /api/pre-resume/sessions/{session_id}/followup",
                        "pre_resume_followups_run": "POST /api/pre-resume/followups/run",
                        "pre_resume_unreachable": "POST /api/pre-resume/sessions/{session_id}/unreachable",
                        "conversation_messages": "GET /api/conversations/{conversation_id}/messages",
                        "chats_overview": "GET /api/chats/overview?limit=200",
                        "add_manual_account": "POST /api/agent/accounts/manual",
                        "unipile_webhook": "POST /api/webhooks/unipile",
                        "conversation_inbound": "POST /api/conversations/{conversation_id}/inbound",
                        "logs": "GET /api/logs?limit=100",
                        "reload_rules": "POST /api/rules/reload",
                    },
                },
            )
            return

        if parsed.path == "/health":
            self._json_response(HTTPStatus.OK, {"status": "ok"})
            return

        if parsed.path == "/api/instructions":
            self._json_response(HTTPStatus.OK, SERVICES["instructions"].to_dict())
            return

        if parsed.path == "/api/agent-system":
            self._json_response(
                HTTPStatus.OK,
                {
                    "agents": {
                        "sourcing_vetting": {
                            "name": SERVICES["workflow"]._agent_name("sourcing_vetting"),
                            "stages": ["source", "enrich", "verify", "add", "vetting"],
                        },
                        "communication": {
                            "name": SERVICES["workflow"]._agent_name("communication"),
                            "stages": ["outreach", "faq", "pre_resume", "dialogue"],
                        },
                        "interview_evaluation": {
                            "name": SERVICES["workflow"]._agent_name("interview_evaluation"),
                            "stages": ["interview_results"],
                        },
                    },
                    "evaluation_playbook": SERVICES["evaluation_playbook"].to_dict(),
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
            items = SERVICES["db"].list_jobs(limit=limit or 100)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/candidates"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/candidates$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            rows = SERVICES["db"].list_candidates_for_job(job_id)
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "items": rows})
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/progress"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)/progress$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            job = SERVICES["db"].get_job(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "job not found"})
                return
            steps = SERVICES["db"].list_job_step_progress(job_id=job_id)
            self._json_response(HTTPStatus.OK, {"job_id": job_id, "items": steps})
            return

        if parsed.path.startswith("/api/jobs/"):
            job_id = self._extract_id(parsed.path, pattern=r"^/api/jobs/(\d+)$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid job id"})
                return
            job = SERVICES["db"].get_job(job_id)
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
            items = SERVICES["db"].list_conversations_overview(limit=limit or 200, job_id=job_id)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        if parsed.path == "/api/logs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100)
            items = SERVICES["db"].list_logs(limit=limit)
            self._json_response(HTTPStatus.OK, {"items": items})
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        if isinstance(payload, dict) and payload.get("_error"):
            self._json_response(HTTPStatus.BAD_REQUEST, payload)
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
                f"{event_type}|{external_chat_id}|{sender_provider_id}|{text}|{occurred_at}".encode("utf-8")
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
            if not text and not connection_event:
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
                    text=text,
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
            title = str(body.get("title") or "").strip()
            jd_text = str(body.get("jd_text") or "").strip()
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
            )
            SERVICES["db"].log_operation(
                operation="job.created",
                status="ok",
                entity_type="job",
                entity_id=str(job_id),
                details={"title": title},
            )
            self._json_response(HTTPStatus.CREATED, {"job_id": job_id})
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
            apply_agent_instructions(SERVICES)
            self._json_response(
                HTTPStatus.OK,
                {
                    "instructions": SERVICES["instructions"].to_dict(),
                    "evaluation_playbook": SERVICES["evaluation_playbook"].to_dict(),
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

    server = ThreadingHTTPServer((host, port), TenerRequestHandler)
    print(f"Tener AI V1 API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        if scheduler_stop is not None:
            scheduler_stop.set()


if __name__ == "__main__":
    run()
