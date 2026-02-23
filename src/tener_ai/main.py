from __future__ import annotations

import json
import os
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .agents import FAQAgent, OutreachAgent, SourcingAgent, VerificationAgent
from .db import Database
from .linkedin_provider import build_linkedin_provider
from .matching import MatchingEngine
from .workflow import WorkflowService


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def build_services() -> Dict[str, Any]:
    root = project_root()
    db_path = os.environ.get("TENER_DB_PATH", str(root / "runtime" / "tener_v1.sqlite3"))
    rules_path = os.environ.get("TENER_MATCHING_RULES_PATH", str(root / "config" / "matching_rules.json"))
    templates_path = os.environ.get("TENER_TEMPLATES_PATH", str(root / "config" / "outreach_templates.json"))
    mock_profiles_path = os.environ.get("TENER_MOCK_LINKEDIN_DATA_PATH", str(root / "data" / "mock_linkedin_profiles.json"))

    db = Database(db_path=db_path)
    db.init_schema()

    matching_engine = MatchingEngine(rules_path=rules_path)
    linkedin_provider = build_linkedin_provider(mock_dataset_path=mock_profiles_path)

    sourcing_agent = SourcingAgent(linkedin_provider=linkedin_provider)
    verification_agent = VerificationAgent(matching_engine=matching_engine)
    outreach_agent = OutreachAgent(templates_path=templates_path, matching_engine=matching_engine)
    faq_agent = FAQAgent(templates_path=templates_path, matching_engine=matching_engine)

    workflow = WorkflowService(
        db=db,
        sourcing_agent=sourcing_agent,
        verification_agent=verification_agent,
        outreach_agent=outreach_agent,
        faq_agent=faq_agent,
        contact_all_mode=env_bool("TENER_CONTACT_ALL_MODE", True),
        require_resume_before_final_verify=env_bool("TENER_REQUIRE_RESUME_BEFORE_FINAL_VERIFY", True),
    )

    return {
        "db": db,
        "matching_engine": matching_engine,
        "workflow": workflow,
    }


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
                        "list_job_candidates": "GET /api/jobs/{job_id}/candidates",
                        "run_workflow": "POST /api/workflows/execute",
                        "source_step": "POST /api/steps/source",
                        "enrich_step": "POST /api/steps/enrich",
                        "verify_step": "POST /api/steps/verify",
                        "add_step": "POST /api/steps/add",
                        "outreach_step": "POST /api/steps/outreach",
                        "conversation_messages": "GET /api/conversations/{conversation_id}/messages",
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
            try:
                summary = SERVICES["workflow"].execute_job_workflow(job_id=job_id, limit=limit)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
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

            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": summary.job_id,
                    "searched": summary.searched,
                    "verified": summary.verified,
                    "needs_resume": summary.needs_resume,
                    "rejected": summary.rejected,
                    "outreached": summary.outreached,
                    "outreach_sent": summary.outreach_sent,
                    "outreach_failed": summary.outreach_failed,
                    "conversation_ids": summary.conversation_ids,
                },
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
            try:
                result = SERVICES["workflow"].source_candidates(job_id=job_id, limit=limit or 30)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "source step failed", "details": str(exc)})
                return
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
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "verify step failed", "details": str(exc)})
                return
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
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "enrich step failed", "details": str(exc)})
                return
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
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "add step failed", "details": str(exc)})
                return
            self._json_response(HTTPStatus.OK, result)
            return

        if parsed.path == "/api/steps/outreach":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            job_id = self._safe_int(body.get("job_id"), None)
            candidate_ids = body.get("candidate_ids")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "job_id is required"})
                return
            if not isinstance(candidate_ids, list):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "candidate_ids must be an array"})
                return
            try:
                result = SERVICES["workflow"].outreach_candidates(job_id=job_id, candidate_ids=candidate_ids)
            except ValueError as exc:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            except Exception as exc:
                self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "outreach step failed", "details": str(exc)})
                return
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

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

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


def run() -> None:
    # Cloud runtimes usually inject PORT; prefer it when TENER_PORT is not set.
    host = os.environ.get("TENER_HOST", "0.0.0.0")
    port = int(os.environ.get("TENER_PORT", os.environ.get("PORT", "8080")))
    server = ThreadingHTTPServer((host, port), TenerRequestHandler)
    print(f"Tener AI V1 API listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
