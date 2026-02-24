from __future__ import annotations

import hashlib
import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from .config import InterviewModuleConfig
from .db import InterviewDatabase
from .providers import HireflixConfig, HireflixHTTPAdapter, HireflixMockAdapter
from .question_generation import InterviewQuestionGenerator
from .scoring import InterviewScoringEngine
from .service import InterviewService
from .source_api import SourceAPIClient
from .source_db import SourceReadDatabase
from .token_service import InterviewTokenService
from .transcription_scoring import TranscriptionScoringEngine


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_services() -> Dict[str, Any]:
    config = InterviewModuleConfig.from_env()
    db = InterviewDatabase(db_path=config.db_path)
    db.init_schema()
    if config.source_api_base:
        source_db = SourceAPIClient(
            base_url=config.source_api_base,
            timeout_seconds=config.source_api_timeout_seconds,
        )
    else:
        source_db = SourceReadDatabase(db_path=config.source_db_path)

    provider_name = config.provider_name
    provider_error = ""

    if provider_name == "hireflix":
        try:
            provider = HireflixHTTPAdapter(
                HireflixConfig(
                    api_key=config.hireflix_api_key,
                    base_url=config.hireflix_base_url,
                    position_id=config.hireflix_position_id,
                    timeout_seconds=config.hireflix_timeout_seconds,
                    public_app_base=config.hireflix_public_app_base,
                    allow_synthetic_email=config.allow_synthetic_email,
                    synthetic_email_domain=config.synthetic_email_domain,
                )
            )
        except Exception as exc:
            provider = HireflixMockAdapter()
            provider_name = "hireflix_mock"
            provider_error = str(exc)
    else:
        provider = HireflixMockAdapter()
        provider_name = "hireflix_mock"

    token_service = InterviewTokenService(secret=config.token_secret)
    scoring_engine = InterviewScoringEngine(
        formula_path=config.total_score_formula_path,
    )
    transcription_scoring_engine = TranscriptionScoringEngine(
        criteria_path=config.transcription_scoring_criteria_path,
    )
    question_generator = InterviewQuestionGenerator(
        guidelines_path=config.question_guidelines_path,
        company_profile_path=config.company_profile_path,
        company_name=config.company_name,
    )
    service = InterviewService(
        db=db,
        provider=provider,
        token_service=token_service,
        scoring_engine=scoring_engine,
        transcription_scoring_engine=transcription_scoring_engine,
        source_catalog=source_db,
        question_generator=question_generator,
        default_ttl_hours=config.token_ttl_hours,
        public_base_url=config.public_base_url,
    )

    return {
        "config": config,
        "db": db,
        "source_db": source_db,
        "provider_name": provider_name,
        "provider_error": provider_error,
        "interview": service,
    }


SERVICES = build_services()


class InterviewRequestHandler(BaseHTTPRequestHandler):
    server_version = "TenerInterview/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path in {"/", "/dashboard"}:
            dashboard = project_root() / "src" / "tener_interview" / "static" / "dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, self._error("DASHBOARD_NOT_FOUND", "dashboard file not found"))
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        if parsed.path == "/health":
            self._json_response(HTTPStatus.OK, {"status": "ok"})
            return

        if parsed.path == "/api":
            self._json_response(
                HTTPStatus.OK,
                {
                    "service": "Tener Interview Module",
                    "status": "ok",
                    "endpoints": {
                        "health": "GET /health",
                        "start_session": "POST /api/interviews/sessions/start",
                        "get_session": "GET /api/interviews/sessions/{session_id}",
                        "get_scorecard": "GET /api/interviews/sessions/{session_id}/scorecard",
                        "refresh_session": "POST /api/interviews/sessions/{session_id}/refresh",
                        "list_sessions": "GET /api/interviews/sessions?job_id=1&status=in_progress&limit=100",
                        "entry_link": "GET /i/{token}",
                        "job_leaderboard": "GET /api/jobs/{job_id}/interview-leaderboard?limit=50",
                        "interview_step": "POST /api/steps/interview",
                        "admin_jobs": "GET /api/admin/jobs",
                        "admin_job_candidates": "GET /api/admin/jobs/{job_id}/candidates",
                        "admin_job_assessment": "GET /api/admin/jobs/{job_id}/assessment",
                        "admin_job_refresh": "POST /api/admin/jobs/{job_id}/sessions/refresh",
                    },
                    "provider": SERVICES.get("provider_name"),
                    "provider_error": SERVICES.get("provider_error") or None,
                    "source_db": SERVICES["source_db"].status(),
                    "transcription_scoring_criteria_path": SERVICES["config"].transcription_scoring_criteria_path,
                    "total_score_formula_path": SERVICES["config"].total_score_formula_path,
                    "question_guidelines_path": SERVICES["config"].question_guidelines_path,
                    "company_profile_path": SERVICES["config"].company_profile_path,
                },
            )
            return

        if parsed.path == "/api/admin/jobs":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["200"])[0], 200) or 200
            items = SERVICES["source_db"].list_jobs(limit=limit)
            self._json_response(
                HTTPStatus.OK,
                {
                    "items": items,
                    "source_db": SERVICES["source_db"].status(),
                },
            )
            return

        if parsed.path.startswith("/api/admin/jobs/") and parsed.path.endswith("/candidates"):
            job_id = self._extract_id(parsed.path, r"^/api/admin/jobs/(\d+)/candidates$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("JOB_ID_INVALID", "invalid job id"))
                return
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["500"])[0], 500) or 500
            items = SERVICES["source_db"].list_candidates_for_job(job_id=job_id, limit=limit)
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": job_id,
                    "items": items,
                    "source_db": SERVICES["source_db"].status(),
                },
            )
            return

        if parsed.path.startswith("/api/admin/jobs/") and parsed.path.endswith("/assessment"):
            job_id = self._extract_id(parsed.path, r"^/api/admin/jobs/(\d+)/assessment$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("JOB_ID_INVALID", "invalid job id"))
                return
            item = SERVICES["db"].get_job_assessment(job_id)
            self._json_response(
                HTTPStatus.OK,
                {
                    "job_id": job_id,
                    "item": item or None,
                },
            )
            return

        if parsed.path.startswith("/i/"):
            token = parsed.path[len("/i/") :]
            try:
                resolved = SERVICES["interview"].resolve_entry_token(token)
            except LookupError:
                self._json_response(HTTPStatus.NOT_FOUND, self._error("INTERVIEW_SESSION_NOT_FOUND", "session not found"))
                return
            except ValueError as exc:
                msg = str(exc)
                if "expired" in msg:
                    self._json_response(HTTPStatus.GONE, self._error("INTERVIEW_LINK_EXPIRED", msg))
                else:
                    self._json_response(HTTPStatus.NOT_FOUND, self._error("INTERVIEW_TOKEN_INVALID", msg))
                return

            target = str(resolved.get("provider_url") or "").strip()
            if not target:
                self._json_response(
                    HTTPStatus.BAD_GATEWAY,
                    self._error("INTERVIEW_PROVIDER_URL_MISSING", "provider interview URL is missing"),
                )
                return

            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", target)
            self.end_headers()
            return

        if parsed.path == "/api/interviews/sessions":
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["100"])[0], 100) or 100
            status = (params.get("status") or [None])[0]
            job_id_raw = (params.get("job_id") or [None])[0]
            job_id = self._safe_int(job_id_raw, None) if job_id_raw is not None else None
            out = SERVICES["interview"].list_sessions(limit=limit, status=status, job_id=job_id)
            self._json_response(HTTPStatus.OK, out)
            return

        if parsed.path.startswith("/api/interviews/sessions/"):
            scorecard_m = re.match(r"^/api/interviews/sessions/([^/]+)/scorecard$", parsed.path)
            if scorecard_m:
                session_id = scorecard_m.group(1)
                scorecard = SERVICES["interview"].get_session_scorecard(session_id)
                if not scorecard:
                    self._json_response(HTTPStatus.NOT_FOUND, self._error("INTERVIEW_SESSION_NOT_FOUND", "session not found"))
                    return
                self._json_response(HTTPStatus.OK, scorecard)
                return

            m = re.match(r"^/api/interviews/sessions/([^/]+)$", parsed.path)
            if not m:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("INTERVIEW_SESSION_ID_INVALID", "invalid session id"))
                return
            session_id = m.group(1)
            session = SERVICES["interview"].get_session_view(session_id)
            if not session:
                self._json_response(HTTPStatus.NOT_FOUND, self._error("INTERVIEW_SESSION_NOT_FOUND", "session not found"))
                return
            self._json_response(HTTPStatus.OK, session)
            return

        if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/interview-leaderboard"):
            job_id = self._extract_id(parsed.path, r"^/api/jobs/(\d+)/interview-leaderboard$")
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("JOB_ID_INVALID", "invalid job id"))
                return
            params = parse_qs(parsed.query or "")
            limit = self._safe_int((params.get("limit") or ["50"])[0], 50) or 50
            out = SERVICES["interview"].get_leaderboard(job_id=job_id, limit=limit)
            self._json_response(HTTPStatus.OK, out)
            return

        self._json_response(HTTPStatus.NOT_FOUND, self._error("ROUTE_NOT_FOUND", "route not found"))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        if isinstance(payload, dict) and payload.get("_error"):
            self._json_response(HTTPStatus.BAD_REQUEST, payload)
            return

        if parsed.path == "/api/interviews/sessions/start":
            self._run_idempotent(
                route=parsed.path,
                payload=(payload or {}),
                callback=lambda: self._post_start_session(payload or {}),
            )
            return

        if parsed.path.startswith("/api/interviews/sessions/") and parsed.path.endswith("/refresh"):
            m = re.match(r"^/api/interviews/sessions/([^/]+)/refresh$", parsed.path)
            if not m:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("INTERVIEW_SESSION_ID_INVALID", "invalid session id"))
                return
            session_id = m.group(1)
            self._run_idempotent(
                route=parsed.path,
                payload=(payload or {}),
                callback=lambda: self._post_refresh_session(session_id=session_id, payload=payload or {}),
            )
            return

        if parsed.path == "/api/steps/interview":
            self._run_idempotent(
                route=parsed.path,
                payload=(payload or {}),
                callback=lambda: self._post_interview_step(payload or {}),
            )
            return

        if parsed.path.startswith("/api/admin/jobs/") and parsed.path.endswith("/sessions/refresh"):
            m = re.match(r"^/api/admin/jobs/(\d+)/sessions/refresh$", parsed.path)
            if not m:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("JOB_ID_INVALID", "invalid job id"))
                return
            job_id = self._safe_int(m.group(1), None)
            if job_id is None:
                self._json_response(HTTPStatus.BAD_REQUEST, self._error("JOB_ID_INVALID", "invalid job id"))
                return
            self._run_idempotent(
                route=parsed.path,
                payload=(payload or {}),
                callback=lambda: self._post_admin_refresh_job(job_id=job_id, body=payload or {}),
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, self._error("ROUTE_NOT_FOUND", "route not found"))

    def _post_start_session(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        job_id = self._safe_int(body.get("job_id"), None)
        candidate_id = self._safe_int(body.get("candidate_id"), None)
        if job_id is None:
            return HTTPStatus.BAD_REQUEST, self._error("JOB_ID_REQUIRED", "job_id is required")
        if candidate_id is None:
            return HTTPStatus.BAD_REQUEST, self._error("CANDIDATE_ID_REQUIRED", "candidate_id is required")

        try:
            result = SERVICES["interview"].start_session(
                job_id=job_id,
                candidate_id=candidate_id,
                candidate_name=str(body.get("candidate_name") or "").strip() or None,
                candidate_email=str(body.get("candidate_email") or "").strip().lower() or None,
                conversation_id=self._safe_int(body.get("conversation_id"), None),
                language=str(body.get("language") or "").strip() or None,
                ttl_hours=self._safe_int(body.get("ttl_hours"), None),
                request_base_url=self._request_base_url(),
            )
        except Exception as exc:
            return (
                HTTPStatus.BAD_GATEWAY,
                self._error("INTERVIEW_PROVIDER_REQUEST_FAILED", str(exc)),
            )
        return HTTPStatus.CREATED, result

    def _post_refresh_session(self, session_id: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        force = bool(payload.get("force") is True)
        try:
            result = SERVICES["interview"].refresh_session(session_id=session_id, force=force)
        except LookupError:
            return HTTPStatus.NOT_FOUND, self._error("INTERVIEW_SESSION_NOT_FOUND", "session not found")
        except ValueError as exc:
            return HTTPStatus.UNPROCESSABLE_ENTITY, self._error("INTERVIEW_REFRESH_REJECTED", str(exc))

        status = str(result.get("status") or "")
        if status == "failed":
            return HTTPStatus.BAD_GATEWAY, result
        return HTTPStatus.OK, result

    def _post_interview_step(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        job_id = self._safe_int(body.get("job_id"), None)
        candidate_ids = body.get("candidate_ids")
        mode = str(body.get("mode") or "start_or_refresh").strip().lower()
        if job_id is None:
            return HTTPStatus.BAD_REQUEST, self._error("JOB_ID_REQUIRED", "job_id is required")
        if not isinstance(candidate_ids, list):
            return HTTPStatus.BAD_REQUEST, self._error("CANDIDATE_IDS_REQUIRED", "candidate_ids must be an array")
        if mode not in {"start_or_refresh"}:
            return HTTPStatus.BAD_REQUEST, self._error("INTERVIEW_MODE_INVALID", "unsupported mode")

        out = SERVICES["interview"].run_interview_step(
            job_id=job_id,
            candidate_ids=candidate_ids,
            mode=mode,
            request_base_url=self._request_base_url(),
        )
        return HTTPStatus.OK, out

    def _post_admin_refresh_job(self, job_id: int, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        force = bool(body.get("force") is True)
        limit = self._safe_int(body.get("limit"), 250) or 250
        sessions_payload = SERVICES["interview"].list_sessions(limit=limit, job_id=job_id)
        sessions = sessions_payload.get("items") if isinstance(sessions_payload.get("items"), list) else []

        skipped_terminal = 0
        refreshed = 0
        scored = 0
        failed = 0
        errors = 0
        items: list[Dict[str, Any]] = []

        for session in sessions:
            if not isinstance(session, dict):
                continue
            session_id = str(session.get("session_id") or "").strip()
            status = str(session.get("status") or "").strip()
            if not session_id:
                continue

            if (not force) and status in {"scored", "failed", "expired", "canceled"}:
                skipped_terminal += 1
                items.append({"session_id": session_id, "status": status, "action": "skipped"})
                continue

            try:
                out = SERVICES["interview"].refresh_session(session_id=session_id, force=force)
                refreshed += 1
                out_status = str(out.get("status") or "")
                if out_status == "scored":
                    scored += 1
                elif out_status == "failed":
                    failed += 1
                items.append(
                    {
                        "session_id": session_id,
                        "previous_status": status,
                        "status": out_status,
                        "action": "refreshed",
                    }
                )
            except Exception as exc:
                errors += 1
                items.append(
                    {
                        "session_id": session_id,
                        "previous_status": status,
                        "status": "failed",
                        "action": "error",
                        "error": str(exc),
                    }
                )

        return (
            HTTPStatus.OK,
            {
                "job_id": job_id,
                "force": force,
                "total_sessions": len(sessions),
                "refreshed": refreshed,
                "skipped_terminal": skipped_terminal,
                "scored": scored,
                "failed": failed,
                "errors": errors,
                "items": items,
            },
        )

    def _run_idempotent(
        self,
        route: str,
        payload: Dict[str, Any],
        callback: Callable[[], Tuple[int, Dict[str, Any]]],
    ) -> None:
        key = str(self.headers.get("Idempotency-Key") or "").strip()
        if not key:
            status, out = callback()
            self._json_response(status, out)
            return

        payload_hash = self._payload_hash(payload)
        existing = SERVICES["db"].get_idempotency_record(route=route, key=key)
        if existing:
            if str(existing.get("payload_hash")) != payload_hash:
                self._json_response(
                    HTTPStatus.CONFLICT,
                    self._error("IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD", "idempotency key reuse with different payload"),
                )
                return
            self._json_response(int(existing.get("status_code") or HTTPStatus.OK), existing.get("response_json") or {})
            return

        status, out = callback()
        if int(status) < 500:
            SERVICES["db"].put_idempotency_record(
                route=route,
                key=key,
                payload_hash=payload_hash,
                status_code=int(status),
                response=out,
            )
        self._json_response(status, out)

    def _read_json_body(self) -> Dict[str, Any]:
        length = self.headers.get("Content-Length")
        if not length:
            return {}
        try:
            size = int(length)
        except ValueError:
            return {"_error": "invalid content-length"}
        raw = self.rfile.read(size)
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {"_error": "invalid json body"}
        if not isinstance(payload, dict):
            return {"_error": "payload must be object"}
        return payload

    def _request_base_url(self) -> str:
        cfg = SERVICES["config"]
        if cfg.public_base_url:
            return cfg.public_base_url.rstrip("/")
        host = self.headers.get("Host")
        if host:
            proto = str(self.headers.get("X-Forwarded-Proto") or "").strip().lower()
            if proto not in {"http", "https"}:
                proto = "https" if ".onrender.com" in host else "http"
            return f"{proto}://{host}"
        return f"http://{cfg.host}:{cfg.port}"

    @staticmethod
    def _safe_int(value: Any, default: Optional[int]) -> Optional[int]:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _extract_id(path: str, pattern: str) -> Optional[int]:
        m = re.match(pattern, path)
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None

    @staticmethod
    def _payload_hash(payload: Dict[str, Any]) -> str:
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _error(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return {
            "error": message,
            "code": code,
            "details": details or {},
        }

    def _json_response(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html_response(self, status: int, body: str) -> None:
        raw = body.encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def run_server() -> None:
    config = SERVICES["config"]
    server = ThreadingHTTPServer((config.host, config.port), InterviewRequestHandler)
    print(f"Interview module listening on http://{config.host}:{config.port}")
    server.serve_forever()
