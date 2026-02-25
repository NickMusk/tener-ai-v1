from __future__ import annotations

import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

from tener_ai.company_culture_profile import (
    BingRssSearchProvider,
    BraveHtmlSearchProvider,
    CompanyCultureProfileService,
    DuckDuckGoHtmlSearchProvider,
    GoogleCSESearchProvider,
    HeuristicCompanyProfileSynthesizer,
    OpenAICompanyProfileSynthesizer,
    SeedSearchProvider,
    SimpleHtmlTextExtractor,
    UrllibPageFetcher,
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return max(minimum, int(default))
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, int(default))


def build_services() -> Dict[str, Any]:
    search_mode = str(os.environ.get("TENER_COMPANY_PROFILE_SEARCH_MODE", "bing_rss")).strip().lower()
    allow_seed_fallback = env_bool("TENER_COMPANY_PROFILE_ALLOW_SEED_FALLBACK", True)

    website_seed = str(os.environ.get("TENER_COMPANY_PROFILE_SEED_WEBSITE", "")).strip()
    company_seed = str(os.environ.get("TENER_COMPANY_PROFILE_SEED_COMPANY", "")).strip()
    google_api_key = str(os.environ.get("GOOGLE_CSE_API_KEY", "")).strip()
    google_cx = str(os.environ.get("GOOGLE_CSE_CX", "")).strip()

    runtime_warnings = []
    if search_mode in {"google", "google_cse"}:
        if google_api_key and google_cx:
            search_provider = GoogleCSESearchProvider(api_key=google_api_key, cx=google_cx)
            search_backend = "google_cse"
        elif allow_seed_fallback:
            search_provider = BingRssSearchProvider()
            search_backend = "bing_rss_fallback"
            runtime_warnings.append("Google CSE is not configured. Bing RSS fallback is active.")
        else:
            raise RuntimeError("Google CSE credentials are required: GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX")
    elif search_mode in {"bing", "bing_rss", "bing_xml"}:
        search_provider = BingRssSearchProvider()
        search_backend = "bing_rss"
    elif search_mode in {"duckduckgo", "duckduckgo_html", "ddg"}:
        search_provider = DuckDuckGoHtmlSearchProvider()
        search_backend = "duckduckgo_html"
    elif search_mode in {"brave", "brave_html"}:
        search_provider = BraveHtmlSearchProvider()
        search_backend = "brave_html"
    elif search_mode == "seed":
        search_provider = SeedSearchProvider(company_name=company_seed, website_url=website_seed)
        search_backend = "seed"
    else:
        raise RuntimeError(
            f"Unsupported search mode: {search_mode}. Supported: bing_rss, duckduckgo_html, google_cse, brave_html, seed"
        )

    llm_enabled = env_bool("TENER_COMPANY_PROFILE_USE_LLM", True)
    llm_api_key = str(os.environ.get("OPENAI_API_KEY", "")).strip()
    if llm_enabled and llm_api_key:
        synthesizer = OpenAICompanyProfileSynthesizer(
            api_key=llm_api_key,
            model=str(os.environ.get("TENER_COMPANY_PROFILE_LLM_MODEL", os.environ.get("TENER_LLM_MODEL", "gpt-4o-mini"))).strip(),
            base_url=str(os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")).strip(),
            timeout_seconds=env_int("TENER_COMPANY_PROFILE_LLM_TIMEOUT_SECONDS", 30, minimum=5),
            max_chars_per_source=env_int("TENER_COMPANY_PROFILE_LLM_MAX_CHARS_PER_SOURCE", 2500, minimum=300),
        )
        llm_backend = "openai"
    else:
        synthesizer = HeuristicCompanyProfileSynthesizer()
        llm_backend = "heuristic"
        runtime_warnings.append("OPENAI_API_KEY is missing or LLM is disabled. Heuristic profile mode is active.")

    service = CompanyCultureProfileService(
        search_provider=search_provider,
        page_fetcher=UrllibPageFetcher(),
        content_extractor=SimpleHtmlTextExtractor(),
        synthesizer=synthesizer,
        max_links=env_int("TENER_COMPANY_PROFILE_MAX_LINKS", 10, minimum=1),
        per_query_limit=env_int("TENER_COMPANY_PROFILE_PER_QUERY_LIMIT", 10, minimum=1),
        min_job_board_links=env_int("TENER_COMPANY_PROFILE_MIN_JOB_BOARD_LINKS", 3, minimum=0),
        fetch_timeout_seconds=env_int("TENER_COMPANY_PROFILE_FETCH_TIMEOUT_SECONDS", 15, minimum=3),
        min_text_chars=env_int("TENER_COMPANY_PROFILE_MIN_TEXT_CHARS", 600, minimum=1),
    )
    return {
        "profile_service": service,
        "runtime": {
            "search_backend": search_backend,
            "llm_backend": llm_backend,
            "warnings": runtime_warnings,
        },
    }


SERVICES = build_services()


class CompanyProfileRequestHandler(BaseHTTPRequestHandler):
    server_version = "TenerCompanyProfile/0.1"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path in {"/", "/dashboard"}:
            dashboard = project_root() / "src" / "tener_company_profile" / "static" / "dashboard.html"
            if not dashboard.exists():
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "dashboard file not found"})
                return
            self._html_response(HTTPStatus.OK, dashboard.read_text(encoding="utf-8"))
            return

        if parsed.path == "/health":
            self._json_response(
                HTTPStatus.OK,
                {
                    "status": "ok",
                    "service": "tener-company-profile",
                    "runtime": SERVICES["runtime"],
                },
            )
            return

        if parsed.path == "/api":
            self._json_response(
                HTTPStatus.OK,
                {
                    "service": "Tener Company Profile API",
                    "status": "ok",
                    "runtime": SERVICES["runtime"],
                    "endpoints": {
                        "health": "GET /health",
                        "dashboard": "GET /dashboard",
                        "generate_profile": "POST /api/company-profiles/generate",
                    },
                },
            )
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self._read_json_body()
        if isinstance(payload, dict) and payload.get("_error"):
            self._json_response(HTTPStatus.BAD_REQUEST, payload)
            return

        if parsed.path == "/api/company-profiles/generate":
            body = payload or {}
            if not isinstance(body, dict):
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "invalid payload"})
                return
            company_name = str(body.get("company_name") or "").strip()
            company_website = str(body.get("company_website_url") or "").strip()
            if not company_name:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "company_name is required"})
                return
            if not company_website:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": "company_website_url is required"})
                return

            started = time.time()
            try:
                result = SERVICES["profile_service"].generate(
                    company_name=company_name,
                    website_url=company_website,
                )
            except ValueError as exc:
                self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            except Exception as exc:
                self._json_response(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "profile generation failed", "details": str(exc)},
                )
                return
            elapsed_ms = int((time.time() - started) * 1000)
            response = {
                "status": "ok",
                "latency_ms": elapsed_ms,
                "runtime": SERVICES["runtime"],
                "result": result,
            }
            self._json_response(HTTPStatus.OK, response)
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "route not found"})

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _read_json_body(self) -> Dict[str, Any]:
        length_raw = self.headers.get("Content-Length", "0")
        try:
            length = int(length_raw)
        except ValueError:
            return {"_error": "invalid content length"}
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {"_error": "invalid json"}
        return data if isinstance(data, dict) else {"_error": "json payload must be object"}

    def _json_response(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _html_response(self, status: HTTPStatus, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)


def run() -> None:
    host = os.environ.get("TENER_COMPANY_PROFILE_HOST", "0.0.0.0")
    port = int(os.environ.get("TENER_COMPANY_PROFILE_PORT", os.environ.get("PORT", "8095")))
    server = ThreadingHTTPServer((host, port), CompanyProfileRequestHandler)
    print(f"Tener Company Profile service listening on http://{host}:{port}")
    print(
        "Runtime:",
        json.dumps(SERVICES["runtime"], ensure_ascii=False),
    )
    server.serve_forever()


if __name__ == "__main__":
    run()
