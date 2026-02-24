from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class InterviewModuleConfig:
    db_path: str
    source_db_path: str
    source_api_base: str
    source_api_timeout_seconds: int
    transcription_scoring_criteria_path: str
    total_score_formula_path: str
    host: str
    port: int
    token_secret: str
    token_ttl_hours: int
    provider_name: str
    public_base_url: str
    hireflix_api_key: str
    hireflix_base_url: str
    hireflix_position_id: str
    hireflix_timeout_seconds: int
    hireflix_public_app_base: str
    allow_synthetic_email: bool
    synthetic_email_domain: str

    @classmethod
    def from_env(cls) -> "InterviewModuleConfig":
        root = Path(__file__).resolve().parents[2]
        db_path = os.environ.get(
            "TENER_INTERVIEW_DB_PATH",
            str(root / "runtime" / "tener_interview.sqlite3"),
        )
        source_db_path = os.environ.get(
            "TENER_INTERVIEW_SOURCE_DB_PATH",
            str(root / "runtime" / "tener_v1.sqlite3"),
        )
        transcription_scoring_criteria_path = os.environ.get(
            "TENER_INTERVIEW_TRANSCRIPTION_SCORING_CRITERIA_PATH",
            str(root / "config" / "interview_transcription_scoring_criteria.json"),
        )
        total_score_formula_path = os.environ.get(
            "TENER_INTERVIEW_TOTAL_SCORE_FORMULA_PATH",
            str(root / "config" / "interview_total_score_formula.json"),
        )
        host = os.environ.get("TENER_INTERVIEW_HOST", "127.0.0.1")
        port_raw = os.environ.get("TENER_INTERVIEW_PORT", "8090")
        ttl_raw = os.environ.get("TENER_INTERVIEW_TOKEN_TTL_HOURS", "72")
        timeout_raw = os.environ.get("TENER_HIREFLIX_TIMEOUT_SECONDS", "30")
        source_timeout_raw = os.environ.get("TENER_INTERVIEW_SOURCE_API_TIMEOUT_SECONDS", "20")

        try:
            port = int(port_raw)
        except ValueError:
            port = 8090

        try:
            token_ttl_hours = int(ttl_raw)
        except ValueError:
            token_ttl_hours = 72

        try:
            timeout_seconds = int(timeout_raw)
        except ValueError:
            timeout_seconds = 30

        try:
            source_timeout_seconds = int(source_timeout_raw)
        except ValueError:
            source_timeout_seconds = 20

        token_secret = os.environ.get("TENER_INTERVIEW_TOKEN_SECRET", "dev-interview-secret")
        provider_name = os.environ.get("TENER_INTERVIEW_PROVIDER", "hireflix_mock")
        public_base_url = os.environ.get("TENER_INTERVIEW_PUBLIC_BASE_URL", "")
        source_api_base = os.environ.get("TENER_INTERVIEW_SOURCE_API_BASE", "").strip()
        hireflix_api_key = os.environ.get("TENER_HIREFLIX_API_KEY", "").strip()
        hireflix_base_url = os.environ.get("TENER_HIREFLIX_BASE_URL", "https://api.hireflix.com/me").strip()
        hireflix_position_id = os.environ.get("TENER_HIREFLIX_POSITION_ID", "").strip()
        hireflix_public_app_base = os.environ.get("TENER_HIREFLIX_PUBLIC_APP_BASE", "https://app.hireflix.com").strip()
        allow_synthetic_email = (
            str(os.environ.get("TENER_INTERVIEW_ALLOW_SYNTHETIC_EMAIL", "true")).strip().lower()
            in {"1", "true", "yes", "on"}
        )
        synthetic_email_domain = os.environ.get("TENER_INTERVIEW_SYNTHETIC_EMAIL_DOMAIN", "interview.local").strip()

        return cls(
            db_path=db_path,
            source_db_path=source_db_path,
            source_api_base=source_api_base,
            source_api_timeout_seconds=max(3, source_timeout_seconds),
            transcription_scoring_criteria_path=transcription_scoring_criteria_path,
            total_score_formula_path=total_score_formula_path,
            host=host,
            port=port,
            token_secret=token_secret,
            token_ttl_hours=max(1, token_ttl_hours),
            provider_name=provider_name.strip().lower() or "hireflix_mock",
            public_base_url=public_base_url.strip(),
            hireflix_api_key=hireflix_api_key,
            hireflix_base_url=hireflix_base_url or "https://api.hireflix.com/me",
            hireflix_position_id=hireflix_position_id,
            hireflix_timeout_seconds=max(5, timeout_seconds),
            hireflix_public_app_base=hireflix_public_app_base or "https://app.hireflix.com",
            allow_synthetic_email=allow_synthetic_email,
            synthetic_email_domain=synthetic_email_domain or "interview.local",
        )
