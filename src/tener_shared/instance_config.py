from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


@dataclass(frozen=True)
class InstanceBranding:
    brand_name: str = "Tener"
    company_name: str = "Tener"
    system_name: str = "Tener"
    main_dashboard_title: str = "Tener.ai Pipeline Control Center"
    main_dashboard_logo_text: str = "Tener.ai"
    main_dashboard_subnote_html: str = "Pipeline Control Center<br>Manual orchestration + live diagnostics"
    main_default_job_company: str = "Tener"
    landing_title: str = "Tener.ai | AI Hiring Agency for Critical Skills"
    landing_headline: str = "Tener hiring operations"
    landing_body: str = "This instance is configured for dedicated hiring operations."
    interview_dashboard_title: str = "Tener Interview Admin"
    interview_dashboard_heading: str = "Interview Admin Dashboard"
    interview_dashboard_subcopy: str = "Pick JD from DB, create candidate interview links, then track scored results."
    interview_candidate_title: str = "Tener Interview"
    interview_company_name: str = "Tener"
    interview_header_note: str = "AI Interview Entry"
    recruiter_company_name: str = "Tener"
    recruiter_signature_title: str = "Senior Talent Acquisition Manager"


@dataclass(frozen=True)
class InstanceAccess:
    require_private_bearer_token: bool = False
    public_api_index: bool = True
    public_dashboard: bool = True
    public_candidate_profiles: bool = True
    public_interview_dashboard: bool = True
    public_interview_api_index: bool = True
    allow_demo_routes: bool = True


@dataclass(frozen=True)
class InstanceFeatures:
    use_generic_public_landing: bool = False
    strict_interview_provider: bool = False


@dataclass(frozen=True)
class InstanceURLs:
    public_app_base_url: str = ""
    public_interview_base_url: str = ""


@dataclass(frozen=True)
class InstanceConfig:
    instance_id: str = "default"
    service_slug: str = ""
    source_path: str = ""
    config_dir: str = ""
    branding: InstanceBranding = field(default_factory=InstanceBranding)
    access: InstanceAccess = field(default_factory=InstanceAccess)
    features: InstanceFeatures = field(default_factory=InstanceFeatures)
    urls: InstanceURLs = field(default_factory=InstanceURLs)

    def resolve_file(self, filename: str) -> str:
        if not self.config_dir:
            return ""
        base_dir = Path(self.config_dir)
        target = base_dir / filename
        return str(target) if target.exists() else ""

    @classmethod
    def from_payload(cls, payload: Dict[str, Any], *, source_path: str = "") -> "InstanceConfig":
        branding_payload = payload.get("branding") if isinstance(payload.get("branding"), dict) else {}
        access_payload = payload.get("access") if isinstance(payload.get("access"), dict) else {}
        features_payload = payload.get("features") if isinstance(payload.get("features"), dict) else {}
        urls_payload = payload.get("urls") if isinstance(payload.get("urls"), dict) else {}
        source = Path(source_path).resolve() if source_path else None

        brand_name = _as_str(branding_payload.get("brand_name"), "Tener")
        company_name = _as_str(branding_payload.get("company_name"), brand_name)
        system_name = _as_str(branding_payload.get("system_name"), company_name)
        interview_company_name = _as_str(branding_payload.get("interview_company_name"), company_name)
        recruiter_company_name = _as_str(branding_payload.get("recruiter_company_name"), company_name)

        return cls(
            instance_id=_as_str(payload.get("instance_id"), "default"),
            service_slug=_as_str(payload.get("service_slug")),
            source_path=str(source) if source else "",
            config_dir=str(source.parent) if source else "",
            branding=InstanceBranding(
                brand_name=brand_name,
                company_name=company_name,
                system_name=system_name,
                main_dashboard_title=_as_str(
                    branding_payload.get("main_dashboard_title"),
                    f"{brand_name} Pipeline Control Center",
                ),
                main_dashboard_logo_text=_as_str(branding_payload.get("main_dashboard_logo_text"), brand_name),
                main_dashboard_subnote_html=_as_str(
                    branding_payload.get("main_dashboard_subnote_html"),
                    "Pipeline Control Center<br>Manual orchestration + live diagnostics",
                ),
                main_default_job_company=_as_str(branding_payload.get("main_default_job_company"), company_name),
                landing_title=_as_str(branding_payload.get("landing_title"), f"{brand_name} Hiring Operations"),
                landing_headline=_as_str(branding_payload.get("landing_headline"), f"{brand_name} hiring operations"),
                landing_body=_as_str(
                    branding_payload.get("landing_body"),
                    "This dedicated instance is configured for private hiring operations.",
                ),
                interview_dashboard_title=_as_str(
                    branding_payload.get("interview_dashboard_title"),
                    f"{brand_name} Interview Admin",
                ),
                interview_dashboard_heading=_as_str(
                    branding_payload.get("interview_dashboard_heading"),
                    "Interview Admin Dashboard",
                ),
                interview_dashboard_subcopy=_as_str(
                    branding_payload.get("interview_dashboard_subcopy"),
                    "Pick JD from DB, create candidate interview links, then track scored results.",
                ),
                interview_candidate_title=_as_str(
                    branding_payload.get("interview_candidate_title"),
                    f"{brand_name} Interview",
                ),
                interview_company_name=interview_company_name,
                interview_header_note=_as_str(branding_payload.get("interview_header_note"), "AI Interview Entry"),
                recruiter_company_name=recruiter_company_name,
                recruiter_signature_title=_as_str(
                    branding_payload.get("recruiter_signature_title"),
                    "Senior Talent Acquisition Manager",
                ),
            ),
            access=InstanceAccess(
                require_private_bearer_token=_as_bool(access_payload.get("require_private_bearer_token"), False),
                public_api_index=_as_bool(access_payload.get("public_api_index"), True),
                public_dashboard=_as_bool(access_payload.get("public_dashboard"), True),
                public_candidate_profiles=_as_bool(access_payload.get("public_candidate_profiles"), True),
                public_interview_dashboard=_as_bool(access_payload.get("public_interview_dashboard"), True),
                public_interview_api_index=_as_bool(access_payload.get("public_interview_api_index"), True),
                allow_demo_routes=_as_bool(access_payload.get("allow_demo_routes"), True),
            ),
            features=InstanceFeatures(
                use_generic_public_landing=_as_bool(features_payload.get("use_generic_public_landing"), False),
                strict_interview_provider=_as_bool(features_payload.get("strict_interview_provider"), False),
            ),
            urls=InstanceURLs(
                public_app_base_url=_as_str(urls_payload.get("public_app_base_url")),
                public_interview_base_url=_as_str(urls_payload.get("public_interview_base_url")),
            ),
        )


def load_instance_config(*, root: Path | None = None) -> InstanceConfig:
    configured = _as_str(os.environ.get("TENER_INSTANCE_CONFIG_PATH"))
    if not configured:
        return InstanceConfig()

    candidate = Path(configured)
    if not candidate.is_absolute():
        base = root if root is not None else Path.cwd()
        candidate = (base / candidate).resolve()
    if not candidate.exists():
        raise RuntimeError(f"TENER_INSTANCE_CONFIG_PATH does not exist: {candidate}")

    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Instance config is not valid JSON: {candidate}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Instance config must be a JSON object: {candidate}")

    config = InstanceConfig.from_payload(payload, source_path=str(candidate))

    if _env_bool("TENER_REQUIRE_INSTANCE_CONFIG", False) and config.instance_id == "default":
        raise RuntimeError("TENER_REQUIRE_INSTANCE_CONFIG is enabled but a default instance config was loaded")
    return config
