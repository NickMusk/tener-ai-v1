from __future__ import annotations

import re
from typing import Any, Dict, Optional


EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)


class LandingValidationError(ValueError):
    def __init__(self, field_errors: Dict[str, str]) -> None:
        self.field_errors = field_errors
        super().__init__("invalid landing payload")


def _normalize_text(value: Any, *, max_length: int, multiline: bool = False) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if multiline:
        text = "\n".join(part.rstrip() for part in text.split("\n"))
    else:
        text = " ".join(text.split())
    return text.strip()[:max_length]


def _normalize_email(value: Any) -> str:
    return _normalize_text(value, max_length=320).lower()


def _validate_email(field_name: str, value: Any, *, required: bool = True) -> str:
    email = _normalize_email(value)
    if not email:
        if required:
            raise LandingValidationError({field_name: "Email is required."})
        return ""
    if not EMAIL_RE.match(email):
        raise LandingValidationError({field_name: "Enter a valid email address."})
    return email


class LandingService:
    def __init__(self, db: Any) -> None:
        self.db = db

    def submit_newsletter(
        self,
        payload: Dict[str, Any],
        *,
        source_path: str,
        ip_address: Optional[str],
        user_agent: Optional[str],
    ) -> Dict[str, Any]:
        email = _validate_email("email", payload.get("email"))
        full_name = _normalize_text(payload.get("full_name"), max_length=120)
        company_name = _normalize_text(payload.get("company_name"), max_length=160)
        notes = _normalize_text(payload.get("notes"), max_length=500, multiline=True)

        if _normalize_text(payload.get("website"), max_length=120):
            return {
                "created": False,
                "subscription": None,
                "message": "Subscription received.",
                "status": "accepted",
            }

        result = self.db.create_newsletter_subscription(
            email=email,
            full_name=full_name or None,
            company_name=company_name or None,
            notes=notes or None,
            source_path=str(source_path or "").strip() or "/landing",
            ip_address=_normalize_text(ip_address, max_length=64) or None,
            user_agent=_normalize_text(user_agent, max_length=500) or None,
        )
        created = bool(result.get("created"))
        return {
            "created": created,
            "subscription": result.get("subscription"),
            "message": "You are on the list." if created else "You are already subscribed.",
            "status": "subscribed" if created else "already_subscribed",
        }

    def submit_contact_request(
        self,
        payload: Dict[str, Any],
        *,
        source_path: str,
        ip_address: Optional[str],
        user_agent: Optional[str],
    ) -> Dict[str, Any]:
        field_errors: Dict[str, str] = {}

        full_name = _normalize_text(payload.get("full_name"), max_length=120)
        if not full_name:
            field_errors["full_name"] = "Full name is required."

        work_email = _normalize_email(payload.get("work_email"))
        if not work_email:
            field_errors["work_email"] = "Work email is required."
        elif not EMAIL_RE.match(work_email):
            field_errors["work_email"] = "Enter a valid work email."

        company_name = _normalize_text(payload.get("company_name"), max_length=160)
        if not company_name:
            field_errors["company_name"] = "Company name is required."

        job_title = _normalize_text(payload.get("job_title"), max_length=160)
        hiring_need = _normalize_text(payload.get("hiring_need"), max_length=4000, multiline=True)
        if not hiring_need:
            field_errors["hiring_need"] = "Describe the role or hiring need."

        if field_errors:
            raise LandingValidationError(field_errors)

        if _normalize_text(payload.get("website"), max_length=120):
            return {
                "request": None,
                "message": "Request received.",
                "status": "accepted",
            }

        row = self.db.create_contact_request(
            full_name=full_name,
            work_email=work_email,
            company_name=company_name,
            job_title=job_title or None,
            hiring_need=hiring_need,
            source_path=str(source_path or "").strip() or "/landing",
            ip_address=_normalize_text(ip_address, max_length=64) or None,
            user_agent=_normalize_text(user_agent, max_length=500) or None,
        )
        return {
            "request": row,
            "message": "Request received. We will follow up shortly.",
            "status": "received",
        }
