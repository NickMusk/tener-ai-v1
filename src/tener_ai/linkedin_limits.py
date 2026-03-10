from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping


DAILY_MESSAGE_LIMIT_MIN = 1
DAILY_MESSAGE_LIMIT_MAX = 200
DAILY_CONNECT_LIMIT_MIN = 1
DAILY_CONNECT_LIMIT_MAX = 200
WEEKLY_CONNECT_LIMIT_MIN = 1
WEEKLY_CONNECT_LIMIT_MAX = 700


def validate_account_limits_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("invalid payload")
    has_message = "daily_message_limit" in payload
    has_connect = "daily_connect_limit" in payload
    if not has_message and not has_connect:
        raise ValueError("daily_message_limit or daily_connect_limit is required")
    return {
        "has_daily_message_limit": has_message,
        "has_daily_connect_limit": has_connect,
        "daily_message_limit": _parse_optional_limit(
            payload.get("daily_message_limit"),
            field_name="daily_message_limit",
            minimum=DAILY_MESSAGE_LIMIT_MIN,
            maximum=DAILY_MESSAGE_LIMIT_MAX,
        )
        if has_message
        else None,
        "daily_connect_limit": _parse_optional_limit(
            payload.get("daily_connect_limit"),
            field_name="daily_connect_limit",
            minimum=DAILY_CONNECT_LIMIT_MIN,
            maximum=DAILY_CONNECT_LIMIT_MAX,
        )
        if has_connect
        else None,
    }


def policy_daily_new_threads_cap(policy: Mapping[str, Any] | None) -> int:
    outbound = (
        policy.get("outbound_messages")
        if isinstance(policy, Mapping) and isinstance(policy.get("outbound_messages"), dict)
        else {}
    )
    per_account = (
        outbound.get("daily_new_threads_per_account")
        if isinstance(outbound.get("daily_new_threads_per_account"), dict)
        else {}
    )
    raw = per_account.get("max")
    try:
        cap = int(raw)
    except (TypeError, ValueError):
        cap = 15
    return max(DAILY_MESSAGE_LIMIT_MIN, min(cap, DAILY_MESSAGE_LIMIT_MAX))


def policy_weekly_connect_cap(policy: Mapping[str, Any] | None) -> int:
    connect = (
        policy.get("connect_invites")
        if isinstance(policy, Mapping) and isinstance(policy.get("connect_invites"), dict)
        else {}
    )
    raw = connect.get("weekly_cap_per_account")
    try:
        cap = int(raw)
    except (TypeError, ValueError):
        cap = 100
    return max(WEEKLY_CONNECT_LIMIT_MIN, min(cap, WEEKLY_CONNECT_LIMIT_MAX))


def effective_daily_message_limit(account: Mapping[str, Any], policy: Mapping[str, Any] | None) -> int:
    override = _coerce_positive_int(account.get("daily_message_limit"))
    if override is not None:
        return max(DAILY_MESSAGE_LIMIT_MIN, min(override, DAILY_MESSAGE_LIMIT_MAX))
    return policy_daily_new_threads_cap(policy)


def effective_daily_connect_limit(
    account: Mapping[str, Any],
    policy: Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
) -> int:
    override = _coerce_positive_int(account.get("daily_connect_limit"))
    if override is not None:
        return max(DAILY_CONNECT_LIMIT_MIN, min(override, DAILY_CONNECT_LIMIT_MAX))
    return policy_allowed_connects_today(policy, account, now=now)


def policy_allowed_connects_today(
    policy: Mapping[str, Any] | None,
    account: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> int:
    weekly_cap = policy_weekly_connect_cap(policy)
    return max(1, min(weekly_cap // 7, weekly_cap))


def resolve_account_limit_snapshot(account: Mapping[str, Any], policy: Mapping[str, Any] | None) -> Dict[str, int]:
    return {
        "effective_daily_message_limit": int(effective_daily_message_limit(account, policy)),
        "effective_daily_connect_limit": int(effective_daily_connect_limit(account, policy)),
        "effective_weekly_connect_limit": int(policy_weekly_connect_cap(policy)),
    }


def _parse_optional_limit(raw: Any, *, field_name: str, minimum: int, maximum: int) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer or null") from exc
    if value < minimum or value > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def _coerce_positive_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def _parse_iso_datetime(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
