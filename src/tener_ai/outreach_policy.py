from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_OUTREACH_POLICY: Dict[str, Any] = {
    "version": "default",
    "provider": "unipile",
    "multi_account_profile_pool": {
        "enabled": True,
        "target_accounts": {"min": 50, "max": 100},
    },
    "connect_invites": {
        "weekly_cap_per_account": 100,
        "same_candidate_multi_account_allowed": True,
    },
    "outbound_messages": {
        "daily_new_threads_per_account": {"min": 10, "max": 15},
        "replies_unlimited": True,
    },
    "priority_routing": {
        "strategy": "highest_priority_first",
        "tie_breaker": "fifo",
    },
    "warmup": {
        "profile_completion_sequence": [],
        "invite_ramp": [],
        "target_stable_volume_week": 3,
    },
    "quiet_hours": {
        "enabled": False,
    },
}


class LinkedInOutreachPolicy:
    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.payload = self._load(path)

    def reload(self) -> None:
        self.payload = self._load(self.path)

    def to_dict(self) -> Dict[str, Any]:
        out = self._deep_copy(self.payload)
        out["source_path"] = self.path
        return out

    @classmethod
    def _load(cls, path: str | None) -> Dict[str, Any]:
        if not path:
            return cls._normalize(cls._deep_copy(DEFAULT_OUTREACH_POLICY))
        file_path = Path(path)
        if not file_path.exists():
            return cls._normalize(cls._deep_copy(DEFAULT_OUTREACH_POLICY))
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return cls._normalize(cls._deep_copy(DEFAULT_OUTREACH_POLICY))
        return cls._normalize(data)

    @classmethod
    def _normalize(cls, raw: Dict[str, Any]) -> Dict[str, Any]:
        base = cls._deep_copy(DEFAULT_OUTREACH_POLICY)
        payload: Dict[str, Any] = {
            "version": str(raw.get("version") or base["version"]).strip() or base["version"],
            "provider": str(raw.get("provider") or base["provider"]).strip().lower() or base["provider"],
        }

        pool_raw = raw.get("multi_account_profile_pool")
        pool_default = base["multi_account_profile_pool"]
        pool = pool_raw if isinstance(pool_raw, dict) else pool_default
        target_raw = pool.get("target_accounts") if isinstance(pool.get("target_accounts"), dict) else {}
        target_min = cls._safe_int(target_raw.get("min"), pool_default["target_accounts"]["min"], minimum=1, maximum=500)
        target_max = cls._safe_int(target_raw.get("max"), pool_default["target_accounts"]["max"], minimum=1, maximum=500)
        if target_min > target_max:
            target_min, target_max = target_max, target_min
        payload["multi_account_profile_pool"] = {
            "enabled": bool(pool.get("enabled", pool_default["enabled"])),
            "target_accounts": {
                "min": target_min,
                "max": target_max,
            },
        }

        connect_raw = raw.get("connect_invites")
        connect_default = base["connect_invites"]
        connect = connect_raw if isinstance(connect_raw, dict) else connect_default
        payload["connect_invites"] = {
            "weekly_cap_per_account": cls._safe_int(
                connect.get("weekly_cap_per_account"),
                connect_default["weekly_cap_per_account"],
                minimum=1,
                maximum=700,
            ),
            "same_candidate_multi_account_allowed": bool(
                connect.get("same_candidate_multi_account_allowed", connect_default["same_candidate_multi_account_allowed"])
            ),
        }

        messages_raw = raw.get("outbound_messages")
        messages_default = base["outbound_messages"]
        messages = messages_raw if isinstance(messages_raw, dict) else messages_default
        range_raw = messages.get("daily_new_threads_per_account")
        range_default = messages_default["daily_new_threads_per_account"]
        range_item = range_raw if isinstance(range_raw, dict) else range_default
        min_daily = cls._safe_int(range_item.get("min"), range_default["min"], minimum=1, maximum=200)
        max_daily = cls._safe_int(range_item.get("max"), range_default["max"], minimum=1, maximum=200)
        if min_daily > max_daily:
            min_daily, max_daily = max_daily, min_daily
        payload["outbound_messages"] = {
            "daily_new_threads_per_account": {
                "min": min_daily,
                "max": max_daily,
            },
            "replies_unlimited": bool(messages.get("replies_unlimited", messages_default["replies_unlimited"])),
        }

        routing_raw = raw.get("priority_routing")
        routing_default = base["priority_routing"]
        routing = routing_raw if isinstance(routing_raw, dict) else routing_default
        payload["priority_routing"] = {
            "strategy": str(routing.get("strategy") or routing_default["strategy"]).strip() or routing_default["strategy"],
            "tie_breaker": str(routing.get("tie_breaker") or routing_default["tie_breaker"]).strip()
            or routing_default["tie_breaker"],
        }

        warmup_raw = raw.get("warmup")
        warmup_default = base["warmup"]
        warmup = warmup_raw if isinstance(warmup_raw, dict) else warmup_default
        sequence = warmup.get("profile_completion_sequence")
        invite_ramp = warmup.get("invite_ramp")
        payload["warmup"] = {
            "profile_completion_sequence": sequence if isinstance(sequence, list) else warmup_default["profile_completion_sequence"],
            "invite_ramp": invite_ramp if isinstance(invite_ramp, list) else warmup_default["invite_ramp"],
            "target_stable_volume_week": cls._safe_int(
                warmup.get("target_stable_volume_week"),
                warmup_default["target_stable_volume_week"],
                minimum=1,
                maximum=12,
            ),
        }

        quiet_raw = raw.get("quiet_hours")
        quiet_default = base["quiet_hours"]
        quiet = quiet_raw if isinstance(quiet_raw, dict) else quiet_default
        payload["quiet_hours"] = {
            "enabled": bool(quiet.get("enabled", quiet_default["enabled"])),
        }
        return payload

    @staticmethod
    def _safe_int(value: Any, fallback: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = int(fallback)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @staticmethod
    def _deep_copy(payload: Dict[str, Any]) -> Dict[str, Any]:
        return json.loads(json.dumps(payload))
