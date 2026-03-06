from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional


VALID_ROLES = {"evaluative", "administrative", "governance"}
VALID_DETECTORS = {"algorithmic", "llm", "hybrid"}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, min_value: float, max_value: float) -> float:
    if value < min_value:
        return min_value
    if value > max_value:
        return max_value
    return value


class SignalRulesEngine:
    """Classifies signals and normalizes their scoring impact from config-driven rules."""

    def __init__(self, rules_path: Optional[str] = None) -> None:
        configured_path = str(rules_path or os.environ.get("TENER_SIGNAL_RULES_PATH", "")).strip()
        self.rules_path = Path(configured_path) if configured_path else (_project_root() / "config" / "signal_rules.json")
        self.rules_version = "builtin"
        self.defaults: Dict[str, Any] = {}
        self.rules: List[Dict[str, Any]] = []
        self.reload()

    def reload(self) -> None:
        payload: Dict[str, Any] = {}
        if self.rules_path.exists():
            try:
                payload = json.loads(self.rules_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}

        defaults = payload.get("defaults") if isinstance(payload.get("defaults"), dict) else {}
        raw_rules = payload.get("rules") if isinstance(payload.get("rules"), list) else []

        self.rules_version = str(payload.get("version") or "builtin").strip() or "builtin"
        self.defaults = self._normalize_fragment(defaults)
        self.rules = []
        for index, rule in enumerate(raw_rules, start=1):
            if not isinstance(rule, dict):
                continue
            when = rule.get("when") if isinstance(rule.get("when"), dict) else {}
            fragment = self._normalize_fragment(rule)
            rule_id = str(rule.get("id") or f"rule_{index}").strip() or f"rule_{index}"
            self.rules.append(
                {
                    "id": rule_id,
                    "when": when,
                    "fragment": fragment,
                }
            )

    def classify_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        meta = signal.get("signal_meta") if isinstance(signal.get("signal_meta"), dict) else {}
        role = self._normalize_role(meta.get("signal_role"), self.defaults.get("role") or "administrative")
        detector = self._normalize_detector(meta.get("detector"), self.defaults.get("detector") or "algorithmic")
        signal_key = self._normalize_optional_str(meta.get("evaluative_signal_key"), self.defaults.get("signal_key"))
        score_weight = _clamp(
            _safe_float(meta.get("score_weight"), _safe_float(self.defaults.get("score_weight"), 0.0)),
            0.0,
            1.0,
        )
        impact_range = self._parse_range(self.defaults.get("impact_range"))
        confidence_range = self._parse_range(self.defaults.get("confidence_range"))
        matched_rule_id: Optional[str] = None

        for rule in self.rules:
            if self._rule_matches(rule.get("when") or {}, signal):
                matched_rule_id = str(rule.get("id") or "").strip() or None
                fragment = rule.get("fragment") if isinstance(rule.get("fragment"), dict) else {}
                role = self._normalize_role(fragment.get("role"), role)
                detector = self._normalize_detector(fragment.get("detector"), detector)
                signal_key = self._normalize_optional_str(fragment.get("signal_key"), signal_key)
                if "score_weight" in fragment:
                    score_weight = _clamp(_safe_float(fragment.get("score_weight"), score_weight), 0.0, 1.0)
                if "impact_range" in fragment:
                    impact_range = self._parse_range(fragment.get("impact_range"))
                if "confidence_range" in fragment:
                    confidence_range = self._parse_range(fragment.get("confidence_range"))
                break

        raw_impact = _safe_float(signal.get("impact_score"), 0.0)
        normalized_impact = raw_impact
        if impact_range is not None:
            normalized_impact = _clamp(normalized_impact, impact_range[0], impact_range[1])

        raw_confidence: Optional[float]
        if signal.get("confidence") is None:
            raw_confidence = None
        else:
            raw_confidence = _safe_float(signal.get("confidence"), 0.0)
        normalized_confidence = raw_confidence
        if normalized_confidence is not None and confidence_range is not None:
            normalized_confidence = _clamp(normalized_confidence, confidence_range[0], confidence_range[1])

        effective_weight = score_weight if role == "evaluative" else 0.0
        effective_impact = normalized_impact * effective_weight

        return {
            "role": role,
            "detector": detector,
            "signal_key": signal_key,
            "rule_id": matched_rule_id,
            "rules_version": self.rules_version,
            "score_weight": round(effective_weight, 3),
            "impact_score": round(normalized_impact, 3),
            "effective_impact": round(effective_impact, 3),
            "confidence": None if normalized_confidence is None else round(normalized_confidence, 3),
            "raw_impact_score": round(raw_impact, 3),
            "raw_confidence": None if raw_confidence is None else round(raw_confidence, 3),
        }

    @staticmethod
    def _normalize_fragment(raw: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if "role" in raw:
            out["role"] = raw.get("role")
        if "detector" in raw:
            out["detector"] = raw.get("detector")
        if "signal_key" in raw:
            out["signal_key"] = raw.get("signal_key")
        if "score_weight" in raw:
            out["score_weight"] = raw.get("score_weight")
        if "impact_range" in raw:
            out["impact_range"] = raw.get("impact_range")
        if "confidence_range" in raw:
            out["confidence_range"] = raw.get("confidence_range")
        return out

    def _rule_matches(self, when: Dict[str, Any], signal: Dict[str, Any]) -> bool:
        for path, expected in when.items():
            actual = self._extract_value(signal=signal, path=str(path))
            if isinstance(expected, list):
                if not any(self._value_matches(actual, item) for item in expected):
                    return False
            else:
                if not self._value_matches(actual, expected):
                    return False
        return True

    def _extract_value(self, *, signal: Dict[str, Any], path: str) -> Any:
        if not path:
            return None
        parts = path.split(".")
        if parts[0] == "meta":
            value: Any = signal.get("signal_meta") if isinstance(signal.get("signal_meta"), dict) else {}
            parts = parts[1:]
        else:
            value = signal
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _value_matches(self, actual: Any, expected: Any) -> bool:
        if isinstance(expected, str):
            expected_text = expected.strip().lower()
            if isinstance(actual, list):
                return any(self._value_matches(item, expected_text) for item in actual)
            actual_text = str(actual or "").strip().lower()
            if expected_text.endswith("*"):
                return actual_text.startswith(expected_text[:-1])
            return actual_text == expected_text
        if isinstance(expected, (int, float)):
            return _safe_float(actual, float("nan")) == float(expected)
        if isinstance(expected, bool):
            return bool(actual) is bool(expected)
        return actual == expected

    @staticmethod
    def _normalize_role(value: Any, fallback: str) -> str:
        text = str(value or "").strip().lower()
        if text in VALID_ROLES:
            return text
        return str(fallback or "administrative").strip().lower() or "administrative"

    @staticmethod
    def _normalize_detector(value: Any, fallback: str) -> str:
        text = str(value or "").strip().lower()
        if text in VALID_DETECTORS:
            return text
        return str(fallback or "algorithmic").strip().lower() or "algorithmic"

    @staticmethod
    def _normalize_optional_str(value: Any, fallback: Any = None) -> Optional[str]:
        text = str(value or "").strip()
        if text:
            return text
        fallback_text = str(fallback or "").strip()
        return fallback_text or None

    @staticmethod
    def _parse_range(value: Any) -> Optional[List[float]]:
        if not isinstance(value, list) or len(value) != 2:
            return None
        lo = _safe_float(value[0], 0.0)
        hi = _safe_float(value[1], 0.0)
        if lo > hi:
            lo, hi = hi, lo
        return [lo, hi]
