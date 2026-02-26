from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

EVENT_TYPES = {
    "sourced",
    "signal_detected",
    "message_received",
    "message_sent",
    "score_update",
    "interview_complete",
    "shortlisted",
    "eliminated",
    "system",
    "phase_marker",
    "batch_eliminated",
    "no_reply",
}

STAGES = {
    "sourced",
    "filtered",
    "in_dialogue",
    "interviewed",
    "shortlisted",
    "eliminated",
}

SENTIMENTS = {"positive", "negative", "neutral"}

SIGNAL_CATEGORIES = {
    "career_trajectory",
    "communication",
    "digital_footprint",
    "interview",
    "timing",
    "linkedin_profile_depth",
    "cv_consistency",
    "skills_depth",
    "domain_expertise",
    "portfolio_quality",
    "github_activity",
    "social_presence",
    "reference_signal",
    "culture_fit",
    "psychological_profile",
    "learning_agility",
    "salary_alignment",
    "availability_risk",
    "leadership_signal",
    "collaboration_style",
}


def _ensure_dict(value: Any, label: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _ensure_list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _required_str(obj: Dict[str, Any], key: str, label: str) -> str:
    raw = obj.get(key)
    if not isinstance(raw, str):
        raise ValueError(f"{label}.{key} must be a string")
    text = raw.strip()
    if not text:
        raise ValueError(f"{label}.{key} cannot be empty")
    return text


def _optional_str(obj: Dict[str, Any], key: str) -> Optional[str]:
    raw = obj.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError(f"{key} must be a string when provided")
    text = raw.strip()
    return text or None


def _required_number(obj: Dict[str, Any], key: str, label: str) -> float:
    raw = obj.get(key)
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        raise ValueError(f"{label}.{key} must be a number")
    return float(raw)


def _optional_number(obj: Dict[str, Any], key: str, label: str) -> Optional[float]:
    raw = obj.get(key)
    if raw is None:
        return None
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        raise ValueError(f"{label}.{key} must be a number when provided")
    return float(raw)


def _optional_bool(obj: Dict[str, Any], key: str, default: bool = False) -> bool:
    raw = obj.get(key)
    if raw is None:
        return default
    if not isinstance(raw, bool):
        raise ValueError(f"{key} must be a boolean when provided")
    return raw


def _sanitize_json(value: Any, depth: int = 0) -> Any:
    if depth > 6:
        raise ValueError("companyProfile nesting is too deep")
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_sanitize_json(item, depth + 1) for item in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("companyProfile keys must be strings")
            clean_key = key.strip()
            if not clean_key:
                continue
            out[clean_key] = _sanitize_json(item, depth + 1)
        return out
    raise ValueError("companyProfile contains unsupported value type")


def _normalize_signal(signal: Any, index: int, candidate_label: str) -> Dict[str, Any]:
    obj = _ensure_dict(signal, f"{candidate_label}.signals[{index}]")
    category = _required_str(obj, "category", f"{candidate_label}.signals[{index}]")
    if category not in SIGNAL_CATEGORIES:
        raise ValueError(f"{candidate_label}.signals[{index}].category has unsupported value '{category}'")
    sentiment = _required_str(obj, "sentiment", f"{candidate_label}.signals[{index}]")
    if sentiment not in SENTIMENTS:
        raise ValueError(f"{candidate_label}.signals[{index}].sentiment has unsupported value '{sentiment}'")
    return {
        "id": _optional_str(obj, "id") or f"sig-{index + 1}",
        "title": _required_str(obj, "title", f"{candidate_label}.signals[{index}]"),
        "detail": _required_str(obj, "detail", f"{candidate_label}.signals[{index}]"),
        "category": category,
        "impact": _optional_number(obj, "impact", f"{candidate_label}.signals[{index}]"),
        "sentiment": sentiment,
    }


def _normalize_candidate(candidate: Any, index: int) -> Dict[str, Any]:
    label = f"candidates[{index}]"
    obj = _ensure_dict(candidate, label)
    stage = _required_str(obj, "stage", label)
    if stage not in STAGES:
        raise ValueError(f"{label}.stage has unsupported value '{stage}'")

    raw_signals = _ensure_list(obj.get("signals") or [], f"{label}.signals")
    signals: List[Dict[str, Any]] = []
    for signal_index, signal in enumerate(raw_signals):
        signals.append(_normalize_signal(signal, signal_index, label))

    confidence = _required_number(obj, "currentConfidence", label)
    if confidence < 0 or confidence > 100:
        raise ValueError(f"{label}.currentConfidence must be in range 0..100")

    return {
        "id": _required_str(obj, "id", label),
        "name": _required_str(obj, "name", label),
        "location": _required_str(obj, "location", label),
        "experience": _required_str(obj, "experience", label),
        "currentScore": _required_number(obj, "currentScore", label),
        "currentConfidence": confidence,
        "stage": stage,
        "signals": signals,
        "highlight": _optional_bool(obj, "highlight", False),
    }


def _normalize_event(event: Any, index: int, known_candidate_ids: Sequence[str]) -> Dict[str, Any]:
    label = f"events[{index}]"
    obj = _ensure_dict(event, label)
    event_type = _required_str(obj, "type", label)
    if event_type not in EVENT_TYPES:
        raise ValueError(f"{label}.type has unsupported value '{event_type}'")

    sentiment = _required_str(obj, "sentiment", label)
    if sentiment not in SENTIMENTS:
        raise ValueError(f"{label}.sentiment has unsupported value '{sentiment}'")

    candidate_id = _optional_str(obj, "candidateId")
    if candidate_id and candidate_id not in known_candidate_ids:
        raise ValueError(f"{label}.candidateId '{candidate_id}' was not declared in candidates")

    candidate_ids_raw = obj.get("candidateIds")
    candidate_ids: List[str] = []
    if candidate_ids_raw is not None:
        for idx, value in enumerate(_ensure_list(candidate_ids_raw, f"{label}.candidateIds")):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label}.candidateIds[{idx}] must be a non-empty string")
            candidate_key = value.strip()
            if candidate_key not in known_candidate_ids:
                raise ValueError(f"{label}.candidateIds[{idx}] references unknown candidate '{candidate_key}'")
            candidate_ids.append(candidate_key)

    if candidate_id and candidate_id not in candidate_ids:
        candidate_ids.append(candidate_id)

    requires_candidate = event_type in {
        "sourced",
        "signal_detected",
        "message_received",
        "message_sent",
        "score_update",
        "interview_complete",
        "shortlisted",
        "eliminated",
        "no_reply",
    }
    if requires_candidate and not candidate_ids:
        raise ValueError(f"{label} requires candidateId or candidateIds")

    signal_category = _optional_str(obj, "signalCategory")
    if signal_category and signal_category not in SIGNAL_CATEGORIES:
        raise ValueError(f"{label}.signalCategory has unsupported value '{signal_category}'")

    new_confidence = _optional_number(obj, "newConfidence", label)
    if new_confidence is not None and (new_confidence < 0 or new_confidence > 100):
        raise ValueError(f"{label}.newConfidence must be in range 0..100")

    return {
        "id": _optional_str(obj, "id") or f"event-{index + 1}",
        "timestamp": _required_str(obj, "timestamp", label),
        "candidateId": candidate_id,
        "candidateIds": candidate_ids,
        "type": event_type,
        "title": _required_str(obj, "title", label),
        "detail": _required_str(obj, "detail", label),
        "scoreChange": _optional_number(obj, "scoreChange", label),
        "newScore": _optional_number(obj, "newScore", label),
        "newConfidence": new_confidence,
        "signalCategory": signal_category,
        "sentiment": sentiment,
    }


def _normalize_reveal(reveal: Any, candidate_ids: Sequence[str]) -> Dict[str, Any]:
    label = "reveal"
    obj = _ensure_dict(reveal, label)
    hired_candidate_id = _required_str(obj, "hiredCandidateId", label)
    if hired_candidate_id not in candidate_ids:
        raise ValueError("reveal.hiredCandidateId was not declared in candidates")
    tener_top_pick = _required_str(obj, "tenerTopPick", label)
    if tener_top_pick not in candidate_ids:
        raise ValueError("reveal.tenerTopPick was not declared in candidates")

    funnel = obj.get("funnel") or {}
    funnel_obj = _ensure_dict(funnel, "reveal.funnel") if funnel else {}

    return {
        "hiredCandidateId": hired_candidate_id,
        "hiredOutcome": _required_str(obj, "hiredOutcome", label),
        "tenerTopPick": tener_top_pick,
        "tenerOutcome": _required_str(obj, "tenerOutcome", label),
        "actualCost": _optional_str(obj, "actualCost"),
        "tenerCost": _optional_str(obj, "tenerCost"),
        "actualTime": _optional_str(obj, "actualTime"),
        "tenerTime": _optional_str(obj, "tenerTime"),
        "funnel": {
            "sourced": int(funnel_obj.get("sourced") or 0),
            "filtered": int(funnel_obj.get("filtered") or 0),
            "outreach": int(funnel_obj.get("outreach") or 0),
            "engaged": int(funnel_obj.get("engaged") or 0),
            "shortlisted": int(funnel_obj.get("shortlisted") or 0),
        },
    }


def normalize_emulator_project(payload: Any, *, source: str) -> Dict[str, Any]:
    obj = _ensure_dict(payload, source)

    candidates_raw = _ensure_list(obj.get("candidates"), f"{source}.candidates")
    if not candidates_raw:
        raise ValueError(f"{source}.candidates cannot be empty")

    candidates: List[Dict[str, Any]] = []
    candidate_ids: List[str] = []
    for index, candidate in enumerate(candidates_raw):
        normalized = _normalize_candidate(candidate, index)
        if normalized["id"] in candidate_ids:
            raise ValueError(f"{source}.candidates[{index}].id is duplicated")
        candidate_ids.append(normalized["id"])
        candidates.append(normalized)

    events_raw = _ensure_list(obj.get("events"), f"{source}.events")
    if not events_raw:
        raise ValueError(f"{source}.events cannot be empty")

    events: List[Dict[str, Any]] = []
    event_ids: List[str] = []
    for index, event in enumerate(events_raw):
        normalized_event = _normalize_event(event, index, candidate_ids)
        if normalized_event["id"] in event_ids:
            raise ValueError(f"{source}.events[{index}].id is duplicated")
        event_ids.append(normalized_event["id"])
        events.append(normalized_event)

    company_profile_raw = obj.get("companyProfile") or {}
    company_profile = _ensure_dict(company_profile_raw, f"{source}.companyProfile")

    return {
        "id": _required_str(obj, "id", source),
        "company": _required_str(obj, "company", source),
        "role": _required_str(obj, "role", source),
        "year": _required_str(obj, "year", source),
        "companyProfile": _sanitize_json(company_profile),
        "candidates": candidates,
        "events": events,
        "reveal": _normalize_reveal(obj.get("reveal"), candidate_ids),
    }
