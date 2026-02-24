from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_SCORING_POLICY: Dict[str, Any] = {
    "version": "default",
    "weights": {
        "sourcing_vetting": 0.45,
        "communication": 0.20,
        "interview_evaluation": 0.35,
    },
    "gates": {
        "blocked_statuses": ["not_interested", "unreachable"],
        "cap_without_cv": 70.0,
        "cap_without_interview_score": 80.0,
    },
    "decisions": {
        "shortlist_min": 80.0,
        "pipeline_min": 65.0,
    },
}


class CandidateScoringPolicy:
    def __init__(self, path: str | None = None) -> None:
        self.path = path
        self.payload = self._load(path)

    def reload(self) -> None:
        self.payload = self._load(self.path)

    def decorate_candidate_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(row)
        scorecard = item.get("agent_scorecard") if isinstance(item.get("agent_scorecard"), dict) else {}
        current_status_key = str(item.get("current_status_key") or "").strip().lower()
        overall = self.compute_overall(scorecard=scorecard, current_status_key=current_status_key)
        item["overall_scoring"] = overall
        item["overall_score"] = overall.get("overall_score")
        item["overall_status"] = overall.get("overall_status")
        item["overall_block_reason"] = overall.get("block_reason")
        return item

    def compute_overall(self, scorecard: Dict[str, Any], current_status_key: str) -> Dict[str, Any]:
        weights = self._weights()
        decisions = self._decisions()
        gates = self._gates()

        source_score = self._score_of(scorecard, "sourcing_vetting")
        communication_score = self._communication_dialogue_score(scorecard)
        interview_score = self._score_of(scorecard, "interview_evaluation")

        inputs = {
            "sourcing_vetting": source_score,
            "communication": communication_score,
            "interview_evaluation": interview_score,
        }
        weighted_sum = 0.0
        total_weight = 0.0
        for key, value in inputs.items():
            if value is None:
                continue
            w = float(weights.get(key, 0.0))
            weighted_sum += w * float(value)
            total_weight += w
        raw_score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
        final_score = float(raw_score)
        gate_reasons: List[str] = []

        blocked_statuses = set(gates["blocked_statuses"])
        communication_status = self._status_of(scorecard, "communication")
        blocked = current_status_key in blocked_statuses or communication_status in blocked_statuses
        block_reason = None
        if blocked:
            final_score = 0.0
            if current_status_key in blocked_statuses:
                block_reason = f"candidate_status:{current_status_key}"
            elif communication_status in blocked_statuses:
                block_reason = f"communication_status:{communication_status}"
            else:
                block_reason = "blocked_gate"
            gate_reasons.append(f"blocked:{block_reason}")

        has_all_scores = all(value is not None for value in (source_score, communication_score, interview_score))
        has_cv = (
            current_status_key in {"cv_received", "interview_invited", "interview_in_progress", "interview_completed", "interview_scored"}
            or communication_status == "cv_received"
            or has_all_scores
        )
        if not has_all_scores and not has_cv and final_score > gates["cap_without_cv"]:
            final_score = gates["cap_without_cv"]
            gate_reasons.append("cap_without_cv")

        has_interview_score = interview_score is not None
        if not has_all_scores and not has_interview_score and final_score > gates["cap_without_interview_score"]:
            final_score = gates["cap_without_interview_score"]
            gate_reasons.append("cap_without_interview_score")

        if blocked:
            overall_status = "blocked"
        elif not has_all_scores:
            overall_status = "review"
        elif final_score >= decisions["shortlist_min"]:
            overall_status = "shortlist"
        elif final_score >= decisions["pipeline_min"]:
            overall_status = "pipeline"
        else:
            overall_status = "review"

        overall_score: float | None = round(max(0.0, min(100.0, float(final_score))), 2) if has_all_scores else None

        return {
            "version": self.payload.get("version", "unknown"),
            "overall_score": overall_score,
            "overall_status": overall_status,
            "block_reason": block_reason,
            "raw_score": round(max(0.0, min(100.0, float(raw_score))), 2),
            "weights": dict(weights),
            "inputs": inputs,
            "gates_applied": gate_reasons,
            "has_cv": has_cv,
            "has_interview_score": has_interview_score,
            "has_all_scores": has_all_scores,
        }

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.payload)

    def _weights(self) -> Dict[str, float]:
        raw = self.payload.get("weights") if isinstance(self.payload.get("weights"), dict) else {}
        return {
            "sourcing_vetting": self._safe_float(raw.get("sourcing_vetting"), 0.45),
            "communication": self._safe_float(raw.get("communication"), 0.20),
            "interview_evaluation": self._safe_float(raw.get("interview_evaluation"), 0.35),
        }

    def _gates(self) -> Dict[str, Any]:
        raw = self.payload.get("gates") if isinstance(self.payload.get("gates"), dict) else {}
        blocked_raw = raw.get("blocked_statuses")
        blocked = blocked_raw if isinstance(blocked_raw, list) else ["not_interested", "unreachable"]
        normalized_blocked = [str(x).strip().lower() for x in blocked if str(x).strip()]
        return {
            "blocked_statuses": normalized_blocked or ["not_interested", "unreachable"],
            "cap_without_cv": self._safe_float(raw.get("cap_without_cv"), 70.0),
            "cap_without_interview_score": self._safe_float(raw.get("cap_without_interview_score"), 80.0),
        }

    def _decisions(self) -> Dict[str, float]:
        raw = self.payload.get("decisions") if isinstance(self.payload.get("decisions"), dict) else {}
        shortlist = self._safe_float(raw.get("shortlist_min"), 80.0)
        pipeline = self._safe_float(raw.get("pipeline_min"), 65.0)
        if pipeline > shortlist:
            pipeline = shortlist
        return {
            "shortlist_min": shortlist,
            "pipeline_min": pipeline,
        }

    @staticmethod
    def _score_of(scorecard: Dict[str, Any], key: str) -> float | None:
        item = scorecard.get(key) if isinstance(scorecard.get(key), dict) else {}
        value = item.get("latest_score")
        if value is None:
            return None
        try:
            score = float(value)
        except (TypeError, ValueError):
            return None
        return max(0.0, min(100.0, score))

    @staticmethod
    def _status_of(scorecard: Dict[str, Any], key: str) -> str:
        item = scorecard.get(key) if isinstance(scorecard.get(key), dict) else {}
        return str(item.get("latest_status") or "").strip().lower()

    @classmethod
    def _communication_dialogue_score(cls, scorecard: Dict[str, Any]) -> float | None:
        entry = scorecard.get("communication") if isinstance(scorecard.get("communication"), dict) else {}
        stage = str(entry.get("latest_stage") or "").strip().lower()
        if stage != "dialogue":
            return None
        return cls._score_of(scorecard, "communication")

    @staticmethod
    def _safe_float(value: Any, fallback: float) -> float:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return fallback
        if num < 0:
            return 0.0
        if num > 100:
            return 100.0
        return num

    @staticmethod
    def _load(path: str | None) -> Dict[str, Any]:
        if not path:
            return dict(DEFAULT_SCORING_POLICY)
        file_path = Path(path)
        if not file_path.exists():
            return dict(DEFAULT_SCORING_POLICY)
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return dict(DEFAULT_SCORING_POLICY)
        return data
