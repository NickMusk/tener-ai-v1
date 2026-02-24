from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class Weights:
    technical: float = 0.5
    soft_skills: float = 0.3
    culture_fit: float = 0.2


DEFAULT_FORMULA: Dict[str, Any] = {
    "version": "1.0",
    "weights": {
        "technical": 0.5,
        "soft_skills": 0.3,
        "culture_fit": 0.2,
    },
    "missing_dimensions_strategy": "renormalize",
    "recommendation_thresholds": {
        "strong_yes": 85.0,
        "yes": 75.0,
        "mixed": 60.0,
    },
}


class InterviewScoringEngine:
    def __init__(
        self,
        weights: Optional[Weights] = None,
        *,
        formula: Optional[Dict[str, Any]] = None,
        formula_path: Optional[str] = None,
    ) -> None:
        loaded = self._load_formula(formula_path=formula_path)
        if formula and isinstance(formula, dict):
            loaded = self._merge_formula(base=loaded, override=formula)

        weight_src = loaded.get("weights") if isinstance(loaded.get("weights"), dict) else {}
        self.weights = weights or Weights(
            technical=self._safe_float(weight_src.get("technical"), DEFAULT_FORMULA["weights"]["technical"]),
            soft_skills=self._safe_float(weight_src.get("soft_skills"), DEFAULT_FORMULA["weights"]["soft_skills"]),
            culture_fit=self._safe_float(weight_src.get("culture_fit"), DEFAULT_FORMULA["weights"]["culture_fit"]),
        )
        self.formula_version = str(loaded.get("version") or DEFAULT_FORMULA["version"])
        self.missing_dimensions_strategy = str(
            loaded.get("missing_dimensions_strategy") or DEFAULT_FORMULA["missing_dimensions_strategy"]
        ).strip().lower()
        if self.missing_dimensions_strategy not in {"renormalize", "strict"}:
            self.missing_dimensions_strategy = "renormalize"

        thr = loaded.get("recommendation_thresholds") if isinstance(loaded.get("recommendation_thresholds"), dict) else {}
        strong_yes = self._safe_float(thr.get("strong_yes"), DEFAULT_FORMULA["recommendation_thresholds"]["strong_yes"])
        yes = self._safe_float(thr.get("yes"), DEFAULT_FORMULA["recommendation_thresholds"]["yes"])
        mixed = self._safe_float(thr.get("mixed"), DEFAULT_FORMULA["recommendation_thresholds"]["mixed"])
        if strong_yes < yes:
            strong_yes = yes
        if yes < mixed:
            yes = mixed
        self.recommendation_thresholds = {
            "strong_yes": strong_yes,
            "yes": yes,
            "mixed": mixed,
        }

    def normalize_provider_result(self, provider_payload: Dict[str, Any]) -> Dict[str, Any]:
        scores = provider_payload.get("scores") if isinstance(provider_payload, dict) else {}
        if not isinstance(scores, dict):
            scores = {}

        technical = self._to_score(scores.get("technical"))
        soft = self._to_score(scores.get("soft_skills"))
        culture = self._to_score(scores.get("culture_fit"))

        available_weights = 0.0
        weighted_sum = 0.0
        missing = []

        if technical is not None:
            weighted_sum += technical * self.weights.technical
            available_weights += self.weights.technical
        else:
            missing.append("technical")

        if soft is not None:
            weighted_sum += soft * self.weights.soft_skills
            available_weights += self.weights.soft_skills
        else:
            missing.append("soft_skills")

        if culture is not None:
            weighted_sum += culture * self.weights.culture_fit
            available_weights += self.weights.culture_fit
        else:
            missing.append("culture_fit")

        if self.missing_dimensions_strategy == "strict" and missing:
            total = None
        else:
            total = round(weighted_sum / available_weights, 2) if available_weights > 0 else None

        total_weight = self.weights.technical + self.weights.soft_skills + self.weights.culture_fit
        confidence = round(available_weights / total_weight, 4) if total_weight > 0 else 0.0
        recommendation = self._recommend(total)

        normalized = {
            "dimensions": {
                "technical": technical,
                "soft_skills": soft,
                "culture_fit": culture,
            },
            "missing_dimensions": missing,
            "weights": {
                "technical": self.weights.technical,
                "soft_skills": self.weights.soft_skills,
                "culture_fit": self.weights.culture_fit,
            },
            "available_weight": available_weights,
            "formula_version": self.formula_version,
            "missing_dimensions_strategy": self.missing_dimensions_strategy,
            "recommendation_thresholds": dict(self.recommendation_thresholds),
        }

        return {
            "technical_score": technical,
            "soft_skills_score": soft,
            "culture_fit_score": culture,
            "total_score": total,
            "score_confidence": confidence,
            "pass_recommendation": recommendation,
            "normalized_json": normalized,
            "raw_payload": provider_payload,
        }

    @staticmethod
    def _to_score(value: Any) -> Optional[float]:
        try:
            raw = float(value)
        except (TypeError, ValueError):
            return None
        if raw < 0:
            return 0.0
        if raw > 100:
            return 100.0
        return round(raw, 2)

    def _recommend(self, total_score: Optional[float]) -> str:
        if total_score is None:
            return "mixed"
        if total_score >= self.recommendation_thresholds["strong_yes"]:
            return "strong_yes"
        if total_score >= self.recommendation_thresholds["yes"]:
            return "yes"
        if total_score >= self.recommendation_thresholds["mixed"]:
            return "mixed"
        return "no"

    @staticmethod
    def _safe_float(value: Any, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _merge_formula(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(base)
        for key, value in override.items():
            if key in {"weights", "recommendation_thresholds"} and isinstance(value, dict):
                current = merged.get(key) if isinstance(merged.get(key), dict) else {}
                merged[key] = {**current, **value}
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _load_formula(formula_path: Optional[str]) -> Dict[str, Any]:
        base = dict(DEFAULT_FORMULA)
        base["weights"] = dict(DEFAULT_FORMULA["weights"])
        base["recommendation_thresholds"] = dict(DEFAULT_FORMULA["recommendation_thresholds"])
        if not formula_path:
            return base
        file_path = Path(formula_path)
        if not file_path.exists():
            return base
        try:
            loaded = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return base
        if not isinstance(loaded, dict):
            return base
        return InterviewScoringEngine._merge_formula(base=base, override=loaded)
