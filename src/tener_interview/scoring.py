from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Weights:
    technical: float = 0.5
    soft_skills: float = 0.3
    culture_fit: float = 0.2


class InterviewScoringEngine:
    def __init__(self, weights: Optional[Weights] = None) -> None:
        self.weights = weights or Weights()

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

        total = round(weighted_sum / available_weights, 2) if available_weights > 0 else None

        confidence = round(available_weights / (self.weights.technical + self.weights.soft_skills + self.weights.culture_fit), 4)
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

    @staticmethod
    def _recommend(total_score: Optional[float]) -> str:
        if total_score is None:
            return "mixed"
        if total_score >= 85:
            return "strong_yes"
        if total_score >= 75:
            return "yes"
        if total_score >= 60:
            return "mixed"
        return "no"
