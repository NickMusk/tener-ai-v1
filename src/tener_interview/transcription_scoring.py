from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


DEFAULT_CRITERIA: Dict[str, Any] = {
    "version": "default",
    "defaults": {
        "weights": {
            "keyword": 0.6,
            "length": 0.25,
            "clarity": 0.15,
        },
        "rubric": {
            "min_words": 20,
            "ideal_words": 80,
            "max_words": 260,
        },
        "disallowed_patterns": ["\\bno\\s+idea\\b", "\\bi\\s+don'?t\\s+know\\b"],
        "filler_words": ["um", "uh", "like", "you know", "kind of", "sort of"],
    },
    "question_rules": [],
}


class TranscriptionScoringEngine:
    def __init__(self, criteria_path: str) -> None:
        self.criteria_path = criteria_path
        self.criteria = self._load(criteria_path)

    def score_provider_payload(self, provider_payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = provider_payload.get("raw") if isinstance(provider_payload, dict) else {}
        interview = raw if isinstance(raw, dict) else {}
        questions = interview.get("questions") if isinstance(interview.get("questions"), list) else []
        if not questions:
            return {
                "applied": False,
                "reason": "no_questions",
                "criteria_version": self.criteria.get("version"),
                "question_scores": [],
                "scores": {},
            }

        scored_questions: List[Dict[str, Any]] = []
        technical_values: List[float] = []
        soft_values: List[float] = []
        culture_values: List[float] = []

        missing_transcriptions = 0
        for idx, question in enumerate(questions):
            if not isinstance(question, dict):
                continue
            title = str(question.get("title") or "").strip()
            description = str(question.get("description") or "").strip()
            answer = question.get("answer") if isinstance(question.get("answer"), dict) else {}
            transcription = self._dig(answer, "transcription", "text")
            text = str(transcription or "").strip()
            if not text:
                missing_transcriptions += 1

            matched_rule = self._match_rule(title=title, description=description)
            dimension = (
                str(matched_rule.get("dimension") or "").strip().lower()
                if isinstance(matched_rule, dict)
                else ""
            )
            if dimension not in {"technical", "soft_skills", "culture_fit"}:
                dimension = self._dimension_from_category(str(question.get("category") or ""))
            if dimension not in {"technical", "soft_skills", "culture_fit"}:
                dimension = self._infer_dimension(title=title, description=description)

            if not text:
                scored_questions.append(
                    {
                        "question_index": idx,
                        "question_id": question.get("id"),
                        "title": title,
                        "dimension": dimension,
                        "score": None,
                        "status": "missing_transcription",
                    }
                )
                continue

            evaluated = self._score_transcription(text=text, rule=matched_rule)
            question_score = float(evaluated["score"])
            scored_questions.append(
                {
                    "question_index": idx,
                    "question_id": question.get("id"),
                    "title": title,
                    "dimension": dimension,
                    "score": question_score,
                    "status": "scored",
                    "details": evaluated["details"],
                }
            )

            if dimension == "culture_fit":
                culture_values.append(question_score)
            elif dimension == "soft_skills":
                soft_values.append(question_score)
            else:
                technical_values.append(question_score)

        scored_count = len([x for x in scored_questions if x.get("score") is not None])
        if scored_count == 0:
            return {
                "applied": False,
                "reason": "no_transcriptions",
                "criteria_version": self.criteria.get("version"),
                "question_scores": scored_questions,
                "scores": {},
                "coverage": {
                    "total_questions": len([q for q in questions if isinstance(q, dict)]),
                    "scored_questions": 0,
                    "missing_transcriptions": missing_transcriptions,
                },
            }

        scores = {
            "technical": self._average(technical_values),
            "soft_skills": self._average(soft_values),
            "culture_fit": self._average(culture_values),
        }
        return {
            "applied": True,
            "reason": "ok",
            "criteria_version": self.criteria.get("version"),
            "scores": scores,
            "question_scores": scored_questions,
            "coverage": {
                "total_questions": len([q for q in questions if isinstance(q, dict)]),
                "scored_questions": scored_count,
                "missing_transcriptions": missing_transcriptions,
            },
        }

    @staticmethod
    def _load(path: str) -> Dict[str, Any]:
        file_path = Path(path)
        if not file_path.exists():
            return dict(DEFAULT_CRITERIA)
        try:
            raw = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            return dict(DEFAULT_CRITERIA)
        if not isinstance(raw, dict):
            return dict(DEFAULT_CRITERIA)
        out = dict(DEFAULT_CRITERIA)
        out.update(raw)
        return out

    def _match_rule(self, title: str, description: str) -> Dict[str, Any]:
        rules = self.criteria.get("question_rules")
        if not isinstance(rules, list):
            return {}
        haystack = f"{title} {description}".lower()
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            match = rule.get("match")
            if not isinstance(match, dict):
                continue
            title_contains = match.get("title_contains")
            if not isinstance(title_contains, list) or not title_contains:
                continue
            if any(str(token).strip().lower() in haystack for token in title_contains if str(token).strip()):
                return rule
        return {}

    def _score_transcription(self, text: str, rule: Dict[str, Any]) -> Dict[str, Any]:
        defaults = self.criteria.get("defaults") if isinstance(self.criteria.get("defaults"), dict) else {}
        default_weights = defaults.get("weights") if isinstance(defaults.get("weights"), dict) else {}
        default_rubric = defaults.get("rubric") if isinstance(defaults.get("rubric"), dict) else {}
        default_disallowed = defaults.get("disallowed_patterns") if isinstance(defaults.get("disallowed_patterns"), list) else []
        default_fillers = defaults.get("filler_words") if isinstance(defaults.get("filler_words"), list) else []

        weights = dict(default_weights)
        if isinstance(rule.get("weights"), dict):
            weights.update(rule.get("weights"))
        w_keyword = self._safe_float(weights.get("keyword"), 0.6)
        w_length = self._safe_float(weights.get("length"), 0.25)
        w_clarity = self._safe_float(weights.get("clarity"), 0.15)

        rubric = dict(default_rubric)
        if isinstance(rule.get("rubric"), dict):
            rubric.update(rule.get("rubric"))
        min_words = max(1, int(self._safe_float(rubric.get("min_words"), 20)))
        ideal_words = max(min_words, int(self._safe_float(rubric.get("ideal_words"), 80)))
        max_words = max(ideal_words, int(self._safe_float(rubric.get("max_words"), 260)))

        required = self._to_str_list(rule.get("required_keywords"))
        optional = self._to_str_list(rule.get("optional_keywords"))
        disallowed = self._to_str_list(rule.get("disallowed_patterns")) or self._to_str_list(default_disallowed)
        fillers = self._to_str_list(rule.get("filler_words")) or self._to_str_list(default_fillers)

        lower = text.lower()
        words = self._words(text)
        word_count = len(words)
        sentences = [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]

        matched_required = [k for k in required if k in lower]
        matched_optional = [k for k in optional if k in lower]
        required_ratio = (len(matched_required) / len(required)) if required else 1.0
        optional_ratio = (len(matched_optional) / len(optional)) if optional else (0.0 if required else 1.0)

        keyword_score = 100.0 * (0.7 * required_ratio + 0.3 * optional_ratio)
        if not required and not optional:
            keyword_score = 70.0

        length_score = self._length_score(word_count=word_count, min_words=min_words, ideal_words=ideal_words, max_words=max_words)
        clarity_score = self._clarity_score(words=words, sentences=sentences, text=text, filler_words=fillers)

        penalties = 0.0
        matched_disallowed: List[str] = []
        for pattern in disallowed:
            try:
                if re.search(pattern, lower, flags=re.IGNORECASE):
                    penalties += 12.0
                    matched_disallowed.append(pattern)
            except re.error:
                if pattern.lower() in lower:
                    penalties += 12.0
                    matched_disallowed.append(pattern)
        penalties = min(36.0, penalties)

        score = max(
            0.0,
            min(
                100.0,
                (keyword_score * w_keyword) + (length_score * w_length) + (clarity_score * w_clarity) - penalties,
            ),
        )
        return {
            "score": round(score, 2),
            "details": {
                "word_count": word_count,
                "required_keywords": required,
                "optional_keywords": optional,
                "matched_required_keywords": matched_required,
                "matched_optional_keywords": matched_optional,
                "matched_disallowed_patterns": matched_disallowed,
                "subscores": {
                    "keyword_score": round(keyword_score, 2),
                    "length_score": round(length_score, 2),
                    "clarity_score": round(clarity_score, 2),
                    "penalties": round(penalties, 2),
                },
            },
        }

    @staticmethod
    def _dimension_from_category(category: str) -> str:
        value = str(category or "").strip().lower()
        if value in {"culture_fit", "cultural_fit", "culture"}:
            return "culture_fit"
        if value in {"soft_skills", "soft skills", "soft"}:
            return "soft_skills"
        if value in {"hard_skills", "hard skills", "technical", "tech"}:
            return "technical"
        return ""

    @staticmethod
    def _infer_dimension(title: str, description: str) -> str:
        text = f"{title} {description}".lower()
        if any(token in text for token in ("culture", "values", "mission", "fit", "motivation")):
            return "culture_fit"
        if any(token in text for token in ("team", "collabor", "communication", "stakeholder", "leadership", "conflict")):
            return "soft_skills"
        return "technical"

    @staticmethod
    def _length_score(word_count: int, min_words: int, ideal_words: int, max_words: int) -> float:
        if word_count <= 0:
            return 0.0
        if word_count < min_words:
            return max(0.0, min(100.0, 25.0 + (75.0 * (word_count / float(min_words)))))
        if word_count <= ideal_words:
            span = max(1, ideal_words - min_words)
            return 70.0 + (30.0 * ((word_count - min_words) / float(span)))
        if word_count <= max_words:
            return 100.0
        overflow = word_count - max_words
        return max(55.0, 100.0 - (overflow * 0.2))

    @staticmethod
    def _clarity_score(words: List[str], sentences: List[str], text: str, filler_words: List[str]) -> float:
        score = 100.0
        if len(sentences) < 2:
            score -= 20.0
        avg_sentence_len = (len(words) / float(len(sentences))) if sentences else float(len(words))
        if avg_sentence_len > 45.0:
            score -= 20.0
        filler_hits = sum(text.lower().count(f.lower()) for f in filler_words if f)
        if filler_hits >= 6:
            score -= 15.0
        if re.search(r"(.)\1\1\1", text):
            score -= 15.0
        return max(40.0, min(100.0, score))

    @staticmethod
    def _words(text: str) -> List[str]:
        return re.findall(r"[A-Za-z0-9\u0400-\u04FF+#.-]+", text)

    @staticmethod
    def _average(values: Iterable[float]) -> Optional[float]:
        items = [float(v) for v in values]
        if not items:
            return None
        return round(sum(items) / float(len(items)), 2)

    @staticmethod
    def _dig(obj: Dict[str, Any], *keys: str) -> Any:
        cur: Any = obj
        for key in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    @staticmethod
    def _safe_float(value: Any, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _to_str_list(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        out: List[str] = []
        for item in value:
            token = str(item or "").strip()
            if token:
                out.append(token.lower())
        return out
