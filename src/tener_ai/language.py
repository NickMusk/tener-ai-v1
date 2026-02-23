from __future__ import annotations

from typing import Iterable


def pick_candidate_language(candidates_languages: Iterable[str] | None, fallback: str = "en") -> str:
    if not candidates_languages:
        return fallback
    for lang in candidates_languages:
        if isinstance(lang, str) and lang.strip():
            return lang.strip().lower()
    return fallback


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    normalized = (text or "").lower()
    if not normalized:
        return fallback

    # Simple heuristic for V1 without external NLP dependencies.
    if any("а" <= ch <= "я" or ch == "ё" for ch in normalized):
        return "ru"
    if any(ch in normalized for ch in ("¿", "¡", "ñ", "á", "é", "í", "ó", "ú")):
        return "es"

    ru_markers = {"зарплат", "вилка", "удален", "стек", "собесед"}
    es_markers = {"salario", "remoto", "proceso", "entrevista", "stack"}

    if any(m in normalized for m in ru_markers):
        return "ru"
    if any(m in normalized for m in es_markers):
        return "es"
    return "en"
