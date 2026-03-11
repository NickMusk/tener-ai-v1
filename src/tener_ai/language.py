from __future__ import annotations

from typing import Iterable


LANGUAGE_ALIASES = {
    "ar": "ar",
    "arabic": "ar",
    "de": "de",
    "deutsch": "de",
    "en": "en",
    "english": "en",
    "es": "es",
    "espanol": "es",
    "español": "es",
    "fr": "fr",
    "francais": "fr",
    "français": "fr",
    "french": "fr",
    "german": "de",
    "it": "it",
    "italian": "it",
    "italiano": "it",
    "pl": "pl",
    "polish": "pl",
    "polski": "pl",
    "portuguese": "pt",
    "portugues": "pt",
    "português": "pt",
    "pt": "pt",
    "ru": "ru",
    "russian": "ru",
    "tr": "tr",
    "turkish": "tr",
    "turkce": "tr",
    "türkçe": "tr",
    "uk": "uk",
    "ukrainian": "uk",
    "русский": "ru",
    "українська": "uk",
    "العربية": "ar",
}

LANGUAGE_MARKERS = {
    "de": {"gehalt", "prozess", "rolle", "danke", "hallo"},
    "es": {"salario", "remoto", "proceso", "entrevista", "hola", "gracias", "aqui esta", "mi cv"},
    "fr": {"salaire", "processus", "entretien", "bonjour", "merci", "poste"},
    "it": {"stipendio", "processo", "colloquio", "ciao", "grazie", "ruolo"},
    "pl": {"wynagrodzenie", "proces", "rozmowa", "czesc", "cześć", "dziekuje", "dziękuję"},
    "pt": {"salario", "salário", "processo", "entrevista", "ola", "olá", "obrigado"},
    "ru": {"зарплат", "вилка", "удален", "стек", "собесед", "привет", "спасибо"},
    "tr": {"maas", "maaş", "surec", "süreç", "mulakat", "mülakat", "merhaba"},
}


def normalize_language(value: str | None, fallback: str = "") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    direct = LANGUAGE_ALIASES.get(text)
    if direct:
        return direct
    primary = text.replace("_", "-").split("-", 1)[0].strip()
    if not primary:
        return fallback
    return LANGUAGE_ALIASES.get(primary, primary)


def pick_candidate_language(candidates_languages: Iterable[str] | None, fallback: str = "en") -> str:
    if not candidates_languages:
        return normalize_language(fallback, fallback="en") or "en"
    for lang in candidates_languages:
        normalized = normalize_language(lang)
        if normalized:
            return normalized
    return normalize_language(fallback, fallback="en") or "en"


def detect_language_from_text_or_none(text: str) -> str | None:
    normalized = (text or "").strip().lower()
    if not normalized:
        return None

    if any("\u0600" <= ch <= "\u06ff" for ch in normalized):
        return "ar"
    if any(ch in normalized for ch in ("і", "ї", "є", "ґ")):
        return "uk"
    if any("а" <= ch <= "я" or ch == "ё" for ch in normalized):
        return "ru"

    char_markers = {
        "de": ("ä", "ö", "ü", "ß"),
        "es": ("¿", "¡", "ñ"),
        "fr": ("à", "â", "ç", "è", "ê", "ë", "î", "ï", "ô", "ù", "û", "ü", "œ"),
        "it": ("à", "è", "é", "ì", "í", "î", "ò", "ó", "ù"),
        "pl": ("ą", "ć", "ę", "ł", "ń", "ś", "ź", "ż"),
        "pt": ("ã", "õ"),
        "tr": ("ç", "ğ", "ı", "ö", "ş", "ü"),
    }
    for language, markers in char_markers.items():
        if any(marker in normalized for marker in markers):
            return language

    for language, markers in LANGUAGE_MARKERS.items():
        if any(marker in normalized for marker in markers):
            return language
    return None


def detect_language_from_text(text: str, fallback: str = "en") -> str:
    detected = detect_language_from_text_or_none(text)
    if detected:
        return detected
    return normalize_language(fallback, fallback="en") or "en"


def resolve_conversation_language(
    *,
    latest_message_text: str | None,
    previous_language: str | None = None,
    profile_languages: Iterable[str] | None = None,
    fallback: str = "en",
) -> str:
    detected = detect_language_from_text_or_none(str(latest_message_text or ""))
    if detected:
        return detected
    normalized_previous = normalize_language(previous_language)
    if normalized_previous:
        return normalized_previous
    return pick_candidate_language(profile_languages, fallback=fallback)
