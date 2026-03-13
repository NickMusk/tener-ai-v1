import unittest

from tener_ai.language import (
    detect_language_from_text,
    normalize_language,
    resolve_conversation_language,
    resolve_outbound_language,
)


class LanguageDetectionTests(unittest.TestCase):
    def test_detect_language_russian(self) -> None:
        self.assertEqual(detect_language_from_text("Какая вилка зарплаты?"), "ru")

    def test_detect_language_spanish(self) -> None:
        self.assertEqual(detect_language_from_text("¿Cuál es el salario?"), "es")

    def test_detect_language_german(self) -> None:
        self.assertEqual(detect_language_from_text("Hallo, wie ist der Interviewprozess?"), "de")

    def test_detect_language_arabic(self) -> None:
        self.assertEqual(detect_language_from_text("مرحبا، ما هو نطاق الراتب؟"), "ar")

    def test_detect_language_default_english(self) -> None:
        self.assertEqual(detect_language_from_text("What is the interview timeline?"), "en")

    def test_normalize_language_handles_aliases_and_locales(self) -> None:
        self.assertEqual(normalize_language("ES_mx"), "es")
        self.assertEqual(normalize_language("Deutsch"), "de")

    def test_resolve_conversation_language_prefers_latest_message(self) -> None:
        self.assertEqual(
            resolve_conversation_language(
                latest_message_text="Hola, gracias",
                previous_language="en",
                profile_languages=["en", "de"],
            ),
            "es",
        )

    def test_resolve_conversation_language_falls_back_to_previous_then_profile(self) -> None:
        self.assertEqual(
            resolve_conversation_language(
                latest_message_text="",
                previous_language="pt-BR",
                profile_languages=["en"],
            ),
            "pt",
        )
        self.assertEqual(
            resolve_conversation_language(
                latest_message_text="",
                previous_language="",
                profile_languages=["fr-FR", "en"],
            ),
            "fr",
        )

    def test_resolve_outbound_language_prefers_primary_locale_over_profile_languages(self) -> None:
        self.assertEqual(
            resolve_outbound_language(
                {
                    "primary_locale": "uk-UA",
                    "languages": ["en", "ru"],
                    "location": "Kyiv, Ukraine",
                }
            ),
            "uk",
        )

    def test_resolve_outbound_language_falls_back_to_ukrainian_location(self) -> None:
        self.assertEqual(
            resolve_outbound_language(
                {
                    "languages": [],
                    "location": "Kyiv, Ukraine",
                }
            ),
            "uk",
        )


if __name__ == "__main__":
    unittest.main()
