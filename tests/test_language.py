import unittest

from tener_ai.language import detect_language_from_text


class LanguageDetectionTests(unittest.TestCase):
    def test_detect_language_russian(self) -> None:
        self.assertEqual(detect_language_from_text("Какая вилка зарплаты?"), "ru")

    def test_detect_language_spanish(self) -> None:
        self.assertEqual(detect_language_from_text("¿Cuál es el salario?"), "es")

    def test_detect_language_default_english(self) -> None:
        self.assertEqual(detect_language_from_text("What is the interview timeline?"), "en")


if __name__ == "__main__":
    unittest.main()
