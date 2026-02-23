import unittest

from tener_ai.linkedin_provider import UnipileLinkedInProvider


class UnipileProviderParsingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = UnipileLinkedInProvider(
            api_key="test_key",
            base_url="https://api.example.com",
            account_id="acc_test",
        )

    def test_extract_results_prefers_items_over_provider_id_envelope(self) -> None:
        payload = {
            "provider_id": "account_meta",
            "items": [
                {"provider_id": "cand_1", "full_name": "Alice Backend", "headline": "Backend Engineer"},
                {"provider_id": "cand_2", "full_name": "Bob Platform", "headline": "Platform Engineer"},
            ],
        }
        results = self.provider._extract_results(payload)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["provider_id"], "cand_1")

    def test_extract_results_accepts_single_user_profile(self) -> None:
        payload = {
            "object": "UserProfile",
            "provider_id": "cand_3",
            "full_name": "Carol Node",
            "headline": "Senior Backend Engineer",
        }
        results = self.provider._extract_results(payload)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["provider_id"], "cand_3")

    def test_extract_results_from_nested_data_items(self) -> None:
        payload = {
            "data": {
                "items": [
                    {"provider_id": "cand_4", "full_name": "Dan Java"},
                    {"provider_id": "cand_5", "full_name": "Emma Go"},
                ]
            }
        }
        results = self.provider._extract_results(payload)
        self.assertEqual(len(results), 2)

    def test_placeholder_detection_for_search_user_profile(self) -> None:
        items = [
            {
                "object": "UserProfile",
                "provider_id": "ACoAAA",
                "public_identifier": "search",
                "full_name": "Someone",
            }
        ]
        self.assertTrue(self.provider._looks_like_search_placeholder(items))

    def test_placeholder_detection_false_for_normal_user_profile(self) -> None:
        items = [
            {
                "object": "UserProfile",
                "provider_id": "ACoBBB",
                "public_identifier": "philippe-g",
                "full_name": "Philippe G.",
            }
        ]
        self.assertFalse(self.provider._looks_like_search_placeholder(items))

    def test_search_profiles_fallback_to_second_path_after_placeholder(self) -> None:
        class FakeProvider(UnipileLinkedInProvider):
            def __init__(self) -> None:
                super().__init__(api_key="k", base_url="https://api.example.com", account_id="acc")
                self.calls = []
                self.search_path = "/api/v1/users/search"

            def _request_json(self, method, url, payload=None):  # type: ignore[override]
                self.calls.append((method, url, payload))
                if "/api/v1/users/search" in url:
                    return {
                        "object": "UserProfile",
                        "public_identifier": "search",
                        "provider_id": "bad_search_user",
                        "full_name": "Search User",
                        "headline": "placeholder",
                    }
                if "/api/v1/linkedin/search" in url:
                    return {
                        "items": [
                            {
                                "object": "UserProfile",
                                "provider_id": "good_1",
                                "public_identifier": "alex-morgan",
                                "full_name": "Alex Morgan",
                                "headline": "Senior Backend Engineer",
                                "location": "Germany",
                                "languages": ["en"],
                                "skills": ["python", "aws"],
                                "years_experience": 7,
                            }
                        ]
                    }
                return {}

        provider = FakeProvider()
        out = provider.search_profiles(query="senior backend", limit=10)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["full_name"], "Alex Morgan")
        self.assertTrue(any("/api/v1/linkedin/search" in call[1] for call in provider.calls))


if __name__ == "__main__":
    unittest.main()
