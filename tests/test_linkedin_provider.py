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


if __name__ == "__main__":
    unittest.main()
