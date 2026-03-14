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

    def test_search_profiles_recovers_when_configured_path_is_users(self) -> None:
        class FakeProvider(UnipileLinkedInProvider):
            def __init__(self) -> None:
                super().__init__(api_key="k", base_url="https://api.example.com", account_id="acc")
                self.calls = []
                self.search_path = "/api/v1/users"

            def _request_json(self, method, url, payload=None):  # type: ignore[override]
                self.calls.append((method, url, payload))
                if "/api/v1/users/search" in url:
                    return {
                        "items": [
                            {
                                "object": "UserProfile",
                                "provider_id": "good_2",
                                "public_identifier": "jane-doe",
                                "full_name": "Jane Doe",
                                "headline": "Fullstack Engineer",
                                "location": "US",
                                "languages": ["en"],
                                "skills": ["python", "react"],
                                "years_experience": 6,
                            }
                        ]
                    }
                if "/api/v1/users?" in url:
                    raise RuntimeError(
                        'Unipile HTTP error 404: {"message":"Cannot GET /api/v1/users?...","error":"Not Found","statusCode":404}'
                    )
                return {}

        provider = FakeProvider()
        out = provider.search_profiles(query="fullstack engineer", limit=10)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["full_name"], "Jane Doe")
        self.assertTrue(any("/api/v1/users/search" in call[1] for call in provider.calls))

    def test_extract_years_from_headline(self) -> None:
        self.assertEqual(self.provider._extract_years_from_text("Senior Backend Engineer | 7.6+ YOE"), 7)
        self.assertEqual(self.provider._extract_years_from_text("Platform engineer with 10 years experience"), 10)

    def test_structured_search_uses_keywords_and_resolved_filters(self) -> None:
        class FakeProvider(UnipileLinkedInProvider):
            def __init__(self) -> None:
                super().__init__(api_key="k", base_url="https://api.example.com", account_id="acc")
                self.calls = []

            def _request_json(self, method, url, payload=None):  # type: ignore[override]
                self.calls.append((method, url, payload))
                if "/api/v1/linkedin/search/parameters" in url and "type=LOCATION" in url:
                    return {"items": [{"id": "loc_remote", "label": "Remote"}]}
                if "/api/v1/linkedin/search" in url:
                    return {
                        "items": [
                            {
                                "provider_id": "cand_1",
                                "full_name": "Alex Morgan",
                                "headline": "Senior Backend Engineer",
                                "location": "Remote",
                                "languages": ["en"],
                                "skills": ["python", "aws"],
                                "years_experience": 7,
                            }
                        ]
                    }
                return {}

        provider = FakeProvider()
        out = provider.search_profiles_structured(
            spec={
                "title_query": "\"Python\"",
                "keyword_query": "\"Django\" AND (\"AWS\" OR \"GCP\")",
                "filters": {
                    "location": "Remote",
                    "keywords": "\"Django\" AND (\"AWS\" OR \"GCP\")",
                    "must_terms": ["django"],
                    "optional_terms": ["aws", "gcp"],
                    "profile_language": ["en"],
                },
            },
            limit=20,
        )
        self.assertEqual(len(out), 1)
        search_call = next(call for call in provider.calls if "/api/v1/linkedin/search?" in call[1])
        payload = search_call[2] or {}
        self.assertEqual(payload.get("job_title"), "\"Python\"")
        self.assertEqual(payload.get("keywords"), "\"Django\" AND (\"AWS\" OR \"GCP\")")
        self.assertEqual(payload.get("profile_language"), ["en"])
        self.assertEqual(payload.get("location"), ["loc_remote"])
        self.assertNotIn("skills", payload)

    def test_structured_search_parameter_resolution_is_cached(self) -> None:
        class FakeProvider(UnipileLinkedInProvider):
            def __init__(self) -> None:
                super().__init__(api_key="k", base_url="https://api.example.com", account_id="acc")
                self.calls = []

            def _request_json(self, method, url, payload=None):  # type: ignore[override]
                self.calls.append((method, url, payload))
                if "/api/v1/linkedin/search/parameters" in url:
                    return {"items": [{"id": "loc_remote", "label": "Remote"}]}
                if "/api/v1/linkedin/search" in url:
                    return {"items": []}
                return {}

        provider = FakeProvider()
        spec = {"title_query": "Manual QA Engineer", "filters": {"location": "Remote"}}
        provider.search_profiles_structured(spec=spec, limit=10)
        provider.search_profiles_structured(spec=spec, limit=10)
        parameter_calls = [call for call in provider.calls if "/api/v1/linkedin/search/parameters" in call[1]]
        self.assertEqual(len(parameter_calls), 1)

    def test_normalize_profile_prefers_primary_locale_before_languages(self) -> None:
        normalized = self.provider._normalize_profile(
            {
                "provider_id": "cand-locale-1",
                "full_name": "Andrey Chaliy",
                "headline": "Manual QA Engineer",
                "location": "Kyiv, Ukraine",
                "primary_locale": "uk-UA",
                "languages": ["en", "ru"],
            }
        )
        self.assertEqual(normalized["languages"], ["uk-ua", "en", "ru"])

    def test_normalize_profile_does_not_default_languages_to_english(self) -> None:
        normalized = self.provider._normalize_profile(
            {
                "provider_id": "cand-locale-2",
                "full_name": "Andrey Chaliy",
                "headline": "Manual QA Engineer",
                "location": "Kyiv, Ukraine",
            }
        )
        self.assertEqual(normalized["languages"], [])

    def test_enrich_profile_requests_linkedin_sections(self) -> None:
        class FakeProvider(UnipileLinkedInProvider):
            def __init__(self) -> None:
                super().__init__(api_key="k", base_url="https://api.example.com", account_id="acc")
                self.calls = []

            def _request_json(self, method, url, payload=None):  # type: ignore[override]
                self.calls.append((method, url, payload))
                return {
                    "provider_id": "cand-sections",
                    "public_identifier": "mykola-berestok",
                    "full_name": "Mykola B.",
                    "headline": "Senior QA Engineer",
                    "skills": [{"name": "Manual Testing"}],
                }

        provider = FakeProvider()
        out = provider.enrich_profile({"provider_id": "cand-sections", "linkedin_id": "mykola-berestok"})
        self.assertEqual(out["skills"], ["manual testing"])
        enrich_url = provider.calls[0][1]
        self.assertIn("linkedin_sections=skills", enrich_url)
        self.assertIn("linkedin_sections=experience", enrich_url)
        self.assertIn("linkedin_sections=languages", enrich_url)

    def test_normalize_profile_extracts_skills_from_linkedin_sections(self) -> None:
        normalized = self.provider._normalize_profile(
            {
                "provider_id": "cand-skills-1",
                "public_identifier": "mykola-berestok",
                "headline": "Senior QA Engineer",
                "primary_locale": {"country": "US", "language": "en"},
                "languages": [
                    {"name": "English", "proficiency": "Limited working proficiency"},
                    {"name": "Russian", "proficiency": "Native or bilingual proficiency"},
                ],
                "skills": [
                    {"name": "Manual Testing", "endorsement_count": 22},
                    {"name": "JIRA", "endorsement_count": 20},
                    {"name": "Regression Testing", "endorsement_count": 18},
                    {"name": "Test Cases", "endorsement_count": 18},
                ],
            }
        )
        self.assertEqual(
            normalized["skills"],
            ["manual testing", "jira", "regression testing", "test cases"],
        )
        self.assertEqual(normalized["languages"], ["en-us", "english", "russian"])

    def test_normalize_profile_estimates_years_from_work_experience(self) -> None:
        normalized = self.provider._normalize_profile(
            {
                "provider_id": "cand-exp-1",
                "public_identifier": "mykola-berestok",
                "headline": "Senior QA Engineer",
                "work_experience": [
                    {
                        "company": "Kaleris",
                        "position": "Senior QA Engineer",
                        "start": "4/2024",
                        "end": None,
                        "skills": [],
                    },
                    {
                        "company": "IT Craft",
                        "position": "QA Engineer",
                        "start": "3/2015",
                        "end": "4/2017",
                        "skills": ["Agile"],
                    },
                ],
            }
        )
        self.assertGreaterEqual(normalized["years_experience"], 9)


if __name__ == "__main__":
    unittest.main()
