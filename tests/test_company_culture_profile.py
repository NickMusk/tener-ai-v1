import unittest
from typing import Dict, List

from tener_ai.company_culture_profile import (
    CompanyCultureProfileService,
    FetchResponse,
    HeuristicCompanyProfileSynthesizer,
    SeedSearchProvider,
    SearchResult,
    build_google_queries,
    canonicalize_url,
    normalize_domain,
    select_top_urls,
)


class _FakeSearchProvider:
    def search(self, query: str, limit: int) -> List[SearchResult]:
        base = [
            SearchResult(
                url="https://www.acme.ai/about?utm_source=google",
                title="About Acme",
                snippet="Mission and values",
                rank=1,
            ),
            SearchResult(
                url="https://www.acme.ai/careers",
                title="Careers",
                snippet="Culture and team",
                rank=2,
            ),
            SearchResult(
                url="https://www.glassdoor.com/Overview/Working-at-Acme",
                title="Glassdoor",
                snippet="Employee reviews",
                rank=3,
            ),
            SearchResult(
                url="https://www.linkedin.com/company/acme-ai/",
                title="LinkedIn",
                snippet="Company page",
                rank=4,
            ),
        ]
        return base[:limit]


class _FakePageFetcher:
    def fetch(self, url: str, timeout_seconds: int) -> FetchResponse:
        payload: Dict[str, FetchResponse] = {
            "https://acme.ai/": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body="<html><body>Acme official site. We value ownership, transparency, and customer obsession.</body></html>",
            ),
            "https://acme.ai/about": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body="<html><body>Our values: ownership, high standards, async collaboration, measurable impact.</body></html>",
            ),
            "https://acme.ai/careers": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body="<html><body>We hire engineers who thrive in fast feedback loops and remote-first teams.</body></html>",
            ),
            "https://www.glassdoor.com/Overview/Working-at-Acme": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body="<html><body>Review highlights mention autonomy and strong peer code reviews.</body></html>",
            ),
            "https://www.linkedin.com/company/acme-ai": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body="<html><body>Acme builds B2B AI software and promotes continuous learning.</body></html>",
            ),
        }
        key = canonicalize_url(url)
        if key in payload:
            return payload[key]
        return FetchResponse(url=url, status_code=404, content_type="text/plain", body="not found")


class _FakeExtractor:
    def extract_text(self, html: str, url: str) -> str:
        return (
            html.replace("<html>", " ")
            .replace("</html>", " ")
            .replace("<body>", " ")
            .replace("</body>", " ")
            .replace("<p>", " ")
            .replace("</p>", " ")
            .replace("<br>", " ")
            .strip()
        )


class _FakeSynthesizer:
    def generate_profile(self, company_name: str, website_url: str, sources):  # type: ignore[no-untyped-def]
        return {
            "summary_200_300_words": f"{company_name} appears as a high-ownership, remote-first engineering culture.",
            "culture_values": ["ownership", "transparency", "continuous learning"],
            "work_style": ["remote-first", "fast feedback loops"],
            "management_style": ["high autonomy", "clear accountability"],
            "hiring_signals": ["peer code reviews", "impact focus"],
            "risks_or_unknowns": [],
            "culture_interview_questions": [
                "How do you handle ownership in ambiguous projects?",
                "Tell us about collaboration in remote teams.",
            ],
        }


class CompanyCultureProfileTests(unittest.TestCase):
    def test_build_google_queries_includes_domain_and_culture_intent(self) -> None:
        queries = build_google_queries("Acme AI", "https://www.acme.ai")
        self.assertGreaterEqual(len(queries), 6)
        self.assertTrue(any("site:acme.ai" in query for query in queries))
        self.assertTrue(any("culture" in query.lower() for query in queries))

    def test_select_top_urls_dedupes_and_prioritizes_official_domain(self) -> None:
        results = [
            SearchResult(url="https://www.acme.ai/about?utm_source=x", title="About", snippet="", rank=4),
            SearchResult(url="https://acme.ai/about", title="About canonical", snippet="", rank=1),
            SearchResult(url="https://example.org/acme", title="Blog", snippet="", rank=1),
        ]
        selected = select_top_urls(
            results,
            official_domain="acme.ai",
            max_links=2,
            force_include_url="https://acme.ai",
        )
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0].url, "https://acme.ai/")
        self.assertEqual(normalize_domain(selected[1].url), "acme.ai")

    def test_generate_profile_runs_full_pipeline(self) -> None:
        service = CompanyCultureProfileService(
            search_provider=_FakeSearchProvider(),
            page_fetcher=_FakePageFetcher(),
            content_extractor=_FakeExtractor(),
            synthesizer=_FakeSynthesizer(),
            max_links=10,
            per_query_limit=4,
            min_text_chars=20,
        )
        out = service.generate(company_name="Acme AI", website_url="https://www.acme.ai")

        self.assertEqual(out["company_name"], "Acme AI")
        self.assertEqual(out["website"], "https://acme.ai/")
        self.assertGreaterEqual(out["searched_links_total"], 4)
        self.assertGreater(out["scraped_success_total"], 0)
        self.assertIn("profile", out)
        self.assertIn("summary_200_300_words", out["profile"])
        self.assertEqual(out["warnings"], [])

    def test_seed_search_provider_returns_urls(self) -> None:
        provider = SeedSearchProvider(company_name="Acme AI", website_url="https://www.acme.ai")
        items = provider.search('"Acme AI" culture', limit=5)
        self.assertEqual(len(items), 5)
        self.assertEqual(items[0].url, "https://acme.ai/")
        self.assertTrue(any("linkedin.com" in row.url for row in items))

    def test_heuristic_synthesizer_returns_profile_shape(self) -> None:
        synthesizer = HeuristicCompanyProfileSynthesizer()
        sources = [
            type(
                "_Source",
                (),
                {
                    "fetch_status": "ok",
                    "extracted_text": "Remote-first collaboration and transparent communication with high ownership.",
                    "domain": "acme.ai",
                    "url": "https://acme.ai/",
                    "title": "About",
                },
            )()
        ]
        profile = synthesizer.generate_profile("Acme AI", "https://acme.ai", sources)  # type: ignore[arg-type]
        self.assertIn("summary_200_300_words", profile)
        self.assertIn("culture_values", profile)
        self.assertIn("culture_interview_questions", profile)
        self.assertGreater(len(profile["culture_interview_questions"]), 1)


if __name__ == "__main__":
    unittest.main()
