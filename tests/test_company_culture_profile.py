import unittest
from typing import Dict, List

from tener_ai.company_culture_profile import (
    BingRssSearchProvider,
    BraveHtmlSearchProvider,
    CompanyCultureProfileService,
    DuckDuckGoHtmlSearchProvider,
    FetchResponse,
    HeuristicCompanyProfileSynthesizer,
    SeedSearchProvider,
    SearchResult,
    build_google_queries,
    canonicalize_url,
    is_job_board_url,
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
                url="https://boards.greenhouse.io/acmeai/jobs/4499111",
                title="Senior Backend Engineer at Acme",
                snippet="Role requirements and team culture",
                rank=3,
            ),
            SearchResult(
                url="https://jobs.lever.co/acmeai/abcd-1234",
                title="Product Manager - Acme",
                snippet="Qualifications and collaboration style",
                rank=4,
            ),
            SearchResult(
                url="https://www.glassdoor.com/Overview/Working-at-Acme",
                title="Glassdoor",
                snippet="Employee reviews",
                rank=5,
            ),
            SearchResult(
                url="https://www.linkedin.com/company/acme-ai/",
                title="LinkedIn",
                snippet="Company page",
                rank=6,
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
            "https://boards.greenhouse.io/acmeai/jobs/4499111": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body=(
                    "<html><body>"
                    "Senior Backend Engineer. Responsibilities: design scalable systems and partner cross-functional teams. "
                    "Requirements: 5+ years experience, strong communication, ownership mindset, code review discipline. "
                    "Our culture values transparency, collaboration, and customer impact."
                    "</body></html>"
                ),
            ),
            "https://jobs.lever.co/acmeai/abcd-1234": FetchResponse(
                url=url,
                status_code=200,
                content_type="text/html; charset=utf-8",
                body=(
                    "<html><body>"
                    "Product Manager role. We are looking for analytical, data-driven candidates with stakeholder management. "
                    "You will lead rapid iterations and work in a fast-paced, inclusive team."
                    "</body></html>"
                ),
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
        self.assertGreaterEqual(len(queries), 10)
        self.assertTrue(any("site:acme.ai" in query for query in queries))
        self.assertTrue(any("culture" in query.lower() for query in queries))
        self.assertTrue(any("greenhouse" in query.lower() for query in queries))

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

    def test_select_top_urls_includes_job_boards_when_available(self) -> None:
        results = [
            SearchResult(url="https://acme.ai/about", title="About", snippet="", rank=1),
            SearchResult(url="https://boards.greenhouse.io/acme/jobs/1", title="Backend Engineer", snippet="", rank=5),
            SearchResult(url="https://jobs.lever.co/acme/2", title="Product Manager", snippet="", rank=6),
        ]
        selected = select_top_urls(
            results,
            official_domain="acme.ai",
            max_links=4,
            force_include_url="https://acme.ai",
            min_job_board_links=2,
        )
        self.assertTrue(any(is_job_board_url(item.url) for item in selected))
        self.assertGreaterEqual(sum(1 for item in selected if is_job_board_url(item.url)), 2)

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
        self.assertIn("job_board_insights", out)
        self.assertGreaterEqual(int(out["job_board_insights"]["job_board_sources_total"]), 1)
        self.assertIn("candidate_profiles_sought", out["profile"])
        self.assertTrue(out["profile"]["candidate_profiles_sought"])
        self.assertEqual(out["warnings"], [])

    def test_seed_search_provider_returns_urls(self) -> None:
        provider = SeedSearchProvider(company_name="Acme AI", website_url="https://www.acme.ai")
        items = provider.search('"Acme AI" culture', limit=7)
        self.assertEqual(len(items), 7)
        self.assertEqual(items[0].url, "https://acme.ai/")
        self.assertTrue(any(is_job_board_url(row.url) for row in items))

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

    def test_brave_html_parser_extracts_result_pairs(self) -> None:
        html = """
        <script>
        const x = {results:[
          {title:"Notion Company Culture \\/ Values",url:"https:\\/\\/www.notion.so\\/culture"},
          {title:"Built In: Notion culture",url:"https:\\/\\/www.builtinnyc.com\\/company\\/notion\\/faq\\/culture-values"}
        ]};
        </script>
        """
        out = BraveHtmlSearchProvider._parse_results_from_html(html=html, query="notion culture", limit=10)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0].url, "https://notion.so/culture")
        self.assertIn("Notion Company Culture", out[0].title)

    def test_duckduckgo_html_parser_extracts_result_pairs(self) -> None:
        html = """
        <div class="results">
          <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fboards.greenhouse.io%2Fnotion%2Fjobs%2F1">
            Senior Backend Engineer - Notion
          </a>
          <a class="result__a" href="https://www.notion.so/careers">Notion Careers</a>
        </div>
        """
        out = DuckDuckGoHtmlSearchProvider._parse_results_from_html(
            html=html,
            query="notion jobs",
            limit=10,
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0].url, "https://boards.greenhouse.io/notion/jobs/1")
        self.assertIn("Senior Backend Engineer", out[0].title)

    def test_is_job_board_url_detects_common_patterns(self) -> None:
        self.assertTrue(is_job_board_url("https://boards.greenhouse.io/acme/jobs/123"))
        self.assertTrue(is_job_board_url("https://acme.ai/careers/backend-engineer"))
        self.assertFalse(is_job_board_url("https://acme.ai/about"))

    def test_bing_rss_parser_extracts_items(self) -> None:
        rss = """<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0"><channel>
          <item>
            <title>Notion Careers</title>
            <link>https://www.notion.so/careers</link>
            <description>Open roles and hiring process</description>
          </item>
          <item>
            <title>Greenhouse Notion Jobs</title>
            <link>https://boards.greenhouse.io/notion/jobs/123</link>
            <description>Backend Engineer</description>
          </item>
        </channel></rss>"""
        out = BingRssSearchProvider._parse_results_from_rss(rss_xml=rss, query="notion jobs", limit=10)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0].url, "https://notion.so/careers")
        self.assertIn("Notion Careers", out[0].title)


if __name__ == "__main__":
    unittest.main()
