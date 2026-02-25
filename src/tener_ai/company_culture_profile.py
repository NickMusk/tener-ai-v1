from __future__ import annotations

import html as html_utils
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol
from urllib import error as urlerror, parse as urlparse, request as urlrequest


TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
}

DEFAULT_FETCH_USER_AGENT = (
    "Mozilla/5.0 (compatible; TenerCompanyProfileBot/1.0; +https://tener.ai)"
)


@dataclass
class SearchResult:
    url: str
    title: str = ""
    snippet: str = ""
    rank: int = 0
    query: str = ""


@dataclass
class FetchResponse:
    url: str
    status_code: int
    content_type: str
    body: str


@dataclass
class ScrapedSource:
    url: str
    domain: str
    title: str
    query: str
    search_rank: int
    fetch_status: str
    http_status: Optional[int]
    text_chars: int
    extracted_text: str = ""
    error_code: str = ""
    error_message: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "domain": self.domain,
            "title": self.title,
            "query": self.query,
            "search_rank": self.search_rank,
            "fetch_status": self.fetch_status,
            "http_status": self.http_status,
            "text_chars": self.text_chars,
            "error_code": self.error_code,
            "error_message": self.error_message,
        }


class SearchProvider(Protocol):
    def search(self, query: str, limit: int) -> List[SearchResult]:
        ...


class PageFetcher(Protocol):
    def fetch(self, url: str, timeout_seconds: int) -> FetchResponse:
        ...


class ContentExtractor(Protocol):
    def extract_text(self, html: str, url: str) -> str:
        ...


class CompanyProfileSynthesizer(Protocol):
    def generate_profile(
        self,
        company_name: str,
        website_url: str,
        sources: List[ScrapedSource],
    ) -> Dict[str, Any]:
        ...


class GoogleCSESearchProvider:
    def __init__(
        self,
        *,
        api_key: str,
        cx: str,
        base_url: str = "https://www.googleapis.com/customsearch/v1",
        timeout_seconds: int = 20,
    ) -> None:
        self.api_key = api_key.strip()
        self.cx = cx.strip()
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds))

    def search(self, query: str, limit: int) -> List[SearchResult]:
        if not self.api_key or not self.cx:
            raise ValueError("Google CSE credentials are missing")

        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            return []
        limit = max(1, min(int(limit or 1), 10))
        params = urlparse.urlencode(
            {
                "key": self.api_key,
                "cx": self.cx,
                "q": normalized_query,
                "num": str(limit),
                "safe": "off",
            }
        )
        url = f"{self.base_url}?{params}"
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={"Accept": "application/json", "User-Agent": DEFAULT_FETCH_USER_AGENT},
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
        data = json.loads(raw) if raw else {}
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return []

        out: List[SearchResult] = []
        for idx, item in enumerate(items[:limit], 1):
            if not isinstance(item, dict):
                continue
            link = str(item.get("link") or "").strip()
            if not link:
                continue
            out.append(
                SearchResult(
                    url=link,
                    title=str(item.get("title") or "").strip(),
                    snippet=str(item.get("snippet") or "").strip(),
                    rank=idx,
                    query=normalized_query,
                )
            )
        return out


class SeedSearchProvider:
    """
    Lightweight local fallback used when Google CSE credentials are unavailable.
    It keeps the same interface and allows end-to-end UI tests without external search API access.
    """

    def __init__(self, *, company_name: str = "", website_url: str = "") -> None:
        self.company_name = " ".join(str(company_name or "").split()).strip()
        self.website_url = canonicalize_url(website_url)

    def search(self, query: str, limit: int) -> List[SearchResult]:
        limit = max(1, int(limit or 1))
        website = self.website_url or "https://example.com/"
        domain = normalize_domain(website)
        slug = re.sub(r"[^a-z0-9]+", "-", self.company_name.lower()).strip("-") or domain.replace(".", "-")

        candidates = [
            (website, "Official website", "Company website"),
            (f"https://{domain}/about", "About", "About company and mission"),
            (f"https://{domain}/careers", "Careers", "Careers and work environment"),
            (f"https://{domain}/culture", "Culture", "Values and team principles"),
            (f"https://www.linkedin.com/company/{slug}/", "LinkedIn company page", "Company updates"),
            (
                f"https://www.glassdoor.com/Search/results.htm?keyword={urlparse.quote(self.company_name or domain)}",
                "Glassdoor search",
                "Employee review signals",
            ),
        ]
        out: List[SearchResult] = []
        for idx, (url, title, snippet) in enumerate(candidates, 1):
            canonical = canonicalize_url(url)
            if not canonical:
                continue
            out.append(SearchResult(url=canonical, title=title, snippet=snippet, rank=idx, query=query))
            if len(out) >= limit:
                break
        return out


def normalize_domain(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse.urlparse(value)
    domain = parsed.netloc.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def canonicalize_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse.urlparse(value)
    scheme = (parsed.scheme or "https").lower()
    if scheme not in {"http", "https"}:
        return ""

    domain = parsed.netloc.strip().lower()
    if not domain:
        return ""
    if domain.startswith("www."):
        domain = domain[4:]

    path = parsed.path or "/"
    path = re.sub(r"/{2,}", "/", path)
    if path != "/":
        path = path.rstrip("/")
        if not path:
            path = "/"

    pairs = urlparse.parse_qsl(parsed.query, keep_blank_values=False)
    filtered = [(k, v) for k, v in pairs if k.lower() not in TRACKING_QUERY_PARAMS]
    query = urlparse.urlencode(filtered, doseq=True)
    return urlparse.urlunparse((scheme, domain, path, "", query, ""))


def build_google_queries(company_name: str, website_url: str) -> List[str]:
    name = " ".join(str(company_name or "").split()).strip()
    if not name:
        return []
    domain = normalize_domain(website_url)

    candidates = [
        f"\"{name}\" company culture values",
        f"\"{name}\" engineering culture",
        f"\"{name}\" leadership principles",
        f"\"{name}\" employee reviews",
        f"\"{name}\" glassdoor",
        f"\"{name}\" linkedin company",
    ]
    if domain:
        candidates.insert(0, f"site:{domain} \"{name}\" values")
        candidates.insert(1, f"site:{domain} \"{name}\" careers team")

    seen: set[str] = set()
    out: List[str] = []
    for item in candidates:
        query = " ".join(item.split())
        key = query.lower()
        if not query or key in seen:
            continue
        seen.add(key)
        out.append(query)
    return out


def score_search_result(result: SearchResult, official_domain: str) -> int:
    score = 0
    domain = normalize_domain(result.url)
    if official_domain and domain == official_domain:
        score += 300
    elif official_domain and domain.endswith(f".{official_domain}"):
        score += 220

    rank = result.rank if result.rank > 0 else 99
    score += max(0, 120 - rank * 8)

    title_blob = f"{result.title} {result.snippet}".lower()
    for token in ("culture", "values", "mission", "about", "careers", "team"):
        if token in title_blob:
            score += 20
    return score


def select_top_urls(
    results: List[SearchResult],
    *,
    official_domain: str,
    max_links: int = 10,
    force_include_url: str = "",
) -> List[SearchResult]:
    normalized_force = canonicalize_url(force_include_url)
    max_links = max(1, int(max_links or 1))

    dedup: Dict[str, SearchResult] = {}
    for item in results:
        canonical = canonicalize_url(item.url)
        if not canonical:
            continue
        normalized = SearchResult(
            url=canonical,
            title=item.title,
            snippet=item.snippet,
            rank=item.rank,
            query=item.query,
        )
        existing = dedup.get(canonical)
        if existing is None or score_search_result(normalized, official_domain) > score_search_result(existing, official_domain):
            dedup[canonical] = normalized

    ranked = sorted(
        dedup.values(),
        key=lambda item: (
            -score_search_result(item, official_domain),
            item.rank if item.rank > 0 else 999,
            item.url,
        ),
    )

    picked: List[SearchResult] = []
    seen: set[str] = set()
    if normalized_force:
        seen.add(normalized_force)
        picked.append(SearchResult(url=normalized_force, title="Official website", rank=0, query="seed"))

    for item in ranked:
        if len(picked) >= max_links:
            break
        if item.url in seen:
            continue
        seen.add(item.url)
        picked.append(item)
    return picked[:max_links]


class UrllibPageFetcher:
    def __init__(self, user_agent: str = DEFAULT_FETCH_USER_AGENT) -> None:
        self.user_agent = user_agent.strip() or DEFAULT_FETCH_USER_AGENT

    def fetch(self, url: str, timeout_seconds: int) -> FetchResponse:
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,text/plain,*/*;q=0.8",
            },
        )
        with urlrequest.urlopen(req, timeout=max(3, int(timeout_seconds))) as response:
            raw = response.read()
            content_type = str(response.headers.get("Content-Type") or "")
            charset = response.headers.get_content_charset() or "utf-8"
            try:
                text = raw.decode(charset, errors="replace")
            except LookupError:
                text = raw.decode("utf-8", errors="replace")
            return FetchResponse(
                url=url,
                status_code=int(getattr(response, "status", 200)),
                content_type=content_type,
                body=text,
            )


class SimpleHtmlTextExtractor:
    def extract_text(self, html: str, url: str) -> str:
        text = str(html or "")
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"(?is)<!--.*?-->", " ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = html_utils.unescape(text)
        text = re.sub(r"&nbsp;?", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text


class HeuristicCompanyProfileSynthesizer:
    VALUE_KEYWORDS = {
        "ownership": ["ownership", "own", "accountability", "responsibility"],
        "transparency": ["transparent", "transparency", "open communication"],
        "collaboration": ["collaboration", "collaborative", "teamwork", "cross-functional"],
        "customer focus": ["customer", "user-centric", "client-first"],
        "learning": ["learning", "mentorship", "growth", "continuous improvement"],
        "speed": ["fast-paced", "speed", "iteration", "rapid"],
        "quality": ["quality", "excellence", "high standards", "craftsmanship"],
        "innovation": ["innovation", "experiment", "research", "creative"],
        "remote-first": ["remote", "distributed", "async", "asynchronous"],
    }

    def generate_profile(
        self,
        company_name: str,
        website_url: str,
        sources: List[ScrapedSource],
    ) -> Dict[str, Any]:
        text_blocks = [source.extracted_text for source in sources if source.fetch_status == "ok" and source.extracted_text]
        corpus = " ".join(text_blocks).lower()
        matched_values: List[str] = []
        for label, keywords in self.VALUE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in corpus:
                    matched_values.append(label)
                    break

        if not matched_values:
            matched_values = ["collaboration", "ownership", "quality"]
        values = self._top_unique(matched_values, limit=5)

        work_style = []
        if "remote-first" in values:
            work_style.append("remote-first collaboration with async communication")
        if "speed" in values:
            work_style.append("short feedback loops and iterative delivery")
        if "quality" in values:
            work_style.append("strong quality bar and engineering discipline")
        if not work_style:
            work_style = ["collaborative execution with measurable delivery expectations"]

        management_style = []
        if "ownership" in values:
            management_style.append("high ownership with clear accountability")
        if "transparency" in values:
            management_style.append("transparent communication and context sharing")
        if not management_style:
            management_style = ["pragmatic management with focus on outcomes"]

        hiring_signals = self._extract_hiring_signals(corpus)
        risks = []
        if len(sources) < 3:
            risks.append("Limited source coverage; regenerate with more publicly available materials.")
        if "glassdoor.com" not in " ".join(source.domain for source in sources):
            risks.append("Employee-review evidence is limited or missing.")

        summary = (
            f"{company_name} appears to emphasize {', '.join(values[:3])}. "
            f"Public signals suggest {work_style[0]}. "
            f"Management style is likely {management_style[0]}. "
            "This profile is generated without LLM synthesis and should be treated as a draft for manual review."
        )
        questions = [
            f"Tell us about a project where you demonstrated {values[0]} in practice.",
            "How do you adapt your collaboration style to match a new team's working norms?",
            "What environment helps you deliver your highest-quality work consistently?",
        ]

        return {
            "summary_200_300_words": summary,
            "culture_values": values,
            "work_style": work_style[:5],
            "management_style": management_style[:5],
            "hiring_signals": hiring_signals[:5],
            "risks_or_unknowns": risks[:5],
            "culture_interview_questions": questions[:3],
        }

    @staticmethod
    def _top_unique(values: List[str], limit: int) -> List[str]:
        counts = Counter(values)
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [item[0] for item in ordered[:limit]]

    @staticmethod
    def _extract_hiring_signals(corpus: str) -> List[str]:
        signals: List[str] = []
        if "code review" in corpus:
            signals.append("mentions code review as part of team process")
        if "fast-paced" in corpus or "rapid" in corpus:
            signals.append("high-velocity execution environment")
        if "remote" in corpus or "distributed" in corpus:
            signals.append("supports distributed or remote collaboration")
        if "customer" in corpus:
            signals.append("customer impact is visible in company narrative")
        if not signals:
            signals.append("public sources contain limited explicit hiring process details")
        return signals


class OpenAICompanyProfileSynthesizer:
    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        base_url: str = "https://api.openai.com/v1",
        timeout_seconds: int = 30,
        max_chars_per_source: int = 2500,
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or "gpt-4o-mini"
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.max_chars_per_source = max(600, int(max_chars_per_source))

    def generate_profile(
        self,
        company_name: str,
        website_url: str,
        sources: List[ScrapedSource],
    ) -> Dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("OPENAI API key is not configured")

        evidence = []
        for source in sources:
            if source.fetch_status != "ok" or not source.extracted_text:
                continue
            evidence.append(
                {
                    "url": source.url,
                    "title": source.title,
                    "text": source.extracted_text[: self.max_chars_per_source],
                }
            )
        if not evidence:
            raise RuntimeError("No evidence available for synthesis")

        payload = {
            "model": self.model,
            "temperature": 0.2,
            "max_tokens": 1000,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an evidence-based company culture analyst. "
                        "Output strict JSON only. Do not invent facts not present in evidence."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": "Build company cultural profile from evidence.",
                            "company_name": company_name,
                            "website_url": website_url,
                            "required_schema": {
                                "summary_200_300_words": "string",
                                "culture_values": "array[string]",
                                "work_style": "array[string]",
                                "management_style": "array[string]",
                                "hiring_signals": "array[string]",
                                "risks_or_unknowns": "array[string]",
                                "culture_interview_questions": "array[string] with 2-3 items",
                            },
                            "evidence": evidence,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        raw = self._chat_completion(payload)
        parsed = json.loads(raw) if raw else {}
        return parsed if isinstance(parsed, dict) else {}

    def _chat_completion(self, payload: Dict[str, Any]) -> str:
        req = urlrequest.Request(
            url=f"{self.base_url}/chat/completions",
            method="POST",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urlerror.HTTPError as exc:
            raise RuntimeError(f"OpenAI HTTP error: {exc.code}") from exc
        except urlerror.URLError as exc:
            raise RuntimeError(f"OpenAI network error: {exc.reason}") from exc

        data = json.loads(raw) if raw else {}
        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else ""
        return content if isinstance(content, str) else ""


class CompanyCultureProfileService:
    def __init__(
        self,
        *,
        search_provider: SearchProvider,
        page_fetcher: PageFetcher,
        content_extractor: ContentExtractor,
        synthesizer: CompanyProfileSynthesizer,
        max_links: int = 10,
        per_query_limit: int = 10,
        fetch_timeout_seconds: int = 15,
        min_text_chars: int = 600,
    ) -> None:
        self.search_provider = search_provider
        self.page_fetcher = page_fetcher
        self.content_extractor = content_extractor
        self.synthesizer = synthesizer
        self.max_links = max(1, int(max_links))
        self.per_query_limit = max(1, int(per_query_limit))
        self.fetch_timeout_seconds = max(3, int(fetch_timeout_seconds))
        self.min_text_chars = max(1, int(min_text_chars))

    def generate(self, company_name: str, website_url: str) -> Dict[str, Any]:
        normalized_name = " ".join(str(company_name or "").split()).strip()
        if not normalized_name:
            raise ValueError("company_name is required")
        normalized_website = canonicalize_url(website_url)
        if not normalized_website:
            raise ValueError("company_website_url must be a valid URL")

        queries = build_google_queries(normalized_name, normalized_website)
        official_domain = normalize_domain(normalized_website)
        raw_results: List[SearchResult] = []

        for query in queries:
            try:
                results = self.search_provider.search(query=query, limit=self.per_query_limit)
            except Exception:
                continue
            for idx, item in enumerate(results):
                raw_results.append(
                    SearchResult(
                        url=item.url,
                        title=item.title,
                        snippet=item.snippet,
                        rank=item.rank if item.rank > 0 else idx + 1,
                        query=query,
                    )
                )

        selected = select_top_urls(
            raw_results,
            official_domain=official_domain,
            max_links=self.max_links,
            force_include_url=normalized_website,
        )

        sources: List[ScrapedSource] = []
        for item in selected:
            source = self._scrape_one(item)
            sources.append(source)

        success_sources = [item for item in sources if item.fetch_status == "ok"]
        warnings: List[str] = []
        profile: Dict[str, Any] = {}
        if success_sources:
            try:
                profile = self.synthesizer.generate_profile(
                    company_name=normalized_name,
                    website_url=normalized_website,
                    sources=success_sources,
                )
            except Exception as exc:
                warnings.append(f"llm_synthesis_failed: {exc}")
                profile = self._fallback_profile(normalized_name)
        else:
            warnings.append("no_scraped_sources_for_synthesis")
            profile = self._fallback_profile(normalized_name)

        return {
            "company_name": normalized_name,
            "website": normalized_website,
            "search_queries": queries,
            "searched_links_total": len(raw_results),
            "selected_links_total": len(selected),
            "scraped_success_total": len(success_sources),
            "scraped_failed_total": len(sources) - len(success_sources),
            "sources": [item.as_dict() for item in sources],
            "profile": profile,
            "warnings": warnings,
        }

    def _scrape_one(self, item: SearchResult) -> ScrapedSource:
        domain = normalize_domain(item.url)
        try:
            response = self.page_fetcher.fetch(item.url, timeout_seconds=self.fetch_timeout_seconds)
        except urlerror.HTTPError as exc:
            return ScrapedSource(
                url=item.url,
                domain=domain,
                title=item.title,
                query=item.query,
                search_rank=item.rank,
                fetch_status="fetch_failed",
                http_status=int(exc.code),
                text_chars=0,
                error_code="http_error",
                error_message=str(exc),
            )
        except Exception as exc:
            return ScrapedSource(
                url=item.url,
                domain=domain,
                title=item.title,
                query=item.query,
                search_rank=item.rank,
                fetch_status="fetch_failed",
                http_status=None,
                text_chars=0,
                error_code="fetch_error",
                error_message=str(exc),
            )

        body = str(response.body or "")
        content_type = str(response.content_type or "").lower()
        if response.status_code >= 400:
            return ScrapedSource(
                url=item.url,
                domain=domain,
                title=item.title,
                query=item.query,
                search_rank=item.rank,
                fetch_status="fetch_failed",
                http_status=response.status_code,
                text_chars=0,
                error_code="http_error",
                error_message=f"status={response.status_code}",
            )

        if "text/html" in content_type or body.lstrip().lower().startswith("<!doctype html") or "<html" in body[:200].lower():
            cleaned = self.content_extractor.extract_text(body, item.url)
        else:
            cleaned = re.sub(r"\s+", " ", body).strip()

        if len(cleaned) < self.min_text_chars:
            return ScrapedSource(
                url=item.url,
                domain=domain,
                title=item.title,
                query=item.query,
                search_rank=item.rank,
                fetch_status="too_short",
                http_status=response.status_code,
                text_chars=len(cleaned),
                error_code="too_short",
                error_message=f"text_chars={len(cleaned)}",
            )

        return ScrapedSource(
            url=item.url,
            domain=domain,
            title=item.title,
            query=item.query,
            search_rank=item.rank,
            fetch_status="ok",
            http_status=response.status_code,
            text_chars=len(cleaned),
            extracted_text=cleaned,
        )

    @staticmethod
    def _fallback_profile(company_name: str) -> Dict[str, Any]:
        return {
            "summary_200_300_words": (
                f"{company_name} profile was generated with limited confidence due to missing or weak source evidence. "
                "Please review source links and regenerate once enough public information is available."
            ),
            "culture_values": [],
            "work_style": [],
            "management_style": [],
            "hiring_signals": [],
            "risks_or_unknowns": ["Insufficient evidence from scraped sources."],
            "culture_interview_questions": [
                "What team values are most important for success in this company?",
                "Describe a project where your collaboration style matched the team's culture.",
            ],
        }
