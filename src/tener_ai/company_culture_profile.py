from __future__ import annotations

import html as html_utils
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
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

JOB_BOARD_DOMAIN_MARKERS = (
    "boards.greenhouse.io",
    "greenhouse.io",
    "jobs.lever.co",
    "lever.co",
    "jobs.ashbyhq.com",
    "ashbyhq.com",
    "workdayjobs.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "jobvite.com",
    "icims.com",
    "applytojob.com",
    "teamtailor.com",
    "recruitee.com",
    "workable.com",
    "bamboohr.com",
    "wellfound.com",
    "indeed.com",
    "ziprecruiter.com",
)

JOB_BOARD_PATH_MARKERS = (
    "/jobs",
    "/job/",
    "/careers",
    "/open-roles",
    "/positions",
    "/vacancies",
    "/join-us",
)

CANDIDATE_SIGNAL_KEYWORDS = {
    "ownership in ambiguous environments": ["ownership", "self-starter", "autonomous", "ambiguity"],
    "cross-functional collaboration": ["cross-functional", "stakeholder", "collaborat", "partner with"],
    "strong communication": ["communication", "written", "verbal", "present", "influence"],
    "analytical problem solving": ["analytical", "problem-solving", "data-driven", "metrics"],
    "leadership and mentoring": ["mentor", "coaching", "people manager", "leadership", "team lead"],
    "high execution velocity": ["fast-paced", "rapid", "iterate", "ship", "execution"],
    "customer orientation": ["customer", "user", "client"],
    "technical depth": ["architecture", "distributed systems", "scalable", "system design", "code review"],
}

CULTURE_ATTRIBUTE_KEYWORDS = {
    "ownership": ["ownership", "accountability", "responsibility"],
    "collaboration": ["collaboration", "teamwork", "cross-functional"],
    "transparency": ["transparent", "open communication", "candor"],
    "learning": ["learning", "growth mindset", "continuous improvement", "mentorship"],
    "inclusion": ["inclusive", "belonging", "diverse", "equity"],
    "customer focus": ["customer", "user-first", "customer obsession"],
    "quality bar": ["high standards", "quality", "craftsmanship", "excellence"],
    "bias for action": ["fast-paced", "urgency", "move fast", "bias for action"],
    "remote-first": ["remote", "distributed", "async", "asynchronous"],
}

JOB_SENTENCE_KEYWORDS = (
    "requirements",
    "qualifications",
    "you will",
    "you'll",
    "you have",
    "you bring",
    "ideal candidate",
    "we're looking for",
    "responsibilities",
    "must have",
    "nice to have",
)

DEFAULT_CULTURE_ANALYSIS_RULES = """
You are a senior organizational psychologist and culture analyst.

You have scraped structured and unstructured data about a company from:
- Official website (About, Mission, Values, Leadership pages)
- Job descriptions across roles and levels
- Public interviews of founders or executives
- Glassdoor or employee reviews (if available)
- Press releases and blog posts

Your task is NOT to summarize the company.
Your task is to infer and reconstruct the real operational culture of the company.

Avoid marketing language. Avoid generic phrases.
Extract implicit signals from patterns in language, expectations, hiring criteria, and leadership communication.

Rules:
- Do not copy phrases from the website.
- Do not repeat generic values like "innovation", "integrity", or "customer-centric".
- Infer from patterns.
- If data suggests high standards, state it clearly.
- If data suggests bureaucracy, state it clearly.
- If data is inconsistent, highlight ambiguity.

Output tone:
Analytical, sharp, direct. No fluff. No PR language.
Write as if advising a candidate whether they truly fit this environment.
""".strip()


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
    source_kind: str = ""

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
            "source_kind": self.source_kind or "general",
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


class BraveHtmlSearchProvider:
    """
    Free, no-key search provider that parses Brave Search web results.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://search.brave.com/search",
        timeout_seconds: int = 20,
        user_agent: str = DEFAULT_FETCH_USER_AGENT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds))
        self.user_agent = user_agent.strip() or DEFAULT_FETCH_USER_AGENT

    def search(self, query: str, limit: int) -> List[SearchResult]:
        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            return []
        limit = max(1, min(int(limit or 1), 20))

        params = urlparse.urlencode(
            {
                "q": normalized_query,
                "source": "web",
            }
        )
        url = f"{self.base_url}?{params}"
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": self.user_agent,
            },
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            html = response.read().decode("utf-8", errors="replace")
        return self._parse_results_from_html(html=html, query=normalized_query, limit=limit)

    @staticmethod
    def _parse_results_from_html(*, html: str, query: str, limit: int) -> List[SearchResult]:
        # Brave embeds hydrated results as JS objects containing title:"...",url:"...".
        pairs = re.findall(
            r'title:"((?:\\.|[^"\\]){1,600})",url:"(https?:\\/\\/(?:\\.|[^"\\])+?)"',
            html,
        )
        seen: set[str] = set()
        out: List[SearchResult] = []
        rank = 0
        for raw_title, raw_url in pairs:
            title = BraveHtmlSearchProvider._decode_js_string(raw_title)
            url = BraveHtmlSearchProvider._decode_js_string(raw_url)
            canonical = canonicalize_url(url)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            rank += 1
            out.append(
                SearchResult(
                    url=canonical,
                    title=title[:300],
                    snippet="",
                    rank=rank,
                    query=query,
                )
            )
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _decode_js_string(value: str) -> str:
        text = str(value or "")
        # Convert common JS escapes to plain text.
        text = text.replace("\\/", "/")
        text = bytes(text, "utf-8").decode("unicode_escape", errors="ignore")
        return html_utils.unescape(text).strip()


class DuckDuckGoHtmlSearchProvider:
    """
    Free, no-key search provider based on DuckDuckGo HTML endpoints.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://duckduckgo.com/html",
        timeout_seconds: int = 20,
        user_agent: str = DEFAULT_FETCH_USER_AGENT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds))
        self.user_agent = user_agent.strip() or DEFAULT_FETCH_USER_AGENT

    def search(self, query: str, limit: int) -> List[SearchResult]:
        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            return []
        limit = max(1, min(int(limit or 1), 20))

        params = urlparse.urlencode({"q": normalized_query})
        url = f"{self.base_url}/?{params}"
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": self.user_agent,
            },
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            html = response.read().decode("utf-8", errors="replace")
        return self._parse_results_from_html(html=html, query=normalized_query, limit=limit)

    @staticmethod
    def _parse_results_from_html(*, html: str, query: str, limit: int) -> List[SearchResult]:
        pattern = re.compile(
            r'<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
            flags=re.IGNORECASE | re.DOTALL,
        )
        seen: set[str] = set()
        out: List[SearchResult] = []
        rank = 0
        for href, raw_title in pattern.findall(str(html or "")):
            url = DuckDuckGoHtmlSearchProvider._extract_target_url(href)
            canonical = canonicalize_url(url)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            rank += 1
            title = re.sub(r"(?is)<[^>]+>", " ", raw_title)
            title = html_utils.unescape(re.sub(r"\s+", " ", title)).strip()
            out.append(
                SearchResult(
                    url=canonical,
                    title=title[:300],
                    snippet="",
                    rank=rank,
                    query=query,
                )
            )
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _extract_target_url(href: str) -> str:
        value = html_utils.unescape(str(href or "").strip())
        if not value:
            return ""
        parsed = urlparse.urlparse(value)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            query_pairs = urlparse.parse_qs(parsed.query)
            uddg = query_pairs.get("uddg")
            if uddg and isinstance(uddg, list):
                decoded = urlparse.unquote(str(uddg[0] or "").strip())
                if decoded:
                    return decoded
            return value
        if value.startswith("//"):
            return f"https:{value}"
        if value.startswith("/"):
            query_pairs = urlparse.parse_qs(urlparse.urlparse(value).query)
            uddg = query_pairs.get("uddg")
            if uddg and isinstance(uddg, list):
                return urlparse.unquote(str(uddg[0] or "").strip())
        return value


class BingRssSearchProvider:
    """
    Free, no-key provider using Bing RSS search output.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://www.bing.com/search",
        timeout_seconds: int = 20,
        user_agent: str = DEFAULT_FETCH_USER_AGENT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(3, int(timeout_seconds))
        self.user_agent = user_agent.strip() or DEFAULT_FETCH_USER_AGENT

    def search(self, query: str, limit: int) -> List[SearchResult]:
        normalized_query = " ".join(str(query or "").split()).strip()
        if not normalized_query:
            return []
        limit = max(1, min(int(limit or 1), 20))

        params = urlparse.urlencode({"q": normalized_query, "format": "rss"})
        url = f"{self.base_url}?{params}"
        req = urlrequest.Request(
            url=url,
            method="GET",
            headers={
                "Accept": "application/rss+xml,application/xml,text/xml,*/*;q=0.8",
                "User-Agent": self.user_agent,
            },
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            xml_text = response.read().decode("utf-8", errors="replace")
        return self._parse_results_from_rss(rss_xml=xml_text, query=normalized_query, limit=limit)

    @staticmethod
    def _parse_results_from_rss(*, rss_xml: str, query: str, limit: int) -> List[SearchResult]:
        try:
            root = ET.fromstring(str(rss_xml or ""))
        except ET.ParseError:
            return []

        seen: set[str] = set()
        out: List[SearchResult] = []
        rank = 0
        for item in root.findall(".//item"):
            link_node = item.find("link")
            title_node = item.find("title")
            desc_node = item.find("description")
            raw_link = str(link_node.text or "").strip() if link_node is not None else ""
            canonical = canonicalize_url(raw_link)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            rank += 1
            title = html_utils.unescape(str(title_node.text or "").strip()) if title_node is not None else ""
            snippet = html_utils.unescape(str(desc_node.text or "").strip()) if desc_node is not None else ""
            out.append(
                SearchResult(
                    url=canonical,
                    title=title[:300],
                    snippet=snippet[:400],
                    rank=rank,
                    query=query,
                )
            )
            if len(out) >= limit:
                break
        return out


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
            (f"https://boards.greenhouse.io/{slug}", "Greenhouse jobs", "Open roles and hiring requirements"),
            (f"https://jobs.lever.co/{slug}", "Lever jobs", "Role descriptions and qualifications"),
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


def is_job_board_url(url: str) -> bool:
    canonical = canonicalize_url(url)
    if not canonical:
        return False
    parsed = urlparse.urlparse(canonical)
    domain = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if any(marker in domain for marker in JOB_BOARD_DOMAIN_MARKERS):
        return True
    if domain.startswith("jobs.") or domain.startswith("careers."):
        return True
    if any(marker in path for marker in JOB_BOARD_PATH_MARKERS):
        return True
    if domain.endswith("linkedin.com") and "/jobs" in path:
        return True
    if domain.endswith("glassdoor.com") and "/job-listing" in path:
        return True
    return False


def classify_source_kind(url: str, official_domain: str = "") -> str:
    domain = normalize_domain(url)
    if not domain:
        return "general"
    if is_job_board_url(url):
        return "job_board"
    if official_domain and (domain == official_domain or domain.endswith(f".{official_domain}")):
        return "official"
    return "general"


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
        f"\"{name}\" jobs",
        f"\"{name}\" careers",
        f"\"{name}\" open roles",
        f"\"{name}\" \"we're hiring\"",
        f"\"{name}\" site:boards.greenhouse.io",
        f"\"{name}\" site:jobs.lever.co",
        f"\"{name}\" site:workdayjobs.com",
        f"\"{name}\" site:jobs.smartrecruiters.com",
        f"\"{name}\" site:jobs.ashbyhq.com",
    ]
    if domain:
        candidates.insert(0, f"site:{domain} \"{name}\" values")
        candidates.insert(1, f"site:{domain} \"{name}\" careers team")
        candidates.insert(2, f"site:{domain} \"{name}\" open roles")

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
    if is_job_board_url(result.url):
        score += 180

    rank = result.rank if result.rank > 0 else 99
    score += max(0, 120 - rank * 8)

    title_blob = f"{result.title} {result.snippet}".lower()
    for token in ("culture", "values", "mission", "about", "careers", "team"):
        if token in title_blob:
            score += 20
    for token in ("job", "role", "hiring", "requirements", "qualifications"):
        if token in title_blob:
            score += 16
    return score


def select_top_urls(
    results: List[SearchResult],
    *,
    official_domain: str,
    max_links: int = 10,
    force_include_url: str = "",
    min_job_board_links: int = 2,
) -> List[SearchResult]:
    normalized_force = canonicalize_url(force_include_url)
    max_links = max(1, int(max_links or 1))
    min_job_board_links = max(0, int(min_job_board_links or 0))

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

    job_board_picked = 0
    for item in ranked:
        if len(picked) >= max_links or job_board_picked >= min_job_board_links:
            break
        if item.url in seen or not is_job_board_url(item.url):
            continue
        seen.add(item.url)
        picked.append(item)
        job_board_picked += 1

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
    MISSION_KEYWORDS = {
        "mission": ["mission", "impact", "purpose", "change", "transform"],
        "revenue": ["revenue", "pipeline", "quota", "growth targets", "upsell", "sales"],
        "execution": ["ship", "delivery", "execution", "deadline", "operator", "ownership"],
        "global": ["global", "worldwide", "category-defining", "industry-leading", "at scale"],
    }

    PERFORMANCE_KEYWORDS = {
        "high": ["high standards", "fast-paced", "urgency", "intense", "ownership", "self-starter"],
        "balanced": ["sustainable", "balance", "well-being", "healthy pace"],
        "lifestyle": ["work-life", "low stress", "flexible schedule"],
        "process": ["process", "compliance", "control", "governance", "risk management"],
        "output": ["outcomes", "results", "ship", "impact", "deliver"],
    }

    DECISION_KEYWORDS = {
        "founder": ["founder", "ceo", "executive team", "leadership team"],
        "data": ["data-driven", "metrics", "experiments", "a/b", "hypothesis"],
        "consensus": ["consensus", "alignment", "cross-functional", "stakeholder"],
        "hierarchy": ["approval", "chain of command", "escalate", "sign-off"],
        "docs": ["written", "documentation", "memo", "spec", "rfc"],
        "autonomy": ["autonomy", "independent", "ownership", "self-directed"],
    }

    RISK_KEYWORDS = {
        "speed": ["move fast", "urgency", "fast-paced", "rapid iteration", "ship quickly"],
        "correctness": ["correctness", "reliability", "quality bar", "compliance", "regulatory"],
        "failure_learning": ["learn from failure", "postmortem", "retrospective", "iterate"],
        "failure_blame": ["zero mistakes", "error-free", "no tolerance for errors"],
        "experimentation": ["experiment", "prototype", "test and learn", "pilot"],
    }

    def generate_profile(
        self,
        company_name: str,
        website_url: str,
        sources: List[ScrapedSource],
    ) -> Dict[str, Any]:
        ok_sources = [x for x in sources if getattr(x, "fetch_status", "") == "ok" and getattr(x, "extracted_text", "")]
        text_blocks = [str(getattr(source, "extracted_text", "")) for source in ok_sources]
        corpus = " ".join(text_blocks).lower()
        job_corpus = " ".join(
            str(getattr(source, "extracted_text", ""))
            for source in ok_sources
            if str(getattr(source, "source_kind", "")).strip().lower() == "job_board"
        ).lower()

        mission = self._infer_mission_orientation(corpus=corpus)
        performance = self._infer_performance_expectations(corpus=corpus, job_corpus=job_corpus)
        decision = self._infer_decision_style(corpus=corpus)
        risk = self._infer_risk_speed_tolerance(corpus=corpus)
        talent = self._infer_talent_profile(corpus=corpus, job_corpus=job_corpus)
        collaboration = self._infer_collaboration_model(corpus=corpus, job_corpus=job_corpus)
        contradictions = self._infer_contradictions(
            performance=performance,
            decision=decision,
            risk=risk,
            collaboration=collaboration,
        )
        who_join, who_avoid = self._infer_join_avoid(talent=talent, performance=performance, decision=decision)

        matched_values = self._extract_values(corpus)
        summary = (
            f"{company_name} looks like a {performance['mode']} environment with "
            f"{risk['speed_vs_perfection']} trade-offs. Decision style appears {decision['style']} "
            f"with {decision['autonomy']} autonomy expectations. "
            f"Primary fit pattern: {talent['thrives'][0] if talent['thrives'] else 'independent operators with strong execution discipline'}."
        )
        gaps = self._evidence_gaps(sources=ok_sources)
        risks = self._unique_preserve_order(contradictions + gaps, limit=6)
        hiring_signals = self._unique_preserve_order(
            [f"expects {x}" for x in (talent["thrives"] or [])] + [f"penalizes {x}" for x in (talent["struggles"] or [])],
            limit=8,
        )

        return {
            "summary_200_300_words": summary,
            "culture_values": matched_values[:8],
            "work_style": [
                performance["assessment"],
                collaboration["assessment"],
                risk["assessment"],
            ][:5],
            "management_style": [decision["assessment"]][:5],
            "hiring_signals": hiring_signals[:8],
            "risks_or_unknowns": risks[:8],
            "culture_interview_questions": [
                "Tell us about a time you challenged a decision with data and changed the outcome.",
                "Describe the highest-pressure environment where you still maintained quality.",
                "What kind of management style makes you underperform?",
            ],
            "mission_orientation": mission,
            "performance_expectations": performance,
            "decision_making_style": decision,
            "risk_speed_tolerance": risk,
            "talent_profile_they_attract": talent,
            "collaboration_model": collaboration,
            "cultural_contradictions": contradictions,
            "who_should_join": who_join,
            "who_should_avoid": who_avoid,
            "evidence_gaps": gaps,
        }

    @staticmethod
    def _top_unique(values: List[str], limit: int) -> List[str]:
        counts = Counter(values)
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [item[0] for item in ordered[:limit]]

    @staticmethod
    def _extract_values(corpus: str) -> List[str]:
        values: List[str] = []
        for label, keywords in CULTURE_ATTRIBUTE_KEYWORDS.items():
            if any(keyword in corpus for keyword in keywords):
                values.append(label)
        return values or ["high standards", "ownership", "cross-functional collaboration"]

    @staticmethod
    def _score_hits(corpus: str, tokens: List[str]) -> int:
        return sum(1 for token in tokens if token in corpus)

    def _infer_mission_orientation(self, *, corpus: str) -> Dict[str, Any]:
        mission_score = self._score_hits(corpus, self.MISSION_KEYWORDS["mission"])
        revenue_score = self._score_hits(corpus, self.MISSION_KEYWORDS["revenue"])
        execution_score = self._score_hits(corpus, self.MISSION_KEYWORDS["execution"])
        global_score = self._score_hits(corpus, self.MISSION_KEYWORDS["global"])

        intensity = "high" if mission_score + execution_score >= 5 else "moderate" if mission_score + execution_score >= 2 else "low"
        if mission_score >= max(revenue_score, execution_score):
            orientation = "mission-driven"
        elif revenue_score >= max(mission_score, execution_score):
            orientation = "revenue-driven"
        else:
            orientation = "execution-driven"
        ambition = "global" if global_score >= 2 else "incremental"
        assessment = (
            f"Mission intensity looks {intensity}. Language leans {orientation} with {ambition} ambition. "
            "The pattern suggests this is judged by shipped outcomes, not narrative alone."
        )
        return {
            "mission_intensity": intensity,
            "orientation": orientation,
            "ambition_scope": ambition,
            "assessment": assessment,
        }

    def _infer_performance_expectations(self, *, corpus: str, job_corpus: str) -> Dict[str, Any]:
        high = self._score_hits(corpus, self.PERFORMANCE_KEYWORDS["high"]) + self._score_hits(job_corpus, self.PERFORMANCE_KEYWORDS["high"])
        balanced = self._score_hits(corpus, self.PERFORMANCE_KEYWORDS["balanced"])
        lifestyle = self._score_hits(corpus, self.PERFORMANCE_KEYWORDS["lifestyle"])
        process_score = self._score_hits(corpus, self.PERFORMANCE_KEYWORDS["process"])
        output_score = self._score_hits(corpus, self.PERFORMANCE_KEYWORDS["output"]) + self._score_hits(
            job_corpus, self.PERFORMANCE_KEYWORDS["output"]
        )

        if high >= max(balanced, lifestyle) + 1:
            mode = "high-performance"
        elif lifestyle > high and lifestyle > balanced:
            mode = "lifestyle-oriented"
        else:
            mode = "balanced"
        output_vs_process = "output-biased" if output_score > process_score else "process-biased" if process_score > output_score else "balanced"
        assessment = (
            f"The environment reads as {mode}. Expectations point to {output_vs_process} evaluation. "
            "Ownership and pace signals are explicit in hiring language."
        )
        return {"mode": mode, "output_vs_process": output_vs_process, "assessment": assessment}

    def _infer_decision_style(self, *, corpus: str) -> Dict[str, Any]:
        founder = self._score_hits(corpus, self.DECISION_KEYWORDS["founder"])
        data = self._score_hits(corpus, self.DECISION_KEYWORDS["data"])
        consensus = self._score_hits(corpus, self.DECISION_KEYWORDS["consensus"])
        hierarchy = self._score_hits(corpus, self.DECISION_KEYWORDS["hierarchy"])
        docs = self._score_hits(corpus, self.DECISION_KEYWORDS["docs"])
        autonomy_hits = self._score_hits(corpus, self.DECISION_KEYWORDS["autonomy"])

        style = "data-driven"
        best = max(founder, data, consensus, hierarchy)
        if best == founder and founder > 0:
            style = "founder-led"
        elif best == consensus and consensus > 0:
            style = "consensus-driven"
        elif best == hierarchy and hierarchy > 0:
            style = "hierarchical"
        autonomy = "high" if autonomy_hits >= 2 else "moderate" if autonomy_hits == 1 else "low"
        documentation = "strong" if docs >= 2 else "moderate" if docs == 1 else "weak"
        assessment = f"Decision-making appears {style}. Autonomy expectation is {autonomy}, and documentation discipline is {documentation}."
        return {
            "style": style,
            "autonomy": autonomy,
            "documentation": documentation,
            "assessment": assessment,
        }

    def _infer_risk_speed_tolerance(self, *, corpus: str) -> Dict[str, Any]:
        speed = self._score_hits(corpus, self.RISK_KEYWORDS["speed"])
        correctness = self._score_hits(corpus, self.RISK_KEYWORDS["correctness"])
        learning = self._score_hits(corpus, self.RISK_KEYWORDS["failure_learning"])
        blame = self._score_hits(corpus, self.RISK_KEYWORDS["failure_blame"])
        experimentation = self._score_hits(corpus, self.RISK_KEYWORDS["experimentation"])

        speed_vs_perfection = "speed-biased" if speed > correctness else "correctness-biased" if correctness > speed else "balanced"
        failure = "learning-oriented" if learning >= blame else "zero-defect"
        experimentation_mode = "visible" if experimentation >= 2 else "limited" if experimentation == 1 else "unclear"
        assessment = (
            f"Risk posture is {speed_vs_perfection}. Failure handling looks {failure}. "
            f"Experimentation is {experimentation_mode}."
        )
        return {
            "speed_vs_perfection": speed_vs_perfection,
            "failure_handling": failure,
            "experimentation": experimentation_mode,
            "assessment": assessment,
        }

    @staticmethod
    def _infer_talent_profile(*, corpus: str, job_corpus: str) -> Dict[str, Any]:
        thriving: List[str] = []
        struggling: List[str] = []
        blob = f"{corpus} {job_corpus}"
        if any(x in blob for x in ["ownership", "self-starter", "autonomy"]):
            thriving.append("high-agency operators who execute without hand-holding")
            struggling.append("people who need constant direction")
        if any(x in blob for x in ["fast-paced", "urgency", "rapid"]):
            thriving.append("people comfortable with sustained urgency")
            struggling.append("people who optimize for slow consensus")
        if any(x in blob for x in ["cross-functional", "stakeholder", "collaboration"]):
            thriving.append("strong cross-functional communicators")
        if any(x in blob for x in ["quality", "high standards", "correctness", "reliability"]):
            thriving.append("people with a strong quality bar under pressure")
            struggling.append("people who trade correctness for convenience")
        if not thriving:
            thriving.append("self-directed contributors with strong execution discipline")
        if not struggling:
            struggling.append("people who avoid accountability in ambiguous environments")
        assessment = (
            f"Talent fit skews toward {thriving[0]}. Friction risk is highest for {struggling[0]}."
        )
        return {
            "thrives": HeuristicCompanyProfileSynthesizer._unique_preserve_order(thriving, limit=6),
            "struggles": HeuristicCompanyProfileSynthesizer._unique_preserve_order(struggling, limit=6),
            "assessment": assessment,
        }

    @staticmethod
    def _infer_collaboration_model(*, corpus: str, job_corpus: str) -> Dict[str, Any]:
        blob = f"{corpus} {job_corpus}"
        cross = sum(1 for x in ["cross-functional", "stakeholder", "partner with", "collaborat"] if x in blob)
        process = sum(1 for x in ["process", "documentation", "compliance", "planning"] if x in blob)
        independent = sum(1 for x in ["ownership", "autonomy", "self-starter", "independent"] if x in blob)

        cross_intensity = "high" if cross >= 3 else "medium" if cross >= 1 else "low"
        if process > independent + 1:
            shape = "structured process"
        elif independent > process + 1:
            shape = "independent operators"
        else:
            shape = "fluid execution"
        assessment = f"Collaboration intensity is {cross_intensity}. Operating shape looks like {shape}."
        return {
            "cross_functional_intensity": cross_intensity,
            "operating_shape": shape,
            "assessment": assessment,
        }

    @staticmethod
    def _infer_contradictions(
        *,
        performance: Dict[str, Any],
        decision: Dict[str, Any],
        risk: Dict[str, Any],
        collaboration: Dict[str, Any],
    ) -> List[str]:
        contradictions: List[str] = []
        if performance.get("mode") == "high-performance" and risk.get("speed_vs_perfection") == "correctness-biased":
            contradictions.append("Hiring language signals urgency, but risk posture emphasizes control and correctness.")
        if decision.get("autonomy") == "high" and decision.get("style") in {"hierarchical", "founder-led"}:
            contradictions.append("Autonomy is requested, yet decision power appears concentrated at the top.")
        if collaboration.get("operating_shape") == "independent operators" and collaboration.get("cross_functional_intensity") == "high":
            contradictions.append("The model expects independent execution and heavy coordination at the same time.")
        return contradictions

    @staticmethod
    def _infer_join_avoid(
        *, talent: Dict[str, Any], performance: Dict[str, Any], decision: Dict[str, Any]
    ) -> tuple[List[str], List[str]]:
        join = list(talent.get("thrives") or [])
        avoid = list(talent.get("struggles") or [])
        if performance.get("mode") == "high-performance":
            avoid.append("if you need a low-pressure environment")
        if decision.get("autonomy") == "high":
            avoid.append("if you expect frequent top-down prioritization")
        join.append("if you prefer direct feedback and clear accountability")
        return (
            HeuristicCompanyProfileSynthesizer._unique_preserve_order(join, limit=6),
            HeuristicCompanyProfileSynthesizer._unique_preserve_order(avoid, limit=6),
        )

    @staticmethod
    def _evidence_gaps(sources: List[ScrapedSource]) -> List[str]:
        gaps: List[str] = []
        domains = " ".join(str(getattr(source, "domain", "")) for source in sources)
        if len(sources) < 4:
            gaps.append("Source coverage is limited; profile confidence is medium-to-low.")
        if "glassdoor.com" not in domains:
            gaps.append("No meaningful employee-review evidence was captured.")
        return gaps

    @staticmethod
    def _unique_preserve_order(values: List[str], limit: int) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for item in values:
            text = str(item or "").strip()
            key = text.lower()
            if not text or key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= limit:
                break
        return out


class OpenAICompanyProfileSynthesizer:
    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o-mini",
        base_url: str = "https://api.openai.com/v1",
        timeout_seconds: int = 30,
        max_chars_per_source: int = 2500,
        analysis_rules_path: str = "",
    ) -> None:
        self.api_key = api_key.strip()
        self.model = model.strip() or "gpt-4o-mini"
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.max_chars_per_source = max(600, int(max_chars_per_source))
        self.analysis_rules = self._load_analysis_rules(path=analysis_rules_path)

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
                    "source_kind": source.source_kind or "general",
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
                        "Output strict JSON only. No markdown. No PR language."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "task": (
                                "Infer the real operating culture from evidence. "
                                "Be explicit about trade-offs, pressure profile, and who will fail in this environment."
                            ),
                            "company_name": company_name,
                            "website_url": website_url,
                            "analysis_rules": self.analysis_rules,
                            "required_schema": {
                                "mission_orientation": {
                                    "mission_intensity": "string",
                                    "orientation": "string",
                                    "ambition_scope": "string",
                                    "assessment": "string",
                                },
                                "performance_expectations": {
                                    "mode": "string",
                                    "output_vs_process": "string",
                                    "assessment": "string",
                                },
                                "decision_making_style": {
                                    "style": "string",
                                    "autonomy": "string",
                                    "documentation": "string",
                                    "assessment": "string",
                                },
                                "risk_speed_tolerance": {
                                    "speed_vs_perfection": "string",
                                    "failure_handling": "string",
                                    "experimentation": "string",
                                    "assessment": "string",
                                },
                                "talent_profile_they_attract": {
                                    "thrives": "array[string]",
                                    "struggles": "array[string]",
                                    "assessment": "string",
                                },
                                "collaboration_model": {
                                    "cross_functional_intensity": "string",
                                    "operating_shape": "string",
                                    "assessment": "string",
                                },
                                "cultural_contradictions": "array[string]",
                                "who_should_join": "array[string]",
                                "who_should_avoid": "array[string]",
                                "evidence_gaps": "array[string]",
                                "summary_200_300_words": "string (direct tone, no marketing language)",
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

    @staticmethod
    def _load_analysis_rules(path: str) -> str:
        cleaned = str(path or "").strip()
        if cleaned:
            target = Path(cleaned)
            if not target.is_absolute() and not target.exists():
                target = Path(__file__).resolve().parents[2] / cleaned
            if target.exists():
                try:
                    text = target.read_text(encoding="utf-8").strip()
                    if text:
                        return text
                except OSError:
                    pass
        return DEFAULT_CULTURE_ANALYSIS_RULES


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
        min_job_board_links: int = 2,
        fetch_timeout_seconds: int = 15,
        min_text_chars: int = 600,
    ) -> None:
        self.search_provider = search_provider
        self.page_fetcher = page_fetcher
        self.content_extractor = content_extractor
        self.synthesizer = synthesizer
        self.max_links = max(1, int(max_links))
        self.per_query_limit = max(1, int(per_query_limit))
        self.min_job_board_links = max(0, int(min_job_board_links))
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
            min_job_board_links=self.min_job_board_links,
        )

        sources: List[ScrapedSource] = []
        for item in selected:
            source = self._scrape_one(item, official_domain=official_domain)
            sources.append(source)

        success_sources = [item for item in sources if item.fetch_status == "ok"]
        job_board_insights = self._extract_job_board_insights(success_sources)
        synthesis_sources = list(success_sources)
        job_board_synthesis_source = self._build_job_board_synthesis_source(job_board_insights)
        if job_board_synthesis_source is not None:
            synthesis_sources.append(job_board_synthesis_source)

        warnings: List[str] = []
        if int(job_board_insights.get("job_board_sources_total") or 0) <= 0:
            warnings.append("job_board_evidence_missing")
        profile: Dict[str, Any] = {}
        if synthesis_sources:
            try:
                profile = self.synthesizer.generate_profile(
                    company_name=normalized_name,
                    website_url=normalized_website,
                    sources=synthesis_sources,
                )
            except Exception as exc:
                warnings.append(f"llm_synthesis_failed: {exc}")
                profile = self._fallback_profile(normalized_name)
        else:
            warnings.append("no_scraped_sources_for_synthesis")
            profile = self._fallback_profile(normalized_name)
        profile = self._merge_profile_with_job_board_insights(profile, job_board_insights)
        profile = self._normalize_profile_shape(profile, company_name=normalized_name)

        return {
            "company_name": normalized_name,
            "website": normalized_website,
            "search_queries": queries,
            "searched_links_total": len(raw_results),
            "selected_links_total": len(selected),
            "scraped_success_total": len(success_sources),
            "scraped_failed_total": len(sources) - len(success_sources),
            "sources": [item.as_dict() for item in sources],
            "job_board_insights": job_board_insights,
            "profile": profile,
            "warnings": warnings,
        }

    def _scrape_one(self, item: SearchResult, *, official_domain: str) -> ScrapedSource:
        domain = normalize_domain(item.url)
        source_kind = classify_source_kind(item.url, official_domain=official_domain)
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
                source_kind=source_kind,
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
                source_kind=source_kind,
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
                source_kind=source_kind,
            )

        if "text/html" in content_type or body.lstrip().lower().startswith("<!doctype html") or "<html" in body[:200].lower():
            cleaned = self.content_extractor.extract_text(body, item.url)
        else:
            cleaned = re.sub(r"\s+", " ", body).strip()

        min_chars = self.min_text_chars
        if source_kind == "job_board":
            min_chars = max(180, min(self.min_text_chars, 320))
        if len(cleaned) < min_chars:
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
                source_kind=source_kind,
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
            source_kind=source_kind,
        )

    def _extract_job_board_insights(self, sources: List[ScrapedSource]) -> Dict[str, Any]:
        job_sources = [x for x in sources if x.source_kind == "job_board"]
        if not job_sources:
            return {
                "job_board_sources_total": 0,
                "candidate_profiles_sought": [],
                "cultural_attributes_in_job_ads": [],
                "example_roles_seen": [],
                "evidence_snippets": [],
            }

        candidate_hits: List[str] = []
        culture_hits: List[str] = []
        snippets: List[str] = []
        roles: List[str] = []

        for source in job_sources:
            text = str(source.extracted_text or "")
            lower = text.lower()
            for label, keywords in CANDIDATE_SIGNAL_KEYWORDS.items():
                if any(keyword in lower for keyword in keywords):
                    candidate_hits.append(label)
            for label, keywords in CULTURE_ATTRIBUTE_KEYWORDS.items():
                if any(keyword in lower for keyword in keywords):
                    culture_hits.append(label)
            snippets.extend(self._extract_job_signal_snippets(text, limit=3))
            roles.extend(self._extract_role_labels(source))

        return {
            "job_board_sources_total": len(job_sources),
            "candidate_profiles_sought": self._top_labels(candidate_hits, limit=8),
            "cultural_attributes_in_job_ads": self._top_labels(culture_hits, limit=8),
            "example_roles_seen": self._unique_preserve_order([r for r in roles if r], limit=8),
            "evidence_snippets": self._unique_preserve_order(snippets, limit=8),
        }

    def _build_job_board_synthesis_source(self, insights: Dict[str, Any]) -> Optional[ScrapedSource]:
        source_total = int(insights.get("job_board_sources_total") or 0)
        if source_total <= 0:
            return None
        candidate_profiles = insights.get("candidate_profiles_sought") or []
        culture_attrs = insights.get("cultural_attributes_in_job_ads") or []
        role_samples = insights.get("example_roles_seen") or []
        snippets = insights.get("evidence_snippets") or []
        summary = (
            "Job board extracted evidence. "
            f"Job-board pages analyzed: {source_total}. "
            f"Candidate profiles sought: {', '.join(candidate_profiles[:6]) or 'n/a'}. "
            f"Cultural attributes in job ads: {', '.join(culture_attrs[:6]) or 'n/a'}. "
            f"Example roles: {', '.join(role_samples[:6]) or 'n/a'}. "
            f"Evidence snippets: {' | '.join(snippets[:4]) or 'n/a'}."
        )
        return ScrapedSource(
            url="job-board://insights",
            domain="job-board",
            title="Job board extracted insights",
            query="job board insights",
            search_rank=0,
            fetch_status="ok",
            http_status=200,
            text_chars=len(summary),
            extracted_text=summary,
            source_kind="job_board",
        )

    @staticmethod
    def _merge_profile_with_job_board_insights(
        profile: Dict[str, Any], insights: Dict[str, Any]
    ) -> Dict[str, Any]:
        merged = dict(profile or {})
        hiring_signals = list(merged.get("hiring_signals") or [])
        candidate_profiles = [str(x).strip() for x in (insights.get("candidate_profiles_sought") or []) if str(x).strip()]
        culture_attrs = [str(x).strip() for x in (insights.get("cultural_attributes_in_job_ads") or []) if str(x).strip()]

        for item in candidate_profiles[:4]:
            hiring_signals.append(f"job-board pattern: seeks {item}")
        for item in culture_attrs[:3]:
            hiring_signals.append(f"job-board culture signal: {item}")

        merged["hiring_signals"] = CompanyCultureProfileService._unique_preserve_order(hiring_signals, limit=8)
        merged["candidate_profiles_sought"] = candidate_profiles[:8]
        merged["cultural_attributes_in_job_ads"] = culture_attrs[:8]
        return merged

    @staticmethod
    def _normalize_profile_shape(profile: Dict[str, Any], *, company_name: str) -> Dict[str, Any]:
        normalized = dict(profile or {})

        def as_list(value: Any, *, limit: int = 8) -> List[str]:
            if isinstance(value, list):
                items = [str(x).strip() for x in value if str(x).strip()]
                return CompanyCultureProfileService._unique_preserve_order(items, limit=limit)
            if isinstance(value, str) and value.strip():
                return [value.strip()][:limit]
            return []

        def as_str(value: Any, fallback: str = "") -> str:
            text = str(value or "").strip()
            return text or fallback

        mission = normalized.get("mission_orientation")
        if not isinstance(mission, dict):
            mission = {"assessment": as_str(mission)}
        performance = normalized.get("performance_expectations")
        if not isinstance(performance, dict):
            performance = {"assessment": as_str(performance)}
        decision = normalized.get("decision_making_style")
        if not isinstance(decision, dict):
            decision = {"assessment": as_str(decision)}
        risk = normalized.get("risk_speed_tolerance")
        if not isinstance(risk, dict):
            risk = {"assessment": as_str(risk)}
        talent = normalized.get("talent_profile_they_attract")
        if not isinstance(talent, dict):
            talent = {"assessment": as_str(talent), "thrives": [], "struggles": []}
        collaboration = normalized.get("collaboration_model")
        if not isinstance(collaboration, dict):
            collaboration = {"assessment": as_str(collaboration)}

        mission.setdefault("mission_intensity", "unclear")
        mission.setdefault("orientation", "mixed")
        mission.setdefault("ambition_scope", "unclear")
        mission["assessment"] = as_str(mission.get("assessment"), "Mission signals are present but not yet conclusive.")

        performance.setdefault("mode", "unclear")
        performance.setdefault("output_vs_process", "unclear")
        performance["assessment"] = as_str(
            performance.get("assessment"),
            "Performance profile is unclear due to limited explicit language in sources.",
        )

        decision.setdefault("style", "mixed")
        decision.setdefault("autonomy", "unclear")
        decision.setdefault("documentation", "unclear")
        decision["assessment"] = as_str(
            decision.get("assessment"),
            "Decision style signals are mixed and require more direct leadership evidence.",
        )

        risk.setdefault("speed_vs_perfection", "unclear")
        risk.setdefault("failure_handling", "unclear")
        risk.setdefault("experimentation", "unclear")
        risk["assessment"] = as_str(
            risk.get("assessment"),
            "Risk posture is not explicit in available materials.",
        )

        talent["thrives"] = as_list(talent.get("thrives"), limit=8)
        talent["struggles"] = as_list(talent.get("struggles"), limit=8)
        talent["assessment"] = as_str(talent.get("assessment"), "Candidate fit profile remains partially inferred.")

        collaboration.setdefault("cross_functional_intensity", "unclear")
        collaboration.setdefault("operating_shape", "unclear")
        collaboration["assessment"] = as_str(
            collaboration.get("assessment"),
            "Collaboration model is partially visible but not fully explicit.",
        )

        contradictions = as_list(normalized.get("cultural_contradictions"), limit=8)
        who_join = as_list(normalized.get("who_should_join"), limit=8)
        who_avoid = as_list(normalized.get("who_should_avoid"), limit=8)
        evidence_gaps = as_list(normalized.get("evidence_gaps"), limit=8)

        summary = as_str(normalized.get("summary_200_300_words"))
        if not summary:
            summary = (
                f"{company_name} appears to operate with {as_str(performance.get('mode'), 'mixed')} expectations, "
                f"{as_str(decision.get('style'), 'mixed')} decision dynamics, and "
                f"{as_str(risk.get('speed_vs_perfection'), 'unclear')} risk trade-offs. "
                "This profile should be treated as directional until more primary sources are added."
            )

        culture_values = as_list(normalized.get("culture_values"), limit=8)
        if not culture_values:
            culture_values = as_list(normalized.get("cultural_attributes_in_job_ads"), limit=8)

        work_style = as_list(normalized.get("work_style"), limit=8)
        if not work_style:
            work_style = [performance["assessment"], collaboration["assessment"]]
        management_style = as_list(normalized.get("management_style"), limit=8)
        if not management_style:
            management_style = [decision["assessment"]]
        hiring_signals = as_list(normalized.get("hiring_signals"), limit=8)
        if not hiring_signals:
            hiring_signals = [f"fit: {x}" for x in talent["thrives"][:4]]
        risks = as_list(normalized.get("risks_or_unknowns"), limit=8)
        if not risks:
            risks = contradictions[:4] + evidence_gaps[:4]
        questions = as_list(normalized.get("culture_interview_questions"), limit=3)
        if len(questions) < 2:
            questions = [
                "Describe a time you pushed back on a decision and what happened.",
                "How do you balance delivery speed against quality when stakes are high?",
                "What type of manager and operating cadence makes you perform poorly?",
            ]

        normalized.update(
            {
                "summary_200_300_words": summary,
                "culture_values": culture_values,
                "work_style": work_style,
                "management_style": management_style,
                "hiring_signals": hiring_signals,
                "risks_or_unknowns": CompanyCultureProfileService._unique_preserve_order(risks, limit=8),
                "culture_interview_questions": questions[:3],
                "mission_orientation": mission,
                "performance_expectations": performance,
                "decision_making_style": decision,
                "risk_speed_tolerance": risk,
                "talent_profile_they_attract": talent,
                "collaboration_model": collaboration,
                "cultural_contradictions": contradictions,
                "who_should_join": who_join,
                "who_should_avoid": who_avoid,
                "evidence_gaps": evidence_gaps,
            }
        )
        return normalized

    @staticmethod
    def _extract_job_signal_snippets(text: str, limit: int = 3) -> List[str]:
        chunks = re.split(r"(?<=[\.\!\?])\s+|\s[•\-]\s", str(text or ""))
        out: List[str] = []
        for chunk in chunks:
            normalized = re.sub(r"\s+", " ", chunk).strip()
            if len(normalized) < 50 or len(normalized) > 240:
                continue
            lower = normalized.lower()
            if any(token in lower for token in JOB_SENTENCE_KEYWORDS):
                out.append(normalized)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _extract_role_labels(source: ScrapedSource) -> List[str]:
        roles: List[str] = []
        title = re.sub(r"\s+", " ", str(source.title or "")).strip()
        if title:
            title = re.sub(r"\s+[-|].*$", "", title).strip()
            if 4 <= len(title) <= 90:
                roles.append(title)

        pattern = re.compile(
            r"\b(?:senior|staff|principal|lead|head|director|vp|junior)?\s*"
            r"(?:software|backend|frontend|full[- ]stack|data|machine learning|ml|product|design|"
            r"operations|marketing|sales|security|devops|qa|clinical|research)?\s*"
            r"(?:engineer|developer|scientist|manager|designer|analyst|architect|specialist)\b",
            flags=re.IGNORECASE,
        )
        for match in pattern.findall(str(source.extracted_text or "")[:4000]):
            candidate = re.sub(r"\s+", " ", str(match)).strip()
            if 4 <= len(candidate) <= 80:
                roles.append(candidate.title())
            if len(roles) >= 6:
                break
        return roles

    @staticmethod
    def _top_labels(values: List[str], limit: int) -> List[str]:
        counts = Counter([str(x).strip().lower() for x in values if str(x).strip()])
        if not counts:
            return []
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return [item[0] for item in ordered[:limit]]

    @staticmethod
    def _unique_preserve_order(values: List[str], limit: int) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for item in values:
            text = str(item or "").strip()
            key = text.lower()
            if not text or key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _fallback_profile(company_name: str) -> Dict[str, Any]:
        return {
            "summary_200_300_words": (
                f"{company_name} has insufficient external evidence for a reliable operating-culture read. "
                "Current output is provisional and should not be used for candidate fit decisions without more sources."
            ),
            "culture_values": [],
            "work_style": [],
            "management_style": [],
            "hiring_signals": [],
            "risks_or_unknowns": ["Insufficient evidence from scraped sources."],
            "culture_interview_questions": [
                "Describe a time you challenged a leadership decision with data.",
                "In high-pressure situations, how do you trade speed against quality?",
            ],
            "mission_orientation": {"assessment": "Not enough evidence."},
            "performance_expectations": {"assessment": "Not enough evidence."},
            "decision_making_style": {"assessment": "Not enough evidence."},
            "risk_speed_tolerance": {"assessment": "Not enough evidence."},
            "talent_profile_they_attract": {"thrives": [], "struggles": [], "assessment": "Not enough evidence."},
            "collaboration_model": {"assessment": "Not enough evidence."},
            "cultural_contradictions": [],
            "who_should_join": [],
            "who_should_avoid": [],
            "evidence_gaps": ["Insufficient evidence from scraped sources."],
        }
