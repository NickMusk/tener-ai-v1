from __future__ import annotations

import html as html_utils
import json
import re
import xml.etree.ElementTree as ET
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
