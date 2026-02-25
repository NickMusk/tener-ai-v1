# Company Culture Profile Module: Architecture + Technical Contract (v1)

## 1. Scope

Purpose: generate a structured `company_culture_profile` from two inputs:
- `company_name`
- `company_website_url`

Pipeline requirement (v1):
1. Build Google queries for the company.
2. Read top search results.
3. Open/scrape top-10 unique links.
4. Run LLM synthesis over scraped evidence.
5. Return structured profile + sources used.

This module is designed for onboarding in `Client Web Portal` and for culture-fit question generation in interview flow.

## 2. Non-goals (v1)

- No deep crawl across full websites (only selected top links).
- No social-login/browser automation (HTTP fetch only).
- No legal decision engine for scraping policy by country.
- No autonomous profile editing by clients (profile is generated and then confirmed).

## 3. Module Boundaries

New internal components:
- `CompanyCultureProfileService` (orchestration)
- `SearchProvider` (Google search adapter)
- `PageFetcher` (HTTP retrieval)
- `ContentExtractor` (HTML -> clean text)
- `CompanyProfileSynthesizer` (LLM synthesis)

Integration points with existing backend:
- triggered during company onboarding
- persisted in DB and linked to company/account
- optionally reused to generate culture-fit interview questions

## 4. Input/Output Contract

### 4.1 Input

```json
{
  "company_name": "Acme Labs",
  "company_website_url": "https://acme.example"
}
```

Validation:
- `company_name`: non-empty, <= 200 chars
- `company_website_url`: valid `http/https` URL or domain-like string

### 4.2 Output

```json
{
  "company_name": "Acme Labs",
  "website": "https://acme.example/",
  "search_queries": [],
  "searched_links_total": 48,
  "selected_links_total": 10,
  "scraped_success_total": 8,
  "scraped_failed_total": 2,
  "sources": [],
  "job_board_insights": {
    "job_board_sources_total": 0,
    "candidate_profiles_sought": [],
    "cultural_attributes_in_job_ads": [],
    "example_roles_seen": [],
    "evidence_snippets": []
  },
  "profile": {
    "summary_200_300_words": "",
    "culture_values": [],
    "work_style": [],
    "management_style": [],
    "hiring_signals": [],
    "risks_or_unknowns": [],
    "culture_interview_questions": [],
    "mission_orientation": {},
    "performance_expectations": {},
    "decision_making_style": {},
    "risk_speed_tolerance": {},
    "talent_profile_they_attract": {},
    "collaboration_model": {},
    "cultural_contradictions": [],
    "who_should_join": [],
    "who_should_avoid": []
  },
  "warnings": []
}
```

## 5. Retrieval + Ranking Pipeline

### 5.1 Query generation

Generate a compact query set (6-8 queries), for example:
- `"Acme Labs" company culture values`
- `"Acme Labs" engineering culture`
- `"Acme Labs" leadership principles`
- `"Acme Labs" employee reviews`
- `"Acme Labs" glassdoor`
- `"Acme Labs" linkedin company`
- `site:acme.example "Acme Labs" values`
- `"Acme Labs" site:boards.greenhouse.io`
- `"Acme Labs" site:jobs.lever.co`
- `"Acme Labs" site:workdayjobs.com`

### 5.2 Search stage

- Provider: Google (via API adapter, not browser scraping)
- Retrieve `per_query_limit` results (default 10)
- Keep URL, title, snippet, query, rank

### 5.3 URL selection (top-10)

Selection rules:
- canonicalize URLs and deduplicate
- prioritize official company domain and subdomains
- enforce a minimum quota of job-board links when available
- score by intent terms (`culture`, `values`, `about`, `careers`, `team`)
- keep source diversity (not only one domain when possible)
- final cap: exactly 10 URLs (or fewer if not available)

### 5.4 Scraping stage

For each selected URL:
- fetch page with timeout and user-agent
- parse HTML and extract visible text
- drop low-signal docs (`text_chars < threshold`)
- store failure reason when fetch/parse fails

## 6. LLM Synthesis Contract

Input to LLM:
- company identity (`company_name`, `website`)
- curated evidence docs (url + title + cleaned text snippets)

Required output (strict JSON object):
- `summary_200_300_words`
- `culture_values` (3-8 items)
- `work_style` (3-8 items)
- `management_style` (3-8 items)
- `hiring_signals` (3-8 observable signals)
- `risks_or_unknowns` (0-5 items; explicitly call data gaps)
- `culture_interview_questions` (2-3 questions)

Guardrails:
- evidence-based phrasing, avoid invented facts
- if confidence is low, fill `risks_or_unknowns`
- no PII extraction

## 7. Reliability and Safety

Failure strategy:
- partial success is valid (return profile if >=1 strong source)
- if LLM fails: return deterministic fallback profile + warning
- include per-source status (`ok|fetch_failed|parse_failed|too_short`)

Operational controls:
- max URLs per run: 10
- max fetch bytes per page
- max chars per doc sent to LLM
- total wall-clock timeout for run

Compliance controls:
- respect robots policy where required by provider policy
- keep source URLs for auditability
- redact tracking params from URLs before persistence

## 8. Data Model (SQLite, proposed)

### 8.1 `company_profile_runs`

```sql
CREATE TABLE IF NOT EXISTS company_profile_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    website_url TEXT NOT NULL,
    status TEXT NOT NULL, -- running|completed|partial|failed
    search_queries_json TEXT NOT NULL,
    searched_links_total INTEGER NOT NULL DEFAULT 0,
    selected_links_total INTEGER NOT NULL DEFAULT 0,
    scraped_success_total INTEGER NOT NULL DEFAULT 0,
    scraped_failed_total INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT NOT NULL DEFAULT '[]',
    profile_json TEXT, -- nullable when hard-failed
    created_at TEXT NOT NULL,
    completed_at TEXT
);
```

### 8.2 `company_profile_sources`

```sql
CREATE TABLE IF NOT EXISTS company_profile_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    domain TEXT,
    title TEXT,
    query_text TEXT,
    search_rank INTEGER,
    fetch_status TEXT NOT NULL, -- ok|fetch_failed|parse_failed|too_short
    http_status INTEGER,
    text_chars INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES company_profile_runs(id)
);
```

## 9. API Contract (v1)

### 9.1 Generate profile

`POST /api/company-profiles/generate`

Request:
```json
{
  "company_name": "Acme Labs",
  "company_website_url": "https://acme.example"
}
```

Response `200` (sync v1):
```json
{
  "run_id": 101,
  "status": "completed",
  "result": {}
}
```

### 9.2 Get run result

`GET /api/company-profiles/runs/{run_id}`

Response:
- current status
- full result (sources + profile + warnings)

## 10. Configuration (env, proposed)

- `TENER_COMPANY_PROFILE_ENABLED` (`true/false`, default `true`)
- `TENER_COMPANY_PROFILE_MAX_LINKS` (default `10`)
- `TENER_COMPANY_PROFILE_PER_QUERY_LIMIT` (default `10`)
- `TENER_COMPANY_PROFILE_FETCH_TIMEOUT_SECONDS` (default `15`)
- `TENER_COMPANY_PROFILE_MIN_TEXT_CHARS` (default `600`)
- `TENER_COMPANY_PROFILE_LLM_MODEL` (default `gpt-4o-mini`)
- `TENER_COMPANY_PROFILE_SEARCH_MODE` (default `bing_rss`; options: `bing_rss|duckduckgo_html|brave_html|google_cse|seed`)
- `TENER_COMPANY_PROFILE_ANALYSIS_RULES_PATH` (default `config/instructions/company_culture_profile_analysis_rules.md`)
- `GOOGLE_CSE_API_KEY` / `GOOGLE_CSE_CX` (if Google CSE is used)

## 11. Metrics

Track per run:
- `search_latency_ms`
- `fetch_success_rate`
- `avg_text_chars_per_source`
- `llm_latency_ms`
- `profile_completion_rate`
- `profile_regeneration_rate`

## 12. Rollout Plan

1. Ship module as isolated service class + unit tests.
2. Add API endpoint for explicit generation.
3. Integrate into onboarding flow with manual trigger.
4. Enable auto-run on onboarding, keep client confirmation step.
5. Add caching/reuse (skip re-scrape if fresh profile exists).
