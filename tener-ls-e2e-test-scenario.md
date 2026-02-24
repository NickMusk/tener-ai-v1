# Tener Life Sciences — Credential Verification
# End-to-End Test Scenario + API Reality Check

---

## ЧАСТЬ 1: ЧЕСТНАЯ ПРОВЕРКА API (что реально работает)

### ✅ TIER 1 — Можем сделать САМИ, бесплатно

| # | Проверка | Реальный метод доступа | Формат | Стоимость | Что я написал в спеке | Реальность |
|---|----------|----------------------|--------|-----------|----------------------|------------|
| 1 | **OIG LEIE** (федеральные исключения) | Скачиваемый CSV файл с oig.hhs.gov. Обновляется ежемесячно. Полная база ~70K записей | CSV файл | Бесплатно | "Downloadable file" | ✅ Верно. Загружаем в Postgres, матчим по имени+DOB. Нет REST API — только файл |
| 2 | **SAM.gov** (правительственные дебарменты) | REST API v4: `api.sam.gov/entity-information/v4/exclusions`. Нужен API key (регистрация бесплатная). Public access = 10 req/day, system account = 1000/day | JSON API | Бесплатно | "REST API" | ✅ Верно. Полноценный API с фильтрацией по имени, штату, дате |
| 3 | **OFAC SDN** (санкции/терроризм) | НЕТ официального API от Treasury. Скачиваемый XML/CSV файл. ИЛИ: третья сторона ofac-api.com (платный), OpenSanctions API (бесплатный, open source) | XML/CSV файл или платный API | Бесплатно (файл) или $50-200/мес (API) | "SDK available" | ⚠️ ЧАСТИЧНО ВЕРНО. Нет официального SDK. Есть файл + сторонние API. OpenSanctions — лучший бесплатный вариант |
| 4 | **FDA Debarment** (дебармент от FDA) | Excel файл на fda.gov. НЕТ endpoint в openFDA API! openFDA покрывает drugs, devices, food — но НЕ debarment list | Excel файл | Бесплатно | "openFDA Elasticsearch API" | ❌ НЕВЕРНО. openFDA не содержит debarment list. Это отдельный Excel файл. Маленький список (~200 записей), можно загрузить в БД |
| 5 | **State Medicaid Exclusions** | Нет единого источника. Каждый штат отдельно. Часть штатов полагается на OIG LEIE. Остальные имеют свои списки на отдельных сайтах | Разнородные форматы | Бесплатно но трудозатратно | "Web scraping / API" | ⚠️ НЕДООЦЕНЕНО. Это 50 отдельных источников. В MVP лучше покрывать top-10 штатов (CA, TX, NY, FL, MA, NJ, PA, IL, NC, MD) где 80% biotech jobs |

### ⚠️ TIER 2 — Требует партнёрства/покупки

| # | Проверка | Реальный метод доступа | Что нужно | Стоимость | Что я написал | Реальность |
|---|----------|----------------------|-----------|-----------|--------------|------------|
| 6 | **DEA Registration** | Через Verisys (использует NTIS). Нет публичного API для поиска по DEA | Контракт с Verisys | Enterprise pricing | "Verisys API" | ⚠️ Верно что через Verisys, но это не plug-and-play API. Enterprise sales cycle |
| 7 | **State License Verification** (50 штатов) | Verisys скрапит данные с 56 юрисдикций. Альтернатива: самим парсить 50 отдельных сайтов лицензионных бордов | Контракт с Verisys ИЛИ кастомные скраперы | $5K-50K/год (Verisys) | "Verisys / aggregator" | ✅ Верно, но стоимость не указана. Скрапить самим = 3-6 месяцев разработки |
| 8 | **RAC/CCEP/CRA Certification** | Каждая организация отдельно: RAPS (RAC), SCCE (CCEP), ACRP (CRA). Нет API. Только ручная проверка или email-запросы | Ручные запросы или email automation | Время + per-request fees | "Registry lookup" | ❌ ПЕРЕОЦЕНЕНО. Нет реестра с API. Это ручная верификация. В MVP — candidate self-report + confirmation email |
| 9 | **FACIS** (Fraud & Abuse) | Продукт Verisys. 868 taxonomies, 10M+ записей, 3500 primary sources. Самый полный но проприетарный | Контракт с Verisys | Enterprise pricing (не публичный) | "Verisys FACIS API" | ✅ Верно что FACIS = Verisys. Но это bundled product, не standalone API call |
| 10 | **Abuse Registry** | Штатные реестры. Нет единого источника. Verisys агрегирует | Через Verisys или вручную | Включено в Verisys | "State database query" | ⚠️ Реалистично только через Verisys partnership |

### 🔴 TIER 3 — Сложно, долго, дорого

| # | Проверка | Реальный метод доступа | Что нужно | Реальность |
|---|----------|----------------------|-----------|------------|
| 11 | **NPDB** | Ограниченный доступ. Нужна регистрация + approval credentialing committee. Verifiable (компания) — одна из 4 организаций с автоматизированным доступом | Партнёрство с Verifiable ($$$) или прямая регистрация в NPDB (долго) | ⚠️ Реально но не для MVP |
| 12 | **Education PSV** | Прямой контакт с университетом. Нет API. Время ответа: 1-6 недель | Email/fax workflow automation | ❌ Не автоматизируется полностью. Можно автоматизировать отправку запросов |
| 13 | **GxP Training** | Контакт с issuing body (ISPE, PDA). Нет API | Email workflow | ❌ Ручной процесс |
| 14 | **Employment History** | HR-департамент предыдущего работодателя. Нет API | Email/phone | ❌ Ручной процесс |
| 15 | **Malpractice / Liability** | Страховые компании. Нет API | Letter/email | ❌ Ручной процесс |

---

## ЧАСТЬ 2: ЧТО РЕАЛЬНО СДЕЛАТЬ В MVP ЗА 2-4 НЕДЕЛИ

### MVP Scope (честный):

**Tier 1 — Full Automation (делаем сами):**
- OIG LEIE: загрузка CSV → PostgreSQL → матчинг по name + DOB
- SAM.gov: прямой REST API call → JSON response
- OFAC: загрузка SDN XML → локальный fuzzy matching ИЛИ OpenSanctions API
- FDA Debarment: загрузка Excel → PostgreSQL → exact match

**Tier 2 — Partial Automation:**
- State license: скрапинг top-5 штатов (CA, TX, NY, MA, NJ) для демо
- Certifications: candidate self-report → manual verification queue

**Tier 3 — Manual + Automation of Workflow:**
- Auto-generate verification request emails
- Track status in dashboard
- Но сама верификация остаётся ручной

---

## ЧАСТЬ 3: END-TO-END ТЕСТОВЫЙ СЦЕНАРИЙ

### Scenario Setup

**Клиент:** NovaBio Inc. — mid-size biotech, San Diego, CA
**Роль:** Senior QA/QC Scientist
**Требования:** cGMP experience, FDA 21 CFR Part 11, RAC certification preferred, CA state license

### Test Candidates (3 профиля для демо)

#### Candidate A: Dr. Sarah Chen (PASS — полностью зелёный)
```
Full Name: Sarah M. Chen
DOB: 1990-03-15
SSN last 4: 4521
State: California
License: CA Pharmacy Board #RPH-87654
DEA: BC7654321
Education: PhD, MIT, 2018
Certifications: RAC (RAPS), cGMP (ISPE)
```

#### Candidate B: Mark Rivera (PARTIAL — жёлтый, license expired)
```
Full Name: Mark J. Rivera
DOB: 1987-08-22
SSN last 4: 3298
State: Texas
License: TX Pharmacy Board #58432 (EXPIRED 2023)
DEA: N/A
Education: MSc, UT Austin, 2012
Certifications: pending RAC
```

#### Candidate C: James Powell (FAIL — красный, OIG exclusion hit)
```
Full Name: James T. Powell
DOB: 1982-11-30
SSN last 4: 7891
State: New York
License: NY Board #PHR-23456
Note: Имя совпадает с исключённой записью в OIG LEIE
```

---

### TEST FLOW — Шаг за шагом

#### Step 0: Data Preparation (one-time setup)
```
Action: Load reference databases
├── Download OIG LEIE CSV → import to PostgreSQL table `leie_exclusions`
│   Fields: lastname, firstname, midname, dob, excltype, excldate, state, specialty
│   Records: ~70,000
│
├── Download OFAC SDN XML → parse → import to `ofac_sdn`
│   Fields: uid, sdnType, lastName, firstName, dob, program, remarks
│   Records: ~12,000
│
├── Download FDA Debarment Excel → import to `fda_debarment`
│   Fields: name, debarment_date, expiration, fr_citation
│   Records: ~200
│
└── Verify SAM.gov API key is active
    Test: GET https://api.sam.gov/entity-information/v4/exclusions?api_key=KEY&q=test
    Expected: 200 OK with JSON response
```

**Validation Criteria:**
- [ ] LEIE table has > 50,000 records
- [ ] OFAC table has > 10,000 records
- [ ] FDA table has > 100 records
- [ ] SAM.gov API returns valid JSON

---

#### Step 1: Candidate Enters Pipeline
```
Trigger: Reed (sourcing agent) adds candidate to pipeline
Input: {name, dob, state, license_number}
Action: Create candidate record, set compliance_status = "QUEUED"
```

**Expected State:**
```
Candidate Card:
├── Compliance Badge: ⏳ "Queued"
├── Traffic Light: [gray] [gray] [gray]
├── Checks Progress: 0/15
└── Compliance Score: —
```

---

#### Step 2: Tier 1 — Instant Screening (automated, < 30 sec)

##### Check 1.1: OIG LEIE
```
Method: SQL query against local leie_exclusions table
Query: SELECT * FROM leie_exclusions 
       WHERE LOWER(lastname) = LOWER($candidate_lastname) 
       AND LOWER(firstname) = LOWER($candidate_firstname)
       AND (dob = $candidate_dob OR dob IS NULL)
       
// Note: fuzzy matching needed for name variants
// Use pg_trgm extension for similarity matching
// Threshold: similarity > 0.8

Expected Results:
  Candidate A (Chen): 0 matches → CLEAR ✅
  Candidate B (Rivera): 0 matches → CLEAR ✅
  Candidate C (Powell): 1 match → FLAG 🔴 (requires SSN verification)
  
// ВАЖНО: Name match ≠ confirmed exclusion
// Need SSN/EIN verification for final confirmation
// OIG FAQ says: "not sufficient to simply find a matching first and last name"
```

##### Check 1.2: SAM.gov Exclusions
```
Method: REST API call
URL: GET https://api.sam.gov/entity-information/v4/exclusions
     ?api_key={KEY}
     &q={candidate_full_name}
     &includeSections=exclusionDetails
     
Headers: Accept: application/json

Rate Limit: 10/day (personal) or 1000/day (system account)
// IMPORTANT: Need system account for production. 
// Personal key = 10 req/day = only for testing

Expected Response (no match):
{
  "totalRecords": 0,
  "excludedRecords": []
}

Expected Results:
  All 3 candidates: 0 matches → CLEAR ✅
```

##### Check 1.3: OFAC SDN
```
Method A (Self-hosted): Fuzzy match against local ofac_sdn table
  // Jaro-Winkler similarity on name
  // DOB cross-reference if available
  // Threshold: score > 0.85

Method B (OpenSanctions API - preferred for MVP):
  URL: POST https://api.opensanctions.org/match/default
  Body: {
    "queries": {
      "candidate": {
        "schema": "Person",
        "properties": {
          "name": ["Sarah M Chen"],
          "birthDate": ["1990-03-15"],
          "country": ["us"]
        }
      }
    }
  }
  
  // OpenSanctions includes OFAC + EU + UN sanctions
  // Free tier: 500 requests/day
  // Paid: from €100/month

Expected Results:
  All 3 candidates: 0 matches → CLEAR ✅
```

##### Check 1.4: FDA Debarment
```
Method: SQL query against local fda_debarment table
Query: SELECT * FROM fda_debarment 
       WHERE LOWER(name) LIKE '%' || LOWER($candidate_lastname) || '%'
       
// Very small list (~200 records)
// Simple exact match sufficient

Expected Results:
  All 3 candidates: 0 matches → CLEAR ✅
```

##### Check 1.5: State Medicaid Exclusion
```
Method: For MVP — include in OIG LEIE check (states submit to OIG)
// Full state-by-state check = post-MVP
// For demo: show as "Included in OIG check" or "State-specific check pending"

Expected Results:
  Show as covered by Check 1.1 or mark as "N/A - covered by federal check"
```

**Post-Tier 1 State:**
```
Candidate A (Chen):
├── Compliance Badge: ⏳ "Tier 1 Passed"  
├── Traffic Light: [GREEN] [gray] [gray]
├── Checks Progress: 5/15
└── Time elapsed: ~2 seconds

Candidate B (Rivera):
├── Same as A
├── Traffic Light: [GREEN] [gray] [gray]
├── Checks Progress: 5/15

Candidate C (Powell):
├── Compliance Badge: ⚠️ "Review Required"
├── Traffic Light: [RED] [gray] [gray]
├── Checks Progress: 5/15
├── Flag: "Potential OIG LEIE match — manual SSN verification required"
└── Action: Candidate paused in pipeline, alert sent to compliance team
```

---

#### Step 3: Tier 2 — Fast Verification (5-30 min)

##### Check 2.1: State License (MVP version — web scraping)
```
// FOR MVP: Scrape CA Board of Pharmacy for demo
// URL: https://www.pharmacy.ca.gov/about/verify_lic.shtml

Method: Puppeteer/Playwright script
Input: License number RPH-87654
Steps:
  1. Navigate to CA Board verification page
  2. Enter license number
  3. Parse response HTML
  4. Extract: status, expiration_date, discipline_history
  
Expected Results:
  Candidate A (Chen): License ACTIVE, expires 2027, no discipline → PASS ✅
  Candidate B (Rivera): TX Board — license EXPIRED 2023 → FLAG ⚠️
  
// POST-MVP: Use Verisys for all 50 states
// Verisys covers 56 jurisdictions + 800 taxonomies
// Need enterprise sales engagement (3-6 month cycle)
```

##### Check 2.2: DEA Registration
```
// No public API for DEA verification
// MVP: Candidate self-reports DEA number
// Verification: Manual check via Verisys OR 
//   NTIS subscription ($$$)

MVP Approach: 
  - Ask candidate for DEA number during intake
  - Store as "self-reported"
  - Mark as "verification pending" 
  - Show: "DEA: BC7654321 (self-reported, verification pending)"

Expected Results:
  Candidate A: "BC7654321 — self-reported, pending verification" ⏳
  Candidate B: "N/A — not applicable for this role" ➖
```

##### Check 2.3: Certification (RAC, CCEP, etc.)
```
// No API exists for RAC (RAPS), CCEP (SCCE), CRA (ACRP)
// These are professional associations with member directories
// Some have online verification portals, most require email

MVP Approach:
  1. Candidate uploads certification document during intake
  2. AI extracts: cert name, number, issue date, expiry
  3. Status set to "document received, verification pending"
  4. Auto-generate verification email to issuing organization
  5. Manual confirmation when response received

Expected Results:
  Candidate A: "RAC — document uploaded, verification email sent" ⏳
  Candidate B: "RAC — candidate reports 'in progress'" ➖
```

##### Check 2.4: FACIS (Fraud & Abuse)
```
// FACIS = Verisys proprietary product
// No alternative that covers all 868 taxonomies
// MVP: Skip or use subset of public data (OIG + SAM + OFAC already covered in Tier 1)

MVP Approach:
  - Note: "Partial coverage via Tier 1 checks (OIG + SAM + OFAC)"
  - Full FACIS = post-MVP with Verisys partnership

Expected Results:
  All candidates: "Covered by Tier 1 federal checks. Full FACIS pending." ⏳
```

##### Check 2.5: Abuse Registry
```
// State-level registries
// No unified access
// MVP: Skip, flag as "post-MVP"

Expected Results:
  All candidates: "State abuse registry — check not yet implemented" ⏳
```

**Post-Tier 2 State:**
```
Candidate A (Chen):
├── Compliance Badge: ⏳ "Tier 2 In Progress"
├── Traffic Light: [GREEN] [YELLOW] [gray]
├── Checks Progress: 8/15 (5 auto + 3 partial)
├── License: CA RPH-87654 ACTIVE (verified via scraping) ✅
├── DEA: self-reported, pending ⏳
├── RAC: document uploaded, email sent ⏳
└── Time elapsed: ~5 minutes

Candidate B (Rivera):
├── Compliance Badge: ⚠️ "Issue Found"
├── Traffic Light: [GREEN] [YELLOW-RED] [gray]
├── Flag: "TX license EXPIRED since 2023"
├── Action Required: "Contact candidate — can license be renewed?"
└── Time elapsed: ~5 minutes
```

---

#### Step 4: Tier 3 — Deep Verification (1-3 days, parallel)

```
// All Tier 3 checks are manual/semi-automated
// System automates the REQUEST, human confirms the RESULT

Check 3.1: NPDB
  MVP: Mark as "requires Verifiable partnership — not available in prototype"
  Post-MVP: Verifiable API integration
  
Check 3.2: Education PSV
  MVP: Auto-generate verification letter to MIT Registrar
  Template: "Dear Registrar, we are conducting employment verification for 
  [name]. Please confirm PhD in Biological Engineering, awarded [year]..."
  Method: Email via SendGrid → track response
  Status: "Verification letter sent [date], awaiting response"
  
Check 3.3: GxP Training
  Same as certifications — document upload + email verification

Check 3.4: Employment History  
  Auto-generate verification email to previous employer HR
  Track responses in dashboard

Check 3.5: Malpractice
  MVP: Skip — very few QA/QC scientists carry malpractice insurance
  Relevant mainly for clinical investigators and physicians
```

**Post-Tier 3 State (after all responses received, ~2-5 days):**
```
Candidate A (Chen) — FINAL:
├── Compliance Badge: ✅ "Compliance Fully Cleared"
├── Traffic Light: [GREEN] [GREEN] [GREEN]
├── Checks Progress: 15/15
├── Compliance Score: 98
├── OIG: CLEAR ✅
├── SAM: CLEAR ✅
├── OFAC: CLEAR ✅
├── FDA Debarment: CLEAR ✅
├── CA License: ACTIVE, exp 2027 ✅
├── DEA: ACTIVE ✅ (confirmed via Verisys*)
├── RAC: VERIFIED ✅ (RAPS confirmed)
├── Education: PhD MIT CONFIRMED ✅ (registrar responded)
├── Employment: Genentech HR CONFIRMED ✅
└── Ready for: Final interview with hiring manager

* Items marked "confirmed" require partnership/manual process
  For demo purposes, these are simulated
```

---

#### Step 5: Dashboard Display

```
Pipeline View (after all checks complete):
┌──────────────────────────────────────────────────────────────┐
│ Senior QA/QC Scientist — NovaBio Inc.                        │
│ Sourced: 142 → Qualified: 38 → Compliance ✓: 29 → Finalists: 3  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Candidate          Stage      Score  Compliance  Checks      │
│ ─────────────────  ─────────  ─────  ──────────  ──────      │
│ Dr. Sarah Chen     Finalist   94     🟢🟢🟢 Cleared  15/15   │
│ Dr. Yuki Tanaka    Finalist   91     🟢🟢🟢 Cleared  15/15   │
│ Mark Rivera        Interview  88     🟢🟡⬜ Pending  11/15   │
│ Klaus Mueller      Screened   86     🟢🟢🟡 Pending  13/15   │
│ James Powell       ──PAUSED── 79     🔴⬜⬜ Flagged   5/15   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

## ЧАСТЬ 4: ЧТО ПОКАЗЫВАТЬ НА ДЕМО

### Демо-скрипт (5 минут):

**Минута 1:** "Вот пайплайн кандидатов на QA/QC позицию. Видите колонку Compliance?"

**Минута 2:** Кликаем на Dr. Sarah Chen → показываем полный профиль → кнопка "Run Full Compliance Check"

**Минута 3:** Запускаем проверку → анимация проходит по 15 чекам:
- Tier 1 пробегает за секунды (OIG, SAM, OFAC, FDA — реальные данные)
- Tier 2 показывает license verification (CA Board парсинг — реальный результат)
- Tier 3 показывает "in progress" (education, employment — отправка запросов)

**Минута 4:** Возвращаемся в pipeline → показываем James Powell с красным флагом → "Вот почему это важно — вы бы потратили 3 недели на интервью с этим кандидатом, а потом узнали бы что он в OIG exclusion list"

**Минута 5:** "Сколько раз за последний год вы дошли до финального кандидата и credential check провалился?"

### Что реально в прототипе vs. что симулировано:

| Элемент | Реально работает в демо | Симулировано |
|---------|------------------------|-------------|
| OIG LEIE check | ✅ Реальный lookup по загруженной базе | |
| SAM.gov check | ✅ Реальный API call | |
| OFAC check | ✅ Реальный lookup (OpenSanctions или локальный) | |
| FDA Debarment | ✅ Реальный lookup по загруженному Excel | |
| State License (CA) | ⚠️ Можно сделать реальный скрапинг CA Board | Для других штатов — симуляция |
| DEA | | ✅ Симулировано |
| Certifications (RAC) | | ✅ Симулировано |
| FACIS | | ✅ Симулировано |
| NPDB | | ✅ Симулировано |
| Education PSV | | ✅ Симулировано (но email sending реальный) |
| Employment | | ✅ Симулировано |

### Итого для прототипа:
- **4 из 15 проверок** полностью автоматизированы с реальными данными (Tier 1)
- **1 проверка** (CA license) может быть реальной со скрапингом
- **10 проверок** симулированы но с реалистичным workflow

Это честнее чем "15 automated checks" но для каздева это ок — клиенту важно видеть:
1. Что мы ЗНАЕМ какие проверки нужны (доменная экспертиза)
2. Что Tier 1 работает мгновенно и реально
3. Что workflow для Tier 2-3 выстроен правильно
4. Что traffic light система решает их главную боль (restarts после failed checks)

---

## ЧАСТЬ 5: ТЕХНИЧЕСКИЙ ПЛАН РЕАЛИЗАЦИИ MVP

### Sprint 1 (неделя 1): Data Foundation
```
Tasks:
□ Скрипт загрузки OIG LEIE CSV → PostgreSQL (cron: monthly)
□ Парсер OFAC SDN XML → PostgreSQL (cron: daily)
□ Парсер FDA Debarment Excel → PostgreSQL (cron: quarterly)  
□ SAM.gov API client (с rate limiting и caching)
□ Fuzzy name matching module (pg_trgm + Jaro-Winkler)
□ Database schema: candidates, credential_checks, check_results
```

### Sprint 2 (неделя 2): Verification Pipeline
```
Tasks:
□ BullMQ queue: credential_verification_tier1
□ Worker: runs all 4 Tier 1 checks in parallel
□ Result aggregation: traffic light status calculation
□ Webhook: notify dashboard when checks complete
□ CA Board of Pharmacy scraper (Puppeteer)
□ Candidate intake form: collect license #, DEA #, cert documents
```

### Sprint 3 (неделя 3): Dashboard Integration
```
Tasks:
□ Pipeline view: compliance column + traffic light
□ Candidate detail: credential verification panel
□ "Run Compliance Check" button with real API calls
□ Progress tracking (BullMQ job progress → WebSocket → UI)
□ Flag/alert system for failed checks
□ Audit log for compliance reporting
```

### Sprint 4 (неделя 4): Demo Polish
```
Tasks:
□ Demo data: 6-8 candidates with mixed compliance statuses
□ Simulated Tier 2-3 results for demo flow
□ Timing/animation for "wow" effect
□ Error handling for API failures
□ Loading states and progress indicators
□ One-pager PDF export of candidate compliance report
```

### Dependencies / Blockers:
- SAM.gov system account approval: может занять 10 business days
- OpenSanctions API key: instant (free tier 500/day)
- CA Board scraping: нужно проверить robots.txt и TOS
- Verisys partnership: длинный sales cycle, НЕ blocker для MVP
