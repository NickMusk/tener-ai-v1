# Credential Verification — Classification by Implementation Type

---

## TYPE 1: REST API (живой запрос → JSON ответ)

**Характеристика:** Отправляем HTTP request, получаем структурированный ответ. Нужен API key. Rate limits.

| Проверка | Endpoint | Auth | Rate Limit | Latency | Cost |
|----------|----------|------|------------|---------|------|
| **SAM.gov Exclusions** | `GET api.sam.gov/entity-information/v4/exclusions?q={name}` | API key (бесплатная регистрация) | 10/day personal, 1000/day system account | ~1-2 sec | Бесплатно |
| **OpenSanctions** (OFAC + EU + UN sanctions) | `POST api.opensanctions.org/match/default` | API key | 500/day free, unlimited paid | ~1-3 sec | Бесплатно / €100+/мес |

**Что строить:**
```
CredentialCheckService
├── ApiClient (generic HTTP client with retry + rate limiting)
├── SamGovAdapter
│   ├── search(name, dob?) → {totalRecords, excludedRecords[]}
│   └── Парсинг: exclusionName, exclusionType, activationDate
└── OpenSanctionsAdapter
    ├── match(name, dob, country) → {results[], score}
    └── Парсинг: datasets matched, sanctions programs, match score
```

**Effort:** 2-3 дня на оба адаптера  
**Reliability:** Высокая (government API + established open-source)  
**Risk:** SAM.gov system account approval = до 10 business days

---

## TYPE 2: File Download → Local DB (скачать файл, загрузить в базу, матчить локально)

**Характеристика:** Государственные списки публикуются как файлы. Скачиваем по расписанию, импортируем в PostgreSQL, делаем локальный поиск. Быстрее и надёжнее чем API — нет зависимости от внешнего сервиса в runtime.

| Проверка | Источник файла | Формат | Размер | Обновление | Records |
|----------|---------------|--------|--------|------------|---------|
| **OIG LEIE** | oig.hhs.gov/exclusions/exclusions_list.asp | CSV | ~15 MB | Monthly (1-го числа) | ~70,000 |
| **OFAC SDN List** | sanctionslist.ofac.treas.gov/Home/SdnList | XML, CSV, PDF | ~5 MB | As published (несколько раз в месяц) | ~12,000 |
| **FDA Debarment** | fda.gov (drug debarment page) | Excel (.xlsx) | < 1 MB | Quarterly | ~200 |
| **FDA Clinical Investigators** (disqualified) | fda.gov/inspections-compliance | HTML table / PDF | < 1 MB | As published | ~300 |

**Что строить:**
```
FileImportService
├── Scheduler (cron jobs)
│   ├── OIG: monthly → download CSV → parse → upsert to `leie_exclusions`
│   ├── OFAC: weekly → download XML → parse → upsert to `ofac_sdn`
│   ├── FDA Debarment: quarterly → download Excel → parse → upsert to `fda_debarment`
│   └── FDA ClinInvestigators: quarterly → scrape table → upsert to `fda_investigators`
│
├── NameMatcher (fuzzy matching engine)
│   ├── Exact match: lastname + firstname + dob
│   ├── Fuzzy match: pg_trgm similarity > 0.8
│   ├── Phonetic: Soundex / Metaphone for name variants
│   └── Result: {match_found, confidence_score, matched_record}
│
└── PostgreSQL Tables
    ├── leie_exclusions (lastname, firstname, midname, dob, excltype, excldate, state, specialty, npi)
    ├── ofac_sdn (uid, sdn_type, last_name, first_name, dob, programs[], aliases[], addresses[])
    ├── fda_debarment (name, debarment_date, expiration, fr_citation, debarment_type)
    └── fda_investigators (name, status, nidpoe_date, action_type)
```

**Key Design Decision:** Fuzzy matching критичен. "James Powell" и "James T. Powell" и "Powell, James Thomas" — это один человек. Нужно:
- Нормализация имён (trim, lowercase, remove middle initials)
- pg_trgm для similar strings
- Дополнительная верификация по DOB когда имя совпадает
- НЕ автоматический reject — только flag для human review

**Effort:** 3-4 дня (download scripts + import + fuzzy matching + scheduling)  
**Reliability:** Очень высокая (данные локальные, нет runtime dependency)  
**Risk:** Нужно мониторить что формат файлов не изменился (alert если парсинг fails)

---

## TYPE 3: Web Scraping (зайти на сайт → заполнить форму → распарсить ответ)

**Характеристика:** Нет API, нет файла для скачивания. Есть только веб-форма на сайте лицензионного борда. Puppeteer/Playwright заходит на сайт, вводит данные, парсит результат.

| Проверка | Сайт | Input | Output | Complexity |
|----------|------|-------|--------|------------|
| **CA Board of Pharmacy** | pharmacy.ca.gov/about/verify_lic.shtml | License # или Name | Status, expiry, discipline | Medium |
| **TX Board of Pharmacy** | tsbp.texas.gov/verification | License # | Status, expiry | Medium |
| **NY Education Dept** | nysed.gov/professions/verify | Name, profession | License #, status | High (CAPTCHA?) |
| **MA Board** | mass.gov/license-verification | License # | Status | Medium |
| **NJ Board** | njconsumeraffairs.gov/verify | License # | Status | Medium |
| **BioSpace profiles** | biospace.com | Name, keywords | Profile data, experience | Medium |
| **ResearchGate** | researchgate.net | Name | Publications, h-index | High (anti-scraping) |

**Что строить:**
```
ScrapingService
├── BrowserPool (Puppeteer instance management)
│   ├── Pool size: 3-5 concurrent browsers
│   ├── Proxy rotation (for anti-bot protection)
│   └── Session management
│
├── StateLicenseScrapers
│   ├── CaliforniaPharmacyAdapter
│   │   ├── searchByLicense(licenseNumber) → {status, expiry, discipline[]}
│   │   └── searchByName(name) → {licenses[]}
│   ├── TexasPharmacyAdapter
│   ├── NewYorkEducationAdapter
│   └── ... (each state = separate adapter, each breaks differently)
│
├── AntiDetection
│   ├── Random delays (2-8 sec between requests)
│   ├── User-agent rotation
│   ├── CAPTCHA handling (2Captcha service for states that use it)
│   └── Retry logic with exponential backoff
│
└── ResultParser
    ├── HTML → structured data
    ├── Validation (is this actually a license record?)
    └── Normalization (different states use different status terms)
```

**ПРОБЛЕМЫ скрапинга:**
- Каждый штат = отдельный формат, отдельная структура HTML
- Сайты меняются без предупреждения → скраперы ломаются
- Некоторые штаты имеют CAPTCHA (NY)
- Некоторые штаты блокируют автоматические запросы
- **50 штатов = 50 адаптеров**. В MVP делаем top-5 где большинство biotech (CA, MA, TX, NJ, NY)

**Effort:** 1-2 недели на 5 штатов (2-3 дня на адаптер + testing + error handling)  
**Reliability:** НИЗКАЯ (сайты меняются, блокировки)  
**Risk:** Высокий maintenance cost. Каждое изменение сайта = broken adapter  
**Verdict:** Для MVP — да, для production — переход на Verisys (Type 5)

---

## TYPE 4: Outbound Email Workflow (отправить запрос → ждать ответ → человек обновляет статус)

**Характеристика:** Нет ни API, ни файла, ни сайта для автоматического поиска. Единственный способ — отправить письмо в организацию и ждать ответ. Автоматизируем генерацию и отправку писем, а подтверждение — ручное.

| Проверка | Адресат | Что запрашиваем | Типичное время ответа |
|----------|---------|----------------|----------------------|
| **Education PSV (PhD/MSc)** | University Registrar | Подтверждение степени, год, специальность | 1-6 недель |
| **GxP Training Cert** | ISPE, PDA, internal training dept | Подтверждение сертификата, дата, validity | 1-3 недели |
| **RAC Certification** | RAPS (Regulatory Affairs Professionals Society) | Certification status, expiry | 1-2 недели |
| **CCEP Certification** | SCCE (Society of Corporate Compliance and Ethics) | Certification status | 1-2 недели |
| **CRA Certification** | ACRP (Association of Clinical Research Professionals) | Certification status | 1-2 недели |
| **Employment History** | Previous employer HR department | Dates, title, reason for leaving | 1-4 недели |
| **Malpractice Insurance** | Insurance carrier | Policy status, claims history (5yr) | 2-4 недели |

**Что строить:**
```
OutboundVerificationService
├── TemplateEngine
│   ├── education_verification_request.hbs
│   │   "Dear Registrar, we are conducting background verification
│   │    for {{candidate_name}}. Please confirm: degree {{degree}},
│   │    field {{field}}, awarded {{year}}..."
│   ├── certification_verification_request.hbs
│   ├── employment_verification_request.hbs
│   └── ... (per check type)
│
├── EmailSender (SendGrid / AWS SES)
│   ├── send(to, template, data) → {messageId, sentAt}
│   ├── Follow-up scheduler (auto-remind after 7 days, 14 days)
│   └── Tracking: opened, replied, bounced
│
├── ResponseTracker
│   ├── Unique verification ID per request
│   ├── Status: SENT → OPENED → RESPONDED → VERIFIED / FAILED
│   ├── Manual update UI for compliance officer
│   └── Document upload (attach response letter/email)
│
└── Dashboard Widget
    ├── "3 verification requests pending response"
    ├── "Education: MIT — sent Jan 15, follow-up sent Jan 22, awaiting"
    └── One-click: "Mark as Verified" / "Mark as Failed" / "Send Reminder"
```

**Effort:** 1 неделя (templates + email sending + tracking UI + reminders)  
**Reliability:** Зависит от организации-получателя  
**Risk:** Долгий response time (weeks). Нужен follow-up mechanism  
**Value:** Даже без автоматического ответа — сам факт что запросы уходят автоматически и трекаются = огромная экономия времени для compliance team

---

## TYPE 5: Third-Party Aggregator API (платный сервис, один вызов → несколько проверок)

**Характеристика:** Коммерческий сервис который уже агрегировал данные из тысяч источников. Один API call заменяет десятки отдельных проверок. Дорого но быстро и надёжно.

| Провайдер | Покрывает | API | Pricing Model |
|-----------|----------|-----|---------------|
| **Verisys** | FACIS (868 taxonomies, 56 jurisdictions), все state licenses, DEA, abuse registries, continuous monitoring | REST API + Web Portal (CheckMedic, VerisysConnect) | Enterprise contract. Не публичный. Estimate: $3-15 per check or $5K-50K/year |
| **Verifiable** | NPDB (один из 4 с automated access), license verification, education, certifications | REST API + Salesforce App | Per-verification pricing. Contact sales |
| **CertifyOS** | End-to-end credentialing. Licenses, education, DEA, board certs | API | 38% cost reduction claim vs manual |
| **OpenSanctions** | OFAC + EU + UN + 80+ datasets. Open source core | REST API | Free tier 500/day, paid from €100/mo |

**Что строить:**
```
AggregatorIntegrationService
├── VerisysAdapter (post-MVP, after partnership signed)
│   ├── facisCheck(name, dob, npi) → {hits[], taxonomies[], jurisdictions[]}
│   ├── licenseVerify(name, state, licenseNum) → {status, expiry, discipline[]}
│   ├── deaLookup(deaNumber) → {status, registration_type, expiry}
│   └── continuousMonitoring.subscribe(provider_id) → webhook alerts
│
├── VerifiableAdapter (post-MVP)
│   ├── npdbQuery(name, dob, npi) → {reports[], adverse_actions[]}
│   └── credentialCheck(name, type) → {verified, details}
│
└── Abstraction Layer
    ├── CredentialChecker.check(candidate, checkType)
    │   → routes to Type 1/2/3/4/5 based on availability
    ├── If Verisys available → use Verisys (fastest, most complete)
    ├── If not → fallback to Type 1+2+3 (self-built)
    └── Track which method was used for audit trail
```

**Key Architecture Decision:** Строить abstraction layer сразу. В MVP используем Type 1+2+3+4. Когда партнёрство с Verisys закроется — просто подключаем новый adapter, workflow не меняется.

**Effort:** 1 день на abstraction layer. Verisys/Verifiable integration = 1-2 недели ПОСЛЕ подписания контракта  
**Reliability:** Очень высокая (это их core business)  
**Risk:** Sales cycle 3-6 months. Pricing может быть prohibitive для pre-seed  

---

## TYPE 6: Candidate Self-Report + Document Upload (кандидат предоставляет → мы храним → проверяем позже)

**Характеристика:** Кандидат сам заполняет данные и загружает подтверждающие документы во время intake. Мы сохраняем как "self-reported" и верифицируем через Type 3/4/5 когда возможно.

| Данные | Что кандидат предоставляет | Как верифицируем |
|--------|--------------------------|-----------------|
| **DEA Number** | Номер регистрации | Type 5 (Verisys) или Type 4 (email to DEA) |
| **Certifications (RAC, CCEP, CRA)** | Скан сертификата + номер + дата выдачи | Type 4 (email to issuing org) |
| **GxP Training** | Скан сертификата о прохождении training | Type 4 (email to training provider) |
| **State License Number** | Номер лицензии + штат | Type 3 (scraping) или Type 5 (Verisys) |
| **Education** | Университет, степень, год | Type 4 (email to registrar) |
| **Publications** | DOI, PubMed ID | Type 7 (PubMed API — see below) |

**Что строить:**
```
CandidateIntakeService
├── IntakeForm (extended for LS)
│   ├── Standard fields: name, email, phone, linkedin
│   ├── LS fields: license_state, license_number, dea_number
│   ├── Certifications: [{name, number, issuer, issue_date, expiry_date}]
│   ├── Document upload: cert scans, license copies, diploma
│   └── Consent: "I authorize Tener to verify my credentials"
│
├── DocumentProcessor
│   ├── OCR (Tesseract or Claude Vision): extract text from uploaded certs
│   ├── Validation: does extracted data match self-reported data?
│   ├── Storage: S3 with encryption at rest
│   └── Audit trail: who uploaded what, when
│
├── VerificationQueue
│   ├── For each self-reported item → create verification task
│   ├── Route to appropriate Type (3, 4, or 5)
│   ├── Status tracking: SELF_REPORTED → VERIFICATION_SENT → VERIFIED / DISCREPANCY
│   └── Alert if self-report doesn't match verification result
│
└── Trust Score
    ├── Self-reported only: 40% confidence
    ├── Document uploaded: 60% confidence  
    ├── Auto-verified (Type 1/2/3): 90% confidence
    ├── Primary-source verified (Type 4/5): 100% confidence
    └── Display on candidate card as confidence indicator
```

**Effort:** 3-4 дня (form + upload + OCR + queue routing)  
**Reliability:** Зависит от кандидата (могут предоставить неверные данные)  
**Value:** Мгновенный старт — не ждём партнёрства с Verisys чтобы начать собирать данные

---

## TYPE 7: Public Data API (научные/профессиональные базы — для sourcing, не для compliance)

**Характеристика:** Открытые API научных баз данных. Используются не для compliance check, а для enrichment профиля кандидата — доказательство экспертизы.

| Источник | API | Что извлекаем | Для чего |
|----------|-----|--------------|----------|
| **PubMed** | `eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi` | Публикации, co-authors, journals, citation count | Proof of expertise. "12 publications in Journal of Pharmaceutical Sciences" |
| **ClinicalTrials.gov** | `clinicaltrials.gov/api/v2/studies` | Trial participation, role (PI/sub-I), therapeutic area, phase | "Principal Investigator on 3 Phase III oncology trials" |
| **Google Patents** | `patents.google.com` (no official API, SerpAPI as proxy) | Patent filings, co-inventors, technology areas | Innovation track record |
| **ORCID** | `pub.orcid.org/v3.0/{orcid-id}` | Unified researcher profile, works, affiliations | Cross-reference all publications and positions |

**Что строить:**
```
CandidateEnrichmentService
├── PubMedAdapter
│   ├── searchByAuthor(name, affiliation?) → {articles[], totalCount}
│   ├── getArticleDetails(pmid) → {title, journal, year, authors[], abstract}
│   └── Metrics: h-index approximation, recent publication activity
│
├── ClinicalTrialsAdapter
│   ├── searchByInvestigator(name) → {studies[]}
│   ├── getStudyDetails(nctId) → {title, phase, status, conditions[], interventions[]}
│   └── Metrics: trial count by phase, therapeutic areas
│
├── OrcidAdapter
│   ├── searchByName(name) → {orcid_ids[]}
│   └── getProfile(orcid) → {works[], education[], employment[]}
│
└── EnrichmentScore
    ├── Publications: 0-100 (based on count, recency, journal impact)
    ├── Clinical trials: 0-100 (based on count, phase, role)
    ├── Patents: 0-100 (based on count, recency)
    └── Combined "Scientific Credibility Score" on candidate card
```

**Effort:** 1 неделя (PubMed + ClinicalTrials are well-documented APIs)  
**Reliability:** Высокая (government APIs, stable)  
**Risk:** Name disambiguation — "Sarah Chen" может быть 50 разных учёных. Нужно: affiliation filter, DOB, ORCID cross-reference  
**Value for demo:** ОГРОМНАЯ. Показать HM что Reed нашёл кандидата через PubMed publications = мощный wow effect

---

## SUMMARY: Все 15 проверок по типам

| # | Проверка | Type | MVP Ready? | Effort |
|---|----------|------|------------|--------|
| 1 | OIG LEIE | **2** (File → DB) | ✅ Yes | 1 day |
| 2 | SAM.gov | **1** (REST API) | ✅ Yes | 0.5 day |
| 3 | OFAC SDN | **2** (File → DB) + **1** (OpenSanctions API) | ✅ Yes | 1 day |
| 4 | FDA Debarment | **2** (File → DB) | ✅ Yes | 0.5 day |
| 5 | FDA Clinical Investigators | **2** (File → DB) | ✅ Yes | 0.5 day |
| 6 | State License | **3** (Scraping) → **5** (Verisys post-MVP) | ⚠️ 5 states | 1-2 weeks |
| 7 | DEA | **6** (Self-report) → **5** (Verisys post-MVP) | ⚠️ Self-report | 0.5 day |
| 8 | FACIS | **5** (Verisys only) | ❌ Post-MVP | — |
| 9 | Abuse Registry | **5** (Verisys only) | ❌ Post-MVP | — |
| 10 | RAC/CCEP/CRA | **6** (Upload) + **4** (Email verify) | ⚠️ Semi-auto | 2 days |
| 11 | NPDB | **5** (Verifiable only) | ❌ Post-MVP | — |
| 12 | Education PSV | **4** (Email workflow) | ⚠️ Semi-auto | 1 day |
| 13 | GxP Training | **6** (Upload) + **4** (Email) | ⚠️ Semi-auto | 1 day |
| 14 | Employment History | **4** (Email workflow) | ⚠️ Semi-auto | 1 day |
| 15 | Malpractice | **4** (Email) | ⚠️ Semi-auto | 0.5 day |

### MVP Build Order (по ROI):

**Week 1:** Type 2 + Type 1 (файлы + API = 5 полностью автоматизированных проверок)
**Week 2:** Type 6 + Type 4 (intake form + email workflows = ещё 6 проверок в semi-auto)
**Week 3:** Type 3 (скрапинг 5 штатов = state license verification)
**Week 4:** Type 7 (PubMed + ClinicalTrials enrichment для wow effect)
**Post-MVP:** Type 5 (Verisys/Verifiable partnership = заменяет Type 3 + добавляет FACIS/NPDB)
