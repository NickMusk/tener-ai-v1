export const TILEMAP_PATH = "./assets/tilemap.json";
export const TILESET_PATH = "./assets/rpg-tileset.png";
export const SPRITE_SHEET_PATH = "./assets/32x32folk.png";
export const PUBLIC_JOBS_API_PATH = "/api/demo/agents-office/jobs?limit=8";
export const FALLBACK_JOBS_PATH = "./demo-jobs.json";
export const SHORTLIST_LIMIT = 3;
export const ACTIVITY_LIMIT = 7;
export const TILESET_COLUMNS = 15;

export const SPRITES = {
  f1: { baseX: 0, baseY: 0 },
  f3: { baseX: 192, baseY: 0 },
  f4: { baseX: 288, baseY: 0 },
  f6: { baseX: 96, baseY: 128 },
  f7: { baseX: 192, baseY: 128 }
};

export const DIRECTION_ROWS = {
  down: 0,
  left: 1,
  right: 2,
  up: 3
};

export const AGENT_DEFS = [
  {
    id: "reed",
    name: "Reed AI",
    role: "Talent Scout",
    spriteKey: "f1",
    start: [8, 8],
    speed: 1.4,
    emoji: "\ud83d\udd0e",
    idle_note: "Watching talent channels for stronger leads.",
    ambient: ["Refreshing candidate graph.", "Cross-checking field certifications.", "Watching new profile activity."]
  },
  {
    id: "spencer",
    name: "Spencer AI",
    role: "Job Architect",
    spriteKey: "f3",
    start: [19, 15],
    speed: 1.2,
    emoji: "\ud83e\udde9",
    idle_note: "Tightening the search brief before the market opens.",
    ambient: ["Rewriting scope summary.", "Updating must-have filters.", "Adjusting pay-band language."]
  },
  {
    id: "harper",
    name: "Harper AI",
    role: "Culture Analyst",
    spriteKey: "f6",
    start: [10, 24],
    speed: 1.15,
    emoji: "\ud83e\udde0",
    idle_note: "Keeping the fit rubric aligned with the buyer.",
    ambient: ["Refreshing culture rubric.", "Checking team signal notes.", "Comparing previous placements."]
  },
  {
    id: "casey",
    name: "Casey AI",
    role: "Hiring Coordinator",
    spriteKey: "f7",
    start: [29, 28],
    speed: 1.3,
    emoji: "\ud83d\udcac",
    idle_note: "Maintaining candidate conversations and follow-ups.",
    ambient: ["Keeping candidate inbox warm.", "Waiting for a fast reply.", "Preparing the next outreach wave."]
  },
  {
    id: "jordan",
    name: "Jordan AI",
    role: "Hiring Advisor",
    spriteKey: "f4",
    start: [30, 10],
    speed: 1.1,
    emoji: "\u2b50",
    idle_note: "Waiting for enough evidence to lock the buyer shortlist.",
    ambient: ["Watching fit signals.", "Holding final scorecards.", "Reviewing the latest evidence."]
  }
];

function fallbackSignal(job, index) {
  const title = String(job?.title || "Field Operator").trim() || "Field Operator";
  const location = String(job?.location || "Texas").trim() || "Texas";
  return {
    headline: `${title} | Market Signal ${index + 1}`,
    location,
    score: 92 - index,
    note: "Useful fallback signal while the office keeps scanning."
  };
}

export function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

export function randomItem(items, rng = Math.random) {
  if (!Array.isArray(items) || !items.length) return null;
  const index = Math.floor(rng() * items.length);
  return items[index] ?? null;
}

export function pickWeighted(options, rng = Math.random) {
  const filtered = (options || []).filter((item) => Number(item?.weight || 0) > 0);
  if (!filtered.length) return null;
  const total = filtered.reduce((sum, item) => sum + Number(item.weight || 0), 0);
  if (total <= 0) return filtered[0] ?? null;
  let cursor = rng() * total;
  for (const item of filtered) {
    cursor -= Number(item.weight || 0);
    if (cursor <= 0) return item;
  }
  return filtered[filtered.length - 1] ?? null;
}

export function formatMetric(value) {
  return new Intl.NumberFormat("en-US").format(Number(value || 0));
}

export function normalizeJob(raw, index = 0) {
  const normalizedSignals = Array.isArray(raw?.candidate_signals) && raw.candidate_signals.length
    ? raw.candidate_signals.map((signal, signalIndex) => ({
        headline: String(signal?.headline || fallbackSignal(raw, signalIndex).headline).trim(),
        location: String(signal?.location || raw?.location || "").trim(),
        score: clamp(Number(signal?.score || 0) || fallbackSignal(raw, signalIndex).score, 1, 99),
        note: String(signal?.note || fallbackSignal(raw, signalIndex).note).trim()
      }))
    : [fallbackSignal(raw, 0), fallbackSignal(raw, 1)];

  return {
    id: String(raw?.id || `fixture-job-${index + 1}`),
    company: String(raw?.company || "Tener Buyer").trim() || "Tener Buyer",
    title: String(raw?.title || "Field Role").trim() || "Field Role",
    location: String(raw?.location || "Texas").trim() || "Texas",
    market: String(raw?.market || "Talent search").trim() || "Talent search",
    summary: String(raw?.summary || "Keeping sourcing, fit and outreach moving in parallel.").trim(),
    phase_label: String(raw?.phase_label || "The office is actively moving this search.").trim(),
    channels: Array.isArray(raw?.channels) && raw.channels.length
      ? raw.channels.map((item) => String(item || "").trim()).filter(Boolean)
      : ["LinkedIn Recruiter", "Indeed", "Referral graph"],
    market_leads: clamp(Number(raw?.market_leads || 0) || 0, 0, 9999),
    live_threads: clamp(Number(raw?.live_threads || 0) || 0, 0, 999),
    buyer_finalists: clamp(Number(raw?.buyer_finalists || 0) || 0, 0, SHORTLIST_LIMIT),
    candidate_signals: normalizedSignals.slice(0, SHORTLIST_LIMIT)
  };
}

export function normalizeJobCatalog(payload) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  return items.map((item, index) => normalizeJob(item, index));
}

async function fetchJson(path, fetchImpl) {
  const response = await fetchImpl(path, { credentials: "same-origin" });
  if (!response.ok) {
    throw new Error(`request_failed:${path}:${response.status}`);
  }
  return response.json();
}

export async function loadJobCatalog(fetchImpl = globalThis.fetch) {
  if (typeof fetchImpl !== "function") {
    return { items: [], source: "missing_fetch" };
  }
  try {
    const payload = await fetchJson(PUBLIC_JOBS_API_PATH, fetchImpl);
    const items = normalizeJobCatalog(payload);
    if (items.length) {
      return { items, source: String(payload?.source || "api") };
    }
  } catch (error) {
    // Fallback to local fixtures below.
  }

  const payload = await fetchJson(FALLBACK_JOBS_PATH, fetchImpl);
  return { items: normalizeJobCatalog(payload), source: "fixture" };
}
