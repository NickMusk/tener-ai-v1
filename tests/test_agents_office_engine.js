import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { pathToFileURL } from "node:url";

const engineModule = await import(pathToFileURL(path.resolve("AgentsOffice-prototype/office-engine.js")).href);
const dataModule = await import(pathToFileURL(path.resolve("AgentsOffice-prototype/office-data.js")).href);

const { buildRuntimeState, materializeEvent, selectNextEventType } = engineModule;
const { FALLBACK_JOBS_PATH, PUBLIC_JOBS_API_PATH, loadJobCatalog } = dataModule;

test("loadJobCatalog falls back to local fixtures when public API fails", async () => {
  const fetchCalls = [];
  const payload = await loadJobCatalog(async (url) => {
    fetchCalls.push(String(url));
    if (String(url) === PUBLIC_JOBS_API_PATH) {
      throw new Error("network_down");
    }
    assert.equal(String(url), FALLBACK_JOBS_PATH);
    return {
      ok: true,
      async json() {
        return {
          items: [
            {
              id: "fixture-1",
              company: "Fallback Buyer",
              title: "Industrial Electrician",
              location: "Midland, TX",
              market_leads: 12,
              live_threads: 1,
              buyer_finalists: 1,
              candidate_signals: []
            }
          ]
        };
      }
    };
  });

  assert.deepEqual(fetchCalls, [PUBLIC_JOBS_API_PATH, FALLBACK_JOBS_PATH]);
  assert.equal(payload.source, "fixture");
  assert.equal(payload.items.length, 1);
  assert.equal(payload.items[0].title, "Industrial Electrician");
});

test("materializeEvent promotes finalists without exceeding shortlist limits", () => {
  const job = {
    id: "job-1",
    company: "Demo Buyer",
    title: "Industrial Electrician",
    location: "Midland, TX",
    market_leads: 25,
    live_threads: 2,
    buyer_finalists: 0,
    candidate_signals: [
      { headline: "Signal A", location: "Odessa, TX", score: 97, note: "A" },
      { headline: "Signal B", location: "Midland, TX", score: 94, note: "B" }
    ]
  };
  const runtime = buildRuntimeState(job);

  const first = materializeEvent(runtime, "finalist_lock", 10_000, () => 0.1);
  assert.equal(runtime.shortlist.length, 1);
  assert.equal(runtime.metrics.buyerFinalists, 1);
  assert.equal(first.shortlist_add.headline, "Signal A");

  const second = materializeEvent(runtime, "finalist_lock", 25_000, () => 0.1);
  assert.equal(runtime.shortlist.length, 2);
  assert.equal(runtime.metrics.buyerFinalists, 2);
  assert.equal(second.shortlist_add.headline, "Signal B");

  const third = materializeEvent(runtime, "finalist_lock", 40_000, () => 0.1);
  assert.equal(runtime.shortlist.length, 2);
  assert.equal(runtime.metrics.buyerFinalists, 2);
  assert.equal(third.shortlist_add, null);
});

test("selectNextEventType does not choose reply-dependent events when no live threads exist", () => {
  const runtime = buildRuntimeState({
    id: "job-2",
    company: "Demo Buyer",
    title: "PLC Technician",
    location: "Houston, TX",
    market_leads: 0,
    live_threads: 0,
    buyer_finalists: 0,
    candidate_signals: [{ headline: "Signal A", location: "Houston, TX", score: 91, note: "A" }]
  });

  const selected = selectNextEventType(runtime, 20_000, () => 0);
  assert.equal(selected, "source_push");
});
