import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

class ClassList {
  constructor() {
    this._classes = new Set();
  }

  add(...names) {
    names.filter(Boolean).forEach((name) => this._classes.add(String(name)));
  }

  remove(...names) {
    names.filter(Boolean).forEach((name) => this._classes.delete(String(name)));
  }

  toggle(name, force) {
    const normalized = String(name);
    if (force === undefined) {
      if (this._classes.has(normalized)) {
        this._classes.delete(normalized);
        return false;
      }
      this._classes.add(normalized);
      return true;
    }
    if (force) {
      this._classes.add(normalized);
      return true;
    }
    this._classes.delete(normalized);
    return false;
  }

  contains(name) {
    return this._classes.has(String(name));
  }
}

class ElementStub {
  constructor(id = "") {
    this.id = id;
    this.textContent = "";
    this.innerHTML = "";
    this.value = "";
    this.hidden = false;
    this.disabled = false;
    this.checked = false;
    this.className = "";
    this.dataset = {};
    this.style = {};
    this.classList = new ClassList();
  }

  addEventListener() {}

  removeEventListener() {}

  querySelectorAll() {
    return [];
  }

  querySelector() {
    return null;
  }

  closest() {
    return null;
  }
}

function buildHarness() {
  const htmlPath = path.resolve("src/tener_ai/static/dashboard.html");
  const html = fs.readFileSync(htmlPath, "utf8");
  const match = html.match(/<script>([\s\S]*)<\/script>/);
  assert.ok(match, "dashboard script block not found");
  const cutoff = match[1].indexOf("const initialDashboardRoute = parseDashboardRoute();");
  assert.ok(cutoff > 0, "dashboard bootstrap marker not found");
  const script = `${match[1].slice(0, cutoff)}\nthis.__dashboard_exports = { state, refreshOutreachOps };`;

  const elements = new Map();
  const responses = new Map();

  function getElementById(id) {
    if (!elements.has(id)) {
      elements.set(id, new ElementStub(id));
    }
    return elements.get(id);
  }

  const sandbox = {
    console,
    Date,
    Math,
    JSON,
    Number,
    String,
    Boolean,
    Array,
    Object,
    RegExp,
    URL,
    URLSearchParams,
    window: {
      location: {
        search: "",
        href: "http://localhost/dashboard",
        origin: "http://localhost",
      },
    },
    document: {
      getElementById,
      querySelectorAll() {
        return [];
      },
      querySelector() {
        return null;
      },
      addEventListener() {},
    },
    localStorage: {
      getItem() {
        return null;
      },
      setItem() {},
      removeItem() {},
    },
    fetch: async (url) => {
      const payload = responses.get(String(url));
      assert.notEqual(payload, undefined, `unexpected fetch for ${url}`);
      if (payload instanceof Error) {
        throw payload;
      }
      if (payload && typeof payload.then === "function") {
        return payload;
      }
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        async text() {
          return JSON.stringify(payload);
        },
      };
    },
    setInterval() {
      return 1;
    },
    clearInterval() {},
    setTimeout() {
      return 1;
    },
    clearTimeout() {},
    alert() {},
  };
  sandbox.window.document = sandbox.document;

  vm.runInNewContext(script, sandbox, { filename: htmlPath });
  const exports = sandbox.__dashboard_exports;
  assert.ok(exports, "dashboard exports missing");

  return {
    state: exports.state,
    refreshOutreachOps: exports.refreshOutreachOps,
    responses,
    element(id) {
      return getElementById(id);
    },
  };
}

test("refreshOutreachOps renders ops data without waiting for ATS board", async () => {
  const harness = buildHarness();
  harness.state.opsJobId = 27;
  harness.responses.set(
    "/api/outreach/ops?limit_logs=1000&limit_chats=800&job_id=27",
    {
      summary: {
        health: "warning",
        delivery_health: "ok",
        backlog_health: "warning",
        active_accounts: 5,
        connected_accounts: 4,
        waiting_connection: 129,
        stuck_threads: 102,
        sent_1h: 0,
        failed_1h: 0,
        last_successful_send_at: null,
        delivery_issues: [],
        backlog_issues: ["102 stuck thread(s)"],
      },
      backlog: {
        summary: {
          new_threads: 0,
          unassigned_recovery: 0,
          waiting_connection: 129,
          stuck_replies: 102,
          selected_jobs: 1,
        },
        items: [],
      },
      accounts: [],
      events: [],
      thresholds: { stale_minutes: 10080 },
    },
  );
  harness.responses.set(
    "/api/outreach/ats-board?limit=200&job_id=27",
    new Promise(() => {}),
  );

  await harness.refreshOutreachOps();

  assert.equal(harness.state.outreachOps.summary.active_accounts, 5);
  assert.equal(harness.element("ops-summary-active-accounts").textContent, "5/4");
  assert.equal(harness.element("ops-summary-waiting-connection").textContent, "129");
  assert.equal(harness.element("ops-summary-stuck").textContent, "102");
  assert.equal(harness.element("ops-ats-summary").textContent, "Loading ATS board...");
});

test("refreshOutreachOps keeps last good ATS board when ATS refresh fails", async () => {
  const harness = buildHarness();
  harness.state.opsJobId = 27;
  harness.state.outreachAtsBoard = {
    status: "ok",
    summary: {
      total_candidates: 12,
      sourced: 3,
      connect_sent: 2,
      responded: 5,
      must_have_approved: 0,
      cv_received: 0,
      interview_pending: 1,
      completed: 1,
    },
    columns: [],
  };
  harness.responses.set(
    "/api/outreach/ops?limit_logs=1000&limit_chats=800&job_id=27",
    {
      summary: {
        health: "ok",
        delivery_health: "ok",
        backlog_health: "ok",
        active_accounts: 2,
        connected_accounts: 2,
        waiting_connection: 5,
        stuck_threads: 1,
        sent_1h: 1,
        failed_1h: 0,
        last_successful_send_at: null,
        delivery_issues: [],
        backlog_issues: [],
      },
      backlog: { summary: {}, items: [] },
      accounts: [],
      events: [],
      thresholds: { stale_minutes: 10080 },
    },
  );
  harness.responses.set(
    "/api/outreach/ats-board?limit=200&job_id=27",
    Promise.reject(new Error("HTTP 502: ATS board unavailable")),
  );

  await harness.refreshOutreachOps();
  await Promise.resolve();

  assert.equal(harness.state.outreachAtsBoard.summary.total_candidates, 12);
  assert.match(harness.element("ops-ats-summary").textContent, /12 candidates/);
  assert.match(harness.element("ops-ats-summary").textContent, /stale:/);
});
