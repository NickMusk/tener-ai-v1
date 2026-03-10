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
  const script = `${match[1].slice(0, cutoff)}\nthis.__dashboard_exports = { state, loadJobProgress };`;

  const elements = new Map();
  const fetchCalls = [];
  const responses = new Map();

  function getElementById(id) {
    if (!elements.has(id)) {
      const el = new ElementStub(id);
      if (id.startsWith("status-") || id.startsWith("nav-")) el.textContent = "idle";
      if (id.startsWith("metric-")) el.textContent = "0";
      elements.set(id, el);
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
      fetchCalls.push(String(url));
      const payload = responses.get(String(url));
      assert.notEqual(payload, undefined, `unexpected fetch for ${url}`);
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
    loadJobProgress: exports.loadJobProgress,
    element(id) {
      return getElementById(id);
    },
    responses,
    fetchCalls,
  };
}

test("loadJobProgress refreshes candidate metrics when a step moves from running to success", async () => {
  const harness = buildHarness();
  const now = new Date().toISOString();

  harness.state.pipelineTracker = {
    active: true,
    jobId: 26,
    mode: "full_pipeline",
    startedAt: Date.now() - 1000,
    pollTimerId: null,
    tickTimerId: null,
    hideTimerId: null,
    pollBusy: false,
  };
  harness.state.progressStepStatuses = {
    source: "success",
    enrich: "success",
    verify: "success",
    add: "running",
    outreach: "idle",
  };

  harness.responses.set("/api/jobs/26/progress", {
    job_id: 26,
    items: [
      {
        step: "add",
        status: "success",
        output_json: {
          total: 3,
          added: [{ candidate_id: 1 }, { candidate_id: 2 }, { candidate_id: 3 }],
        },
        updated_at: now,
      },
    ],
  });
  harness.responses.set("/api/jobs/26/candidates", {
    job_id: 26,
    items: [
      { candidate_id: 1, status: "verified", current_status_key: "verified", conversation_status: "" },
      { candidate_id: 2, status: "verified", current_status_key: "outreached", conversation_status: "active" },
      { candidate_id: 3, status: "needs_resume", current_status_key: "needs_resume", conversation_status: "" },
    ],
  });

  await harness.loadJobProgress(26, { preserveCurrentVisuals: true });

  assert.equal(harness.element("metric-searched").textContent, 3);
  assert.equal(harness.element("metric-verified").textContent, 2);
  assert.equal(harness.element("metric-outreached").textContent, 1);
  assert.equal(harness.element("step-source-meta").textContent, "Candidates in job: 3");
  assert.equal(harness.element("step-add-meta").textContent, "Added: 3");
  assert.ok(harness.fetchCalls.includes("/api/jobs/26/candidates"));
});

test("loadJobProgress does not refetch candidate metrics when status stays success", async () => {
  const harness = buildHarness();
  const now = new Date().toISOString();

  harness.state.pipelineTracker = {
    active: true,
    jobId: 26,
    mode: "full_pipeline",
    startedAt: Date.now() - 1000,
    pollTimerId: null,
    tickTimerId: null,
    hideTimerId: null,
    pollBusy: false,
  };
  harness.state.progressStepStatuses = {
    source: "success",
    enrich: "success",
    verify: "success",
    add: "success",
    outreach: "idle",
  };

  harness.responses.set("/api/jobs/26/progress", {
    job_id: 26,
    items: [
      {
        step: "add",
        status: "success",
        output_json: {
          total: 3,
          added: [{ candidate_id: 1 }, { candidate_id: 2 }, { candidate_id: 3 }],
        },
        updated_at: now,
      },
    ],
  });

  await harness.loadJobProgress(26, { preserveCurrentVisuals: true });

  assert.deepEqual(harness.fetchCalls, ["/api/jobs/26/progress"]);
});
