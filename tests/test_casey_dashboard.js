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
    this.title = "";
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
  const script = `${match[1].slice(0, cutoff)}\nthis.__dashboard_exports = { state, refreshAgentAccounts, renderCaseyOutreachHealth, renderAgentDialogueTabs };`;

  const elements = new Map();
  const fetchCalls = [];
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
    html,
    state: exports.state,
    refreshAgentAccounts: exports.refreshAgentAccounts,
    renderCaseyOutreachHealth: exports.renderCaseyOutreachHealth,
    renderAgentDialogueTabs: exports.renderAgentDialogueTabs,
    element(id) {
      return getElementById(id);
    },
    responses,
    fetchCalls,
  };
}

test("Casey active dialogues fetches started-only chats and filters manual rows", async () => {
  const harness = buildHarness();
  harness.state.activeView = "pipeline";
  harness.state.agentJobId = 42;
  harness.state.agentDialogueTab = "candidate_replied";
  harness.responses.set("/api/chats/overview?limit=300&started_only=1&dialogue_bucket=candidate_replied&job_id=42", {
    items: [
      { conversation_id: 11, channel: "linkedin", candidate_name: "Live Candidate", job_id: 42, job_title: "Backend" },
      { conversation_id: 12, channel: "manual", candidate_name: "Manual Candidate", job_id: 42, job_title: "Backend" },
    ],
  });

  await harness.refreshAgentAccounts();

  assert.deepEqual(harness.fetchCalls, ["/api/chats/overview?limit=300&started_only=1&dialogue_bucket=candidate_replied&job_id=42"]);
  assert.equal(harness.state.agentAccounts.length, 1);
  assert.equal(harness.state.agentAccounts[0].candidate_name, "Live Candidate");
});

test("Casey stuck health exposes a tooltip with the stale threshold", () => {
  const harness = buildHarness();
  harness.state.outreachOps = {
    thresholds: { stale_minutes: 45 },
    summary: {
      delivery_health: "warning",
      sent_1h: 1,
      sent_24h: 2,
      failed_1h: 0,
      stuck_threads: 3,
      last_successful_send_at: null,
      delivery_issues: [],
    },
  };

  harness.renderCaseyOutreachHealth();

  assert.match(harness.element("casey-health-stuck-card").title, /45 minutes/);
  assert.match(harness.element("casey-health-stuck").title, /awaiting a reply/i);
});

test("Casey dashboard markup removes legacy controls and manual account block", () => {
  const harness = buildHarness();

  assert.ok(!harness.html.includes("Save Token"));
  assert.ok(!harness.html.includes("Connect LinkedIn"));
  assert.ok(!harness.html.includes("Add Manual Account (test)"));
  assert.ok(harness.html.includes("Active Candidate Dialogues"));
  assert.ok(harness.html.includes("casey-dialogues-wrap"));
  assert.ok(harness.html.includes("Candidate Replied"));
  assert.ok(harness.html.includes("Outbound Only"));
});

test("Casey dialogue tabs update the endpoint label for the active bucket", () => {
  const harness = buildHarness();
  harness.state.agentDialogueTab = "outbound_only";

  harness.renderAgentDialogueTabs();

  assert.match(harness.element("agent-dialogues-endpoint-label").textContent, /dialogue_bucket=outbound_only/);
});
