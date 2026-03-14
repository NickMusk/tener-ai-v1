import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";

class ElementStub {
  constructor(id = "") {
    this.id = id;
    this.textContent = "";
    this.innerHTML = "";
    this.value = "";
    this.disabled = false;
    this.href = "";
    this.options = [];
    this.style = {};
    this.dataset = {};
  }

  addEventListener() {}

  removeAttribute(name) {
    if (name === "href") {
      this.href = "";
    }
  }

  closest() {
    return null;
  }
}

function buildHarness() {
  const htmlPath = path.resolve("src/tener_ai/static/candidate_profile.html");
  const html = fs.readFileSync(htmlPath, "utf8");
  const match = html.match(/<script>([\s\S]*)<\/script>/);
  assert.ok(match, "candidate profile script block not found");
  const cutoff = match[1].lastIndexOf("\n    init();");
  assert.ok(cutoff > 0, "candidate profile bootstrap marker not found");
  const script = `${match[1].slice(0, cutoff)}\nthis.__candidate_profile_exports = { state, renderScores, renderConversation, renderResume, toggleConversationMessages };`;

  const elements = new Map();
  const fetchCalls = [];
  const responses = new Map();

  function getElementById(id) {
    if (!elements.has(id)) {
      const el = new ElementStub(id);
      if (id === "status-select") {
        el.options = [{ value: "review" }, { value: "pipeline" }, { value: "shortlist" }, { value: "blocked" }];
      }
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
        pathname: "/candidate/676",
        search: "?job_id=27",
      },
      history: {
        replaceState() {},
      },
      open() {},
    },
    document: {
      getElementById,
      body: new ElementStub("body"),
    },
    fetch: async (url) => {
      fetchCalls.push(String(url));
      const payload = responses.get(String(url));
      assert.notEqual(payload, undefined, `unexpected fetch for ${url}`);
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify(payload);
        },
      };
    },
  };
  sandbox.window.document = sandbox.document;

  vm.runInNewContext(script, sandbox, { filename: htmlPath });
  const exports = sandbox.__candidate_profile_exports;
  assert.ok(exports, "candidate profile exports missing");

  return {
    html,
    state: exports.state,
    renderScores: exports.renderScores,
    renderConversation: exports.renderConversation,
    renderResume: exports.renderResume,
    toggleConversationMessages: exports.toggleConversationMessages,
    element(id) {
      return getElementById(id);
    },
    responses,
    fetchCalls,
  };
}

test("candidate profile markup aligns score labels and removes inline resume iframe", () => {
  const harness = buildHarness();

  assert.ok(harness.html.includes("Profile and CV"));
  assert.ok(harness.html.includes("Interview"));
  assert.ok(harness.html.includes("Preview is hidden on this page"));
  assert.ok(!harness.html.includes('iframe id="resume-frame"'));
});

test("renderScores keeps overall pending while showing dashboard-aligned partial scores", () => {
  const harness = buildHarness();
  harness.state.profile = {
    generated_at: "2026-03-14T02:40:12+00:00",
    jobs: [
      {
        job: { id: 27 },
        scorecard: {
          sourcing_vetting: { latest_score: 82, latest_details: { match_score: 50 } },
          communication: { latest_stage: "dialogue", latest_score: 92 },
          interview_evaluation: { latest_status: "not_started", latest_score: null },
        },
        overall_scoring: {
          overall_status: "review",
          overall_score: null,
          has_interview_score: false,
          has_cv: true,
          has_all_scores: false,
        },
      },
    ],
  };
  harness.state.selectedJobId = 27;

  harness.renderScores();

  assert.equal(harness.element("score-tech").textContent, "50.0");
  assert.equal(harness.element("score-soft").textContent, "92.0");
  assert.equal(harness.element("score-culture").textContent, "N/A");
  assert.match(harness.element("score-meta").textContent, /interview score not available yet/);
});

test("renderResume builds a download URL without fetching preview content", () => {
  const harness = buildHarness();
  harness.state.candidateId = 676;
  harness.state.profile = {
    jobs: [
      {
        job: { id: 27 },
        resumes: {
          items: [
            {
              url: "att://resume-1",
              label: "resume.pdf",
              processing_status: "processed",
              storage_available: true,
            },
          ],
        },
      },
    ],
  };
  harness.state.selectedJobId = 27;

  harness.renderResume();

  assert.equal(harness.fetchCalls.length, 0);
  assert.equal(
    harness.state.selectedResumeDownloadUrl,
    "/api/candidates/676/resume-preview/content?url=att%3A%2F%2Fresume-1",
  );
  assert.equal(harness.element("resume-open-btn").disabled, false);
  assert.equal(harness.element("resume-note").style.display, "block");
});

test("conversation messages load lazily and the section collapses without refetching", async () => {
  const harness = buildHarness();
  harness.state.profile = {
    jobs: [
      {
        job: { id: 27 },
        conversation: {
          conversation_id: 817,
          conversation_status: "active",
          linkedin_account_label: "Andres Servin",
          external_chat_id: "chat-817",
          dashboard_path: "/dashboard?view=agent&conversation_id=817",
        },
      },
    ],
  };
  harness.state.selectedJobId = 27;
  harness.responses.set("/api/conversations/817/messages", {
    items: [
      { direction: "outbound", created_at: "2026-03-14T06:30:00Z", content: "Hi Mykola" },
      { direction: "inbound", created_at: "2026-03-14T06:31:00Z", content: "Hello there" },
    ],
  });

  harness.renderConversation();
  await harness.toggleConversationMessages();

  assert.deepEqual(harness.fetchCalls, ["/api/conversations/817/messages"]);
  assert.match(harness.element("conversation-messages").innerHTML, /Hello there/);
  assert.equal(harness.element("conversation-toggle-btn").textContent, "Hide Messages");

  await harness.toggleConversationMessages();

  assert.deepEqual(harness.fetchCalls, ["/api/conversations/817/messages"]);
  assert.equal(harness.element("conversation-messages-wrap").style.display, "none");
});
