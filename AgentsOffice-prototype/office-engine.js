import {
  ACTIVITY_LIMIT,
  AGENT_DEFS,
  DIRECTION_ROWS,
  FALLBACK_JOBS_PATH,
  PUBLIC_JOBS_API_PATH,
  SHORTLIST_LIMIT,
  SPRITE_SHEET_PATH,
  SPRITES,
  TILEMAP_PATH,
  TILESET_PATH,
  clamp,
  formatMetric,
  loadJobCatalog,
  pickWeighted,
  randomItem
} from "./office-data.js";

const EVENT_COOLDOWNS_MS = {
  spec_sync: 18000,
  source_push: 6000,
  filter_pass: 9000,
  outreach_open: 8500,
  reply_signal: 9500,
  finalist_lock: 12000,
  fit_check: 14000,
  board_refresh: 12000,
  keep_warm: 8000
};

function randomInt(min, max, rng = Math.random) {
  const safeMin = Math.ceil(Number(min || 0));
  const safeMax = Math.floor(Number(max || 0));
  return Math.floor(rng() * (safeMax - safeMin + 1)) + safeMin;
}

function cloneSignals(items) {
  return (items || []).map((item) => ({ ...item }));
}

function nextSignal(runtime) {
  const items = Array.isArray(runtime?.job?.candidate_signals) ? runtime.job.candidate_signals : [];
  if (!items.length) return null;
  const index = Number(runtime?.nextSignalIndex || 0);
  if (index < 0 || index >= items.length) return null;
  return items[index] ? { ...items[index] } : null;
}

function shortlistMax(runtime) {
  const jobSignals = Array.isArray(runtime?.job?.candidate_signals) ? runtime.job.candidate_signals.length : 0;
  return clamp(Math.max(Number(runtime?.baseline?.buyerFinalists || 0), Math.min(jobSignals, 2)), 1, SHORTLIST_LIMIT);
}

function metricCeiling(runtime, key) {
  if (key === "marketLeads") return Math.max(Number(runtime?.baseline?.marketLeads || 0) + 24, 18);
  if (key === "liveThreads") return Math.max(Number(runtime?.baseline?.liveThreads || 0) + 4, 2);
  if (key === "buyerFinalists") return shortlistMax(runtime);
  return 999;
}

function applyMetricDelta(runtime, key, delta) {
  const current = Number(runtime.metrics[key] || 0);
  const ceiling = metricCeiling(runtime, key);
  runtime.metrics[key] = clamp(current + Number(delta || 0), 0, ceiling);
  return runtime.metrics[key];
}

function eventBubble(text, emoji, duration = 3.8) {
  return { text, emoji, duration };
}

function headlineForJob(job) {
  return String(job?.title || "the search").trim() || "the search";
}

function marketLabel(job) {
  const location = String(job?.location || "").trim();
  if (!location) return headlineForJob(job);
  return `${headlineForJob(job)} in ${location}`;
}

function buildPhaseText(runtime, options, rng = Math.random) {
  return randomItem(options, rng) || String(runtime?.job?.phase_label || "The office is moving the search.");
}

export function buildRuntimeState(job) {
  const baseline = {
    marketLeads: clamp(Number(job?.market_leads || 0), 0, 9999),
    liveThreads: clamp(Number(job?.live_threads || 0), 0, 999),
    buyerFinalists: clamp(Number(job?.buyer_finalists || 0), 0, SHORTLIST_LIMIT)
  };
  const shortlist = cloneSignals((job?.candidate_signals || []).slice(0, baseline.buyerFinalists));
  return {
    job,
    baseline,
    metrics: {
      marketLeads: baseline.marketLeads,
      liveThreads: baseline.liveThreads,
      buyerFinalists: shortlist.length
    },
    shortlist,
    activity: [
      {
        kind: "buyer",
        title: "Buyer brief loaded",
        detail: String(job?.phase_label || "The office is live on this search."),
        timestamp: new Date().toISOString()
      }
    ],
    phase: String(job?.phase_label || "The office is live on this search."),
    nextSignalIndex: shortlist.length,
    lastEventAtByType: {},
    nextEventAtMs: 0,
    nextAmbientAtMs: 0
  };
}

export function selectNextEventType(runtime, nowMs, rng = Math.random) {
  const history = runtime?.lastEventAtByType || {};
  const cooled = (type) => nowMs - Number(history[type] || 0) >= Number(EVENT_COOLDOWNS_MS[type] || 0);
  const shortlistSpace = runtime.shortlist.length < shortlistMax(runtime) && Boolean(nextSignal(runtime));

  const options = [
    { type: "source_push", weight: cooled("source_push") ? 7 : 0 },
    {
      type: "filter_pass",
      weight: cooled("filter_pass") && Number(runtime.metrics.marketLeads || 0) > 8 ? 4 : 0
    },
    {
      type: "outreach_open",
      weight: cooled("outreach_open") && Number(runtime.metrics.marketLeads || 0) > 6
        && Number(runtime.metrics.liveThreads || 0) < metricCeiling(runtime, "liveThreads")
        ? 6
        : 0
    },
    {
      type: "reply_signal",
      weight: cooled("reply_signal") && Number(runtime.metrics.liveThreads || 0) > 0 ? 5 : 0
    },
    {
      type: "finalist_lock",
      weight: cooled("finalist_lock") && shortlistSpace
        && (Number(runtime.metrics.liveThreads || 0) > 0 || Number(runtime.metrics.marketLeads || 0) > 16)
        ? 4
        : 0
    },
    {
      type: "fit_check",
      weight: cooled("fit_check") && runtime.shortlist.length > 0 ? 2 : 0
    },
    {
      type: "board_refresh",
      weight: cooled("board_refresh") && runtime.shortlist.length > 0 ? 2 : 0
    },
    {
      type: "keep_warm",
      weight: cooled("keep_warm") && Number(runtime.metrics.liveThreads || 0) > 0 ? 3 : 0
    },
    { type: "spec_sync", weight: cooled("spec_sync") ? 1 : 0 }
  ];
  const selected = pickWeighted(options, rng);
  return selected?.type || "source_push";
}

export function materializeEvent(runtime, type, nowMs, rng = Math.random) {
  runtime.lastEventAtByType[type] = nowMs;
  const job = runtime.job;
  const roleLabel = marketLabel(job);

  if (type === "spec_sync") {
    runtime.phase = buildPhaseText(runtime, [
      `Spencer and Harper are tightening the brief for ${roleLabel}.`,
      `The office is refreshing buyer-fit rules for ${roleLabel}.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "spencer",
          move: { to: [19, 12], speed: 1.2 },
          status: {
            key: "syncing",
            label: "syncing",
            note: `Refining the brief for ${headlineForJob(job)}.`
          },
          bubble: eventBubble("Tightening the brief", "\ud83e\udde9", 4.0)
        },
        {
          agent: "harper",
          move: { to: [13, 22], speed: 1.05 },
          status: {
            key: "reviewing",
            label: "reviewing",
            note: "Keeping the fit rubric aligned with the buyer."
          },
          bubble: eventBubble("Refreshing fit rules", "\ud83e\udde0", 4.0)
        }
      ],
      activity: {
        kind: "buyer",
        title: "Buyer brief refreshed",
        detail: `The office recalibrated ${headlineForJob(job)} around buyer-fit and field reality.`
      }
    };
  }

  if (type === "source_push") {
    const leads = applyMetricDelta(runtime, "marketLeads", randomInt(2, 6, rng));
    runtime.phase = buildPhaseText(runtime, [
      `Reed is widening the market scan for ${roleLabel}.`,
      `The office is stacking fresh market leads for ${roleLabel}.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "reed",
          move: { to: [12 + randomInt(0, 3, rng), 9 + randomInt(0, 3, rng)], speed: 1.45 },
          status: {
            key: "sourcing",
            label: "sourcing",
            note: `Opening more channels for ${headlineForJob(job)}.`
          },
          bubble: eventBubble("Pulling fresh market signal", "\ud83d\udd0e", 4.1),
          metrics: { marketLeads: leads }
        }
      ],
      activity: {
        kind: "signal",
        title: "Market scan widened",
        detail: `Reed opened another sourcing pass for ${headlineForJob(job)} and the board picked up fresh signal.`
      }
    };
  }

  if (type === "filter_pass") {
    const leads = applyMetricDelta(runtime, "marketLeads", -randomInt(0, 2, rng));
    runtime.phase = buildPhaseText(runtime, [
      `Harper and Reed are filtering the strongest signal for ${roleLabel}.`,
      `The office is cutting noise and keeping only buyer-fit profiles.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "reed",
          move: { to: [15, 10], speed: 1.3 },
          status: {
            key: "reviewing",
            label: "reviewing",
            note: "Cutting weaker leads before they hit the board."
          },
          bubble: eventBubble("Filtering the top wave", "\ud83d\udd0e", 3.9),
          metrics: { marketLeads: leads }
        },
        {
          agent: "harper",
          move: { to: [15, 18], speed: 1.1 },
          status: {
            key: "monitoring",
            label: "ready",
            note: "Keeping the search biased toward buyer-native operators."
          },
          bubble: eventBubble("Holding the fit line", "\u2705", 3.9)
        }
      ],
      activity: {
        kind: "signal",
        title: "Signal quality pass",
        detail: "Harper pushed the board back toward buyer-fit while Reed trimmed weaker leads."
      }
    };
  }

  if (type === "outreach_open") {
    const threads = applyMetricDelta(runtime, "liveThreads", 1);
    runtime.phase = buildPhaseText(runtime, [
      `Casey is opening fresh threads for ${roleLabel}.`,
      `The office is turning qualified signal into live conversations.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "reed",
          status: {
            key: "reviewing",
            label: "reviewing",
            note: "Passing strong signal into outreach."
          },
          bubble: eventBubble("Handing over a clean lead", "\ud83d\udce6", 3.8)
        },
        {
          agent: "casey",
          move: { to: [29, 25], speed: 1.25 },
          status: {
            key: "messaging",
            label: "messaging",
            note: "Opening a candidate thread with buyer-context already attached."
          },
          bubble: eventBubble("Launching fresh outreach", "\ud83d\udcac", 4.1),
          metrics: { liveThreads: threads }
        }
      ],
      handoff: {
        from: "reed",
        to: "casey",
        label: "Qualified lead -> outreach",
        duration: 3.8
      },
      activity: {
        kind: "outreach",
        title: "New outreach wave",
        detail: "Casey opened another live thread while Reed kept the market scan moving."
      }
    };
  }

  if (type === "reply_signal") {
    runtime.phase = buildPhaseText(runtime, [
      `Casey is routing a live reply into review for ${roleLabel}.`,
      `A fast candidate reply just pushed more signal into the board.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "casey",
          status: {
            key: "messaging",
            label: "messaging",
            note: "The candidate thread is active and moving quickly."
          },
          bubble: eventBubble("Fast reply came back", "\ud83d\udce8", 4.0)
        },
        {
          agent: "jordan",
          move: { to: [28, 14], speed: 1.05 },
          status: {
            key: "scoring",
            label: "scoring",
            note: "Pulling a warm thread into buyer-facing review."
          },
          bubble: eventBubble("Pulling it into review", "\u2b50", 4.0)
        }
      ],
      handoff: {
        from: "casey",
        to: "jordan",
        label: "Warm reply -> review",
        duration: 3.8
      },
      activity: {
        kind: "handoff",
        title: "Reply converted into evidence",
        detail: "Casey escalated a live reply to Jordan as soon as the signal looked real."
      }
    };
  }

  if (type === "finalist_lock") {
    const signal = nextSignal(runtime);
    if (signal) {
      runtime.nextSignalIndex += 1;
      runtime.shortlist.unshift(signal);
      runtime.shortlist = runtime.shortlist.slice(0, SHORTLIST_LIMIT);
      runtime.metrics.buyerFinalists = runtime.shortlist.length;
    }
    runtime.phase = buildPhaseText(runtime, [
      `Jordan is promoting a buyer-ready finalist for ${roleLabel}.`,
      `The office is turning the funnel into a real buyer shortlist.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "jordan",
          move: { to: [26, 12], speed: 1.0 },
          status: {
            key: "ready",
            label: "ready",
            note: "Packaging a buyer-facing finalist card."
          },
          bubble: eventBubble("Finalist promoted", "\ud83c\udfc1", 4.4),
          metrics: { buyerFinalists: runtime.metrics.buyerFinalists }
        }
      ],
      activity: {
        kind: "buyer",
        title: "Buyer finalist promoted",
        detail: signal
          ? `${signal.headline} moved into the buyer shortlist with stronger signal than the rest of the board.`
          : "Jordan refreshed the buyer board with another credible finalist."
      },
      shortlist_add: signal || null
    };
  }

  if (type === "fit_check") {
    runtime.phase = buildPhaseText(runtime, [
      `Harper and Spencer are sanity-checking the shortlist for ${roleLabel}.`,
      `The office is validating that the shortlist still matches the buyer brief.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "harper",
          move: { to: [12, 24], speed: 1.0 },
          status: {
            key: "reviewing",
            label: "reviewing",
            note: "Confirming shortlist fit against the buyer environment."
          },
          bubble: eventBubble("Fit still looks clean", "\ud83e\udde0", 4.0)
        },
        {
          agent: "spencer",
          move: { to: [20, 14], speed: 1.05 },
          status: {
            key: "syncing",
            label: "syncing",
            note: "Checking the shortlist against the opening brief."
          },
          bubble: eventBubble("Brief still matches the board", "\ud83e\udde9", 4.0)
        }
      ],
      activity: {
        kind: "buyer",
        title: "Buyer-fit check",
        detail: "Harper and Spencer validated that the current shortlist still matches the buyer brief."
      }
    };
  }

  if (type === "board_refresh") {
    runtime.phase = buildPhaseText(runtime, [
      `Jordan is refreshing the buyer board for ${roleLabel}.`,
      `The office is packaging the strongest signal into a cleaner board.`
    ], rng);
    return {
      phase: runtime.phase,
      actions: [
        {
          agent: "jordan",
          status: {
            key: "ready",
            label: "ready",
            note: "Reordering the board by current confidence."
          },
          bubble: eventBubble("Refreshing the buyer board", "\u2b50", 4.0)
        },
        {
          agent: "casey",
          status: {
            key: "messaging",
            label: "messaging",
            note: "Keeping live threads warm while the board updates."
          },
          bubble: eventBubble("Keeping the shortlist warm", "\ud83d\udcac", 3.8)
        }
      ],
      activity: {
        kind: "buyer",
        title: "Board refreshed",
        detail: "Jordan cleaned up the buyer-facing board while Casey kept candidate momentum intact."
      }
    };
  }

  runtime.phase = buildPhaseText(runtime, [
    `Casey is keeping active threads warm for ${roleLabel}.`,
    `The office is maintaining candidate momentum without slowing the board.`
  ], rng);
  return {
    phase: runtime.phase,
    actions: [
      {
        agent: "casey",
        move: { to: [30, 26], speed: 1.15 },
        status: {
          key: "messaging",
          label: "messaging",
          note: "Keeping warm conversations moving."
        },
        bubble: eventBubble("Sending next-step details", "\ud83d\udcac", 3.8)
      }
    ],
    activity: {
      kind: "outreach",
      title: "Threads kept warm",
      detail: "Casey pushed next-step details so active candidates do not cool off."
    }
  };
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function createDomContext(documentRef) {
  const elements = {
    canvas: documentRef.getElementById("world-canvas"),
    overlay: documentRef.getElementById("world-overlay"),
    progressFill: documentRef.getElementById("progress-fill"),
    errorBanner: documentRef.getElementById("error-banner"),
    jobTitle: documentRef.getElementById("job-title"),
    jobSubtitle: documentRef.getElementById("job-subtitle"),
    metricProfiles: documentRef.getElementById("metric-profiles"),
    metricConversations: documentRef.getElementById("metric-conversations"),
    metricShortlisted: documentRef.getElementById("metric-shortlisted"),
    runClock: documentRef.getElementById("run-clock"),
    runPhase: documentRef.getElementById("run-phase"),
    briefCompany: documentRef.getElementById("brief-company"),
    briefRole: documentRef.getElementById("brief-role"),
    briefSummary: documentRef.getElementById("brief-summary"),
    briefSources: documentRef.getElementById("brief-sources"),
    jobList: documentRef.getElementById("job-list"),
    agentList: documentRef.getElementById("agent-list"),
    shortlist: documentRef.getElementById("shortlist"),
    activityList: documentRef.getElementById("activity-list"),
    handoff: documentRef.getElementById("handoff"),
    handoffLine: documentRef.getElementById("handoff-line"),
    handoffLabel: documentRef.getElementById("handoff-label")
  };
  if (!elements.canvas || !elements.overlay) {
    throw new Error("agents_office_dom_missing");
  }
  return elements;
}

function setError(dom, message) {
  if (!dom.errorBanner) return;
  dom.errorBanner.textContent = message || "";
  dom.errorBanner.classList.toggle("visible", Boolean(message));
}

function updateMetrics(dom, metrics) {
  dom.metricProfiles.textContent = formatMetric(metrics.marketLeads || 0);
  dom.metricConversations.textContent = formatMetric(metrics.liveThreads || 0);
  dom.metricShortlisted.textContent = formatMetric(metrics.buyerFinalists || 0);
}

function renderActivity(dom, runtime) {
  const items = (runtime.activity || []).slice(0, ACTIVITY_LIMIT);
  if (!items.length) {
    dom.activityList.innerHTML = '<article class="activity-item"><span>No office events yet.</span></article>';
    return;
  }
  dom.activityList.innerHTML = items.map((item) => `
    <article class="activity-item">
      <div class="activity-head">
        <strong>${escapeHtml(item.title)}</strong>
        <span class="activity-kind ${escapeHtml(item.kind || "signal")}">${escapeHtml(item.kind || "signal")}</span>
      </div>
      <span>${escapeHtml(item.detail)}</span>
      <time>${escapeHtml(new Date(item.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }))}</time>
    </article>
  `).join("");
}

function renderShortlist(dom, runtime) {
  if (!runtime.shortlist.length) {
    dom.shortlist.innerHTML = '<article class="candidate-card"><span class="candidate-meta">No finalists locked yet.</span></article>';
    return;
  }
  dom.shortlist.innerHTML = runtime.shortlist.map((item) => `
    <article class="candidate-card">
      <div class="candidate-head">
        <strong>${escapeHtml(item.headline)}</strong>
        <span class="candidate-score">${escapeHtml(item.score)}</span>
      </div>
      <div class="candidate-meta">${escapeHtml(item.location)}</div>
      <div class="candidate-meta">${escapeHtml(item.note)}</div>
    </article>
  `).join("");
}

function renderAgents(dom, agents) {
  dom.agentList.innerHTML = agents.map((agent) => `
    <article class="agent-card">
      <div class="agent-head">
        <strong>${escapeHtml(agent.name)}</strong>
        <span class="status ${escapeHtml(agent.status)}">${escapeHtml(agent.status_label)}</span>
      </div>
      <div class="agent-meta">${escapeHtml(agent.role)}</div>
      <div class="agent-meta">${escapeHtml(agent.note)}</div>
    </article>
  `).join("");
}

function renderBrief(dom, runtime) {
  const job = runtime.job;
  dom.jobTitle.textContent = job.title;
  dom.jobSubtitle.textContent = `${job.location} | ${job.market}`;
  dom.briefCompany.textContent = job.company;
  dom.briefRole.textContent = `${job.title} | ${job.location}`;
  dom.briefSummary.textContent = job.summary;
  dom.briefSources.textContent = `Channels: ${(job.channels || []).join(", ")}`;
  dom.runPhase.textContent = runtime.phase;
}

function renderJobs(dom, state) {
  if (!state.jobCatalog.length) {
    dom.jobList.innerHTML = '<article class="job-card"><span class="job-card-meta">No jobs available.</span></article>';
    return;
  }
  dom.jobList.innerHTML = state.jobCatalog.map((job) => {
    const selected = String(job.id) === String(state.selectedJobId);
    const metrics = selected && state.runtime ? state.runtime.metrics : {
      marketLeads: job.market_leads,
      liveThreads: job.live_threads,
      buyerFinalists: job.buyer_finalists
    };
    const phaseLabel = selected && state.runtime ? state.runtime.phase : job.phase_label;
    return `
      <button class="job-card${selected ? " active" : ""}" data-job-id="${escapeHtml(job.id)}" type="button">
        <div class="job-card-head">
          <strong>${escapeHtml(job.title)}</strong>
          <span class="job-card-location">${escapeHtml(job.location)}</span>
        </div>
        <div class="job-card-meta">${escapeHtml(job.company)}</div>
        <div class="job-card-meta">${escapeHtml(phaseLabel)}</div>
        <div class="job-chip-row">
          <span class="job-chip">${escapeHtml(formatMetric(metrics.marketLeads))} leads</span>
          <span class="job-chip">${escapeHtml(formatMetric(metrics.liveThreads))} threads</span>
          <span class="job-chip">${escapeHtml(formatMetric(metrics.buyerFinalists))} finalists</span>
        </div>
      </button>
    `;
  }).join("");
}

function syncAgentDom(dom, agent) {
  if (!agent.el) return;
  const left = ((agent.x + 0.5) / 40) * 100;
  const top = ((agent.y + 0.7) / 40) * 100;
  agent.el.style.left = `${left}%`;
  agent.el.style.top = `${top}%`;
  agent.el.classList.toggle("speaking", Boolean(agent.bubbleText));
  const bubble = agent.el.querySelector(".bubble");
  if (bubble) bubble.textContent = agent.bubbleText || "";
  const mood = agent.el.querySelector(".mood");
  if (mood) mood.textContent = agent.emoji || "";
  const sprite = agent.el.querySelector(".sprite");
  if (sprite) {
    const config = SPRITES[agent.spriteKey] || SPRITES.f1;
    const frame = agent.frameIndex % 3;
    const x = config.baseX + frame * 32;
    const y = config.baseY + DIRECTION_ROWS[agent.direction] * 32;
    sprite.style.backgroundPosition = `-${x}px -${y}px`;
  }
}

function createAgentDom(dom, agent) {
  const root = document.createElement("div");
  root.className = "agent";
  root.dataset.agentId = agent.id;
  root.innerHTML = `
    <div class="nameplate">${escapeHtml(agent.name)}</div>
    <div class="mood">${escapeHtml(agent.emoji || "")}</div>
    <div class="bubble"></div>
    <div class="avatar-ring"></div>
    <div class="sprite"></div>
  `;
  dom.overlay.appendChild(root);
  agent.el = root;
  syncAgentDom(dom, agent);
}

function clearWorldAgents(dom) {
  dom.overlay.querySelectorAll(".agent").forEach((node) => node.remove());
}

function clearHandoff(dom, state) {
  state.handoff = null;
  dom.handoff.classList.remove("visible");
}

function syncHandoff(dom, state) {
  if (!state.handoff) return;
  const from = state.agents.find((item) => item.id === state.handoff.fromId);
  const to = state.agents.find((item) => item.id === state.handoff.toId);
  if (!from || !to) {
    clearHandoff(dom, state);
    return;
  }
  const x1 = ((from.x + 0.5) / 40) * dom.overlay.clientWidth;
  const y1 = ((from.y + 0.7) / 40) * dom.overlay.clientHeight;
  const x2 = ((to.x + 0.5) / 40) * dom.overlay.clientWidth;
  const y2 = ((to.y + 0.7) / 40) * dom.overlay.clientHeight;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const length = Math.hypot(dx, dy);
  const angle = Math.atan2(dy, dx) * 180 / Math.PI;
  dom.handoffLine.style.left = `${x1}px`;
  dom.handoffLine.style.top = `${y1}px`;
  dom.handoffLine.style.width = `${Math.max(24, length)}px`;
  dom.handoffLine.style.transform = `rotate(${angle}deg)`;
  dom.handoffLabel.style.left = `${x1 + dx / 2}px`;
  dom.handoffLabel.style.top = `${y1 + dy / 2 - 14}px`;
}

function setHandoff(dom, state, fromId, toId, label, durationSeconds) {
  state.handoff = {
    fromId,
    toId,
    label: label || "Agent handoff",
    until: performance.now() + (durationSeconds || 3.2) * 1000
  };
  dom.handoffLabel.textContent = state.handoff.label;
  dom.handoff.classList.add("visible");
  syncHandoff(dom, state);
}

function setAgentBubble(dom, state, agentId, text, durationSeconds, emoji) {
  const agent = state.agents.find((item) => item.id === agentId);
  if (!agent) return;
  agent.bubbleText = text;
  agent.bubbleUntil = performance.now() + durationSeconds * 1000;
  if (emoji !== undefined) agent.emoji = emoji;
  syncAgentDom(dom, agent);
}

function updateAgentCard(dom, state, agentId, patch) {
  const agent = state.agents.find((item) => item.id === agentId);
  if (!agent) return;
  Object.assign(agent, patch);
  renderAgents(dom, state.agents);
}

function moveAgent(state, agentId, to, speed) {
  const agent = state.agents.find((item) => item.id === agentId);
  if (!agent) return;
  agent.targetX = to[0];
  agent.targetY = to[1];
  if (speed) agent.speed = speed;
}

function pushActivity(dom, state, runtime, activity) {
  if (!activity) return;
  runtime.activity.unshift({
    ...activity,
    timestamp: new Date().toISOString()
  });
  runtime.activity = runtime.activity.slice(0, ACTIVITY_LIMIT);
  renderActivity(dom, runtime);
}

function applyAction(dom, state, runtime, action) {
  if (!action) return;
  if (action.metrics) {
    runtime.metrics = {
      ...runtime.metrics,
      ...action.metrics
    };
    updateMetrics(dom, runtime.metrics);
  }
  if (action.move) moveAgent(state, action.agent, action.move.to, action.move.speed);
  if (action.status) {
    updateAgentCard(dom, state, action.agent, {
      status: action.status.key,
      status_label: action.status.label,
      note: action.status.note || ""
    });
  }
  if (action.bubble) {
    setAgentBubble(dom, state, action.agent, action.bubble.text, action.bubble.duration || 3.8, action.bubble.emoji);
  }
}

function applyEvent(dom, state, runtime, event) {
  runtime.phase = event.phase || runtime.phase;
  dom.runPhase.textContent = runtime.phase;
  if (Array.isArray(event.actions) && event.actions.length) {
    event.actions.forEach((action) => applyAction(dom, state, runtime, action));
  }
  if (event.handoff) {
    setHandoff(dom, state, event.handoff.from, event.handoff.to, event.handoff.label, event.handoff.duration || 3.4);
  }
  if (event.shortlist_add) {
    renderShortlist(dom, runtime);
  }
  if (event.activity) {
    pushActivity(dom, state, runtime, event.activity);
  }
  renderJobs(dom, state);
}

function resetAgents(dom, state) {
  clearWorldAgents(dom);
  clearHandoff(dom, state);
  state.agents = AGENT_DEFS.map((agent) => ({
    ...agent,
    x: agent.start[0],
    y: agent.start[1],
    targetX: agent.start[0],
    targetY: agent.start[1],
    speed: agent.speed || 1.25,
    direction: "down",
    moving: false,
    frameIndex: 0,
    frameTick: 0,
    bubbleText: "",
    bubbleUntil: 0,
    el: null,
    note: agent.idle_note,
    status: "idle",
    status_label: "idle"
  }));
  state.agents.forEach((agent) => createAgentDom(dom, agent));
  renderAgents(dom, state.agents);
}

function updateAgents(dom, state, deltaSeconds, nowMs) {
  for (const agent of state.agents) {
    const dx = agent.targetX - agent.x;
    const dy = agent.targetY - agent.y;
    const distance = Math.hypot(dx, dy);
    if (distance > 0.01) {
      const step = Math.min(distance, agent.speed * deltaSeconds);
      agent.x += (dx / distance) * step;
      agent.y += (dy / distance) * step;
      agent.moving = true;
      if (Math.abs(dx) > Math.abs(dy)) {
        agent.direction = dx > 0 ? "right" : "left";
      } else {
        agent.direction = dy > 0 ? "down" : "up";
      }
      agent.frameTick += deltaSeconds * 7;
      if (agent.frameTick >= 1) {
        agent.frameTick = 0;
        agent.frameIndex = (agent.frameIndex + 1) % 3;
      }
    } else {
      agent.x = agent.targetX;
      agent.y = agent.targetY;
      agent.moving = false;
      agent.frameIndex = 1;
    }
    if (agent.bubbleUntil && nowMs >= agent.bubbleUntil) {
      agent.bubbleText = "";
      agent.bubbleUntil = 0;
    }
    syncAgentDom(dom, agent);
  }
  if (state.handoff) {
    if (nowMs >= state.handoff.until) {
      clearHandoff(dom, state);
    } else {
      syncHandoff(dom, state);
    }
  }
}

function queueNextEvent(runtime, nowMs, rng = Math.random) {
  runtime.nextEventAtMs = nowMs + 1600 + randomInt(0, 2600, rng);
}

function queueAmbient(runtime, nowMs, rng = Math.random) {
  runtime.nextAmbientAtMs = nowMs + 4000 + randomInt(0, 5000, rng);
}

function maybeEmitAmbientBubble(dom, state, nowMs) {
  if (!state.runtime || nowMs < Number(state.runtime.nextAmbientAtMs || 0)) return;
  const idleAgents = state.agents.filter((agent) => !agent.bubbleText && !agent.moving);
  if (!idleAgents.length) {
    queueAmbient(state.runtime, nowMs, state.rng);
    return;
  }
  const agent = randomItem(idleAgents, state.rng);
  const ambient = Array.isArray(agent.ambient) && agent.ambient.length ? agent.ambient : ["Syncing signal..."];
  const next = randomItem(ambient, state.rng) || "Syncing signal...";
  setAgentBubble(dom, state, agent.id, next, 2.6, agent.emoji);
  queueAmbient(state.runtime, nowMs, state.rng);
}

function drawFallbackWorld(dom) {
  const ctx = dom.ctx;
  dom.canvas.width = 640;
  dom.canvas.height = 640;
  ctx.clearRect(0, 0, dom.canvas.width, dom.canvas.height);
  ctx.fillStyle = "#29304a";
  ctx.fillRect(0, 0, dom.canvas.width, dom.canvas.height);
  for (let y = 0; y < 40; y += 1) {
    for (let x = 0; x < 40; x += 1) {
      ctx.fillStyle = (x + y) % 2 === 0 ? "#33405a" : "#2f3850";
      ctx.fillRect(x * 16, y * 16, 16, 16);
    }
  }
  ctx.fillStyle = "#4f6b4f";
  ctx.fillRect(48, 60, 176, 160);
  ctx.fillStyle = "#594d7a";
  ctx.fillRect(344, 52, 196, 162);
  ctx.fillStyle = "#645162";
  ctx.fillRect(72, 320, 176, 176);
  ctx.fillStyle = "#6a5d3e";
  ctx.fillRect(356, 338, 186, 160);
}

async function loadImage(src) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = reject;
    image.src = src;
  });
}

async function renderTileMap(dom) {
  try {
    const [tileMapRes, tilesetImage] = await Promise.all([
      fetch(TILEMAP_PATH),
      loadImage(TILESET_PATH)
    ]);
    if (!tileMapRes.ok) throw new Error("tilemap unavailable");
    const tileMap = await tileMapRes.json();
    const ctx = dom.ctx;
    const tileWidth = Number(tileMap.tilewidth || 16);
    const tileHeight = Number(tileMap.tileheight || 16);
    const columns = Math.max(1, Math.floor((tilesetImage.width || tileWidth) / tileWidth));
    dom.canvas.width = tileMap.width * tileWidth;
    dom.canvas.height = tileMap.height * tileHeight;
    ctx.clearRect(0, 0, dom.canvas.width, dom.canvas.height);
    for (const layer of tileMap.layers || []) {
      if (layer.type !== "tilelayer" || !Array.isArray(layer.data) || layer.visible === false) continue;
      for (let index = 0; index < layer.data.length; index += 1) {
        const gid = Number(layer.data[index] || 0);
        if (!gid) continue;
        const tileId = gid - 1;
        const sx = (tileId % columns) * tileWidth;
        const sy = Math.floor(tileId / columns) * tileHeight;
        const dx = (index % tileMap.width) * tileWidth;
        const dy = Math.floor(index / tileMap.width) * tileHeight;
        ctx.drawImage(tilesetImage, sx, sy, tileWidth, tileHeight, dx, dy, tileWidth, tileHeight);
      }
    }
  } catch (error) {
    drawFallbackWorld(dom);
    setError(dom, "AI Town map assets failed to load. Falling back to a lightweight office grid.");
  }
}

function activateJob(dom, state, jobId) {
  const nextJob = state.jobCatalog.find((item) => String(item.id) === String(jobId)) || state.jobCatalog[0] || null;
  if (!nextJob) return;
  state.selectedJobId = String(nextJob.id);
  state.runtime = buildRuntimeState(nextJob);
  state.runtime.nextEventAtMs = performance.now() + 1200;
  state.runtime.nextAmbientAtMs = performance.now() + 3000;
  resetAgents(dom, state);
  updateMetrics(dom, state.runtime.metrics);
  renderBrief(dom, state.runtime);
  renderShortlist(dom, state.runtime);
  renderActivity(dom, state.runtime);
  renderJobs(dom, state);
}

function mergeJobCatalog(current, fresh) {
  const map = new Map((fresh || []).map((item) => [String(item.id), item]));
  return (current || []).map((item) => map.get(String(item.id)) || item).concat(
    (fresh || []).filter((item) => !current.some((existing) => String(existing.id) === String(item.id)))
  );
}

async function refreshJobCatalog(dom, state, { preserveSelection = true } = {}) {
  const payload = await loadJobCatalog();
  if (!payload.items.length) {
    throw new Error(`agents_office_jobs_unavailable:${PUBLIC_JOBS_API_PATH}:${FALLBACK_JOBS_PATH}`);
  }
  state.jobCatalog = preserveSelection && state.jobCatalog.length
    ? mergeJobCatalog(state.jobCatalog, payload.items)
    : payload.items;
  if (!preserveSelection || !state.selectedJobId || !state.jobCatalog.some((item) => String(item.id) === String(state.selectedJobId))) {
    activateJob(dom, state, state.jobCatalog[0].id);
    return;
  }
  const freshSelected = state.jobCatalog.find((item) => String(item.id) === String(state.selectedJobId));
  if (freshSelected && state.runtime) {
    state.runtime.job = freshSelected;
    state.runtime.baseline = {
      marketLeads: freshSelected.market_leads,
      liveThreads: freshSelected.live_threads,
      buyerFinalists: freshSelected.buyer_finalists
    };
    state.runtime.metrics.marketLeads = clamp(state.runtime.metrics.marketLeads, 0, metricCeiling(state.runtime, "marketLeads"));
    state.runtime.metrics.liveThreads = clamp(state.runtime.metrics.liveThreads, 0, metricCeiling(state.runtime, "liveThreads"));
    state.runtime.metrics.buyerFinalists = clamp(state.runtime.shortlist.length, 0, shortlistMax(state.runtime));
    renderBrief(dom, state.runtime);
    renderShortlist(dom, state.runtime);
    renderJobs(dom, state);
  }
}

function getElapsedSeconds(state, nowMs) {
  return Math.max(0, (Number(nowMs || performance.now()) - state.sessionStartMs) / 1000);
}

function refreshTicker(dom, state) {
  if (!state.runtime) return;
  const elapsed = getElapsedSeconds(state);
  dom.runClock.textContent = `Live ${new Date(elapsed * 1000).toISOString().slice(14, 19)}`;
  const fill = clamp(
    28
      + state.runtime.metrics.buyerFinalists * 18
      + state.runtime.metrics.liveThreads * 9
      + Math.min(24, Math.round(state.runtime.metrics.marketLeads / 3)),
    26,
    94
  );
  dom.progressFill.style.width = `${fill}%`;
  dom.runPhase.textContent = state.runtime.phase;
}

function tickFrame(dom, state, nowMs) {
  const previousFrameMs = state.previousFrameMs || nowMs;
  const deltaSeconds = Math.min(0.05, (nowMs - previousFrameMs) / 1000);
  state.previousFrameMs = nowMs;
  updateAgents(dom, state, deltaSeconds, nowMs);
  if (state.runtime) {
    if (nowMs >= Number(state.runtime.nextEventAtMs || 0)) {
      const type = selectNextEventType(state.runtime, nowMs, state.rng);
      const event = materializeEvent(state.runtime, type, nowMs, state.rng);
      applyEvent(dom, state, state.runtime, event);
      renderBrief(dom, state.runtime);
      renderShortlist(dom, state.runtime);
      queueNextEvent(state.runtime, nowMs, state.rng);
    }
    maybeEmitAmbientBubble(dom, state, nowMs);
  }
  state.frameHandle = requestAnimationFrame((frameMs) => tickFrame(dom, state, frameMs));
}

export async function bootstrap() {
  const dom = createDomContext(document);
  dom.ctx = dom.canvas.getContext("2d");
  const state = {
    rng: Math.random,
    sessionStartMs: performance.now(),
    previousFrameMs: 0,
    frameHandle: 0,
    clockHandle: 0,
    refreshHandle: 0,
    spriteSheet: null,
    selectedJobId: "",
    jobCatalog: [],
    runtime: null,
    agents: [],
    handoff: null
  };

  try {
    setError(dom, "");
    state.spriteSheet = await loadImage(SPRITE_SHEET_PATH);
    await renderTileMap(dom);
    await refreshJobCatalog(dom, state, { preserveSelection: false });
    state.clockHandle = setInterval(() => refreshTicker(dom, state), 250);
    state.refreshHandle = setInterval(() => {
      refreshJobCatalog(dom, state).catch(() => {
        // Keep the current scene alive if catalog refresh fails.
      });
    }, 90000);
    dom.jobList.addEventListener("click", (event) => {
      const target = event.target instanceof Element ? event.target.closest("[data-job-id]") : null;
      if (!target) return;
      const jobId = target.getAttribute("data-job-id");
      if (!jobId || jobId === state.selectedJobId) return;
      activateJob(dom, state, jobId);
    });
    refreshTicker(dom, state);
    state.frameHandle = requestAnimationFrame((nowMs) => tickFrame(dom, state, nowMs));
  } catch (error) {
    setError(dom, "Agents office failed to load. Please refresh the page.");
  }
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  if (document.readyState === "loading") {
    window.addEventListener("DOMContentLoaded", () => {
      bootstrap().catch(() => {});
    }, { once: true });
  } else {
    bootstrap().catch(() => {});
  }
}
