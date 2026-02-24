const jdListEl = document.getElementById("jd-list");
const jdFormEl = document.getElementById("jd-form");
const linkedInStatusEl = document.getElementById("linkedin-status");
const DEFAULT_TEST_JD_ID = "jd-default-ls-smoke";

const request = async (url, options = {}) => {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options
  });

  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || payload.error || "Request failed");
  }

  return payload;
};

const statusPill = (label, value) =>
  `<span class="pill">${label}: <strong>${value || "NOT_STARTED"}</strong></span>`;

const renderCandidate = (candidate) => {
  const compliance = candidate.compliance || {};
  return `
    <li class="candidate">
      <div><strong>${candidate.profile.fullName}</strong> (${candidate.profile.source || "MANUAL"})</div>
      <div class="muted">${candidate.profile.headline || "-"}</div>
      <div class="muted">Traffic light: ${compliance.trafficLight || "-"}, Progress: ${compliance.progress || "-"}</div>
    </li>
  `;
};

const renderJdCard = async (jd) => {
  const candidatesResponse = await request(`/api/v1/jds/${jd.id}/candidates`);
  const candidates = candidatesResponse.items || [];
  const titleSuffix = jd.id === DEFAULT_TEST_JD_ID ? " (default)" : "";

  return `
    <article class="jd-card" data-id="${jd.id}">
      <div class="row">
        <h3>${jd.title}${titleSuffix}</h3>
        <span class="muted">${jd.company}${jd.location ? ` • ${jd.location}` : ""}</span>
      </div>
      <div class="row">
        ${statusPill("LinkedIn Search", jd.steps.linkedinSearch.status)}
        ${statusPill("Import", jd.steps.importCandidates.status)}
        ${statusPill("Verification", jd.steps.runVerification.status)}
      </div>
      <div class="row">
        <button data-action="linkedin-search" data-id="${jd.id}">1) Run LinkedIn Search</button>
        <button class="secondary" data-action="import" data-id="${jd.id}">2) Import Candidates</button>
        <button class="warn" data-action="verify" data-id="${jd.id}">3) Run Verification</button>
      </div>
      <div class="muted">Found in LinkedIn: ${jd.linkedinCandidates.length} | Imported: ${jd.importedCandidateIds.length}</div>
      <ul class="list">
        ${candidates.map(renderCandidate).join("")}
      </ul>
    </article>
  `;
};

const loadLinkedInStatus = async () => {
  const status = await request("/api/v1/linkedin/status");
  linkedInStatusEl.textContent = status.connected
    ? "LinkedIn: Connected by default"
    : "LinkedIn: Not connected";
};

const loadJds = async () => {
  const response = await request("/api/v1/jds");
  const jds = response.items || [];

  if (jds.length === 0) {
    jdListEl.innerHTML = `<p class="muted">No JD yet. Add your first JD above.</p>`;
    return;
  }

  const cards = await Promise.all(jds.map((jd) => renderJdCard(jd)));
  jdListEl.innerHTML = cards.join("");
};

jdFormEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(jdFormEl);

  try {
    await request("/api/v1/jds", {
      method: "POST",
      body: JSON.stringify({
        title: String(formData.get("title") || ""),
        company: String(formData.get("company") || ""),
        location: String(formData.get("location") || ""),
        keywords: String(formData.get("keywords") || "")
      })
    });

    jdFormEl.reset();
    await loadJds();
  } catch (error) {
    window.alert(error.message);
  }
});

jdListEl.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement)) {
    return;
  }

  const jdId = target.dataset.id;
  const action = target.dataset.action;
  if (!jdId || !action) {
    return;
  }

  try {
    if (action === "linkedin-search") {
      await request(`/api/v1/jds/${jdId}/steps/linkedin-search`, { method: "POST" });
    }

    if (action === "import") {
      await request(`/api/v1/jds/${jdId}/steps/import-candidates`, { method: "POST" });
    }

    if (action === "verify") {
      await request(`/api/v1/jds/${jdId}/steps/run-verification`, { method: "POST" });
    }

    await loadJds();
  } catch (error) {
    window.alert(error.message);
  }
});

document.body.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement)) {
    return;
  }

  const action = target.dataset.quickAction;
  if (!action) {
    return;
  }

  try {
    if (action === "linkedin-search") {
      await request(`/api/v1/jds/${DEFAULT_TEST_JD_ID}/steps/linkedin-search`, { method: "POST" });
    }

    if (action === "import") {
      await request(`/api/v1/jds/${DEFAULT_TEST_JD_ID}/steps/import-candidates`, { method: "POST" });
    }

    if (action === "verify") {
      await request(`/api/v1/jds/${DEFAULT_TEST_JD_ID}/steps/run-verification`, { method: "POST" });
    }

    await loadJds();
  } catch (error) {
    window.alert(error.message);
  }
});

const boot = async () => {
  try {
    await loadLinkedInStatus();
    await loadJds();
  } catch (error) {
    window.alert(error.message);
  }
};

boot();
