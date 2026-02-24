const jdListEl = document.getElementById("jd-list");
const jdFormEl = document.getElementById("jd-form");
const linkedInStatusEl = document.getElementById("linkedin-status");
const headerSubtitleEl = document.getElementById("header-subtitle");
const defaultJdStatusEl = document.getElementById("default-jd-status");
const refreshBtnEl = document.getElementById("refresh-btn");
const toastEl = document.getElementById("toast");

const statTotalJdEl = document.getElementById("stat-total-jd");
const statImportedEl = document.getElementById("stat-imported");
const statVerificationEl = document.getElementById("stat-verification");
const statRedEl = document.getElementById("stat-red");

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

const showToast = (message) => {
  toastEl.textContent = message;
  toastEl.classList.remove("hidden");

  setTimeout(() => {
    toastEl.classList.add("hidden");
  }, 2200);
};

const normalizeStatus = (status) => String(status || "NOT_STARTED").toUpperCase();

const statusClass = (status) => {
  const value = normalizeStatus(status);
  if (value === "COMPLETED") {
    return "status-completed";
  }

  if (value === "RUNNING") {
    return "status-running";
  }

  if (value === "FAILED") {
    return "status-failed";
  }

  return "";
};

const trafficClass = (trafficLight) => {
  const value = String(trafficLight || "").toUpperCase();
  if (value === "RED") {
    return "light-red";
  }

  if (value === "YELLOW") {
    return "light-yellow";
  }

  if (value === "GREEN") {
    return "light-green";
  }

  return "";
};

const renderCandidate = (candidate) => {
  const profile = candidate.profile || {};
  const compliance = candidate.compliance || {};
  const light = compliance.trafficLight || "N/A";

  return `
    <li class="candidate-item">
      <div class="candidate-top">
        <span class="candidate-name">${profile.fullName || "Unknown Candidate"}</span>
        <span class="candidate-light ${trafficClass(light)}">${light}</span>
      </div>
      <div class="candidate-meta">${profile.headline || "No headline"} • ${profile.source || "MANUAL"}</div>
      <div class="candidate-meta">Progress: ${compliance.progress || "0/0"}</div>
    </li>
  `;
};

const renderStepPill = (label, status) =>
  `<span class="status-pill ${statusClass(status)}">${label}: ${normalizeStatus(status)}</span>`;

const renderJdCard = ({ jd, candidates }) => {
  const isDefault = jd.id === DEFAULT_TEST_JD_ID;
  const metaLocation = jd.location ? ` • ${jd.location}` : "";
  const metaKeywords = jd.keywords ? ` • ${jd.keywords}` : "";

  return `
    <article class="search-card">
      <div class="search-header">
        <div class="search-info">
          <h3 class="search-title">${jd.title}${isDefault ? " (default)" : ""}</h3>
          <div class="search-meta">
            <span class="meta-item">${jd.company}${metaLocation}${metaKeywords}</span>
          </div>
          <div class="search-meta">
            ${renderStepPill("LinkedIn", jd.steps?.linkedinSearch?.status)}
            ${renderStepPill("Import", jd.steps?.importCandidates?.status)}
            ${renderStepPill("Verify", jd.steps?.runVerification?.status)}
          </div>
        </div>
      </div>

      <div class="action-row">
        <button class="btn btn-secondary btn-small" data-action="linkedin-search" data-id="${jd.id}">
          1) Run LinkedIn Search
        </button>
        <button class="btn btn-secondary btn-small" data-action="import" data-id="${jd.id}">
          2) Import Candidates
        </button>
        <button class="btn btn-primary btn-small" data-action="verify" data-id="${jd.id}">
          3) Run Verification
        </button>
      </div>

      <div class="search-meta">
        <span class="meta-item">Found: ${jd.linkedinCandidates?.length || 0}</span>
        <span class="meta-item">Imported: ${jd.importedCandidateIds?.length || 0}</span>
        <span class="meta-item">Verification jobs: ${jd.verificationJobIds?.length || 0}</span>
      </div>

      <ul class="candidate-list">
        ${candidates.length ? candidates.map(renderCandidate).join("") : `<li class="candidate-item"><div class="candidate-meta">No imported candidates yet.</div></li>`}
      </ul>
    </article>
  `;
};

const updateStats = (jdWithCandidates) => {
  const totalJd = jdWithCandidates.length;
  const importedCandidates = jdWithCandidates.reduce((sum, item) => sum + (item.jd.importedCandidateIds?.length || 0), 0);
  const verificationJobs = jdWithCandidates.reduce((sum, item) => sum + (item.jd.verificationJobIds?.length || 0), 0);
  const redFlags = jdWithCandidates.reduce(
    (sum, item) => sum + item.candidates.filter((candidate) => candidate.compliance?.trafficLight === "RED").length,
    0
  );

  statTotalJdEl.textContent = String(totalJd);
  statImportedEl.textContent = String(importedCandidates);
  statVerificationEl.textContent = String(verificationJobs);
  statRedEl.textContent = String(redFlags);

  headerSubtitleEl.textContent = `${totalJd} JDs • ${importedCandidates} imported candidates • ${verificationJobs} verification jobs`;
};

const updateLinkedInStatus = (status) => {
  if (status.connected) {
    linkedInStatusEl.textContent = "LinkedIn connected by default";
    linkedInStatusEl.classList.remove("status-failed");
    return;
  }

  linkedInStatusEl.textContent = "LinkedIn not connected";
  linkedInStatusEl.classList.add("status-failed");
};

const updateDefaultStepSummary = (defaultJd) => {
  if (!defaultJd) {
    defaultJdStatusEl.textContent = "Default JD was not found.";
    return;
  }

  const s1 = normalizeStatus(defaultJd.steps?.linkedinSearch?.status);
  const s2 = normalizeStatus(defaultJd.steps?.importCandidates?.status);
  const s3 = normalizeStatus(defaultJd.steps?.runVerification?.status);
  defaultJdStatusEl.textContent = `Steps: LinkedIn ${s1} • Import ${s2} • Verification ${s3}`;
};

const loadDashboard = async () => {
  const [linkedInStatus, jdResponse, defaultJd] = await Promise.all([
    request("/api/v1/linkedin/status"),
    request("/api/v1/jds"),
    request("/api/v1/jds/default")
  ]);

  updateLinkedInStatus(linkedInStatus);
  updateDefaultStepSummary(defaultJd);

  const jds = jdResponse.items || [];
  const jdWithCandidates = await Promise.all(
    jds.map(async (jd) => {
      const candidatesResponse = await request(`/api/v1/jds/${jd.id}/candidates`);
      return { jd, candidates: candidatesResponse.items || [] };
    })
  );

  updateStats(jdWithCandidates);

  if (!jdWithCandidates.length) {
    jdListEl.innerHTML = `<div class="empty-state">No JD yet. Add the first job description to start the pipeline.</div>`;
    return;
  }

  jdListEl.innerHTML = jdWithCandidates.map(renderJdCard).join("");
};

const runJdStep = async (jdId, action) => {
  if (action === "linkedin-search") {
    await request(`/api/v1/jds/${jdId}/steps/linkedin-search`, { method: "POST" });
    return;
  }

  if (action === "import") {
    await request(`/api/v1/jds/${jdId}/steps/import-candidates`, { method: "POST" });
    return;
  }

  if (action === "verify") {
    await request(`/api/v1/jds/${jdId}/steps/run-verification`, { method: "POST" });
  }
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
    showToast("JD added.");
    await loadDashboard();
  } catch (error) {
    showToast(error.message);
  }
});

jdListEl.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement)) {
    return;
  }

  const action = target.dataset.action;
  const jdId = target.dataset.id;
  if (!action || !jdId) {
    return;
  }

  try {
    await runJdStep(jdId, action);
    showToast(`Step ${action} started for JD.`);
    await loadDashboard();
  } catch (error) {
    showToast(error.message);
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
    await runJdStep(DEFAULT_TEST_JD_ID, action);
    showToast(`Quick step ${action} completed.`);
    await loadDashboard();
  } catch (error) {
    showToast(error.message);
  }
});

refreshBtnEl.addEventListener("click", async () => {
  try {
    await loadDashboard();
    showToast("Dashboard refreshed.");
  } catch (error) {
    showToast(error.message);
  }
});

const boot = async () => {
  try {
    await loadDashboard();
  } catch (error) {
    showToast(error.message);
  }
};

boot();
