import { Candidate, VerificationCheckResult } from "../domain/model";

export type FullCheckLifecycle =
  | "READY_NOW"
  | "RUNNING"
  | "COMPLETED"
  | "WAITING_INTEGRATION"
  | "WAITING_PARTNERSHIP"
  | "WAITING_MANUAL_RESPONSE"
  | "BLOCKED";

export interface FullCheckStatus {
  key: string;
  title: string;
  tier: string;
  source: string;
  lifecycle: FullCheckLifecycle;
  result: "PASS" | "FLAG" | "PENDING" | "BLOCKED";
  eta: string;
  canRunNow: boolean;
  details: string;
}

export interface FullComplianceView {
  summary: {
    total: number;
    pass: number;
    flagged: number;
    pending: number;
    blocked: number;
    canRunNow: number;
  };
  checks: FullCheckStatus[];
}

interface CheckTemplate {
  key: string;
  title: string;
  tier: string;
  source: string;
  eta: string;
  lifecycle: FullCheckLifecycle;
  canRunNow: boolean;
  details: string;
}

const templates: CheckTemplate[] = [
  {
    key: "OIG_LEIE",
    title: "OIG LEIE Exclusions",
    tier: "Tier 1",
    source: "OIG (local import)",
    lifecycle: "READY_NOW",
    eta: "Запускается сразу",
    canRunNow: true,
    details: "Проверка по федеральному exclusion list."
  },
  {
    key: "SAM_EXCLUSIONS",
    title: "SAM.gov Exclusions",
    tier: "Tier 1",
    source: "SAM.gov API",
    lifecycle: "READY_NOW",
    eta: "Запускается сразу",
    canRunNow: true,
    details: "Требуется рабочий SAM API key."
  },
  {
    key: "OFAC_SDN",
    title: "OFAC SDN Sanctions",
    tier: "Tier 1",
    source: "OFAC (local import/OpenSanctions)",
    lifecycle: "READY_NOW",
    eta: "Запускается сразу",
    canRunNow: true,
    details: "Санкционные списки и совпадения по именам."
  },
  {
    key: "FDA_DEBARMENT",
    title: "FDA Debarment",
    tier: "Tier 1",
    source: "FDA Debarment (local import)",
    lifecycle: "READY_NOW",
    eta: "Запускается сразу",
    canRunNow: true,
    details: "Проверка на debarment записи FDA."
  },
  {
    key: "FDA_CLINICAL_INVESTIGATORS",
    title: "FDA Clinical Investigators",
    tier: "Tier 1",
    source: "FDA investigators list",
    lifecycle: "WAITING_INTEGRATION",
    eta: "1 неделя",
    canRunNow: false,
    details: "Интеграция импортера в roadmap v1.0."
  },
  {
    key: "STATE_LICENSE_TOP5",
    title: "State License (Top-5 states)",
    tier: "Tier 2",
    source: "State boards (scraping)",
    lifecycle: "WAITING_INTEGRATION",
    eta: "1-2 недели",
    canRunNow: false,
    details: "CA/TX/NY/MA/NJ скрапинг адаптеры."
  },
  {
    key: "DEA_REGISTRATION",
    title: "DEA Registration",
    tier: "Tier 2",
    source: "Verisys/NTIS",
    lifecycle: "WAITING_PARTNERSHIP",
    eta: "1-3 месяца",
    canRunNow: false,
    details: "Сейчас фиксируем self-reported DEA."
  },
  {
    key: "FACIS",
    title: "FACIS Fraud & Abuse",
    tier: "Tier 2",
    source: "Verisys FACIS",
    lifecycle: "WAITING_PARTNERSHIP",
    eta: "1-3 месяца",
    canRunNow: false,
    details: "Требуется enterprise договор с Verisys."
  },
  {
    key: "ABUSE_REGISTRY",
    title: "Abuse Registry",
    tier: "Tier 2",
    source: "State registries / Verisys",
    lifecycle: "WAITING_PARTNERSHIP",
    eta: "1-3 месяца",
    canRunNow: false,
    details: "Единый доступ через агрегатор."
  },
  {
    key: "RAC_CCEP_CRA",
    title: "RAC/CCEP/CRA Certifications",
    tier: "Tier 2",
    source: "Issuing bodies + email workflow",
    lifecycle: "WAITING_INTEGRATION",
    eta: "1 неделя",
    canRunNow: false,
    details: "Полуавтоматический процесс через запросы."
  },
  {
    key: "NPDB",
    title: "NPDB Query",
    tier: "Tier 3",
    source: "Verifiable / NPDB direct",
    lifecycle: "WAITING_PARTNERSHIP",
    eta: "2-6 месяцев",
    canRunNow: false,
    details: "Партнерский доступ и approval."
  },
  {
    key: "EDUCATION_PSV",
    title: "Education PSV",
    tier: "Tier 3",
    source: "University registrar",
    lifecycle: "WAITING_MANUAL_RESPONSE",
    eta: "1-6 недель",
    canRunNow: false,
    details: "Будет через email automation и follow-up."
  },
  {
    key: "GXP_TRAINING",
    title: "GxP Training Validation",
    tier: "Tier 3",
    source: "Training providers",
    lifecycle: "WAITING_MANUAL_RESPONSE",
    eta: "1-3 недели",
    canRunNow: false,
    details: "Нужны ответы от issuing организаций."
  },
  {
    key: "EMPLOYMENT_HISTORY",
    title: "Employment History",
    tier: "Tier 3",
    source: "Previous employers",
    lifecycle: "WAITING_MANUAL_RESPONSE",
    eta: "1-4 недели",
    canRunNow: false,
    details: "HR verification workflow."
  },
  {
    key: "MALPRACTICE",
    title: "Malpractice / Liability",
    tier: "Tier 3",
    source: "Insurance carriers",
    lifecycle: "WAITING_MANUAL_RESPONSE",
    eta: "2-4 недели",
    canRunNow: false,
    details: "Проверка policy/claims по запросам."
  }
];

const asMap = (checks: VerificationCheckResult[]): Map<string, VerificationCheckResult> =>
  new Map(checks.map((check) => [check.checkType, check]));

const runtimeStatusForTier1 = (template: CheckTemplate, runtime?: VerificationCheckResult): FullCheckStatus => {
  if (!runtime) {
    return {
      ...template,
      result: "PENDING"
    };
  }

  if (runtime.status === "CLEAR") {
    return {
      ...template,
      lifecycle: "COMPLETED",
      result: "PASS",
      eta: "Завершено",
      details: runtime.summary
    };
  }

  if (runtime.status === "FLAGGED") {
    return {
      ...template,
      lifecycle: "COMPLETED",
      result: "FLAG",
      eta: "Завершено, нужен review",
      details: runtime.summary
    };
  }

  if (runtime.status === "ERROR") {
    return {
      ...template,
      lifecycle: "BLOCKED",
      result: "BLOCKED",
      eta: "После исправления источника",
      details: runtime.error ? `${runtime.summary} (${runtime.error})` : runtime.summary
    };
  }

  const waitingForSamKey = runtime.summary.toLowerCase().includes("api key is not configured");
  if (waitingForSamKey) {
    return {
      ...template,
      lifecycle: "WAITING_INTEGRATION",
      result: "PENDING",
      canRunNow: false,
      eta: "1 день (после настройки SAM key)",
      details: runtime.summary
    };
  }

  return {
    ...template,
    lifecycle: "RUNNING",
    result: "PENDING",
    details: runtime.summary
  };
};

export const buildFullComplianceView = (candidate: Candidate): FullComplianceView => {
  const runtimeMap = asMap(candidate.compliance.checks);

  const checks = templates.map((template) => {
    if (template.key === "OIG_LEIE" || template.key === "SAM_EXCLUSIONS" || template.key === "OFAC_SDN" || template.key === "FDA_DEBARMENT") {
      return runtimeStatusForTier1(template, runtimeMap.get(template.key));
    }

    return {
      ...template,
      result: "PENDING" as const
    };
  });

  const summary = {
    total: checks.length,
    pass: checks.filter((check) => check.result === "PASS").length,
    flagged: checks.filter((check) => check.result === "FLAG").length,
    pending: checks.filter((check) => check.result === "PENDING").length,
    blocked: checks.filter((check) => check.result === "BLOCKED").length,
    canRunNow: checks.filter((check) => check.canRunNow).length
  };

  return { summary, checks };
};
