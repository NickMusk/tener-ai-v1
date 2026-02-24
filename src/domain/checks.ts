export const tier1CheckTypes = [
  "OIG_LEIE",
  "SAM_EXCLUSIONS",
  "OFAC_SDN",
  "FDA_DEBARMENT"
] as const;

export type CheckType = (typeof tier1CheckTypes)[number];

export const checkSources: Record<CheckType, string> = {
  OIG_LEIE: "OIG LEIE (local import)",
  SAM_EXCLUSIONS: "SAM.gov API",
  OFAC_SDN: "OFAC SDN (local import/OpenSanctions)",
  FDA_DEBARMENT: "FDA Debarment (local import)"
};
