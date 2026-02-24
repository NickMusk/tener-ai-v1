export interface LocalDatasetRecord {
  fullName: string;
  dob?: string;
  details: Record<string, unknown>;
}

export const leieDataset: LocalDatasetRecord[] = [
  {
    fullName: "James T. Powell",
    dob: "1982-11-30",
    details: {
      exclusionType: "OIG Exclusion",
      activationDate: "2020-06-01",
      state: "NY"
    }
  }
];

export const ofacDataset: LocalDatasetRecord[] = [
  {
    fullName: "Ivan Petrov",
    details: {
      sanctionsProgram: "SDN",
      jurisdiction: "US"
    }
  }
];

export const fdaDebarmentDataset: LocalDatasetRecord[] = [
  {
    fullName: "John Doe",
    details: {
      debarmentType: "Drug Product Applications",
      debarmentDate: "2021-04-14"
    }
  }
];
