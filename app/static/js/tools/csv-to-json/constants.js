export const STORAGE_KEY = "hotdock:tools:csv-to-json:presets:v1";
export const BLANK_ROW_POLICY = "skip"; // Ignore parsed rows whose cells are all empty.
export const RULE_MATCH_POLICY = "last-match-wins"; // Later matching rules overwrite earlier values for the same key.

export const DEFAULT_PREVIEW_ROWS = 5;
export const PROGRESS_THRESHOLD = 10000;
export const PROGRESS_CHUNK_SIZE = 250;

export const VALUE_TYPES = [
  { value: "string", label: "string" },
  { value: "integer", label: "integer" },
  { value: "float", label: "float" },
  { value: "null", label: "null" },
  { value: "boolean", label: "boolean" },
];

export const SOURCE_TYPES = [
  { value: "csv", label: "CSV列" },
  { value: "fixed", label: "固定値" },
  { value: "custom", label: "カスタム" },
];

export const CONDITION_TYPES = [
  { value: "contains", label: "含む" },
  { value: "equals", label: "完全一致" },
  { value: "startsWith", label: "で始まる" },
  { value: "endsWith", label: "で終わる" },
];
