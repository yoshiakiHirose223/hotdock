import {
  CONDITION_TYPES,
  PROGRESS_CHUNK_SIZE,
  PROGRESS_THRESHOLD,
  RULE_MATCH_POLICY,
  VALUE_TYPES,
} from "./constants.js";

export const RULE_CONFLICT_POLICY = RULE_MATCH_POLICY;

const VALUE_TYPE_SET = new Set(VALUE_TYPES.map((entry) => entry.value));
const CONDITION_TYPE_SET = new Set(CONDITION_TYPES.map((entry) => entry.value));

export function validateConfiguration(parsed, outputColumns, rules) {
  const summaryErrors = [];
  const columnErrors = {};
  const ruleErrors = {};

  if (!parsed) {
    summaryErrors.push("先に CSV を読み込んでください。");
    return { isValid: false, summaryErrors, columnErrors, ruleErrors };
  }

  if (parsed.widthErrors.length) {
    summaryErrors.push("要素数不一致の行があるため変換できません。問題行を修正してください。");
  }

  if (!outputColumns.length) {
    summaryErrors.push("出力列がありません。少なくとも 1 列は残してください。");
  }

  const keyOwners = new Map();

  outputColumns.forEach((column) => {
    const errors = {};
    const key = column.key.trim();

    if (!key) {
      errors.key = "key 名を入力してください。";
    } else if (keyOwners.has(key)) {
      errors.key = "key 名が重複しています。";
      const ownerId = keyOwners.get(key);
      columnErrors[ownerId] = {
        ...(columnErrors[ownerId] ?? {}),
        key: "key 名が重複しています。",
      };
    } else {
      keyOwners.set(key, column.id);
    }

    if (column.sourceType === "csv") {
      if (!Number.isInteger(column.sourceColumnIndex) || column.sourceColumnIndex < 0 || column.sourceColumnIndex >= parsed.sourceColumns.length) {
        errors.sourceColumnIndex = "CSV 列を選択してください。";
      }
    } else if (column.sourceType === "fixed") {
      if (!VALUE_TYPE_SET.has(column.fixedValueType)) {
        errors.fixedValueType = "型の指定が不正です。";
      } else {
        const valueError = validateTypedValue(column.fixedValueType, column.fixedValue, "固定値");
        if (valueError) {
          errors.fixedValue = valueError;
        }
      }
    } else if (column.sourceType === "custom") {
      const targetRules = rules.filter((rule) => rule.targetKey.trim() === key);
      if (!targetRules.length) {
        errors.customRules = "カスタム列には条件ルールを 1 件以上追加してください。";
      }
    }

    if (Object.keys(errors).length) {
      columnErrors[column.id] = errors;
    }
  });

  const availableKeys = new Set(outputColumns.map((column) => column.key.trim()).filter(Boolean));

  rules.forEach((rule) => {
    const errors = {};

    if (!Number.isInteger(rule.sourceColumnIndex) || rule.sourceColumnIndex < 0 || rule.sourceColumnIndex >= parsed.sourceColumns.length) {
      errors.sourceColumnIndex = "対象カラムを選択してください。";
    }

    if (!CONDITION_TYPE_SET.has(rule.conditionType)) {
      errors.conditionType = "条件種別が不正です。";
    }

    if (!rule.compareValue.trim()) {
      errors.compareValue = "比較値を入力してください。";
    }

    if (!rule.targetKey.trim()) {
      errors.targetKey = "書き込み先 key を選択してください。";
    } else if (!availableKeys.has(rule.targetKey.trim())) {
      errors.targetKey = "存在しない key が指定されています。";
    }

    if (!VALUE_TYPE_SET.has(rule.valueType)) {
      errors.valueType = "書き込み値の型が不正です。";
    } else {
      const valueError = validateTypedValue(rule.valueType, rule.value, "書き込み値");
      if (valueError) {
        errors.value = valueError;
      }
    }

    if (Object.keys(errors).length) {
      ruleErrors[rule.id] = errors;
    }
  });

  const isValid = !summaryErrors.length
    && !Object.keys(columnErrors).length
    && !Object.keys(ruleErrors).length;

  return { isValid, summaryErrors, columnErrors, ruleErrors };
}

export async function convertRowsToJson(parsed, outputColumns, rules, onProgress) {
  const rows = [];
  const total = parsed.dataRows.length;

  for (let index = 0; index < total; index += 1) {
    const csvRow = parsed.dataRows[index];
    const output = {};

    outputColumns.forEach((column) => {
      const key = column.key.trim();
      if (!key) {
        return;
      }

      output[key] = resolveColumnValue(column, csvRow);
    });

    for (const rule of rules) {
      const sourceValue = csvRow[rule.sourceColumnIndex] ?? "";

      if (matchesCondition(sourceValue, rule.conditionType, rule.compareValue)) {
        output[rule.targetKey.trim()] = parseTypedValue(rule.valueType, rule.value);
      }
    }

    rows.push(output);

    if (total >= PROGRESS_THRESHOLD) {
      if ((index + 1) % PROGRESS_CHUNK_SIZE === 0 || index === total - 1) {
        onProgress?.({
          total,
          completed: index + 1,
          text: `変換中... ${index + 1} / ${total}`,
        });
        await yieldToBrowser();
      }
    } else if ((index + 1) % 1000 === 0) {
      onProgress?.({
        total,
        completed: index + 1,
        text: "変換中...",
      });
      await yieldToBrowser();
    }
  }

  return rows;
}

function resolveColumnValue(column, csvRow) {
  if (column.sourceType === "csv") {
    return csvRow[column.sourceColumnIndex] ?? "";
  }

  if (column.sourceType === "fixed") {
    return parseTypedValue(column.fixedValueType, column.fixedValue);
  }

  return null;
}

function matchesCondition(sourceValue, conditionType, compareValue) {
  const left = String(sourceValue ?? "");
  const right = compareValue ?? "";

  switch (conditionType) {
    case "contains":
      return left.includes(right);
    case "equals":
      return left === right;
    case "startsWith":
      return left.startsWith(right);
    case "endsWith":
      return left.endsWith(right);
    default:
      return false;
  }
}

function yieldToBrowser() {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

function validateTypedValue(type, rawValue, label) {
  try {
    parseTypedValue(type, rawValue);
    return null;
  } catch (error) {
    return `${label}: ${error.message}`;
  }
}

export function parseTypedValue(type, rawValue) {
  const value = rawValue ?? "";

  switch (type) {
    case "string":
      return value;
    case "integer":
      if (!/^-?\d+$/.test(value.trim())) {
        throw new Error("integer は整数だけを入力してください。");
      }
      return Number.parseInt(value, 10);
    case "float":
      if (!/^-?(?:\d+|\d*\.\d+)$/.test(value.trim())) {
        throw new Error("float は小数または整数を入力してください。");
      }
      return Number.parseFloat(value);
    case "null":
      if (value.trim() !== "") {
        throw new Error("null 型では値を空にしてください。");
      }
      return null;
    case "boolean": {
      const normalized = value.trim().toLowerCase();
      if (normalized === "true") {
        return true;
      }
      if (normalized === "false") {
        return false;
      }
      throw new Error("boolean は true または false を入力してください。");
    }
    default:
      throw new Error("未対応の型です。");
  }
}
