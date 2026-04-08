import { createJsonDownloadUrl, revokeDownloadUrl } from "./exporter.js";
import { readCsvFile, parseCsvDocument } from "./parser.js";
import {
  createConditionRule,
  createInitialState,
  createManualOutputColumn,
  createOutputColumnFromSource,
  createStore,
  hydrateOutputColumns,
  hydrateRules,
  serializePreset,
} from "./state.js";
import { deletePreset, loadPresetCollection, savePresetCollection, upsertPreset } from "./storage.js";
import { convertRowsToJson, validateConfiguration } from "./transform.js";
import { render } from "./view.js";

const root = document.querySelector("[data-csv-json-app]");

if (!root) {
  throw new Error("CSV to JSON app root was not found.");
}

const refs = {
  root,
  inputCard: root.querySelector("[data-role='input-card']"),
  fileInput: root.querySelector("[data-role='csv-file']"),
  textInput: root.querySelector("[data-role='csv-text']"),
  hasHeader: root.querySelector("[data-role='has-header']"),
  workspace: root.querySelector("[data-role='workspace']"),
  loadFeedback: root.querySelector("[data-role='load-feedback']"),
  parseSummary: root.querySelector("[data-role='parse-summary']"),
  previewPanel: root.querySelector("[data-role='preview-panel']"),
  outputColumns: root.querySelector("[data-role='output-columns']"),
  presetName: root.querySelector("[data-role='preset-name']"),
  presetSelect: root.querySelector("[data-role='preset-select']"),
  presetFeedback: root.querySelector("[data-role='preset-feedback']"),
  conversionFeedback: root.querySelector("[data-role='conversion-feedback']"),
  progressPanel: root.querySelector("[data-role='progress-panel']"),
  downloadPanel: root.querySelector("[data-role='download-panel']"),
};

const store = createStore(createInitialState(loadPresetCollection()));

store.subscribe((state) => {
  const focusSnapshot = captureFocusSnapshot(root);
  render(state, refs, validateConfiguration(state.parsed, state.outputColumns, state.rules));
  restoreFocusSnapshot(root, focusSnapshot);
});

render(store.getState(), refs, validateConfiguration(null, [], []));

root.addEventListener("click", handleClick);
root.addEventListener("input", handleInput);
root.addEventListener("change", handleInput);
window.addEventListener("beforeunload", () => {
  revokeDownloadUrl();
});

function clearPresetNoticeState(state, extra = {}) {
  return {
    ...state,
    ...extra,
    presetMessage: null,
    presetMessageType: null,
  };
}

function withPresetNotice(state, message, type = "info", extra = {}) {
  return {
    ...state,
    ...extra,
    presetMessage: message,
    presetMessageType: type,
  };
}

function captureFocusSnapshot(container) {
  const activeElement = document.activeElement;
  if (!(activeElement instanceof HTMLElement) || !container.contains(activeElement)) {
    return null;
  }

  const snapshot = {
    id: activeElement.id || "",
    role: activeElement.dataset.role || "",
    action: activeElement.dataset.action || "",
    columnId: activeElement.dataset.columnId || "",
    columnField: activeElement.dataset.columnField || "",
    ruleId: activeElement.dataset.ruleId || "",
    ruleField: activeElement.dataset.ruleField || "",
    scrollX: window.scrollX,
    scrollY: window.scrollY,
  };

  if (activeElement instanceof HTMLInputElement || activeElement instanceof HTMLTextAreaElement) {
    snapshot.selectionStart = activeElement.selectionStart;
    snapshot.selectionEnd = activeElement.selectionEnd;
  }

  return snapshot;
}

function restoreFocusSnapshot(container, snapshot) {
  if (!snapshot) {
    return;
  }

  const target = findMatchingElement(container, snapshot);
  if (!(target instanceof HTMLElement)) {
    return;
  }

  if (document.activeElement !== target) {
    target.focus({ preventScroll: true });
  }

  if (
    (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement)
    && typeof snapshot.selectionStart === "number"
    && typeof snapshot.selectionEnd === "number"
  ) {
    target.setSelectionRange(snapshot.selectionStart, snapshot.selectionEnd);
  }

  window.scrollTo(snapshot.scrollX, snapshot.scrollY);
}

function findMatchingElement(container, snapshot) {
  if (snapshot.columnId && snapshot.columnField) {
    return container.querySelector(
      `[data-column-id="${CSS.escape(snapshot.columnId)}"][data-column-field="${CSS.escape(snapshot.columnField)}"]`,
    );
  }

  if (snapshot.ruleId && snapshot.ruleField) {
    return container.querySelector(
      `[data-rule-id="${CSS.escape(snapshot.ruleId)}"][data-rule-field="${CSS.escape(snapshot.ruleField)}"]`,
    );
  }

  if (snapshot.role) {
    return container.querySelector(`[data-role="${CSS.escape(snapshot.role)}"]`);
  }

  if (snapshot.action) {
    return container.querySelector(`[data-action="${CSS.escape(snapshot.action)}"]`);
  }

  if (snapshot.id) {
    return container.querySelector(`#${CSS.escape(snapshot.id)}`);
  }

  return null;
}

async function handleClick(event) {
  const target = event.target.closest("[data-action]");
  if (!target) {
    return;
  }

  const action = target.dataset.action;

  switch (action) {
    case "load-csv":
      await loadCsv();
      break;
    case "reset-all":
      resetAll();
      break;
    case "add-column":
      updateColumns((columns, state) => [...columns, createManualOutputColumn(columns.length, state.parsed?.sourceColumns ?? [])]);
      break;
    case "add-rule-for-column":
      store.setState((state) => {
        const column = state.outputColumns.find((entry) => entry.id === target.dataset.columnId);
        if (!column || column.sourceType !== "custom") {
          return state;
        }

        const nextRule = createConditionRule(state.parsed?.sourceColumns ?? [], state.outputColumns);
        nextRule.targetKey = column.key.trim();

        return clearPresetNoticeState(state, {
          rules: [...state.rules, nextRule],
        });
      });
      break;
    case "remove-column":
      store.setState((state) => {
        const column = state.outputColumns.find((entry) => entry.id === target.dataset.columnId);
        if (!column) {
          return state;
        }

        return clearPresetNoticeState(state, {
          outputColumns: state.outputColumns.filter((entry) => entry.id !== target.dataset.columnId),
          rules: state.rules.filter((rule) => rule.targetKey !== column.key),
        });
      });
      break;
    case "move-column-up":
      updateColumns((columns) => moveItem(columns, target.dataset.columnId, -1));
      break;
    case "move-column-down":
      updateColumns((columns) => moveItem(columns, target.dataset.columnId, 1));
      break;
    case "add-rule":
      store.setState((state) => clearPresetNoticeState(state, {
        rules: [...state.rules, createConditionRule(state.parsed?.sourceColumns ?? [], state.outputColumns)],
      }));
      break;
    case "remove-rule":
      store.setState((state) => clearPresetNoticeState(state, {
        rules: state.rules.filter((rule) => rule.id !== target.dataset.ruleId),
      }));
      break;
    case "move-rule-up":
      store.setState((state) => clearPresetNoticeState(state, {
        rules: moveItem(state.rules, target.dataset.ruleId, -1),
      }));
      break;
    case "move-rule-down":
      store.setState((state) => clearPresetNoticeState(state, {
        rules: moveItem(state.rules, target.dataset.ruleId, 1),
      }));
      break;
    case "save-preset":
      savePreset();
      break;
    case "load-preset":
      applyPreset();
      break;
    case "delete-preset":
      removePreset();
      break;
    case "start-convert":
      await startConversion();
      break;
    case "reset-conversion":
      clearConversionResult();
      break;
    default:
      break;
  }
}

function handleInput(event) {
  const target = event.target;

  if (target === refs.hasHeader) {
    store.setState((state) => clearPresetNoticeState(state, { headerEnabled: refs.hasHeader.checked }));
    return;
  }

  if (target === refs.presetSelect) {
    store.setState((state) => clearPresetNoticeState(state, { selectedPresetName: refs.presetSelect.value }));
    return;
  }

  const columnId = target.dataset.columnId;
  const columnField = target.dataset.columnField;
  if (columnId && columnField) {
    store.setState((state) => {
      const previousColumn = state.outputColumns.find((column) => column.id === columnId);
      if (!previousColumn) {
        return state;
      }

      const nextValue = normalizeFieldValue(columnField, target.value);
      const outputColumns = state.outputColumns.map((column) => (
        column.id === columnId
          ? { ...column, [columnField]: nextValue }
          : column
      ));

      let rules = state.rules;

      if (columnField === "key") {
        rules = state.rules.map((rule) => (
          rule.targetKey === previousColumn.key ? { ...rule, targetKey: String(nextValue) } : rule
        ));
      }

      if (columnField === "sourceType" && nextValue !== "custom") {
        rules = state.rules.filter((rule) => rule.targetKey !== previousColumn.key);
      }

      return clearPresetNoticeState(state, {
        outputColumns,
        rules,
      });
    });
    return;
  }

  const ruleId = target.dataset.ruleId;
  const ruleField = target.dataset.ruleField;
  if (ruleId && ruleField) {
    store.setState((state) => clearPresetNoticeState(state, {
      rules: state.rules.map((rule) => (
        rule.id === ruleId
          ? { ...rule, [ruleField]: normalizeFieldValue(ruleField, target.value) }
          : rule
      )),
    }));
  }
}

async function loadCsv() {
  clearConversionResult(false);

  const csvText = refs.textInput.value;
  const selectedFile = refs.fileInput.files?.[0] ?? null;

  if (!csvText.trim() && !selectedFile) {
    store.setState((state) => clearPresetNoticeState(state, {
      parsed: null,
      outputColumns: [],
      rules: [],
      loadMessages: ["CSV テキストを入力するか、CSV ファイルを選択してください。"],
      conversion: idleConversionState(),
    }));
    return;
  }

  try {
    const inputMode = csvText.trim() ? "text" : "file";
    const rawCsvText = csvText.trim() ? csvText : await readCsvFile(selectedFile);
    const parsed = parseCsvDocument(rawCsvText, {
      headerEnabled: refs.hasHeader.checked,
      inputMode,
      fileName: selectedFile?.name ?? "",
    });

    if (!parsed.ok) {
      store.setState((state) => clearPresetNoticeState(state, {
        parsed: null,
        outputColumns: [],
        rules: [],
        loadMessages: [parsed.fatalError.message],
        conversion: idleConversionState(),
      }));
      return;
    }

    const outputColumns = parsed.sourceColumns.map(createOutputColumnFromSource);
    const messages = buildLoadMessages(parsed);

    store.setState((state) => clearPresetNoticeState(state, {
      headerEnabled: refs.hasHeader.checked,
      parsed,
      outputColumns,
      rules: [],
      loadMessages: messages,
      conversion: idleConversionState(),
    }));
  } catch (error) {
    store.setState((state) => clearPresetNoticeState(state, {
      parsed: null,
      outputColumns: [],
      rules: [],
      loadMessages: [error instanceof Error ? error.message : "CSV の読み込みに失敗しました。"],
      conversion: idleConversionState(),
    }));
  }
}

function buildLoadMessages(parsed) {
  if (!parsed.widthErrors.length) {
    return [];
  }

  return [
    parsed.inputMode === "file"
      ? "ファイル内に要素数不一致の行があります。プレビューの赤行を確認してください。"
      : `テキスト入力内に要素数不一致の行があります。問題行: ${parsed.widthErrors.map((error) => error.lineNumber).join(", ")}`,
  ];
}

function updateColumns(updater) {
  store.setState((state) => clearPresetNoticeState(state, {
    outputColumns: updater(state.outputColumns, state),
  }));
}

function savePreset() {
  const state = store.getState();
  if (!state.parsed) {
    store.setState((current) => withPresetNotice(current, "設定保存の前に CSV を読み込んでください。", "error"));
    return;
  }

  const presetName = refs.presetName.value.trim();
  if (!presetName) {
    store.setState((current) => withPresetNotice(current, "保存名を入力してください。", "error"));
    return;
  }

  const presets = upsertPreset(state.presets, presetName, serializePreset(state));
  savePresetCollection(presets);

  store.setState((current) => withPresetNotice(current, `設定「${presetName}」を保存しました。`, "info", {
    presets,
    selectedPresetName: presetName,
  }));
}

function applyPreset() {
  const state = store.getState();
  const presetName = refs.presetSelect.value;
  const preset = state.presets.find((entry) => entry.name === presetName);

  if (!preset) {
    store.setState((current) => withPresetNotice(current, "読み込む設定を選択してください。", "error"));
    return;
  }

  if (!state.parsed?.rawCsvText) {
    store.setState((current) => withPresetNotice(current, "設定を適用する前に CSV を読み込んでください。", "error"));
    return;
  }

  const reparsed = parseCsvDocument(state.parsed.rawCsvText, {
    headerEnabled: Boolean(preset.config.headerEnabled),
    inputMode: state.parsed.inputMode,
    fileName: state.parsed.fileName,
  });

  if (!reparsed.ok) {
    store.setState((current) => withPresetNotice(current, reparsed.fatalError.message, "error"));
    return;
  }

  refs.hasHeader.checked = Boolean(preset.config.headerEnabled);
  const outputColumns = hydrateOutputColumns(preset.config.outputColumns, reparsed.sourceColumns);
  const rules = hydrateRules(preset.config.rules, reparsed.sourceColumns, outputColumns);

  store.setState((current) => withPresetNotice(current, `設定「${preset.name}」を読み込みました。`, "info", {
    headerEnabled: Boolean(preset.config.headerEnabled),
    parsed: reparsed,
    outputColumns,
    rules,
    loadMessages: buildLoadMessages(reparsed),
    selectedPresetName: preset.name,
    conversion: idleConversionState(),
  }));
}

function removePreset() {
  const state = store.getState();
  const presetName = refs.presetSelect.value;
  if (!presetName) {
    store.setState((current) => withPresetNotice(current, "削除する設定を選択してください。", "error"));
    return;
  }

  const presets = deletePreset(state.presets, presetName);
  savePresetCollection(presets);

  store.setState((current) => withPresetNotice(current, `設定「${presetName}」を削除しました。`, "info", {
    presets,
    selectedPresetName: presets[0]?.name ?? "",
  }));
}

async function startConversion() {
  clearConversionResult(false);

  const state = store.getState();
  const validation = validateConfiguration(state.parsed, state.outputColumns, state.rules);

  if (!validation.isValid) {
    store.setState((current) => ({
      ...current,
      conversion: {
        ...idleConversionState(),
        status: "error",
        errors: [
          ...validation.summaryErrors,
          ...collectFieldErrorMessages(validation),
        ],
      },
    }));
    return;
  }

  store.setState((current) => ({
    ...current,
    conversion: {
      ...idleConversionState(),
      status: "running",
      progressText: "変換中...",
    },
  }));

  try {
    const rows = await convertRowsToJson(
      state.parsed,
      state.outputColumns,
      state.rules,
      (progress) => {
        store.setState((current) => ({
          ...current,
          conversion: {
            ...current.conversion,
            status: "running",
            progressText: progress.text,
          },
        }));
      },
    );

    const jsonText = JSON.stringify(rows, null, 2);
    const downloadUrl = createJsonDownloadUrl(jsonText);
    const downloadName = `${buildDownloadBaseName(state.parsed.fileName)}.json`;

    store.setState((current) => ({
      ...current,
      conversion: {
        status: "done",
        errors: [],
        progressText: rows.length >= 10000 ? `変換完了 ${rows.length} / ${rows.length}` : "変換完了",
        downloadUrl,
        downloadName,
        rowCount: rows.length,
      },
    }));
  } catch (error) {
    store.setState((current) => ({
      ...current,
      conversion: {
        ...idleConversionState(),
        status: "error",
        errors: [error instanceof Error ? error.message : "JSON 変換に失敗しました。"],
      },
    }));
  }
}

function collectFieldErrorMessages(validation) {
  const messages = [];

  Object.values(validation.columnErrors).forEach((errors) => {
    Object.values(errors).forEach((message) => messages.push(message));
  });

  Object.values(validation.ruleErrors).forEach((errors) => {
    Object.values(errors).forEach((message) => messages.push(message));
  });

  return [...new Set(messages)];
}

function clearConversionResult(shouldRender = true) {
  revokeDownloadUrl();
  if (!shouldRender) {
    return;
  }

  store.setState((state) => ({
    ...state,
    conversion: idleConversionState(),
  }));
}

function resetAll() {
  revokeDownloadUrl();
  refs.textInput.value = "";
  refs.fileInput.value = "";
  refs.presetName.value = "";
  refs.hasHeader.checked = true;

  store.setState(createInitialState(loadPresetCollection()));
}

function idleConversionState() {
  return {
    status: "idle",
    errors: [],
    progressText: "",
    downloadUrl: "",
    downloadName: "",
    rowCount: 0,
  };
}

function moveItem(items, itemId, delta) {
  const index = items.findIndex((item) => item.id === itemId);
  if (index === -1) {
    return items;
  }

  const nextIndex = index + delta;
  if (nextIndex < 0 || nextIndex >= items.length) {
    return items;
  }

  const copy = [...items];
  const [target] = copy.splice(index, 1);
  copy.splice(nextIndex, 0, target);
  return copy;
}

function normalizeFieldValue(fieldName, value) {
  if (fieldName.endsWith("Index")) {
    return Number.parseInt(value, 10);
  }
  return value;
}

function buildDownloadBaseName(fileName) {
  if (!fileName) {
    return "converted";
  }
  return fileName.replace(/\.[^.]+$/, "") || "converted";
}
