import {
  CONDITION_TYPES,
  DEFAULT_PREVIEW_ROWS,
  RULE_MATCH_POLICY,
  SOURCE_TYPES,
  VALUE_TYPES,
} from "./constants.js";
import { RULE_CONFLICT_POLICY } from "./transform.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildOptions(options, selectedValue) {
  return options
    .map((option) => {
      const selected = option.value === selectedValue ? " selected" : "";
      return `<option value="${escapeHtml(option.value)}"${selected}>${escapeHtml(option.label)}</option>`;
    })
    .join("");
}

function renderMessages(messages, type = "error") {
  if (!messages?.length) {
    return "";
  }

  return `
    <div class="notice ${escapeHtml(type)}">
      <ul class="csv-json-message-list">
        ${messages.map((message) => `<li>${escapeHtml(message)}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderInlineError(message) {
  if (!message) {
    return "";
  }

  return `<p class="field-error">${escapeHtml(message)}</p>`;
}

function renderPreview(state) {
  const parsed = state.parsed;
  if (!parsed) {
    return "<p>読み込み後にプレビューを表示します。</p>";
  }

  const previewRows = parsed.dataRows.slice(0, DEFAULT_PREVIEW_ROWS);
  const previewMeta = parsed.dataMeta.slice(0, DEFAULT_PREVIEW_ROWS);
  const errorLineSet = new Set(parsed.widthErrors.map((error) => error.lineNumber));
  const sourceHeaders = parsed.sourceColumns.map((column) => column.label);
  const displayColumnCount = Math.max(
    sourceHeaders.length,
    ...previewRows.map((row) => row.length),
    0,
  );

  const issueList = parsed.widthErrors.length
    ? `
      <div class="csv-json-issue-box">
        <p class="csv-json-issue-title">要素数不一致の問題行</p>
        <ul class="csv-json-message-list">
          ${parsed.widthErrors.map((error) => (
            `<li>行 ${error.lineNumber}: ${error.expected} 列想定に対して ${error.actual} 列です。</li>`
          )).join("")}
        </ul>
      </div>
    `
    : "";

  if (!previewRows.length) {
    return `
      ${issueList}
      <p>データ行は 0 件です。変換結果は空の JSON 配列になります。</p>
    `;
  }

  const headerRow = `
    <tr>
      <th>行</th>
      ${Array.from({ length: displayColumnCount }, (_, index) => (
        `<th>${escapeHtml(sourceHeaders[index] ?? `column${index + 1}`)}</th>`
      )).join("")}
    </tr>
  `;

  const bodyRows = previewRows
    .map((row, index) => {
      const meta = previewMeta[index];
      const isError = state.parsed.inputMode === "file" && errorLineSet.has(meta.startLine);
      const className = isError ? " class=\"is-error-row\"" : "";

      return `
        <tr${className}>
          <th>${meta.startLine}</th>
          ${Array.from({ length: displayColumnCount }, (_, cellIndex) => (
            `<td>${escapeHtml(row[cellIndex] ?? "")}</td>`
          )).join("")}
        </tr>
      `;
    })
    .join("");

  return `
    ${issueList}
    <p class="form-hint">プレビューは先頭 ${Math.min(DEFAULT_PREVIEW_ROWS, previewRows.length)} 行を表示します。空行は読み込み時に無視します。</p>
    <div class="csv-json-table-wrap">
      <table class="csv-json-table">
        <thead>${headerRow}</thead>
        <tbody>${bodyRows}</tbody>
      </table>
    </div>
  `;
}

function renderOutputColumns(state, validation) {
  if (!state.parsed) {
    return "";
  }

  const sourceColumnOptions = state.parsed.sourceColumns.map((column) => ({
    value: String(column.index),
    label: column.label,
  }));

  if (!state.outputColumns.length) {
    return "<p>出力列がありません。「出力列を追加」から追加してください。</p>";
  }

  return state.outputColumns
    .map((column, index) => {
      const errors = validation.columnErrors[column.id] ?? {};
      const sourceTypeOptions = buildOptions(SOURCE_TYPES, column.sourceType);
      const typeOptions = buildOptions(VALUE_TYPES, column.fixedValueType);
      const sourceOptions = buildOptions(sourceColumnOptions, String(column.sourceColumnIndex));
      const targetRules = state.rules.filter((rule) => rule.targetKey.trim() === column.key.trim());
      const sourceLabel = column.sourceType === "csv"
        ? "CSV列"
        : column.sourceType === "fixed"
          ? "固定値"
          : "カスタム";

      const sourceControl = column.sourceType === "csv"
        ? `
          <label>CSV列</label>
          <select data-column-id="${column.id}" data-column-field="sourceColumnIndex">
            ${sourceOptions}
          </select>
          ${renderInlineError(errors.sourceColumnIndex)}
        `
        : column.sourceType === "fixed"
          ? `
            <label>固定値の型</label>
            <select data-column-id="${column.id}" data-column-field="fixedValueType">
              ${typeOptions}
            </select>
            <label>固定値</label>
            <input
              type="text"
              value="${escapeHtml(column.fixedValue)}"
              data-column-id="${column.id}"
              data-column-field="fixedValue"
              ${column.fixedValueType === "null" ? "placeholder=\"null の場合は空欄\"" : ""}
            >
            ${renderInlineError(errors.fixedValueType || errors.fixedValue)}
          `
        : `
          <div class="csv-json-policy-note">
            <p>この列は条件ルールの一致結果で値を書き込みます。</p>
            <p>ルール未一致時の値は <code>null</code> です。</p>
            <p>同じ key に複数ルールが一致した場合: <code>${escapeHtml(RULE_CONFLICT_POLICY)}</code></p>
            <p class="form-hint">ポリシー定数: <code>${escapeHtml(RULE_MATCH_POLICY)}</code></p>
          </div>
          ${renderInlineError(errors.customRules)}
          <div class="csv-json-inline-rules">
            ${renderColumnRules(column, targetRules, state, validation, sourceColumnOptions)}
          </div>
        `;

      return `
        <article class="csv-json-item-card">
          <div class="csv-json-item-head">
            <div class="csv-json-item-title">
              <span>出力列 ${index + 1}</span>
              <p>source: ${escapeHtml(sourceLabel)}</p>
            </div>
            <div class="csv-json-icon-row">
              <button type="button" class="ghost-button" data-action="move-column-up" data-column-id="${column.id}" ${index === 0 ? "disabled" : ""}>↑</button>
              <button type="button" class="ghost-button" data-action="move-column-down" data-column-id="${column.id}" ${index === state.outputColumns.length - 1 ? "disabled" : ""}>↓</button>
              <button type="button" class="ghost-button danger-text" data-action="remove-column" data-column-id="${column.id}">削除</button>
            </div>
          </div>

          <div class="csv-json-form-grid">
            <div>
              <label>key</label>
              <input type="text" value="${escapeHtml(column.key)}" data-column-id="${column.id}" data-column-field="key">
              ${renderInlineError(errors.key)}
            </div>
            <div>
              <label>値の取得元</label>
              <select data-column-id="${column.id}" data-column-field="sourceType">
                ${sourceTypeOptions}
              </select>
            </div>
            <div class="csv-json-field-span">
              ${sourceControl}
            </div>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderColumnRules(column, rules, state, validation, sourceColumnOptions) {
  if (!rules.length) {
    return `
      <p class="form-hint">条件ルールはまだありません。</p>
      <div class="action-row">
        <button type="button" class="secondary" data-action="add-rule-for-column" data-column-id="${column.id}">条件ルールを追加</button>
      </div>
    `;
  }

  return `
    <div class="action-row">
      <button type="button" class="secondary" data-action="add-rule-for-column" data-column-id="${column.id}">条件ルールを追加</button>
    </div>
    ${rules.map((rule, index) => {
      const errors = validation.ruleErrors[rule.id] ?? {};
      const conditionOptions = buildOptions(CONDITION_TYPES, rule.conditionType);
      const sourceOptions = buildOptions(sourceColumnOptions, String(rule.sourceColumnIndex));
      const typeOptions = buildOptions(VALUE_TYPES, rule.valueType);

      return `
        <section class="csv-json-inline-rule-card">
          <div class="csv-json-item-head">
            <div class="csv-json-item-title">
              <span>条件ルール ${index + 1}</span>
              <p>書き込み先 key: ${escapeHtml(column.key || "(未設定)")}</p>
            </div>
            <div class="csv-json-icon-row">
              <button type="button" class="ghost-button" data-action="move-rule-up" data-rule-id="${rule.id}">↑</button>
              <button type="button" class="ghost-button" data-action="move-rule-down" data-rule-id="${rule.id}">↓</button>
              <button type="button" class="ghost-button danger-text" data-action="remove-rule" data-rule-id="${rule.id}">削除</button>
            </div>
          </div>

          <div class="csv-json-form-grid">
            <div>
              <label>対象カラム</label>
              <select data-rule-id="${rule.id}" data-rule-field="sourceColumnIndex">${sourceOptions}</select>
              ${renderInlineError(errors.sourceColumnIndex)}
            </div>
            <div>
              <label>条件種別</label>
              <select data-rule-id="${rule.id}" data-rule-field="conditionType">${conditionOptions}</select>
              ${renderInlineError(errors.conditionType)}
            </div>
            <div>
              <label>比較値</label>
              <input type="text" value="${escapeHtml(rule.compareValue)}" data-rule-id="${rule.id}" data-rule-field="compareValue">
              ${renderInlineError(errors.compareValue)}
            </div>
            <div>
              <label>書き込む値の型</label>
              <select data-rule-id="${rule.id}" data-rule-field="valueType">${typeOptions}</select>
              ${renderInlineError(errors.valueType)}
            </div>
            <div class="csv-json-field-span">
              <label>書き込む値</label>
              <input type="text" value="${escapeHtml(rule.value)}" data-rule-id="${rule.id}" data-rule-field="value">
              ${renderInlineError(errors.value || errors.targetKey)}
            </div>
          </div>
        </section>
      `;
    }).join("")}
  `;
}

function renderParseSummary(state) {
  if (!state.parsed) {
    return "";
  }

  const source = state.parsed.inputMode === "file"
    ? `file: ${state.parsed.fileName || "selected.csv"}`
    : "text input";

  return `
    <dl class="csv-json-summary-list">
      <div><dt>入力元</dt><dd>${escapeHtml(source)}</dd></div>
      <div><dt>データ行</dt><dd>${state.parsed.dataRows.length}</dd></div>
      <div><dt>列数</dt><dd>${state.parsed.expectedColumnCount}</dd></div>
    </dl>
  `;
}

function renderPresetSelect(state, refs) {
  const options = state.presets.length
    ? state.presets.map((preset) => {
      const selected = preset.name === state.selectedPresetName ? " selected" : "";
      return `<option value="${escapeHtml(preset.name)}"${selected}>${escapeHtml(preset.name)}</option>`;
    }).join("")
    : '<option value="">保存済み設定はありません</option>';

  refs.presetSelect.innerHTML = options;
}

function renderConversion(state) {
  const { conversion } = state;
  const feedback = conversion.errors.length
    ? renderMessages(conversion.errors, "error")
    : "";

  const progress = conversion.progressText
    ? `<p class="csv-json-progress-text">${escapeHtml(conversion.progressText)}</p>`
    : '<p class="form-hint">変換前に列と条件ルールを確認してください。</p>';

  const download = conversion.status === "done" && conversion.downloadUrl
    ? `
      <a class="button-link" href="${escapeHtml(conversion.downloadUrl)}" download="${escapeHtml(conversion.downloadName)}">
        JSON をダウンロード
      </a>
      <p class="form-hint">${conversion.rowCount} 件の JSON オブジェクトを生成しました。</p>
    `
    : "";

  return { feedback, progress, download };
}

function renderLoadFeedback(state) {
  return state.loadMessages.length ? renderMessages(state.loadMessages, "error") : "";
}

export function render(state, refs, validation) {
  refs.hasHeader.checked = state.headerEnabled;
  refs.inputCard.hidden = Boolean(state.parsed);
  refs.workspace.hidden = !state.parsed;
  refs.loadFeedback.innerHTML = renderLoadFeedback(state);
  refs.parseSummary.innerHTML = renderParseSummary(state);
  refs.previewPanel.innerHTML = renderPreview(state);
  refs.outputColumns.innerHTML = renderOutputColumns(state, validation);
  refs.presetFeedback.innerHTML = state.presetMessage
    ? renderMessages([state.presetMessage], state.presetMessageType ?? "info")
    : "";

  renderPresetSelect(state, refs);

  const conversion = renderConversion(state);
  refs.conversionFeedback.innerHTML = conversion.feedback;
  refs.progressPanel.innerHTML = conversion.progress;
  refs.downloadPanel.innerHTML = conversion.download;
}
