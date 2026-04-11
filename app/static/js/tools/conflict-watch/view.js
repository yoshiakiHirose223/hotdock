import { PROVIDERS, QUICK_WEBHOOK_PRESETS } from "./constants.js?v=conflict-watch-20250410-22";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat("ja-JP", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function formatFullDateTime(value) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function formatRelativeDays(value, now) {
  if (!value) {
    return "-";
  }
  const days = Math.floor((new Date(now).getTime() - new Date(value).getTime()) / 86400000);
  if (days <= 0) {
    return "今日";
  }
  return `${days}日前`;
}

function renderStatusPill(value, tone = value) {
  return `<span class="cw-pill cw-pill-${escapeHtml(tone)}">${escapeHtml(value)}</span>`;
}

function renderBranchFileStatus(changeType) {
  const labels = {
    modified: "変更",
    removed: "削除",
    added: "追加",
    renamed: "リネーム",
    copied: "コピー",
  };
  return labels[changeType] ?? changeType ?? "-";
}

function renderResolvedReason(reason, context = null) {
  const labels = {
    webhook_branch_deleted: "Webhook で branch 削除が来て解消",
    webhook_observed_resolution: "push 再計算で解消",
    branch_excluded: "branch を除外して解消",
    branch_included: "除外解除後の再計算で解消",
    merged_to_main_or_master: "main/master マージ扱いで解消",
    branch_deleted: "手動で branch 削除して解消",
    manual_reset: "手動リセットで解消",
    branch_file_ignored: "branch-file ignore で解消",
    branch_file_ignore_removed: "ignore 解除後の再計算で解消",
    ignore_rule_added: "repository ignore rule 追加で解消",
    ignore_rule_enabled: "repository ignore rule 有効化で解消",
    ignore_rule_disabled: "repository ignore rule 無効化後の再計算で解消",
    manual_resolved: "手動で resolved に変更",
    other_observed_resolution: "上記に当てはまらない再計算の fallback",
  };
  return labels[reason] ?? reason ?? "-";
}

function renderConflictHistoryItems(history) {
  if (!Array.isArray(history) || history.length === 0) {
    return "<li>履歴はまだありません。</li>";
  }
  return [...history]
    .sort((left, right) => new Date(right.happenedAt ?? 0).getTime() - new Date(left.happenedAt ?? 0).getTime())
    .map((entry) => `
    <li>
      <strong>${escapeHtml(entry.label ?? "-")}</strong>
      <span>${escapeHtml(formatFullDateTime(entry.happenedAt))}</span>
      <p>${escapeHtml(entry.note ?? "-")}</p>
    </li>
  `).join("");
}

function renderResolutionContext(context) {
  if (!context) {
    return "";
  }
  const items = [
    { label: "ブランチ", value: context.branchName },
    { label: "ファイル", value: context.normalizedFilePath },
    { label: "pattern", value: context.pattern },
    { label: "provider", value: context.providerType },
    { label: "delivery_id", value: context.deliveryId },
    { label: "after SHA", value: context.afterSha },
    { label: "pusher", value: context.pusher },
  ].filter((item) => item.value);
  if (!items.length) {
    return "";
  }
  return `
    <div class="cw-resolution-detail-list">
      ${items.map((item) => `
        <div class="cw-resolution-detail-item">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `).join("")}
    </div>
  `;
}

function renderDashboard(viewModel) {
  const items = [
    { label: "監視 repository", value: viewModel.dashboard.repositories, tone: "neutral" },
    { label: "active branch", value: viewModel.dashboard.activeBranches, tone: "active" },
    { label: "stale branch", value: viewModel.dashboard.staleBranches, tone: "stale" },
    { label: "warning", value: viewModel.dashboard.warningCount, tone: "warning" },
    { label: "notice", value: viewModel.dashboard.noticeCount, tone: "notice" },
    { label: "conflict_ignored", value: viewModel.dashboard.ignoredCount, tone: "ignored" },
    { label: "長期未解消", value: viewModel.dashboard.longUnresolvedCount, tone: "stale" },
  ];

  return `
    <section class="cw-summary-grid">
      ${items.map((item) => `
        <article class="cw-summary-card">
          <p>${escapeHtml(item.label)}</p>
          <strong class="cw-tone-${escapeHtml(item.tone)}">${escapeHtml(item.value)}</strong>
        </article>
      `).join("")}
    </section>
  `;
}

function renderDrawerToggle(viewModel) {
  return `
    <button
      type="button"
      class="cw-drawer-toggle"
      data-action="toggle-side-drawer"
      aria-expanded="${viewModel.ui.isSideDrawerOpen ? "true" : "false"}"
      aria-controls="cw-side-drawer"
    >
      <span class="cw-drawer-toggle-icon" aria-hidden="true">
        <span></span>
        <span></span>
        <span></span>
      </span>
      <span>詳細メニュー</span>
    </button>
  `;
}

function renderMainTabs(viewModel) {
  const tabs = [
    { id: "simulator", eyebrow: "Simulator", label: "Webhook を適用する" },
    { id: "branches", eyebrow: "Branches", label: "branch 一覧" },
    { id: "conflicts", eyebrow: "Conflicts", label: "conflict 一覧" },
  ];

  return `
    <div class="cw-tab-strip" role="tablist" aria-label="repository 操作タブ">
      ${tabs.map((tab) => `
        <button
          type="button"
          class="cw-tab-button${viewModel.ui.activeMainTab === tab.id ? " is-active" : ""}"
          data-action="switch-main-tab"
          data-tab-id="${tab.id}"
          role="tab"
          aria-selected="${viewModel.ui.activeMainTab === tab.id ? "true" : "false"}"
          aria-controls="cw-main-tab-${tab.id}"
          id="cw-main-tab-trigger-${tab.id}"
        >
          <span class="cw-tab-eyebrow">${escapeHtml(tab.eyebrow)}</span>
          <strong>${escapeHtml(tab.label)}</strong>
        </button>
      `).join("")}
    </div>
  `;
}

function renderArchitecture() {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Webhook-only</p>
          <h2>導入の軽さを優先した競合予兆監視</h2>
        </div>
      </div>
      <div class="cw-architecture-grid">
        <div>
          <h3>保証すること</h3>
          <ul class="cw-bullet-list">
            <li>Webhook で観測した branch の変更だけを監視</li>
            <li>branch_files を累積し、同一 normalized_file_path の重複作業を検知</li>
            <li>新規発生、再発、長期未解消、status 変更を通知</li>
          </ul>
        </div>
        <div>
          <h3>保証しないこと</h3>
          <ul class="cw-bullet-list">
            <li>Git の完全現在状態の再現</li>
            <li>Webhook 欠損後の完全回復</li>
            <li>merge 競合の完全検出</li>
          </ul>
        </div>
        <div>
          <h3>Provider 共通イベント</h3>
          <p class="cw-mono-text">provider / delivery_id / branch_name / before_sha / after_sha / pusher / pushed_at / is_deleted / is_forced / files_added[] / files_modified[] / files_removed[] / files_renamed[]</p>
        </div>
      </div>
    </section>
  `;
}

function renderRepositoryPanel(viewModel) {
  const openLabel = viewModel.ui.pageMode === "repository-detail" ? "この repository を表示" : "branch 情報を見る";
  const providerOptions = PROVIDERS
    .map((provider) => `<option value="${provider.value}"${provider.value === viewModel.ui.newRepositoryProvider ? " selected" : ""}>${escapeHtml(provider.label)}</option>`)
    .join("");

  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Repositories</p>
          <h2>repository 管理</h2>
        </div>
      </div>

      <div class="cw-repository-list">
        ${viewModel.repositories.map((repository) => `
          <article class="cw-repository-card ${repository.id === viewModel.selectedRepository?.id ? "is-selected" : ""}">
            <div>
              <strong>${escapeHtml(repository.repositoryName)}</strong>
              <p class="cw-table-subline">${escapeHtml(repository.providerType)} / ${escapeHtml(repository.externalRepoId)}</p>
            </div>
            <div class="cw-action-stack">
              ${renderStatusPill(repository.isActive ? "active" : "inactive", repository.isActive ? "active" : "stale")}
              <button type="button" class="ghost-button" data-action="select-repository" data-repository-id="${repository.id}">${escapeHtml(openLabel)}</button>
              <button type="button" class="ghost-button" data-action="toggle-repository-active" data-repository-id="${repository.id}">${repository.isActive ? "無効化" : "有効化"}</button>
            </div>
          </article>
        `).join("")}
      </div>

      <div class="cw-subsection">
        <h3>repository を追加</h3>
        <div class="cw-form-grid">
          <div>
            <label>provider_type</label>
            <select data-field="newRepositoryProvider">${providerOptions}</select>
          </div>
          <div>
            <label>repository_name</label>
            <input type="text" value="${escapeHtml(viewModel.ui.newRepositoryName ?? "")}" data-field="newRepositoryName">
          </div>
          <div>
            <label>external_repo_id</label>
            <input type="text" value="${escapeHtml(viewModel.ui.newRepositoryExternalId ?? "")}" data-field="newRepositoryExternalId" placeholder="owner/repo">
          </div>
        </div>
        <div class="action-row">
          <button type="button" data-action="add-repository">repository を追加</button>
        </div>
      </div>
    </section>
  `;
}

function renderWebhookForm(viewModel) {
  const draft = viewModel.ui.webhookDraft;
  const providerOptions = PROVIDERS
    .map((provider) => `<option value="${provider.value}"${draft.provider === provider.value ? " selected" : ""}>${escapeHtml(provider.label)}</option>`)
    .join("");
  const deletedOptions = [
    { value: "false", label: "false" },
    { value: "true", label: "true" },
    { value: "unknown", label: "unknown" },
  ].map((item) => `<option value="${item.value}"${draft.deletedState === item.value ? " selected" : ""}>${item.label}</option>`).join("");
  const feedback = viewModel.ui.feedbackMessage
    ? `<div class="notice ${escapeHtml(viewModel.ui.feedbackTone)}">${escapeHtml(viewModel.ui.feedbackMessage)}</div>`
    : "";

  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Simulator</p>
          <h2>Webhook を適用する</h2>
        </div>
      </div>

      <div class="cw-webhook-toolbar">
        <div>
          <p class="cw-muted-label">対象 repository</p>
          <strong>${escapeHtml(viewModel.selectedRepository?.repositoryName ?? "-")}</strong>
          <p class="cw-table-subline">${escapeHtml(viewModel.selectedRepository?.externalRepoId ?? "-")}</p>
        </div>
        <div>
          <p class="cw-muted-label">仮想現在時刻</p>
          <strong>${escapeHtml(formatDateTime(viewModel.ui.now ?? viewModel.now))}</strong>
        </div>
        <div class="cw-preset-row">
          ${QUICK_WEBHOOK_PRESETS.map((preset) => `
            <button type="button" class="secondary" data-action="load-preset" data-preset-id="${preset.id}">${escapeHtml(preset.label)}</button>
          `).join("")}
        </div>
      </div>

      ${feedback}

      <div class="cw-form-grid">
        <div>
          <label>provider</label>
          <select data-field="provider">${providerOptions}</select>
        </div>
        <div>
          <label>delivery_id</label>
          <input type="text" value="${escapeHtml(draft.deliveryId)}" data-field="deliveryId" placeholder="空欄なら自動採番">
        </div>
        <div>
          <label>branch_name</label>
          <input type="text" value="${escapeHtml(draft.branchName)}" data-field="branchName">
        </div>
        <div>
          <label>pusher</label>
          <input type="text" value="${escapeHtml(draft.pusher)}" data-field="pusher">
        </div>
        <div>
          <label>signature</label>
          <select data-field="signatureStatus">
            <option value="valid"${draft.signatureStatus === "valid" ? " selected" : ""}>valid</option>
            <option value="invalid"${draft.signatureStatus === "invalid" ? " selected" : ""}>invalid</option>
          </select>
        </div>
        <div>
          <label>is_deleted</label>
          <select data-field="deletedState">${deletedOptions}</select>
        </div>
        <label class="cw-checkbox">
          <input type="checkbox" data-field="isForced"${draft.isForced ? " checked" : ""}>
          <span>force push として扱う</span>
        </label>
        <label class="cw-checkbox">
          <input type="checkbox" data-field="simulateFailure"${draft.simulateFailure ? " checked" : ""}>
          <span>queue worker の failed を再現する</span>
        </label>
      </div>

      <div class="cw-form-grid cw-form-grid-wide">
        <div>
          <label>files_added[]</label>
          <textarea rows="4" data-field="added" placeholder="app/notifications/slack.py">${escapeHtml(draft.added)}</textarea>
        </div>
        <div>
          <label>files_modified[]</label>
          <textarea rows="4" data-field="modified" placeholder="app/conflicts/service.py">${escapeHtml(draft.modified)}</textarea>
        </div>
        <div>
          <label>files_removed[]</label>
          <textarea rows="4" data-field="removed" placeholder="app/export/csv.py">${escapeHtml(draft.removed)}</textarea>
        </div>
        <div>
          <label>files_renamed[]</label>
          <textarea rows="4" data-field="renamed" placeholder="old/file.php -> new/file.php">${escapeHtml(draft.renamed)}</textarea>
        </div>
      </div>

      <div class="action-row">
        <button type="button" data-action="apply-webhook">Webhook を適用</button>
      </div>
    </section>
  `;
}

function renderBranchTable(viewModel) {
  const statusOptions = [
    { value: "all", label: "全て" },
    { value: "active", label: "active" },
    { value: "quiet", label: "quiet" },
    { value: "stale", label: "stale" },
    { value: "branch_excluded", label: "除外" },
  ];
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">ブランチ</p>
          <h2>branch 一覧</h2>
        </div>
      </div>
      <div class="cw-branch-toolbar">
        <div class="cw-inline-form cw-branch-search-row">
          <select data-field="branchSearchMode" aria-label="branch 検索対象">
            <option value="both"${viewModel.ui.branchSearchMode === "both" ? " selected" : ""}>ブランチ名・ファイル名</option>
            <option value="branch"${viewModel.ui.branchSearchMode === "branch" ? " selected" : ""}>ブランチ名のみ</option>
            <option value="file"${viewModel.ui.branchSearchMode === "file" ? " selected" : ""}>ファイル名のみ</option>
          </select>
          <input
            type="text"
            data-field="branchSearchInput"
            value="${escapeHtml(viewModel.ui.branchSearchInput ?? "")}"
            placeholder="ブランチ名 / ファイル名で検索"
          >
          <button type="button" data-action="apply-branch-search">検索</button>
        </div>
        <div class="cw-branch-filter-row">
          <label class="cw-filter-field">
            <span>並び替え</span>
            <select data-field="branchSortOrder">
              <option value="updated_desc"${viewModel.ui.branchSortOrder === "updated_desc" ? " selected" : ""}>更新日が新しい順</option>
              <option value="updated_asc"${viewModel.ui.branchSortOrder === "updated_asc" ? " selected" : ""}>更新日が古い順</option>
            </select>
          </label>
          <label class="cw-filter-field">
            <span>状態</span>
            <select data-field="branchStatusFilter">
              ${statusOptions.map((item) => `
                <option value="${item.value}"${viewModel.ui.branchStatusFilter === item.value ? " selected" : ""}>${item.label}</option>
              `).join("")}
            </select>
          </label>
          <label class="cw-checkbox cw-branch-filter-checkbox">
            <input type="checkbox" data-field="branchConflictOnly"${viewModel.ui.branchConflictOnly ? " checked" : ""}>
            <span>競合のみ表示</span>
          </label>
        </div>
        <p class="cw-table-subline">
          ${escapeHtml(viewModel.branchSummary.displayedCount)} / ${escapeHtml(viewModel.branchSummary.totalCount)} 件を
          ${viewModel.branchSummary.hasActiveFilters ? "表示中" : "表示"}
        </p>
      </div>
      <div class="cw-table-wrap">
        <table class="cw-table">
          <thead>
            <tr>
              <th>ブランチ名</th>
              <th>状態</th>
              <th>更新日</th>
              <th>ファイル数</th>
              <th>監視結果</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${viewModel.branches.length ? viewModel.branches.map((branch) => {
              const rowClass = branch.id === viewModel.ui.highlightedBranchId
                ? "is-focused"
                : branch.id === viewModel.ui.selectedBranchId
                  ? "is-selected"
                  : "";
              return `
              <tr
                class="${rowClass}"
                data-branch-row-id="${branch.id}"
                data-action="toggle-branch-row"
                data-branch-id="${branch.id}"
              >
                <td>
                  <span class="cw-row-title">${escapeHtml(branch.branchName)}</span>
                  ${branch.memo ? `<p class="cw-row-note">${escapeHtml(branch.memo)}</p>` : ""}
                </td>
                <td>${renderStatusPill(branch.status)}</td>
                <td>${escapeHtml(formatFullDateTime(branch.lastPushAt))}</td>
                <td>
                  <button
                    type="button"
                    class="cw-branch-files-toggle"
                    data-action="toggle-branch-files"
                    data-branch-id="${branch.id}"
                    aria-expanded="${branch.isFileListOpen ? "true" : "false"}"
                    aria-controls="cw-branch-files-${branch.id}"
                  >
                    <strong>${escapeHtml(branch.observedFileCount)}</strong>
                    <span class="cw-branch-files-chevron" aria-hidden="true">${branch.isFileListOpen ? "▴" : "▾"}</span>
                  </button>
                  <div class="cw-table-subline">ルール除外: ${escapeHtml(branch.ignoredFileCount)}</div>
                </td>
                <td>${renderStatusPill(branch.healthLabel, branch.healthStatus)}</td>
                <td>
                  <div class="cw-action-stack">
                    <button type="button" class="ghost-button cw-compact-button" data-action="toggle-excluded" data-branch-id="${branch.id}">${branch.isBranchExcluded ? "除外解除" : "除外"}</button>
                    <button type="button" class="ghost-button danger-text cw-compact-button" data-action="delete-branch" data-branch-id="${branch.id}">削除</button>
                  </div>
                </td>
              </tr>
              ${branch.isFileListOpen ? `
                <tr class="cw-branch-files-row">
                  <td colspan="6">
                    <div id="cw-branch-files-${branch.id}" class="cw-branch-files-table-wrap">
                      <table class="cw-inline-table cw-branch-files-table">
                        <thead>
                          <tr>
                            <th>ファイル名</th>
                            <th>状態</th>
                            <th>IGNORE</th>
                          </tr>
                        </thead>
                        <tbody>
                          ${branch.observedFiles.length ? branch.observedFiles.map((branchFile) => `
                            <tr class="cw-branch-file-row${branchFile.isInConflict ? " is-conflicting" : ""}${branchFile.isBranchFileIgnored ? " is-ignored" : ""}">
                              <td>
                                <div class="cw-branch-file-title-row">
                                  ${branchFile.isInConflict ? `
                                    <button
                                      type="button"
                                      class="cw-inline-link cw-branch-file-link"
                                      data-action="jump-to-conflict"
                                      data-conflict-key="${escapeHtml(branchFile.activeConflictKey)}"
                                    >
                                      ${escapeHtml(branchFile.normalizedFilePath)}
                                    </button>
                                  ` : `
                                    <span
                                      class="cw-branch-file-name"
                                      ${branchFile.isBranchFileIgnored && branchFile.branchFileIgnoreMemo
                                        ? `title="${escapeHtml(branchFile.branchFileIgnoreMemo)}"`
                                        : ""}
                                    >
                                      ${escapeHtml(branchFile.normalizedFilePath)}
                                    </span>
                                  `}
                                  ${branchFile.isBranchFileIgnored && branchFile.branchFileIgnoreMemo ? `
                                    <span
                                      class="cw-branch-file-ignore-memo"
                                      title="${escapeHtml(branchFile.branchFileIgnoreMemo)}"
                                    >
                                      ${escapeHtml(branchFile.branchFileIgnoreMemo)}
                                    </span>
                                  ` : ""}
                                </div>
                                ${branchFile.previousPath ? `<div class="cw-table-subline">旧: ${escapeHtml(branchFile.previousPath)}</div>` : ""}
                              </td>
                              <td>${escapeHtml(renderBranchFileStatus(branchFile.changeType))}</td>
                              <td>
                                ${branchFile.isBranchFileIgnored ? `
                                  <button
                                    type="button"
                                    class="ghost-button cw-compact-button cw-branch-file-ignore-button"
                                    data-action="open-branch-file-ignore-modal"
                                    data-ignore-mode="remove"
                                    data-ignore-id="${branchFile.branchFileIgnoreId ?? ""}"
                                    data-branch-id="${branch.id}"
                                    data-branch-name="${escapeHtml(branch.branchName)}"
                                    data-file-path="${escapeHtml(branchFile.normalizedFilePath)}"
                                    data-ignore-memo="${escapeHtml(branchFile.branchFileIgnoreMemo || "")}"
                                  >
                                    ignore 済み
                                  </button>
                                ` : `
                                  <button
                                    type="button"
                                    class="ghost-button cw-compact-button cw-branch-file-ignore-button"
                                    data-action="open-branch-file-ignore-modal"
                                    data-ignore-mode="create"
                                    data-branch-id="${branch.id}"
                                    data-branch-name="${escapeHtml(branch.branchName)}"
                                    data-file-path="${escapeHtml(branchFile.normalizedFilePath)}"
                                  >
                                    ignore
                                  </button>
                                `}
                              </td>
                            </tr>
                          `).join("") : `
                            <tr>
                              <td colspan="3" class="cw-inline-table-empty">観測対象のファイルはありません。</td>
                            </tr>
                          `}
                        </tbody>
                      </table>
                    </div>
                  </td>
                </tr>
              ` : ""}
            `;
            }).join("") : `
              <tr>
                <td colspan="6" class="cw-inline-table-empty">検索条件に一致するブランチはありません。</td>
              </tr>
            `}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderConflictTable(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Conflicts</p>
          <h2>conflict 一覧</h2>
        </div>
      </div>
      <div class="cw-table-wrap">
        <table class="cw-table">
          <thead>
            <tr>
              <th>ファイル名</th>
              <th>状態</th>
              <th>関連ブランチ</th>
              <th>通知</th>
            </tr>
          </thead>
          <tbody>
            ${viewModel.conflicts.map((conflict) => {
              const rowClass = conflict.conflictKey === viewModel.ui.highlightedConflictKey
                ? "is-forcused"
                : conflict.conflictKey === viewModel.ui.selectedConflictKey
                  ? "is-selected"
                  : "";
              return `
              <tr
                class="${rowClass}"
                data-conflict-row-key="${escapeHtml(conflict.conflictKey)}"
                data-action="toggle-conflict-row"
                data-conflict-id="${conflict.id}"
                data-conflict-key="${escapeHtml(conflict.conflictKey)}"
              >
                <td>
                  <span class="cw-row-title">${escapeHtml(conflict.normalizedFilePath)}</span>
                  ${conflict.memo ? `<p class="cw-row-note">${escapeHtml(conflict.memo)}</p>` : ""}
                </td>
                <td>${renderStatusPill(conflict.status)}</td>
                <td>
                  ${conflict.relatedBranchCount > 0 ? `
                    <button
                      type="button"
                      class="cw-branch-files-toggle"
                      data-action="toggle-conflict-branches"
                      data-conflict-id="${conflict.id}"
                      data-conflict-key="${escapeHtml(conflict.conflictKey)}"
                      aria-expanded="${conflict.isBranchListOpen ? "true" : "false"}"
                      aria-controls="cw-conflict-branches-${conflict.id}"
                    >
                      <strong>${escapeHtml(conflict.relatedBranchCount)}</strong>
                      <span class="cw-branch-files-chevron" aria-hidden="true">${conflict.isBranchListOpen ? "▴" : "▾"}</span>
                    </button>
                  ` : `
                    <strong>-</strong>
                  `}
                </td>
                <td>
                  <strong>${escapeHtml(conflict.notificationCount)}</strong>
                </td>
              </tr>
              ${conflict.isBranchListOpen && conflict.canExpand ? `
                <tr class="cw-branch-files-row">
                  <td colspan="4">
                    <div id="cw-conflict-branches-${conflict.id}" class="cw-branch-files-panel">
                      ${conflict.relatedBranches.length ? `
                        <div class="cw-conflict-detail-section">
                          <div class="cw-conflict-detail-header">
                            <strong>関連ブランチ</strong>
                          </div>
                          <div class="cw-branch-files-grid cw-branch-files-grid-header">
                            <strong>ブランチ名</strong>
                            <strong class="cw-branch-files-grid-meta">変更種別</strong>
                            <strong class="cw-branch-files-grid-date">更新日</strong>
                          </div>
                          ${conflict.relatedBranches.map((branch) => `
                            <div class="cw-branch-files-grid">
                              <div>
                                ${branch.isNavigable ? `
                                  <button
                                    type="button"
                                    class="cw-inline-link"
                                    data-action="jump-to-branch"
                                    data-branch-id="${branch.id}"
                                  >
                                    ${escapeHtml(branch.branchName)}
                                  </button>
                                ` : `
                                  <span class="cw-row-title">${escapeHtml(branch.branchName)}</span>
                                `}
                                ${branch.isDeletedSnapshot ? `<div class="cw-table-subline">現在は一覧から削除済み</div>` : ""}
                                ${branch.previousPath ? `<div class="cw-table-subline">旧: ${escapeHtml(branch.previousPath)}</div>` : ""}
                              </div>
                              <div class="cw-branch-files-grid-meta">${escapeHtml(renderBranchFileStatus(branch.changeType))}</div>
                              <div class="cw-branch-files-grid-date">${escapeHtml(formatFullDateTime(branch.lastPushAt))}</div>
                            </div>
                          `).join("")}
                        </div>
                      ` : ""}
                      ${conflict.status === "resolved" ? `
                        <div class="cw-conflict-detail-section">
                          <div class="cw-conflict-detail-header">
                            <strong>解消の詳細</strong>
                          </div>
                          <div class="cw-conflict-resolution-meta">
                            <div>
                              <span>解消日時</span>
                              <strong>${escapeHtml(formatFullDateTime(conflict.resolvedAt))}</strong>
                            </div>
                            <div>
                              <span>解消理由</span>
                              <strong>${escapeHtml(renderResolvedReason(conflict.resolvedReason, conflict.resolvedContext))}</strong>
                            </div>
                          </div>
                          ${renderResolutionContext(conflict.resolvedContext)}
                          <ul class="cw-history-list cw-conflict-history-list">
                            ${renderConflictHistoryItems(conflict.history)}
                          </ul>
                        </div>
                      ` : ""}
                    </div>
                  </td>
                </tr>
              ` : ""}
            `;
            }).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderNotifications(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Slack</p>
          <h2>通知ログ</h2>
        </div>
      </div>
      <ul class="cw-history-list">
        ${viewModel.recentNotifications.map((notification) => `
          <li>
            <strong>${escapeHtml(notification.notificationType)}</strong>
            <span>${escapeHtml(formatDateTime(notification.sentAt))}</span>
            <p>${escapeHtml(notification.normalizedFilePath ?? "削除済み conflict")} → ${escapeHtml(notification.destinationValue)}</p>
            <p class="cw-table-subline">status: ${escapeHtml(notification.status)}</p>
          </li>
        `).join("") || "<li>通知はまだありません。</li>"}
      </ul>
    </section>
  `;
}

function renderWebhookPayloadPanel(payloadState, event) {
  if (payloadState?.isLoading) {
    return `
      <div class="cw-webhook-payload">
        <p class="cw-table-subline">raw payload を読み込んでいます...</p>
      </div>
    `;
  }
  if (payloadState?.errorMessage) {
    return `
      <div class="cw-webhook-payload">
        <p class="cw-row-note">${escapeHtml(payloadState.errorMessage)}</p>
      </div>
    `;
  }
  if (!payloadState?.isAvailable) {
    const message = event.rawPayloadExpiredAt
      ? `保持期限切れです (${formatDateTime(event.rawPayloadExpiredAt)})`
      : "raw payload はまだ取得されていません。";
    return `
      <div class="cw-webhook-payload">
        <p class="cw-table-subline">${escapeHtml(message)}</p>
      </div>
    `;
  }
  return `
    <div class="cw-webhook-payload">
      <div class="cw-webhook-payload-header">
        <span>raw payload</span>
        <strong>${escapeHtml(payloadState.rawPayloadRef || event.rawPayloadRef || "-")}</strong>
      </div>
      <pre class="cw-webhook-payload-code"><code>${escapeHtml(payloadState.content)}</code></pre>
    </div>
  `;
}

function renderWebhookEvents(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Webhook events</p>
          <h2>観測履歴 / raw payload / 再現</h2>
        </div>
      </div>
      <div class="cw-webhook-endpoints">
        <div class="cw-webhook-endpoint-row">
          <span>GitHub endpoint</span>
          <code>${escapeHtml(viewModel.settings.githubWebhookEndpoint)}</code>
        </div>
        <div class="cw-webhook-endpoint-row">
          <span>Backlog endpoint</span>
          <code>${escapeHtml(viewModel.settings.backlogWebhookEndpoint)}</code>
        </div>
      </div>
      <ul class="cw-history-list">
        ${viewModel.webhookEvents.map((event) => {
          const isPayloadOpen = (viewModel.ui.expandedWebhookPayloadIds ?? []).includes(event.id);
          const payloadState = viewModel.ui.webhookPayloadsByEventId?.[event.id] ?? null;
          return `
          <li>
            <strong>${escapeHtml(event.providerType)} / ${escapeHtml(event.branchName)}</strong>
            <span>${escapeHtml(formatDateTime(event.receivedAt))}</span>
            <p>delivery_id: ${escapeHtml(event.deliveryId)} / process_status: ${escapeHtml(event.processStatus)} / payload_hash: ${escapeHtml(event.payloadHash)}</p>
            <p class="cw-table-subline">raw_payload_ref: ${escapeHtml(event.rawPayloadRef ?? "expired")} ${event.rawPayloadExpiredAt ? `(expired ${escapeHtml(formatDateTime(event.rawPayloadExpiredAt))})` : ""}</p>
            <p class="cw-table-subline">before ${escapeHtml(event.beforeSha)} / after ${escapeHtml(event.afterSha)} / processed_at ${escapeHtml(formatDateTime(event.processedAt))}</p>
            <p class="cw-table-subline">added ${event.filesAdded.length} / modified ${event.filesModified.length} / removed ${event.filesRemoved.length} / deleted ${event.isDeleted === null ? "unknown" : event.isDeleted ? "true" : "false"}${event.isForced ? " / force push" : ""}</p>
            ${event.errorMessage ? `<p class="cw-row-note">${escapeHtml(event.errorMessage)}</p>` : ""}
            <div class="action-row">
              <button type="button" class="secondary" data-action="load-webhook-into-simulator" data-webhook-id="${event.id}">シミュレータへ反映</button>
              <button type="button" class="secondary" data-action="toggle-webhook-payload" data-webhook-id="${event.id}">
                ${isPayloadOpen ? "raw payload を閉じる" : "raw payload を表示"}
              </button>
              ${event.processStatus === "processing_failed"
                ? `<button type="button" class="secondary" data-action="reprocess-webhook" data-webhook-id="${event.id}"${event.rawPayloadRef ? "" : " disabled"}>raw payload から再処理</button>`
                : ""}
            </div>
            ${isPayloadOpen ? renderWebhookPayloadPanel(payloadState, event) : ""}
          </li>
        `;
        }).join("") || "<li>Webhook はまだ受信していません。</li>"}
      </ul>
    </section>
  `;
}

function renderSecurityLogs(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Security</p>
          <h2>署名検証失敗ログ</h2>
        </div>
      </div>
      <ul class="cw-history-list">
        ${viewModel.securityLogs.map((log) => `
          <li>
            <strong>${escapeHtml(log.providerType)} / ${escapeHtml(log.deliveryId)}</strong>
            <span>${escapeHtml(formatDateTime(log.receivedAt))}</span>
            <p>${escapeHtml(log.branchName)} / status ${escapeHtml(log.statusCode)} / ${escapeHtml(log.reason)}</p>
          </li>
        `).join("") || "<li>security log はありません。</li>"}
      </ul>
    </section>
  `;
}

function renderSettings(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Settings</p>
          <h2>通知・ignore rule・保持期間</h2>
        </div>
      </div>

      <div class="cw-form-grid">
        <div>
          <label>stale 日数</label>
          <input type="number" min="1" value="${escapeHtml(viewModel.settings.staleDays)}" data-setting="staleDays">
        </div>
        <div>
          <label>長期未解消閾値</label>
          <input type="number" min="1" value="${escapeHtml(viewModel.settings.longUnresolvedDays)}" data-setting="longUnresolvedDays">
        </div>
        <div>
          <label>raw payload 保持日数</label>
          <input type="number" min="1" value="${escapeHtml(viewModel.settings.rawPayloadRetentionDays)}" data-setting="rawPayloadRetentionDays">
        </div>
        <div>
          <label>Slack 送信先</label>
          <input type="text" value="${escapeHtml(viewModel.settings.notificationDestination)}" data-setting="notificationDestination">
        </div>
        <div>
          <label>Slack webhook URL</label>
          <input type="text" value="${escapeHtml(viewModel.settings.slackWebhookUrl)}" data-setting="slackWebhookUrl" placeholder="https://hooks.slack.com/services/...">
        </div>
        <label class="cw-checkbox">
          <input type="checkbox" data-setting="forcePushNoteEnabled"${viewModel.settings.forcePushNoteEnabled ? " checked" : ""}>
          <span>force push 注記を通知本文へ出す</span>
        </label>
        <label class="cw-checkbox">
          <input type="checkbox" data-setting="suppressNoticeNotifications"${viewModel.settings.suppressNoticeNotifications ? " checked" : ""}>
          <span>notice 変更時の通知を抑制する</span>
        </label>
      </div>

      <div class="cw-subsection">
        <h3>provider 設定 / secret</h3>
        <div class="cw-form-grid">
          <div>
            <label>GitHub webhook endpoint</label>
            <input type="text" value="${escapeHtml(viewModel.settings.githubWebhookEndpoint)}" data-setting="githubWebhookEndpoint">
          </div>
          <div>
            <label>GitHub webhook secret</label>
            <input type="text" value="${escapeHtml(viewModel.settings.githubWebhookSecret)}" data-setting="githubWebhookSecret">
          </div>
          <div>
            <label>Backlog webhook endpoint</label>
            <input type="text" value="${escapeHtml(viewModel.settings.backlogWebhookEndpoint)}" data-setting="backlogWebhookEndpoint">
          </div>
          <div>
            <label>Backlog webhook secret</label>
            <input type="text" value="${escapeHtml(viewModel.settings.backlogWebhookSecret)}" data-setting="backlogWebhookSecret">
          </div>
        </div>
      </div>

      <div class="action-row">
        <button type="button" data-action="apply-settings">設定を再計算へ反映</button>
      </div>

      <div class="cw-subsection">
        <h3>ignore rule</h3>
        <div class="cw-inline-form">
          <input type="text" placeholder="例: generated/**" value="${escapeHtml(viewModel.ui.newIgnorePattern ?? "")}" data-field="newIgnorePattern">
          <button type="button" class="secondary" data-action="add-ignore-rule">追加</button>
        </div>
        <ul class="cw-plain-list">
          ${viewModel.ignoreRules.map((rule) => `
            <li class="cw-rule-row">
              <code>${escapeHtml(rule.pattern)}</code>
              <div class="action-row">
                ${renderStatusPill(rule.isActive ? "active" : "inactive", rule.isActive ? "active" : "stale")}
                <button type="button" class="ghost-button" data-action="toggle-ignore-rule" data-rule-id="${escapeHtml(rule.id)}">${rule.isActive ? "無効化" : "有効化"}</button>
              </div>
            </li>
          `).join("")}
        </ul>
      </div>
    </section>
  `;
}

function renderSideDrawer(viewModel) {
  const isOpen = Boolean(viewModel.ui.isSideDrawerOpen);

  return `
    <div class="cw-drawer-layer${isOpen ? " is-open" : ""}" aria-hidden="${isOpen ? "false" : "true"}">
      <div class="cw-drawer-backdrop" data-action="close-side-drawer"></div>
      <aside
        id="cw-side-drawer"
        class="cw-side-drawer"
        role="dialog"
        aria-modal="true"
        aria-labelledby="cw-side-drawer-title"
      >
        <div class="cw-drawer-header">
          <div>
            <p class="eyebrow">Workspace panels</p>
            <h2 id="cw-side-drawer-title">詳細ビュー</h2>
          </div>
          <button type="button" class="cw-drawer-close" data-action="close-side-drawer" aria-label="詳細メニューを閉じる">x</button>
        </div>

        <nav class="cw-drawer-nav" aria-label="詳細セクション">
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-slack">Slack</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-webhooks">Webhook events</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-security">Security</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-settings">Settings</button>
        </nav>

        <div class="cw-side-column cw-drawer-scroll">
          <div id="cw-panel-slack" class="cw-drawer-section">
            ${renderNotifications(viewModel)}
          </div>
          <div id="cw-panel-webhooks" class="cw-drawer-section">
            ${renderWebhookEvents(viewModel)}
          </div>
          <div id="cw-panel-security" class="cw-drawer-section">
            ${renderSecurityLogs(viewModel)}
          </div>
          <div id="cw-panel-settings" class="cw-drawer-section">
            ${renderSettings(viewModel)}
          </div>
        </div>
      </aside>
    </div>
  `;
}

function renderBranchFileIgnoreModal(viewModel) {
  const dialog = viewModel.ui.branchFileIgnoreDialog ?? {};
  const isOpen = Boolean(dialog.isOpen);
  if (!isOpen) {
    return "";
  }
  const isRemoveMode = dialog.mode === "remove";
  return `
    <div class="cw-modal-layer" aria-hidden="false">
      <div class="cw-modal-backdrop" data-action="close-branch-file-ignore-modal"></div>
      <section class="cw-modal" role="dialog" aria-modal="true" aria-labelledby="cw-branch-file-ignore-title">
        <div class="cw-modal-header">
          <div>
            <p class="eyebrow">Ignore</p>
            <h2 id="cw-branch-file-ignore-title">${isRemoveMode ? "branch file の ignore を取り消す" : "branch file を ignore 登録"}</h2>
          </div>
          <button type="button" class="cw-drawer-close" data-action="close-branch-file-ignore-modal" aria-label="ignore モーダルを閉じる">x</button>
        </div>
        <p><strong>${escapeHtml(dialog.branchName ?? "-")}</strong></p>
        <p class="cw-table-subline">${escapeHtml(dialog.normalizedFilePath ?? "-")}</p>
        ${isRemoveMode ? `
          <p class="cw-modal-copy">ignore を取り消すと、この branch / file は再び conflict 判定の対象に戻ります。</p>
          <label class="cw-label-block">
            <span>ignore メモ</span>
            <textarea rows="4" data-field="branchFileIgnoreMemo">${escapeHtml(dialog.memo ?? "")}</textarea>
          </label>
        ` : `
          <label class="cw-label-block">
            <span>ignore メモ</span>
            <textarea rows="4" data-field="branchFileIgnoreMemo">${escapeHtml(dialog.memo ?? "")}</textarea>
          </label>
        `}
        <div class="action-row">
          <button type="button" class="secondary" data-action="close-branch-file-ignore-modal">キャンセル</button>
          ${isRemoveMode ? `<button type="button" class="secondary" data-action="update-branch-file-ignore-memo">メモ更新</button>` : ""}
          <button type="button" data-action="confirm-branch-file-ignore">${isRemoveMode ? "ignore 取り消し" : "ignore 登録"}</button>
        </div>
      </section>
    </div>
  `;
}

function renderRepositoryIndexPage(viewModel) {
  return `
    ${renderDashboard(viewModel)}
    ${renderArchitecture()}
    ${renderRepositoryPanel(viewModel)}
  `;
}

function renderRepositoryDetailHeader(viewModel) {
  const repository = viewModel.selectedRepository;
  if (!repository) {
    return `
      <section class="cw-card">
        <div class="cw-card-header">
          <div>
            <p class="eyebrow">Repository</p>
            <h2>repository が見つかりません</h2>
          </div>
          <div class="action-row">
            <a class="secondary" href="/tools/conflict-watch">一覧へ戻る</a>
          </div>
        </div>
      </section>
    `;
  }

  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Repository detail</p>
          <h2>${escapeHtml(repository.repositoryName)}</h2>
          <p class="cw-table-subline">${escapeHtml(repository.providerType)} / ${escapeHtml(repository.externalRepoId)}</p>
        </div>
        <div class="action-row">
          ${renderDrawerToggle(viewModel)}
          <a class="secondary" href="/tools/conflict-watch">一覧へ戻る</a>
        </div>
      </div>
    </section>
  `;
}

export function renderConflictWatch(root, viewModel) {
  if (viewModel.ui.pageMode === "repositories") {
    root.innerHTML = renderRepositoryIndexPage(viewModel);
    return;
  }

  root.innerHTML = `
    ${renderRepositoryDetailHeader(viewModel)}
    <div class="cw-main-grid">
      <div class="cw-main-column">
        ${renderMainTabs(viewModel)}
        <div class="cw-tab-panels">
          <div
            id="cw-main-tab-simulator"
            class="cw-tab-panel${viewModel.ui.activeMainTab === "simulator" ? " is-active" : ""}"
            role="tabpanel"
            aria-labelledby="cw-main-tab-trigger-simulator"
            ${viewModel.ui.activeMainTab === "simulator" ? "" : "hidden"}
          >
            ${renderWebhookForm(viewModel)}
          </div>
          <div
            id="cw-main-tab-branches"
            class="cw-tab-panel${viewModel.ui.activeMainTab === "branches" ? " is-active" : ""}"
            role="tabpanel"
            aria-labelledby="cw-main-tab-trigger-branches"
            ${viewModel.ui.activeMainTab === "branches" ? "" : "hidden"}
          >
            ${renderBranchTable(viewModel)}
          </div>
          <div
            id="cw-main-tab-conflicts"
            class="cw-tab-panel${viewModel.ui.activeMainTab === "conflicts" ? " is-active" : ""}"
            role="tabpanel"
            aria-labelledby="cw-main-tab-trigger-conflicts"
            ${viewModel.ui.activeMainTab === "conflicts" ? "" : "hidden"}
          >
            ${renderConflictTable(viewModel)}
          </div>
        </div>
      </div>
    </div>
    ${renderSideDrawer(viewModel)}
    ${renderBranchFileIgnoreModal(viewModel)}
  `;
}
