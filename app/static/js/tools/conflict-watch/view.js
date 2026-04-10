import { CONFLICT_ACTIONS, PROVIDERS, QUICK_WEBHOOK_PRESETS } from "./constants.js";

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
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Branches</p>
          <h2>branch 一覧</h2>
        </div>
      </div>
      <div class="cw-table-wrap">
        <table class="cw-table">
          <thead>
            <tr>
              <th>repository / branch</th>
              <th>status</th>
              <th>last_push_at</th>
              <th>観測 file 数</th>
              <th>confidence</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${viewModel.branches.map((branch) => `
              <tr class="${branch.id === viewModel.ui.selectedBranchId ? "is-selected" : ""}">
                <td>
                  <button type="button" class="cw-inline-link" data-action="select-branch" data-branch-id="${branch.id}">${escapeHtml(branch.branchName)}</button>
                  <div class="cw-table-subline">
                    <span>${escapeHtml(branch.repositoryName)}</span>
                    <span>latest SHA: ${escapeHtml(branch.latestAfterSha ?? "-")}</span>
                    ${branch.possiblyInconsistent ? "<span>possibly_inconsistent</span>" : ""}
                    ${branch.isBranchExcluded ? "<span>branch_excluded</span>" : ""}
                    ${branch.monitoringClosedReason ? `<span>closed: ${escapeHtml(branch.monitoringClosedReason)}</span>` : ""}
                  </div>
                  ${branch.memo ? `<p class="cw-row-note">${escapeHtml(branch.memo)}</p>` : ""}
                </td>
                <td>${renderStatusPill(branch.status)}</td>
                <td>
                  <div>${escapeHtml(formatDateTime(branch.lastPushAt))}</div>
                  <div class="cw-table-subline">${escapeHtml(formatRelativeDays(branch.lastSeenAt, viewModel.now))}</div>
                </td>
                <td>
                  <strong>${escapeHtml(branch.observedFileCount)}</strong>
                  <div class="cw-table-subline">ignored: ${escapeHtml(branch.ignoredFileCount)}</div>
                </td>
                <td>${renderStatusPill(branch.confidence, branch.confidence)}</td>
                <td>
                  <div class="cw-action-stack">
                    <button type="button" class="ghost-button" data-action="toggle-excluded" data-branch-id="${branch.id}">${branch.isBranchExcluded ? "除外解除" : "除外"}</button>
                    <button type="button" class="ghost-button" data-action="merge-branch" data-branch-id="${branch.id}">main/master マージ</button>
                    <button type="button" class="ghost-button" data-action="reset-branch" data-branch-id="${branch.id}">手動リセット</button>
                    <button type="button" class="ghost-button danger-text" data-action="delete-branch" data-branch-id="${branch.id}">deleted</button>
                  </div>
                </td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderBranchDetail(viewModel) {
  const branch = viewModel.selectedBranch;
  if (!branch) {
    return `
      <section class="cw-card">
        <div class="cw-card-header">
          <div>
            <p class="eyebrow">Branch detail</p>
            <h2>branch 詳細</h2>
          </div>
        </div>
        <p>branch を選択してください。</p>
      </section>
    `;
  }

  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Branch detail</p>
          <h2>${escapeHtml(branch.branchName)}</h2>
        </div>
      </div>
      <div class="cw-detail-metrics">
        <div>${renderStatusPill(branch.status)}</div>
        <div>${renderStatusPill(branch.confidence, branch.confidence)}</div>
        <div>latest SHA: ${escapeHtml(branch.latestAfterSha ?? "-")}</div>
      </div>

      <label class="cw-label-block">
        <span>branch memo</span>
        <textarea rows="3" data-role="branch-memo" data-branch-id="${branch.id}">${escapeHtml(branch.memo ?? "")}</textarea>
      </label>
      <div class="action-row">
        <button type="button" data-action="save-branch-memo" data-branch-id="${branch.id}">memo を保存</button>
      </div>

      <div class="cw-subsection">
        <h3>branch_files</h3>
        <ul class="cw-plain-list">
          ${viewModel.selectedBranchFiles.map((branchFile) => `
            <li>
              <strong>${escapeHtml(branchFile.normalizedFilePath)}</strong>
              <div class="cw-table-subline">
                <span>change_type: ${escapeHtml(branchFile.changeType)}</span>
                <span>first_seen_at: ${escapeHtml(formatDateTime(branchFile.firstSeenAt))}</span>
                <span>last_seen_at: ${escapeHtml(formatDateTime(branchFile.lastSeenAt))}</span>
                ${branchFile.ignored ? "<span>ignored</span>" : ""}
                ${branchFile.previousPath ? `<span>old_path: ${escapeHtml(branchFile.previousPath)}</span>` : ""}
              </div>
            </li>
          `).join("") || "<li>branch_files はまだありません。</li>"}
        </ul>
      </div>

      <div class="cw-subsection">
        <h3>関連 webhook</h3>
        <ul class="cw-history-list">
          ${viewModel.selectedBranchEvents.map((event) => `
            <li>
              <strong>${escapeHtml(event.deliveryId)}</strong>
              <span>${escapeHtml(formatDateTime(event.receivedAt))}</span>
              <p>status: ${escapeHtml(event.processStatus)} / before ${escapeHtml(event.beforeSha)} / after ${escapeHtml(event.afterSha)}</p>
            </li>
          `).join("") || "<li>観測イベントはまだありません。</li>"}
        </ul>
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
              <th>normalized_file_path</th>
              <th>status</th>
              <th>関連 branch</th>
              <th>first / last</th>
              <th>通知</th>
              <th>confidence</th>
            </tr>
          </thead>
          <tbody>
            ${viewModel.conflicts.map((conflict) => `
              <tr class="${conflict.conflictKey === viewModel.ui.selectedConflictKey ? "is-selected" : ""}">
                <td>
                  <button type="button" class="cw-inline-link" data-action="select-conflict" data-conflict-key="${escapeHtml(conflict.conflictKey)}">${escapeHtml(conflict.normalizedFilePath)}</button>
                  <div class="cw-table-subline">${escapeHtml(conflict.conflictKey)}</div>
                  ${conflict.memo ? `<p class="cw-row-note">${escapeHtml(conflict.memo)}</p>` : ""}
                </td>
                <td>${renderStatusPill(conflict.status)}</td>
                <td>
                  <strong>${escapeHtml(conflict.activeBranchIds?.length ?? 0)}</strong>
                  <div class="cw-table-subline">${(conflict.activeBranchIds ?? []).map((branchId) => {
                    const branch = viewModel.branches.find((item) => item.id === branchId);
                    return escapeHtml(branch?.branchName ?? branchId);
                  }).join(", ")}</div>
                </td>
                <td>
                  <div>first: ${escapeHtml(formatDateTime(conflict.firstDetectedAt))}</div>
                  <div class="cw-table-subline">last: ${escapeHtml(formatDateTime(conflict.lastDetectedAt))}</div>
                  ${conflict.reopenedAt ? `<div class="cw-table-subline">再発: ${escapeHtml(formatDateTime(conflict.reopenedAt))}</div>` : ""}
                </td>
                <td>
                  <strong>${escapeHtml(conflict.notificationCount)}</strong>
                  <div class="cw-table-subline">${escapeHtml(conflict.lastNotificationType ?? "-")}</div>
                </td>
                <td>${renderStatusPill(conflict.confidence ?? "low", conflict.confidence ?? "low")}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderConflictDetail(viewModel) {
  const conflict = viewModel.selectedConflict;
  if (!conflict) {
    return `
      <section class="cw-card">
        <div class="cw-card-header">
          <div>
            <p class="eyebrow">Detail</p>
            <h2>conflict 詳細</h2>
          </div>
        </div>
        <p>まだ conflict はありません。</p>
      </section>
    `;
  }

  const actionButtons = CONFLICT_ACTIONS
    .filter((action) => action.value !== "resolved" || (conflict.activeBranchIds?.length ?? 0) < 2)
    .map((action) => `
      <button type="button" class="secondary" data-action="set-conflict-status" data-conflict-key="${escapeHtml(conflict.conflictKey)}" data-status="${escapeHtml(action.value)}">${escapeHtml(action.label)}</button>
    `)
    .join("");

  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Detail</p>
          <h2>${escapeHtml(conflict.normalizedFilePath)}</h2>
        </div>
        <div class="action-row">${actionButtons}</div>
      </div>

      <div class="cw-detail-metrics">
        <div>${renderStatusPill(conflict.status)}</div>
        <div>${renderStatusPill(conflict.confidence ?? "low", conflict.confidence ?? "low")}</div>
        <div>first_detected_at: ${escapeHtml(formatDateTime(conflict.firstDetectedAt))}</div>
        <div>last_detected_at: ${escapeHtml(formatDateTime(conflict.lastDetectedAt))}</div>
        <div>resolved_reason: ${escapeHtml(conflict.resolvedReason ?? "-")}</div>
      </div>

      <label class="cw-label-block">
        <span>memo</span>
        <textarea rows="3" data-role="conflict-memo" data-conflict-key="${escapeHtml(conflict.conflictKey)}">${escapeHtml(conflict.memo ?? "")}</textarea>
      </label>

      <div class="action-row">
        <button type="button" data-action="save-conflict-memo" data-conflict-key="${escapeHtml(conflict.conflictKey)}">memo を保存</button>
      </div>

      <div class="cw-subsection">
        <h3>関連 branch</h3>
        ${viewModel.selectedConflictBranches.length ? `
          <ul class="cw-plain-list">
            ${viewModel.selectedConflictBranches.map((branch) => `
              <li>
                <strong>${escapeHtml(branch.branchName)}</strong>
                <div class="cw-table-subline">
                  <span>change_type: ${escapeHtml(branch.changeType ?? "-")}</span>
                  <span>latest SHA: ${escapeHtml(branch.latestAfterSha ?? "-")}</span>
                  <span>status: ${escapeHtml(branch.status)}</span>
                  ${branch.previousPath ? `<span>old_path: ${escapeHtml(branch.previousPath)}</span>` : ""}
                  ${branch.possiblyInconsistent ? "<span>possibly_inconsistent</span>" : ""}
                </div>
              </li>
            `).join("")}
          </ul>
        ` : "<p>現在ぶら下がっている branch はありません。</p>"}
      </div>

      <div class="cw-subsection">
        <h3>通知履歴</h3>
        <ul class="cw-history-list">
          ${viewModel.selectedNotifications.map((notification) => `
            <li>
              <strong>${escapeHtml(notification.notificationType)}</strong>
              <span>${escapeHtml(formatDateTime(notification.sentAt))}</span>
              <p>${escapeHtml(notification.destinationType)} → ${escapeHtml(notification.destinationValue)} / ${escapeHtml(notification.status)}</p>
            </li>
          `).join("") || "<li>通知履歴はありません。</li>"}
        </ul>
      </div>

      <div class="cw-subsection">
        <h3>状態遷移履歴</h3>
        <ol class="cw-history-list">
          ${(conflict.history ?? []).slice().reverse().map((entry) => `
            <li>
              <strong>${escapeHtml(entry.label)}</strong>
              <span>${escapeHtml(formatDateTime(entry.happenedAt))}</span>
              <p>${escapeHtml(entry.note)}</p>
            </li>
          `).join("")}
        </ol>
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
            <p>${escapeHtml(notification.conflictKey)} → ${escapeHtml(notification.destinationValue)}</p>
          </li>
        `).join("") || "<li>通知はまだありません。</li>"}
      </ul>
    </section>
  `;
}

function renderWebhookEvents(viewModel) {
  return `
    <section class="cw-card">
      <div class="cw-card-header">
        <div>
          <p class="eyebrow">Webhook events</p>
          <h2>観測履歴と再処理</h2>
        </div>
      </div>
      <ul class="cw-history-list">
        ${viewModel.webhookEvents.map((event) => `
          <li>
            <strong>${escapeHtml(event.providerType)} / ${escapeHtml(event.branchName)}</strong>
            <span>${escapeHtml(formatDateTime(event.receivedAt))}</span>
            <p>delivery_id: ${escapeHtml(event.deliveryId)} / process_status: ${escapeHtml(event.processStatus)} / payload_hash: ${escapeHtml(event.payloadHash)}</p>
            <p class="cw-table-subline">raw_payload_ref: ${escapeHtml(event.rawPayloadRef ?? "expired")} ${event.rawPayloadExpiredAt ? `(expired ${escapeHtml(formatDateTime(event.rawPayloadExpiredAt))})` : ""}</p>
            <p class="cw-table-subline">before ${escapeHtml(event.beforeSha)} / after ${escapeHtml(event.afterSha)} / processed_at ${escapeHtml(formatDateTime(event.processedAt))}</p>
            <p class="cw-table-subline">added ${event.filesAdded.length} / modified ${event.filesModified.length} / removed ${event.filesRemoved.length} / deleted ${event.isDeleted === null ? "unknown" : event.isDeleted ? "true" : "false"}${event.isForced ? " / force push" : ""}</p>
            ${event.errorMessage ? `<p class="cw-row-note">${escapeHtml(event.errorMessage)}</p>` : ""}
            ${event.processStatus === "processing_failed" ? `
              <div class="action-row">
                <button type="button" class="secondary" data-action="reprocess-webhook" data-webhook-id="${event.id}"${event.rawPayloadRef ? "" : " disabled"}>raw payload から再処理</button>
              </div>
            ` : ""}
          </li>
        `).join("") || "<li>Webhook はまだ受信していません。</li>"}
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
  const selectedBranchLabel = viewModel.selectedBranch?.branchName ?? "未選択";
  const selectedConflictLabel = viewModel.selectedConflict?.normalizedFilePath ?? "なし";
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
            <p class="cw-table-subline">branch: ${escapeHtml(selectedBranchLabel)}</p>
            <p class="cw-table-subline">conflict: ${escapeHtml(selectedConflictLabel)}</p>
          </div>
          <button type="button" class="cw-drawer-close" data-action="close-side-drawer" aria-label="詳細メニューを閉じる">x</button>
        </div>

        <nav class="cw-drawer-nav" aria-label="詳細セクション">
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-branch">Branch detail</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-conflict">Detail</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-slack">Slack</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-security">Security</button>
          <button type="button" class="cw-drawer-chip" data-action="jump-side-section" data-section-id="cw-panel-settings">Settings</button>
        </nav>

        <div class="cw-side-column cw-drawer-scroll">
          <div id="cw-panel-branch" class="cw-drawer-section">
            ${renderBranchDetail(viewModel)}
          </div>
          <div id="cw-panel-conflict" class="cw-drawer-section">
            ${renderConflictDetail(viewModel)}
          </div>
          <div id="cw-panel-slack" class="cw-drawer-section">
            ${renderNotifications(viewModel)}
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
        ${renderWebhookForm(viewModel)}
        ${renderBranchTable(viewModel)}
        ${renderConflictTable(viewModel)}
        ${renderWebhookEvents(viewModel)}
      </div>
    </div>
    ${renderSideDrawer(viewModel)}
  `;
}
