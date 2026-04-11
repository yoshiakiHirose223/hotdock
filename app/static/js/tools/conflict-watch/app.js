import { QUICK_WEBHOOK_PRESETS, WEBHOOK_FORM_DEFAULTS } from "./constants.js?v=conflict-watch-20250410-22";
import { buildViewModel } from "./domain.js?v=conflict-watch-20250410-22";
import { renderConflictWatch } from "./view.js?v=conflict-watch-20250410-22";

const root = document.querySelector("[data-conflict-watch-app]");
const API_BASE = "/tools/conflict-watch/api";
const pageMode = root?.dataset.pageMode ?? "repositories";
const initialSelectedRepositoryId = root?.dataset.selectedRepositoryId ?? "";

if (!root) {
  throw new Error("Conflict Watch root element was not found.");
}

let snapshot = null;
const uiState = {
  selectedRepositoryId: initialSelectedRepositoryId ? Number(initialSelectedRepositoryId) : null,
  selectedConflictKey: "",
  selectedBranchId: null,
  expandedBranchIds: [],
  expandedConflictIds: [],
  expandedWebhookPayloadIds: [],
  pageMode,
  activeMainTab: "simulator",
  isSideDrawerOpen: false,
  webhookDraft: { ...WEBHOOK_FORM_DEFAULTS },
  webhookPayloadsByEventId: {},
  newIgnorePattern: "",
  newRepositoryName: "",
  newRepositoryExternalId: "",
  newRepositoryProvider: "github",
  branchSearchInput: "",
  branchSearchQuery: "",
  branchSearchMode: "both",
  branchSortOrder: "updated_desc",
  branchConflictOnly: false,
  branchStatusFilter: "all",
  pendingConflictScrollKey: "",
  pendingBranchScrollId: null,
  highlightedBranchId: null,
  highlightedConflictKey: "",
  branchFileIgnoreDialog: {
    isOpen: false,
    mode: "create",
    ignoreId: null,
    branchId: null,
    branchName: "",
    normalizedFilePath: "",
    memo: "",
  },
  feedbackMessage: "",
  feedbackTone: "info",
};

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isNaN(parsed) ? null : parsed;
}

function syncSelections() {
  const repositories = snapshot?.repositories ?? [];
  if (!repositories.length) {
    uiState.selectedRepositoryId = null;
    uiState.selectedBranchId = null;
    uiState.selectedConflictKey = "";
    uiState.expandedBranchIds = [];
    uiState.expandedConflictIds = [];
    uiState.expandedWebhookPayloadIds = [];
    uiState.webhookPayloadsByEventId = {};
    return;
  }

  const repositoryExists = repositories.some((repository) => repository.id === uiState.selectedRepositoryId);
  if (!repositoryExists && uiState.pageMode === "repository-detail") {
    uiState.selectedRepositoryId = repositories[0].id;
  }
  if (!repositoryExists && uiState.pageMode !== "repository-detail") {
    uiState.selectedRepositoryId = null;
  }

  if (uiState.selectedRepositoryId === null) {
    uiState.selectedBranchId = null;
    uiState.selectedConflictKey = "";
    return;
  }

  if (!repositoryExists && repositories.length) {
    uiState.selectedRepositoryId = repositories[0].id;
  }

  const branches = (snapshot?.branches ?? []).filter((branch) => branch.repositoryId === uiState.selectedRepositoryId);
  uiState.expandedBranchIds = uiState.expandedBranchIds.filter((branchId) => (
    branches.some((branch) => branch.id === branchId)
  ));
  if (!branches.some((branch) => branch.id === uiState.selectedBranchId)) {
    uiState.selectedBranchId = branches[0]?.id ?? null;
  }
  if (uiState.highlightedBranchId !== null && !branches.some((branch) => branch.id === uiState.highlightedBranchId)) {
    uiState.highlightedBranchId = null;
  }

  const conflicts = (snapshot?.conflicts ?? []).filter((conflict) => conflict.repositoryId === uiState.selectedRepositoryId);
  uiState.expandedConflictIds = uiState.expandedConflictIds.filter((conflictId) => (
    conflicts.some((conflict) => conflict.id === conflictId)
  ));
  if (!conflicts.some((conflict) => conflict.conflictKey === uiState.selectedConflictKey)) {
    uiState.selectedConflictKey = conflicts[0]?.conflictKey ?? "";
  }
  if (uiState.highlightedConflictKey && !conflicts.some((conflict) => conflict.conflictKey === uiState.highlightedConflictKey)) {
    uiState.highlightedConflictKey = "";
  }

  const selectedRepository = repositories.find((repository) => repository.id === uiState.selectedRepositoryId) ?? null;
  const webhookEventIds = new Set(
    (snapshot?.webhookEvents ?? [])
      .filter((event) => (
        event.repositoryId === uiState.selectedRepositoryId
        || event.repositoryExternalId === selectedRepository?.externalRepoId
      ))
      .map((event) => event.id),
  );
  uiState.expandedWebhookPayloadIds = uiState.expandedWebhookPayloadIds.filter((eventId) => webhookEventIds.has(eventId));
  uiState.webhookPayloadsByEventId = Object.fromEntries(
    Object.entries(uiState.webhookPayloadsByEventId).filter(([eventId]) => webhookEventIds.has(Number(eventId))),
  );
}

function render() {
  if (!snapshot) {
    document.body.classList.remove("cw-drawer-open");
    document.body.classList.remove("cw-overlay-open");
    root.innerHTML = '<p class="notice">Conflict Watch を読み込んでいます...</p>';
    return;
  }
  if (uiState.pendingConflictScrollKey) {
    uiState.activeMainTab = "conflicts";
  }
  if (uiState.pendingBranchScrollId !== null) {
    uiState.activeMainTab = "branches";
  }
  syncSelections();
  const viewModel = buildViewModel({
    ...snapshot,
    ui: uiState,
  });
  viewModel.now = snapshot.now;
  viewModel.ui.now = snapshot.now;
  document.body.classList.toggle("cw-drawer-open", uiState.pageMode === "repository-detail" && uiState.isSideDrawerOpen);
  document.body.classList.toggle(
    "cw-overlay-open",
    (uiState.pageMode === "repository-detail" && uiState.isSideDrawerOpen)
      || uiState.branchFileIgnoreDialog.isOpen,
  );
  renderConflictWatch(root, viewModel);
  if (uiState.pendingConflictScrollKey) {
    const conflictRow = root.querySelector(
      `[data-conflict-row-key="${CSS.escape(uiState.pendingConflictScrollKey)}"]`,
    );
    if (conflictRow instanceof HTMLElement) {
      conflictRow.scrollIntoView({ behavior: "smooth", block: "center" });
    }
    uiState.pendingConflictScrollKey = "";
  }
  if (uiState.pendingBranchScrollId !== null) {
    const branchRow = root.querySelector(
      `[data-branch-row-id="${CSS.escape(String(uiState.pendingBranchScrollId))}"]`,
    );
    if (branchRow instanceof HTMLElement) {
      branchRow.scrollIntoView({ behavior: "smooth", block: "center" });
    }
    uiState.pendingBranchScrollId = null;
  }
}

async function fetchState() {
  const response = await fetch(`${API_BASE}/state`, {
    credentials: "same-origin",
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error("Conflict Watch state の取得に失敗しました。");
  }
  snapshot = await response.json();
}

async function applyResponse(response) {
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail ?? payload?.message ?? "処理に失敗しました。";
    await fetchState();
    uiState.feedbackMessage = detail;
    uiState.feedbackTone = "warning";
    render();
    return false;
  }
  snapshot = payload.state;
  uiState.feedbackMessage = payload.message ?? "更新しました。";
  uiState.feedbackTone = payload.tone ?? "success";
  render();
  return true;
}

async function requestJson(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {}),
    },
    ...options,
  });
  return applyResponse(response);
}

function getSelectedRepositoryId() {
  return uiState.selectedRepositoryId;
}

function updateSettingField(key, value) {
  if (!snapshot) {
    return;
  }
  snapshot.settings = {
    ...snapshot.settings,
    [key]: value,
  };
}

function closeSideDrawer() {
  if (!uiState.isSideDrawerOpen) {
    return;
  }
  uiState.isSideDrawerOpen = false;
  render();
}

function closeBranchFileIgnoreDialog() {
  if (!uiState.branchFileIgnoreDialog.isOpen) {
    return;
  }
  uiState.branchFileIgnoreDialog = {
    isOpen: false,
    mode: "create",
    ignoreId: null,
    branchId: null,
    branchName: "",
    normalizedFilePath: "",
    memo: "",
  };
  render();
}

function toggleExpandedBranch(branchId) {
  if (uiState.expandedBranchIds.includes(branchId)) {
    uiState.expandedBranchIds = uiState.expandedBranchIds.filter((id) => id !== branchId);
  } else {
    uiState.expandedBranchIds = [...uiState.expandedBranchIds, branchId];
  }
}

function toggleExpandedConflict(conflictId) {
  if (uiState.expandedConflictIds.includes(conflictId)) {
    uiState.expandedConflictIds = uiState.expandedConflictIds.filter((id) => id !== conflictId);
  } else {
    uiState.expandedConflictIds = [...uiState.expandedConflictIds, conflictId];
  }
}

function toggleExpandedWebhookPayload(eventId) {
  if (uiState.expandedWebhookPayloadIds.includes(eventId)) {
    uiState.expandedWebhookPayloadIds = uiState.expandedWebhookPayloadIds.filter((id) => id !== eventId);
  } else {
    uiState.expandedWebhookPayloadIds = [...uiState.expandedWebhookPayloadIds, eventId];
  }
}

function buildWebhookDraftFromEvent(event) {
  const renamed = Array.isArray(event.filesRenamed)
    ? event.filesRenamed
      .map((item) => {
        const oldPath = String(item?.oldPath ?? "").trim();
        const newPath = String(item?.newPath ?? "").trim();
        if (!oldPath || !newPath) {
          return "";
        }
        return `${oldPath} -> ${newPath}`;
      })
      .filter(Boolean)
      .join("\n")
    : "";

  return {
    provider: event.providerType ?? uiState.webhookDraft.provider,
    deliveryId: "",
    branchName: event.branchName ?? "",
    pusher: event.pusher ?? "",
    signatureStatus: "valid",
    deletedState: event.isDeleted === null ? "unknown" : event.isDeleted ? "true" : "false",
    simulateFailure: false,
    isForced: Boolean(event.isForced),
    added: Array.isArray(event.filesAdded) ? event.filesAdded.join("\n") : "",
    modified: Array.isArray(event.filesModified) ? event.filesModified.join("\n") : "",
    removed: Array.isArray(event.filesRemoved) ? event.filesRemoved.join("\n") : "",
    renamed,
  };
}

async function fetchWebhookPayload(eventId) {
  uiState.webhookPayloadsByEventId = {
    ...uiState.webhookPayloadsByEventId,
    [eventId]: {
      ...(uiState.webhookPayloadsByEventId[eventId] ?? {}),
      isLoading: true,
      errorMessage: "",
    },
  };
  render();

  try {
    const response = await fetch(`${API_BASE}/webhook-events/${eventId}/raw-payload`, {
      credentials: "same-origin",
      cache: "no-store",
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(payload?.detail ?? "raw payload の取得に失敗しました。");
    }
    uiState.webhookPayloadsByEventId = {
      ...uiState.webhookPayloadsByEventId,
      [eventId]: {
        ...payload,
        isLoading: false,
        errorMessage: "",
      },
    };
  } catch (error) {
    uiState.webhookPayloadsByEventId = {
      ...uiState.webhookPayloadsByEventId,
      [eventId]: {
        eventId,
        rawPayloadRef: "",
        rawPayloadExpiredAt: null,
        isAvailable: false,
        content: "",
        isLoading: false,
        errorMessage: error instanceof Error ? error.message : "raw payload の取得に失敗しました。",
      },
    };
    uiState.feedbackMessage = error instanceof Error ? error.message : "raw payload の取得に失敗しました。";
    uiState.feedbackTone = "warning";
  }
  render();
}

async function boot() {
  await fetchState();
  render();
}

root.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  if (target.matches("[data-field]")) {
    const field = target.getAttribute("data-field");
    const value = target instanceof HTMLInputElement && target.type === "checkbox" ? target.checked : target.value;

    if (field === "newIgnorePattern") {
      uiState.newIgnorePattern = String(value);
      return;
    }

    if (field === "newRepositoryName") {
      uiState.newRepositoryName = String(value);
      return;
    }
    if (field === "newRepositoryExternalId") {
      uiState.newRepositoryExternalId = String(value);
      return;
    }
    if (field === "newRepositoryProvider") {
      uiState.newRepositoryProvider = String(value);
      return;
    }
    if (field === "branchSearchInput") {
      uiState.branchSearchInput = String(value);
      return;
    }
    if (field === "branchSearchMode") {
      uiState.branchSearchMode = String(value);
      return;
    }
    if (field === "branchSortOrder") {
      uiState.branchSortOrder = String(value);
      render();
      return;
    }
    if (field === "branchConflictOnly") {
      uiState.branchConflictOnly = Boolean(value);
      render();
      return;
    }
    if (field === "branchStatusFilter") {
      uiState.branchStatusFilter = String(value);
      render();
      return;
    }
    if (field === "branchFileIgnoreMemo") {
      uiState.branchFileIgnoreDialog = {
        ...uiState.branchFileIgnoreDialog,
        memo: String(value),
      };
      return;
    }

    uiState.webhookDraft = {
      ...uiState.webhookDraft,
      [field]: value,
    };
    return;
  }

  if (target.matches("[data-setting]")) {
    const key = target.getAttribute("data-setting");
    const value = target instanceof HTMLInputElement && target.type === "checkbox"
      ? target.checked
      : target instanceof HTMLInputElement && target.type === "number"
        ? Number(target.value)
        : target.value;
    updateSettingField(key, value);
    render();
  }
});

root.addEventListener("click", async (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const clickedBranchJumpTrigger = target.closest('[data-action="jump-to-branch"]');
  const clickedJumpTrigger = target.closest('[data-action="jump-to-conflict"]');
  let shouldRender = false;
  if (!clickedBranchJumpTrigger && uiState.highlightedBranchId !== null) {
    uiState.highlightedBranchId = null;
    shouldRender = true;
  }
  if (!clickedJumpTrigger && uiState.highlightedConflictKey) {
    uiState.highlightedConflictKey = "";
    shouldRender = true;
  }

  const actionTarget = target.closest("[data-action]");
  if (!(actionTarget instanceof HTMLElement)) {
    if (shouldRender) {
      render();
    }
    return;
  }

  const action = actionTarget.getAttribute("data-action");
  if (!action) {
    return;
  }

  if (action === "toggle-side-drawer") {
    uiState.isSideDrawerOpen = !uiState.isSideDrawerOpen;
    render();
    return;
  }

  if (action === "close-side-drawer") {
    closeSideDrawer();
    return;
  }

  if (action === "close-branch-file-ignore-modal") {
    closeBranchFileIgnoreDialog();
    return;
  }

  if (action === "switch-main-tab") {
    const tabId = actionTarget.getAttribute("data-tab-id");
    if (tabId === "simulator" || tabId === "branches" || tabId === "conflicts") {
      uiState.activeMainTab = tabId;
      render();
    }
    return;
  }

  if (action === "jump-side-section") {
    const sectionId = actionTarget.getAttribute("data-section-id");
    if (!sectionId) {
      return;
    }
    const section = root.querySelector(`#${CSS.escape(sectionId)}`);
    if (section instanceof HTMLElement) {
      section.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    return;
  }

  if (action === "select-repository") {
    const repositoryId = toNumber(actionTarget.getAttribute("data-repository-id"));
    if (repositoryId !== null) {
      window.location.href = `/tools/conflict-watch/${repositoryId}`;
    }
    return;
  }

  if (action === "toggle-repository-active") {
    const repositoryId = toNumber(actionTarget.getAttribute("data-repository-id"));
    await requestJson(`${API_BASE}/repositories/${repositoryId}/toggle-active`, {
      method: "POST",
    });
    return;
  }

  if (action === "add-repository") {
    await requestJson(`${API_BASE}/repositories`, {
      method: "POST",
      body: JSON.stringify({
        providerType: uiState.newRepositoryProvider,
        repositoryName: uiState.newRepositoryName,
        externalRepoId: uiState.newRepositoryExternalId,
      }),
    });
    uiState.newRepositoryName = "";
    uiState.newRepositoryExternalId = "";
    return;
  }

  if (action === "apply-settings") {
    await requestJson(`${API_BASE}/settings`, {
      method: "PATCH",
      body: JSON.stringify(snapshot.settings),
    });
    return;
  }

  if (action === "apply-webhook") {
    await requestJson(`${API_BASE}/simulate-webhook`, {
      method: "POST",
      body: JSON.stringify({
        repositoryId: getSelectedRepositoryId(),
        ...uiState.webhookDraft,
      }),
    });
    return;
  }

  if (action === "load-preset") {
    const presetId = actionTarget.getAttribute("data-preset-id");
    const preset = QUICK_WEBHOOK_PRESETS.find((item) => item.id === presetId);
    if (!preset) {
      return;
    }
    uiState.webhookDraft = {
      ...uiState.webhookDraft,
      ...preset.draft,
    };
    uiState.feedbackMessage = "プリセット payload をフォームへ反映しました。";
    uiState.feedbackTone = "info";
    render();
    return;
  }

  if (action === "load-webhook-into-simulator") {
    const webhookId = toNumber(actionTarget.getAttribute("data-webhook-id"));
    if (webhookId === null) {
      return;
    }
    const webhookEvent = (snapshot?.webhookEvents ?? []).find((event) => event.id === webhookId);
    if (!webhookEvent) {
      return;
    }
    uiState.webhookDraft = buildWebhookDraftFromEvent(webhookEvent);
    uiState.activeMainTab = "simulator";
    uiState.feedbackMessage = `${webhookEvent.deliveryId} をシミュレータへ反映しました。再現しやすいように delivery_id は空欄にしています。`;
    uiState.feedbackTone = "info";
    render();
    return;
  }

  if (action === "toggle-webhook-payload") {
    const webhookId = toNumber(actionTarget.getAttribute("data-webhook-id"));
    if (webhookId === null) {
      return;
    }
    const wasExpanded = uiState.expandedWebhookPayloadIds.includes(webhookId);
    toggleExpandedWebhookPayload(webhookId);
    if (!wasExpanded && !uiState.webhookPayloadsByEventId[webhookId]) {
      await fetchWebhookPayload(webhookId);
      return;
    }
    render();
    return;
  }

  if (action === "select-branch") {
    uiState.selectedBranchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    uiState.activeMainTab = "branches";
    render();
    return;
  }

  if (action === "toggle-branch-row") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    if (branchId === null) {
      return;
    }
    uiState.selectedBranchId = branchId;
    uiState.activeMainTab = "branches";
    toggleExpandedBranch(branchId);
    render();
    return;
  }

  if (action === "jump-to-branch") {
    uiState.selectedBranchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    uiState.branchSearchInput = "";
    uiState.branchSearchQuery = "";
    uiState.branchSearchMode = "both";
    uiState.branchConflictOnly = false;
    uiState.branchStatusFilter = "all";
    uiState.pendingBranchScrollId = uiState.selectedBranchId;
    uiState.highlightedBranchId = uiState.selectedBranchId;
    uiState.activeMainTab = "branches";
    render();
    return;
  }

  if (action === "apply-branch-search") {
    uiState.branchSearchQuery = uiState.branchSearchInput.trim();
    render();
    return;
  }

  if (action === "select-conflict") {
    uiState.selectedConflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    uiState.activeMainTab = "conflicts";
    render();
    return;
  }

  if (action === "toggle-conflict-row") {
    const conflictId = toNumber(actionTarget.getAttribute("data-conflict-id"));
    const conflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    if (conflictId === null) {
      return;
    }
    uiState.selectedConflictKey = conflictKey;
    uiState.activeMainTab = "conflicts";
    toggleExpandedConflict(conflictId);
    render();
    return;
  }

  if (action === "jump-to-conflict") {
    uiState.selectedConflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    uiState.pendingConflictScrollKey = uiState.selectedConflictKey;
    uiState.highlightedConflictKey = uiState.selectedConflictKey;
    uiState.activeMainTab = "conflicts";
    render();
    return;
  }

  if (action === "toggle-conflict-branches") {
    const conflictId = toNumber(actionTarget.getAttribute("data-conflict-id"));
    const conflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    if (conflictId === null) {
      return;
    }
    uiState.selectedConflictKey = conflictKey || uiState.selectedConflictKey;
    toggleExpandedConflict(conflictId);
    render();
    return;
  }

  if (action === "toggle-branch-files") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    if (branchId === null) {
      return;
    }
    uiState.selectedBranchId = branchId;
    toggleExpandedBranch(branchId);
    render();
    return;
  }

  if (action === "toggle-excluded" || action === "delete-branch") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    const actionMap = {
      "toggle-excluded": "toggle-excluded",
      "delete-branch": "delete",
    };
    await requestJson(`${API_BASE}/branches/${branchId}/actions`, {
      method: "POST",
      body: JSON.stringify({ action: actionMap[action] }),
    });
    return;
  }

  if (action === "open-branch-file-ignore-modal") {
    uiState.branchFileIgnoreDialog = {
      isOpen: true,
      mode: actionTarget.getAttribute("data-ignore-mode") === "remove" ? "remove" : "create",
      ignoreId: toNumber(actionTarget.getAttribute("data-ignore-id")),
      branchId: toNumber(actionTarget.getAttribute("data-branch-id")),
      branchName: actionTarget.getAttribute("data-branch-name") ?? "",
      normalizedFilePath: actionTarget.getAttribute("data-file-path") ?? "",
      memo: actionTarget.getAttribute("data-ignore-memo") ?? "",
    };
    render();
    return;
  }

  if (action === "confirm-branch-file-ignore") {
    const dialog = uiState.branchFileIgnoreDialog;
    if (dialog.mode === "remove") {
      if (!dialog.branchId || !dialog.normalizedFilePath) {
        return;
      }
      closeBranchFileIgnoreDialog();
      await requestJson(`${API_BASE}/branch-file-ignores/remove`, {
        method: "POST",
        body: JSON.stringify({
          branchId: dialog.branchId,
          normalizedFilePath: dialog.normalizedFilePath,
        }),
      });
      return;
    }
    if (!dialog.branchId || !dialog.normalizedFilePath) {
      return;
    }
    const memoField = root.querySelector('[data-field="branchFileIgnoreMemo"]');
    const memo = memoField instanceof HTMLTextAreaElement ? memoField.value : dialog.memo;
    closeBranchFileIgnoreDialog();
    await requestJson(`${API_BASE}/branch-file-ignores`, {
      method: "POST",
      body: JSON.stringify({
        branchId: dialog.branchId,
        normalizedFilePath: dialog.normalizedFilePath,
        memo,
      }),
    });
    return;
  }

  if (action === "update-branch-file-ignore-memo") {
    const dialog = uiState.branchFileIgnoreDialog;
    if (!dialog.branchId || !dialog.normalizedFilePath) {
      return;
    }
    const memoField = root.querySelector('[data-field="branchFileIgnoreMemo"]');
    const memo = memoField instanceof HTMLTextAreaElement ? memoField.value : dialog.memo;
    uiState.branchFileIgnoreDialog = {
      ...dialog,
      memo,
    };
    await requestJson(`${API_BASE}/branch-file-ignores/memo`, {
      method: "PATCH",
      body: JSON.stringify({
        branchId: dialog.branchId,
        normalizedFilePath: dialog.normalizedFilePath,
        memo,
      }),
    });
    return;
  }

  if (action === "delete-conflict") {
    const conflictId = toNumber(actionTarget.getAttribute("data-conflict-id"));
    if (conflictId === null) {
      return;
    }
    await requestJson(`${API_BASE}/conflicts/${conflictId}/delete`, {
      method: "POST",
    });
    return;
  }

  if (action === "add-ignore-rule") {
    await requestJson(`${API_BASE}/ignore-rules`, {
      method: "POST",
      body: JSON.stringify({
        repositoryId: getSelectedRepositoryId(),
        pattern: uiState.newIgnorePattern,
      }),
    });
    uiState.newIgnorePattern = "";
    return;
  }

  if (action === "toggle-ignore-rule") {
    const ruleId = toNumber(actionTarget.getAttribute("data-rule-id"));
    await requestJson(`${API_BASE}/ignore-rules/${ruleId}/toggle`, {
      method: "POST",
    });
    return;
  }

  if (action === "reprocess-webhook") {
    const webhookId = toNumber(actionTarget.getAttribute("data-webhook-id"));
    await requestJson(`${API_BASE}/webhook-events/${webhookId}/reprocess`, {
      method: "POST",
    });
    return;
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (uiState.branchFileIgnoreDialog.isOpen) {
      closeBranchFileIgnoreDialog();
      return;
    }
    closeSideDrawer();
  }
});

document.addEventListener("click", (event) => {
  if (!uiState.highlightedConflictKey && uiState.highlightedBranchId === null) {
    return;
  }
  const eventPath = typeof event.composedPath === "function" ? event.composedPath() : [];
  if (eventPath.includes(root)) {
    return;
  }
  const target = event.target;
  if (!(target instanceof Node) || root.contains(target)) {
    return;
  }
  if (uiState.highlightedConflictKey) {
    uiState.highlightedConflictKey = "";
  }
  if (uiState.highlightedBranchId !== null) {
    uiState.highlightedBranchId = null;
  }
  render();
});

boot().catch((error) => {
  document.body.classList.remove("cw-drawer-open");
  document.body.classList.remove("cw-overlay-open");
  root.innerHTML = `<p class="notice error">${error.message}</p>`;
});
