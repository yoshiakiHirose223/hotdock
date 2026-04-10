import { QUICK_WEBHOOK_PRESETS, WEBHOOK_FORM_DEFAULTS } from "./constants.js?v=conflict-watch-20250410-09";
import { buildViewModel } from "./domain.js?v=conflict-watch-20250410-09";
import { renderConflictWatch } from "./view.js?v=conflict-watch-20250410-09";

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
  pageMode,
  isSideDrawerOpen: false,
  webhookDraft: { ...WEBHOOK_FORM_DEFAULTS },
  newIgnorePattern: "",
  newRepositoryName: "",
  newRepositoryExternalId: "",
  newRepositoryProvider: "github",
  pendingConflictScrollKey: "",
  pendingBranchScrollId: null,
  highlightedBranchId: null,
  highlightedConflictKey: "",
  branchFileIgnoreDialog: {
    isOpen: false,
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
}

function render() {
  if (!snapshot) {
    document.body.classList.remove("cw-drawer-open");
    document.body.classList.remove("cw-overlay-open");
    root.innerHTML = '<p class="notice">Conflict Watch を読み込んでいます...</p>';
    return;
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

function getSelectedConflict() {
  return (snapshot?.conflicts ?? []).find((conflict) => conflict.conflictKey === uiState.selectedConflictKey) ?? null;
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
    branchId: null,
    branchName: "",
    normalizedFilePath: "",
    memo: "",
  };
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

  if (action === "select-branch") {
    uiState.selectedBranchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    render();
    return;
  }

  if (action === "jump-to-branch") {
    uiState.selectedBranchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    uiState.pendingBranchScrollId = uiState.selectedBranchId;
    uiState.highlightedBranchId = uiState.selectedBranchId;
    render();
    return;
  }

  if (action === "select-conflict") {
    uiState.selectedConflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    render();
    return;
  }

  if (action === "jump-to-conflict") {
    uiState.selectedConflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    uiState.pendingConflictScrollKey = uiState.selectedConflictKey;
    uiState.highlightedConflictKey = uiState.selectedConflictKey;
    render();
    return;
  }

  if (action === "toggle-conflict-branches") {
    const conflictId = toNumber(actionTarget.getAttribute("data-conflict-id"));
    if (conflictId === null) {
      return;
    }
    if (uiState.expandedConflictIds.includes(conflictId)) {
      uiState.expandedConflictIds = uiState.expandedConflictIds.filter((id) => id !== conflictId);
    } else {
      uiState.expandedConflictIds = [...uiState.expandedConflictIds, conflictId];
    }
    render();
    return;
  }

  if (action === "toggle-branch-files") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    if (branchId === null) {
      return;
    }
    if (uiState.expandedBranchIds.includes(branchId)) {
      uiState.expandedBranchIds = uiState.expandedBranchIds.filter((id) => id !== branchId);
    } else {
      uiState.expandedBranchIds = [...uiState.expandedBranchIds, branchId];
    }
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

  if (action === "save-branch-memo") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    const memoField = root.querySelector(`[data-role="branch-memo"][data-branch-id="${CSS.escape(String(branchId))}"]`);
    const memo = memoField instanceof HTMLTextAreaElement ? memoField.value : "";
    await requestJson(`${API_BASE}/branches/${branchId}/memo`, {
      method: "PATCH",
      body: JSON.stringify({ memo }),
    });
    return;
  }

  if (action === "open-branch-file-ignore-modal") {
    uiState.branchFileIgnoreDialog = {
      isOpen: true,
      branchId: toNumber(actionTarget.getAttribute("data-branch-id")),
      branchName: actionTarget.getAttribute("data-branch-name") ?? "",
      normalizedFilePath: actionTarget.getAttribute("data-file-path") ?? "",
      memo: "",
    };
    render();
    return;
  }

  if (action === "confirm-branch-file-ignore") {
    const dialog = uiState.branchFileIgnoreDialog;
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

  if (action === "set-conflict-status") {
    const conflict = getSelectedConflict();
    if (!conflict) {
      return;
    }
    await requestJson(`${API_BASE}/conflicts/${conflict.id}/status`, {
      method: "PATCH",
      body: JSON.stringify({ status: actionTarget.getAttribute("data-status") }),
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

  if (action === "save-conflict-memo") {
    const conflict = getSelectedConflict();
    if (!conflict) {
      return;
    }
    const memoField = root.querySelector(`[data-role="conflict-memo"][data-conflict-key="${CSS.escape(conflict.conflictKey)}"]`);
    const memo = memoField instanceof HTMLTextAreaElement ? memoField.value : "";
    await requestJson(`${API_BASE}/conflicts/${conflict.id}/memo`, {
      method: "PATCH",
      body: JSON.stringify({ memo }),
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
