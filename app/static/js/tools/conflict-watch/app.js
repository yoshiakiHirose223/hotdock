import { QUICK_WEBHOOK_PRESETS, WEBHOOK_FORM_DEFAULTS } from "./constants.js";
import { buildViewModel } from "./domain.js";
import { renderConflictWatch } from "./view.js";

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
  pageMode,
  isSideDrawerOpen: false,
  webhookDraft: { ...WEBHOOK_FORM_DEFAULTS },
  newIgnorePattern: "",
  newRepositoryName: "",
  newRepositoryExternalId: "",
  newRepositoryProvider: "github",
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
  if (!branches.some((branch) => branch.id === uiState.selectedBranchId)) {
    uiState.selectedBranchId = branches[0]?.id ?? null;
  }

  const conflicts = (snapshot?.conflicts ?? []).filter((conflict) => conflict.repositoryId === uiState.selectedRepositoryId);
  if (!conflicts.some((conflict) => conflict.conflictKey === uiState.selectedConflictKey)) {
    uiState.selectedConflictKey = conflicts[0]?.conflictKey ?? "";
  }
}

function render() {
  if (!snapshot) {
    document.body.classList.remove("cw-drawer-open");
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
  renderConflictWatch(root, viewModel);
}

async function fetchState() {
  const response = await fetch(`${API_BASE}/state`, {
    credentials: "same-origin",
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
    return;
  }
  snapshot = payload.state;
  uiState.feedbackMessage = payload.message ?? "更新しました。";
  uiState.feedbackTone = payload.tone ?? "success";
  render();
}

async function requestJson(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {}),
    },
    ...options,
  });
  await applyResponse(response);
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

  const actionTarget = target.closest("[data-action]");
  if (!(actionTarget instanceof HTMLElement)) {
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

  if (action === "select-conflict") {
    uiState.selectedConflictKey = actionTarget.getAttribute("data-conflict-key") ?? "";
    render();
    return;
  }

  if (action === "toggle-excluded" || action === "merge-branch" || action === "delete-branch" || action === "reset-branch") {
    const branchId = toNumber(actionTarget.getAttribute("data-branch-id"));
    const actionMap = {
      "toggle-excluded": "toggle-excluded",
      "merge-branch": "merge",
      "delete-branch": "delete",
      "reset-branch": "reset",
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
    closeSideDrawer();
  }
});

boot().catch((error) => {
  document.body.classList.remove("cw-drawer-open");
  root.innerHTML = `<p class="notice error">${error.message}</p>`;
});
