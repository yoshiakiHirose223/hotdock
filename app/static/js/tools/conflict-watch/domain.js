import {
  BRANCH_STATUS_LABELS,
  CHANGE_TYPE_LABELS,
  CONFLICT_STATUS_LABELS,
  DEFAULT_SETTINGS,
} from "./constants.js?v=conflict-watch-20250410-21";

function deepClone(value) {
  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }
  return JSON.parse(JSON.stringify(value));
}

function addDays(isoValue, days) {
  const next = new Date(isoValue);
  next.setUTCDate(next.getUTCDate() + days);
  return next.toISOString();
}

function diffDays(fromIso, toIsoValue) {
  const diff = new Date(toIsoValue).getTime() - new Date(fromIso).getTime();
  return Math.floor(diff / 86400000);
}

export function normalizePath(filePath) {
  return String(filePath ?? "")
    .trim()
    .replaceAll("\\", "/")
    .replace(/^\.\//, "")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "");
}

function createConflictKey(repositoryId, normalizedFilePath) {
  return `${repositoryId}::${normalizedFilePath}`;
}

function createBranchFileKey(branchId, normalizedFilePath) {
  return `${branchId}::${normalizedFilePath}`;
}

function compareBranchUpdatedAt(left, right, order) {
  const leftTime = left.lastPushAt ? new Date(left.lastPushAt).getTime() : null;
  const rightTime = right.lastPushAt ? new Date(right.lastPushAt).getTime() : null;
  if (leftTime === null && rightTime === null) {
    return left.branchName.localeCompare(right.branchName);
  }
  if (leftTime === null) {
    return 1;
  }
  if (rightTime === null) {
    return -1;
  }
  if (leftTime !== rightTime) {
    return order === "updated_asc" ? leftTime - rightTime : rightTime - leftTime;
  }
  return left.branchName.localeCompare(right.branchName);
}

function filterDisplayedBranches(branches, ui) {
  const query = String(ui.branchSearchQuery ?? "").trim().toLowerCase();
  const searchMode = ui.branchSearchMode ?? "both";
  const statusFilter = ui.branchStatusFilter ?? "all";
  const conflictOnly = Boolean(ui.branchConflictOnly);
  const sortOrder = ui.branchSortOrder ?? "updated_desc";

  return branches
    .filter((branch) => {
      if (conflictOnly && branch.healthStatus !== "abnormal") {
        return false;
      }
      if (statusFilter !== "all" && branch.status !== statusFilter) {
        return false;
      }
      if (!query) {
        return true;
      }
      const branchMatched = branch.branchName.toLowerCase().includes(query);
      const fileMatched = branch.observedFiles.some((branchFile) => branchFile.normalizedFilePath.toLowerCase().includes(query));
      if (searchMode === "branch") {
        return branchMatched;
      }
      if (searchMode === "file") {
        return fileMatched;
      }
      return branchMatched || fileMatched;
    })
    .sort((left, right) => compareBranchUpdatedAt(left, right, sortOrder));
}

function nextId(state, counterKey, prefix) {
  const value = state.counters[counterKey];
  state.counters[counterKey] += 1;
  return `${prefix}-${value}`;
}

function nextSha(state, prefix = "sha") {
  state.counters.sha += 1;
  return `${prefix}-${state.counters.sha}`;
}

function nextDeliveryId(state, provider) {
  state.counters.delivery += 1;
  return `${provider}-delivery-${state.counters.delivery}`;
}

function makePayloadHash(event) {
  const seed = [
    event.providerType,
    event.repositoryExternalId,
    event.branchName,
    event.afterSha,
    event.filesAdded.join(","),
    event.filesModified.join(","),
    event.filesRemoved.join(","),
    event.filesRenamed.map((item) => `${item.oldPath}->${item.newPath}`).join(","),
  ].join("|");
  return `hash-${seed.replaceAll(/[^a-z0-9]+/gi, "-").toLowerCase()}`;
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function patternToRegex(pattern) {
  const normalized = normalizePath(pattern);
  if (!normalized) {
    return null;
  }
  const placeholder = "__double_star__";
  const escaped = escapeRegex(normalized)
    .replaceAll("\\*\\*", placeholder)
    .replaceAll("\\*", "[^/]*")
    .replaceAll(placeholder, ".*");
  return new RegExp(`^${escaped}$`, "i");
}

function isIgnoredFile(normalizedFilePath, ignoreRules) {
  return ignoreRules
    .filter((rule) => rule.isActive)
    .some((rule) => {
      const regex = patternToRegex(rule.pattern);
      return regex ? regex.test(normalizedFilePath) : false;
    });
}

function parseList(text) {
  return String(text ?? "")
    .split(/\r?\n/)
    .map((entry) => normalizePath(entry))
    .filter(Boolean);
}

function parseRenamed(text) {
  return String(text ?? "")
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [oldPath, newPath] = entry.split(/\s*->\s*/);
      if (!oldPath || !newPath) {
        return null;
      }
      return {
        oldPath: normalizePath(oldPath),
        newPath: normalizePath(newPath),
      };
    })
    .filter(Boolean);
}

function getSelectedRepository(state) {
  return state.repositories.find((repository) => repository.id === state.ui.selectedRepositoryId)
    ?? state.repositories[0]
    ?? null;
}

function ensureBranch(state, repositoryId, branchName, nowIso) {
  const existing = state.branches.find((branch) => (
    branch.repositoryId === repositoryId && branch.branchName === branchName
  ));
  if (existing) {
    return existing;
  }

  const branch = {
    id: nextId(state, "branch", "branch"),
    repositoryId,
    branchName,
    isMonitored: true,
    lastPushAt: nowIso,
    latestAfterSha: nextSha(state, "branch"),
    lastSeenAt: nowIso,
    isDeleted: false,
    isBranchExcluded: false,
    possiblyInconsistent: false,
    memo: "",
    monitoringClosedReason: null,
    monitoringClosedAt: null,
    createdAt: nowIso,
    updatedAt: nowIso,
  };
  state.branches.push(branch);
  return branch;
}

function pushConflictHistory(conflict, label, note, happenedAt) {
  conflict.history = Array.isArray(conflict.history) ? conflict.history : [];
  const lastEntry = conflict.history[conflict.history.length - 1];
  if (lastEntry && lastEntry.label === label && lastEntry.note === note) {
    return;
  }
  conflict.history.push({
    happenedAt,
    label,
    note,
  });
}

function upsertBranchFile(state, repositoryId, branchId, normalizedFilePath, changeType, nowIso, previousPath = null) {
  const existing = state.branchFiles.find((branchFile) => (
    branchFile.repositoryId === repositoryId
    && branchFile.branchId === branchId
    && branchFile.normalizedFilePath === normalizedFilePath
  ));

  if (existing) {
    existing.filePath = normalizedFilePath;
    existing.changeType = changeType;
    existing.lastSeenAt = nowIso;
    existing.updatedAt = nowIso;
    if (previousPath) {
      existing.previousPath = previousPath;
    }
    return existing;
  }

  const branchFile = {
    id: nextId(state, "branchFile", "branch-file"),
    repositoryId,
    branchId,
    filePath: normalizedFilePath,
    normalizedFilePath,
    changeType,
    firstSeenAt: nowIso,
    lastSeenAt: nowIso,
    updatedAt: nowIso,
  };
  if (previousPath) {
    branchFile.previousPath = previousPath;
  }
  state.branchFiles.push(branchFile);
  return branchFile;
}

function removeBranchFromState(state, branchId) {
  state.branchFileIgnores = state.branchFileIgnores.filter((item) => item.branchId !== branchId);
  state.branchFiles = state.branchFiles.filter((branchFile) => branchFile.branchId !== branchId);
  state.branches = state.branches.filter((branch) => branch.id !== branchId);
}

function computeBranchStatus(branch, settings, nowIso) {
  if (branch.isBranchExcluded) {
    return "branch_excluded";
  }
  if (!branch.lastSeenAt) {
    return "quiet";
  }
  const days = diffDays(branch.lastSeenAt, nowIso);
  if (days >= settings.staleDays) {
    return "stale";
  }
  if (days >= 7 || !branch.isMonitored) {
    return "quiet";
  }
  return "active";
}

function computeBranchConfidence(branch, settings, nowIso) {
  if (branch.possiblyInconsistent || branch.isDeleted) {
    return "low";
  }
  const days = branch.lastSeenAt ? diffDays(branch.lastSeenAt, nowIso) : settings.staleDays;
  if (days >= settings.staleDays) {
    return "low";
  }
  if (days >= 7 || branch.isBranchExcluded) {
    return "medium";
  }
  return "high";
}

function computeConflictConfidence(branches) {
  if (!branches.length) {
    return "low";
  }
  if (branches.some((branch) => branch.confidence === "low")) {
    return "low";
  }
  if (branches.some((branch) => branch.confidence === "medium")) {
    return "medium";
  }
  return "high";
}

function snapshotConflictBranches(branchIds, state, branchEntries) {
  return branchIds.map((branchId) => {
    const branch = state.branches.find((item) => item.id === branchId);
    const branchEntry = branchEntries.find((entry) => entry.branchId === branchId);
    if (!branch) {
      return null;
    }
    return {
      branchId: branch.id,
      branchName: branch.branchName,
      status: branch.status,
      lastPushAt: branch.lastPushAt ?? null,
      lastSeenAt: branch.lastSeenAt ?? null,
      changeType: branchEntry?.changeType ?? null,
      previousPath: branchEntry?.previousPath ?? null,
    };
  }).filter(Boolean);
}

function buildConflictGroups(state) {
  const branchMap = new Map(state.branches.map((branch) => [branch.id, branch]));
  const groups = new Map();

  state.branchFiles.forEach((branchFile) => {
    const branch = branchMap.get(branchFile.branchId);
    if (!branch || branch.isDeleted || !branch.isMonitored || branch.isBranchExcluded) {
      return;
    }
    if (isIgnoredFile(branchFile.normalizedFilePath, state.ignoreRules)) {
      return;
    }
    const key = createConflictKey(branchFile.repositoryId, branchFile.normalizedFilePath);
    const group = groups.get(key) ?? {
      repositoryId: branchFile.repositoryId,
      normalizedFilePath: branchFile.normalizedFilePath,
      branchEntries: [],
    };
    group.branchEntries.push({
      branchId: branch.id,
      changeType: branchFile.changeType,
      previousPath: branchFile.previousPath ?? null,
      lastSeenAt: branchFile.lastSeenAt,
    });
    groups.set(key, group);
  });

  return groups;
}

function appendNotification(state, conflict, notificationType, sentAt, status = "sent", errorMessage = null) {
  state.notifications.unshift({
    id: nextId(state, "notification", "notification"),
    repositoryId: conflict.repositoryId,
    conflictId: conflict.id,
    conflictKey: conflict.conflictKey,
    normalizedFilePath: conflict.normalizedFilePath,
    notificationType,
    destinationType: "slack",
    destinationValue: state.settings.notificationDestination,
    sentAt,
    status,
    errorMessage,
  });
}

function generateNotifications(state, previousMap, nextMap, nowIso) {
  nextMap.forEach((nextConflict, conflictKey) => {
    const previousConflict = previousMap.get(conflictKey);
    const currentCount = nextConflict.activeBranchIds.length;
    const previousCount = previousConflict?.activeBranchIds?.length ?? 0;

    if (!previousConflict && currentCount >= 2 && nextConflict.status !== "conflict_ignored") {
      appendNotification(state, nextConflict, "conflict_created", nowIso);
      return;
    }

    if (!previousConflict) {
      return;
    }

    if (previousConflict.status === "resolved" && nextConflict.status === "warning" && currentCount >= 2) {
      appendNotification(state, nextConflict, "conflict_reopened", nowIso);
      return;
    }

    if (currentCount > previousCount && nextConflict.status !== "conflict_ignored") {
      appendNotification(state, nextConflict, "conflict_scope_expanded", nowIso);
    }

    if (previousConflict.status !== nextConflict.status) {
      const isNoticeSuppressed = (
        state.settings.suppressNoticeNotifications
        && nextConflict.status === "notice"
      );
      if (!isNoticeSuppressed) {
        appendNotification(state, nextConflict, "conflict_status_changed", nowIso);
      }
    }

    const threshold = state.settings.longUnresolvedDays;
    const ageDays = diffDays(nextConflict.firstDetectedAt, nowIso);
    const currentBucket = nextConflict.status === "warning" || nextConflict.status === "notice"
      ? Math.floor(ageDays / threshold)
      : 0;
    const previousBucket = previousConflict.lastLongUnresolvedBucket ?? 0;

    if (currentBucket > previousBucket && currentBucket >= 1) {
      appendNotification(state, nextConflict, "long_unresolved", nowIso);
    }
  });
}

function reconcileConflicts(state, options = {}) {
  const nowIso = state.now;
  const previousConflicts = new Map(state.conflicts.map((conflict) => [conflict.conflictKey, deepClone(conflict)]));
  const previousMapForNotifications = new Map(state.conflicts.map((conflict) => [conflict.conflictKey, deepClone(conflict)]));
  const groups = buildConflictGroups(state);
  const nextConflicts = [];

  groups.forEach((group, conflictKey) => {
    if (group.branchEntries.length < 2) {
      return;
    }

    const existing = previousConflicts.get(conflictKey);
    const branchIds = [...new Set(group.branchEntries.map((entry) => entry.branchId))].sort();
    const activeBranches = branchIds
      .map((branchId) => state.branches.find((branch) => branch.id === branchId))
      .filter(Boolean);

    const conflict = existing ?? {
      id: nextId(state, "conflict", "conflict"),
      repositoryId: group.repositoryId,
      conflictKey,
      normalizedFilePath: group.normalizedFilePath,
      status: "warning",
      memo: "",
      firstDetectedAt: nowIso,
      lastDetectedAt: nowIso,
      resolvedAt: null,
      reopenedAt: null,
      ignoredAt: null,
      resolvedReason: null,
      lastLongUnresolvedBucket: 0,
      createdAt: nowIso,
      updatedAt: nowIso,
      history: [],
    };

    conflict.activeBranchIds = branchIds;
    conflict.branchEntries = group.branchEntries;
    conflict.lastDetectedAt = nowIso;
    conflict.updatedAt = nowIso;
    conflict.resolvedAt = null;
    conflict.resolvedReason = null;
    conflict.confidence = computeConflictConfidence(activeBranches);
    conflict.lastLongUnresolvedBucket = conflict.lastLongUnresolvedBucket ?? 0;
    conflict.lastRelatedBranches = snapshotConflictBranches(branchIds, state, group.branchEntries);

    if (!existing) {
      conflict.status = "warning";
      pushConflictHistory(conflict, "warning", "新しい競合を検知", nowIso);
    } else if (existing.status === "resolved") {
      conflict.status = "warning";
      conflict.reopenedAt = nowIso;
      pushConflictHistory(conflict, "warning", "resolved 済み conflict が再発", nowIso);
    }

    nextConflicts.push(conflict);
    previousConflicts.delete(conflictKey);
  });

  previousConflicts.forEach((existing) => {
    const conflict = existing;
    conflict.activeBranchIds = [];
    conflict.branchEntries = [];
    conflict.confidence = "low";

    if (conflict.status === "warning" || conflict.status === "notice") {
      conflict.status = "resolved";
      conflict.resolvedAt = nowIso;
      conflict.resolvedReason = options.resolutionReason ?? "other_observed_resolution";
      conflict.updatedAt = nowIso;
      pushConflictHistory(conflict, "resolved", `観測上解消 (${conflict.resolvedReason})`, nowIso);
    }

    nextConflicts.push(conflict);
  });

  nextConflicts.forEach((conflict) => {
    const threshold = state.settings.longUnresolvedDays;
    const ageDays = diffDays(conflict.firstDetectedAt, nowIso);
    if ((conflict.status === "warning" || conflict.status === "notice") && threshold > 0) {
      conflict.lastLongUnresolvedBucket = Math.max(
        conflict.lastLongUnresolvedBucket ?? 0,
        Math.floor(ageDays / threshold),
      );
    }
  });

  const nextMap = new Map(nextConflicts.map((conflict) => [conflict.conflictKey, conflict]));
  if (!options.suppressNotifications) {
    generateNotifications(state, previousMapForNotifications, nextMap, nowIso);
  }
  state.conflicts = nextConflicts.sort((left, right) => new Date(right.updatedAt) - new Date(left.updatedAt));
}

function reconcileBranches(state) {
  state.branches = state.branches.filter((branch) => !branch.isDeleted);
  state.branches.forEach((branch) => {
    branch.status = computeBranchStatus(branch, state.settings, state.now);
    branch.confidence = computeBranchConfidence(branch, state.settings, state.now);
    branch.updatedAt = branch.updatedAt ?? state.now;
  });
}

function applyWebhookRetentionPolicy(state) {
  state.webhookEvents.forEach((event) => {
    if (!event.rawPayloadRef) {
      return;
    }
    const ageDays = diffDays(event.receivedAt, state.now);
    if (ageDays < state.settings.rawPayloadRetentionDays) {
      return;
    }
    event.rawPayloadRef = null;
    event.rawPayloadExpiredAt = event.rawPayloadExpiredAt ?? state.now;
  });
}

function syncUiSelection(state) {
  const selectedRepository = getSelectedRepository(state);
  if (!selectedRepository && state.repositories.length) {
    state.ui.selectedRepositoryId = state.repositories[0].id;
  }

  const repositoryId = getSelectedRepository(state)?.id;
  if (!state.conflicts.some((conflict) => (
    conflict.conflictKey === state.ui.selectedConflictKey && conflict.repositoryId === repositoryId
  ))) {
    state.ui.selectedConflictKey = state.conflicts.find((conflict) => conflict.repositoryId === repositoryId)?.conflictKey ?? "";
  }

  if (!state.branches.some((branch) => (
    branch.id === state.ui.selectedBranchId && branch.repositoryId === repositoryId
  ))) {
    state.ui.selectedBranchId = state.branches.find((branch) => branch.repositoryId === repositoryId)?.id ?? "";
  }
}

function reconcileState(rawState, options = {}) {
  const state = rawState;
  state.settings = { ...DEFAULT_SETTINGS, ...state.settings };
  applyWebhookRetentionPolicy(state);
  reconcileBranches(state);
  reconcileConflicts(state, options);
  reconcileBranches(state);
  syncUiSelection(state);
  return state;
}

function recordSecurityLog(state, payload) {
  state.securityLogs.unshift({
    id: nextId(state, "securityLog", "security-log"),
    providerType: payload.providerType,
    deliveryId: payload.deliveryId,
    repositoryExternalId: payload.repositoryExternalId,
    branchName: payload.branchName,
    receivedAt: state.now,
    statusCode: payload.statusCode,
    reason: payload.reason,
  });
}

function buildEventFromDraft(state, repository) {
  const draft = state.ui.webhookDraft;
  const deliveryId = draft.deliveryId.trim() || nextDeliveryId(state, draft.provider);

  const event = {
    id: nextId(state, "webhook", "webhook"),
    repositoryId: repository.id,
    providerType: draft.provider,
    deliveryId,
    eventType: "push",
    repositoryExternalId: repository.externalRepoId,
    branchName: draft.branchName.trim(),
    beforeSha: nextSha(state, "before"),
    afterSha: nextSha(state, "after"),
    receivedAt: state.now,
    processedAt: null,
    processStatus: "queued",
    payloadHash: "",
    rawPayloadRef: `payloads/${draft.provider}/${draft.branchName.trim().replaceAll("/", "_")}.json`,
    errorMessage: null,
    pusher: draft.pusher.trim() || null,
    pushedAt: state.now,
    isDeleted: draft.deletedState === "unknown" ? null : draft.deletedState === "true",
    isForced: Boolean(draft.isForced),
    filesAdded: parseList(draft.added),
    filesModified: parseList(draft.modified),
    filesRemoved: parseList(draft.removed),
    filesRenamed: parseRenamed(draft.renamed),
  };
  event.payloadHash = makePayloadHash(event);
  return event;
}

function applyEventToBranches(state, event) {
  const repository = state.repositories.find((item) => item.id === event.repositoryId)
    ?? state.repositories.find((item) => item.externalRepoId === event.repositoryExternalId);
  if (!repository) {
    return false;
  }

  const branch = ensureBranch(state, repository.id, event.branchName, state.now);
  branch.lastPushAt = event.pushedAt ?? state.now;
  branch.lastSeenAt = state.now;
  branch.latestAfterSha = event.afterSha;
  branch.updatedAt = state.now;
  branch.isDeleted = false;
  branch.isMonitored = true;
  branch.monitoringClosedReason = null;
  branch.monitoringClosedAt = null;
  if (event.isForced) {
    branch.possiblyInconsistent = true;
  }

  if (event.isDeleted === true) {
    removeBranchFromState(state, branch.id);
    return true;
  }

  event.filesAdded.forEach((filePath) => {
    upsertBranchFile(state, repository.id, branch.id, filePath, "added", state.now);
  });
  event.filesModified.forEach((filePath) => {
    upsertBranchFile(state, repository.id, branch.id, filePath, "modified", state.now);
  });
  event.filesRemoved.forEach((filePath) => {
    upsertBranchFile(state, repository.id, branch.id, filePath, "removed", state.now);
  });
  event.filesRenamed.forEach((renamePair) => {
    upsertBranchFile(state, repository.id, branch.id, renamePair.newPath, "renamed", state.now, renamePair.oldPath);
  });

  return true;
}

export function initializeState(rawState) {
  return reconcileState(deepClone(rawState), { suppressNotifications: true });
}

export function buildViewModel(rawState) {
  const state = rawState;
  const selectedRepository = getSelectedRepository(state);
  const repositoryId = selectedRepository?.id ?? "";
  const branchFileIgnoreByKey = new Map(
    (state.branchFileIgnores ?? [])
      .filter((item) => item.isActive)
      .map((item) => [createBranchFileKey(item.branchId, item.normalizedFilePath), item]),
  );
  const conflictByBranchFileKey = new Map();
  state.conflicts
    .filter((conflict) => (
      conflict.repositoryId === repositoryId
      && (conflict.status === "warning" || conflict.status === "notice")
    ))
    .forEach((conflict) => {
      (conflict.activeBranchIds ?? []).forEach((branchId) => {
        conflictByBranchFileKey.set(
          createBranchFileKey(branchId, conflict.normalizedFilePath),
          conflict,
        );
      });
    });

  const branchFileCounts = new Map();
  const branchObservedFiles = new Map();
  state.branches.forEach((branch) => {
    branchFileCounts.set(branch.id, { visible: 0, ignored: 0 });
    branchObservedFiles.set(branch.id, []);
  });

  state.branchFiles.forEach((branchFile) => {
    const counts = branchFileCounts.get(branchFile.branchId) ?? { visible: 0, ignored: 0 };
    const files = branchObservedFiles.get(branchFile.branchId) ?? [];
    const repositoryIgnored = isIgnoredFile(branchFile.normalizedFilePath, state.ignoreRules);
    if (repositoryIgnored) {
      counts.ignored += 1;
    } else {
      counts.visible += 1;
      const branchFileIgnore = branchFileIgnoreByKey.get(
        createBranchFileKey(branchFile.branchId, branchFile.normalizedFilePath),
      );
      const activeConflict = conflictByBranchFileKey.get(
        createBranchFileKey(branchFile.branchId, branchFile.normalizedFilePath),
      );
      files.push({
        ...branchFile,
        ignored: false,
        branchFileIgnoreId: branchFileIgnore?.id ?? null,
        isBranchFileIgnored: Boolean(branchFileIgnore),
        branchFileIgnoreMemo: branchFileIgnore?.memo ?? "",
        activeConflictKey: activeConflict?.conflictKey ?? "",
        activeConflictStatus: activeConflict?.status ?? null,
        isInConflict: Boolean(activeConflict),
      });
    }
    branchFileCounts.set(branchFile.branchId, counts);
    branchObservedFiles.set(branchFile.branchId, files);
  });

  const allBranches = [...state.branches]
    .filter((branch) => branch.repositoryId === repositoryId && !branch.isDeleted)
    .map((branch) => {
      const observedFiles = (branchObservedFiles.get(branch.id) ?? [])
        .slice()
        .sort((left, right) => left.normalizedFilePath.localeCompare(right.normalizedFilePath));
      const hasConflict = observedFiles.some((branchFile) => branchFile.isInConflict);
      return {
        ...branch,
        observedFileCount: branchFileCounts.get(branch.id)?.visible ?? 0,
        ignoredFileCount: branchFileCounts.get(branch.id)?.ignored ?? 0,
        observedFiles,
        healthStatus: hasConflict ? "abnormal" : "normal",
        healthLabel: hasConflict ? "競合" : "正常",
        isFileListOpen: (state.ui.expandedBranchIds ?? []).includes(branch.id),
        repositoryName: selectedRepository?.repositoryName ?? "",
      };
    });
  const branches = filterDisplayedBranches(allBranches, state.ui);

  const selectedConflict = state.conflicts.find((conflict) => (
    conflict.conflictKey === state.ui.selectedConflictKey && conflict.repositoryId === repositoryId
  )) ?? state.conflicts.find((conflict) => conflict.repositoryId === repositoryId) ?? null;

  const selectedConflictBranches = selectedConflict
    ? (selectedConflict.activeBranchIds ?? []).map((branchId) => {
      const branch = state.branches.find((candidate) => candidate.id === branchId);
      if (!branch) {
        return null;
      }
      const branchEntry = selectedConflict.branchEntries?.find((entry) => entry.branchId === branchId);
      return {
        ...branch,
        changeType: branchEntry?.changeType ?? null,
        previousPath: branchEntry?.previousPath ?? null,
      };
    }).filter(Boolean)
    : [];

  const selectedNotifications = selectedConflict
    ? state.notifications.filter((notification) => notification.conflictKey === selectedConflict.conflictKey)
    : [];

  const selectedBranch = branches.find((branch) => branch.id === state.ui.selectedBranchId) ?? branches[0] ?? null;

  const selectedBranchFiles = selectedBranch
    ? state.branchFiles
      .filter((branchFile) => branchFile.branchId === selectedBranch.id)
      .sort((left, right) => left.normalizedFilePath.localeCompare(right.normalizedFilePath))
      .map((branchFile) => ({
        ...branchFile,
        ignored: isIgnoredFile(branchFile.normalizedFilePath, state.ignoreRules),
        isBranchFileIgnored: Boolean(
          branchFileIgnoreByKey.get(createBranchFileKey(branchFile.branchId, branchFile.normalizedFilePath)),
        ),
        branchFileIgnoreMemo:
          branchFileIgnoreByKey.get(createBranchFileKey(branchFile.branchId, branchFile.normalizedFilePath))?.memo ?? "",
      }))
    : [];

  const selectedBranchEvents = selectedBranch
    ? state.webhookEvents.filter((event) => (
      (event.repositoryId === repositoryId || event.repositoryExternalId === selectedRepository?.externalRepoId)
      && event.branchName === selectedBranch.branchName
    ))
    : [];

  const conflicts = state.conflicts
    .filter((conflict) => conflict.repositoryId === repositoryId)
    .map((conflict) => {
      const notifications = state.notifications.filter((item) => item.conflictKey === conflict.conflictKey);
      const activeRelatedBranches = (conflict.activeBranchIds ?? []).map((branchId) => {
        const branch = state.branches.find((item) => item.id === branchId);
        const branchEntry = conflict.branchEntries?.find((entry) => entry.branchId === branchId);
        if (!branch) {
          return null;
        }
        return {
          id: branch.id,
          branchName: branch.branchName,
          status: branch.status,
          lastPushAt: branch.lastPushAt ?? null,
          changeType: branchEntry?.changeType ?? null,
          previousPath: branchEntry?.previousPath ?? null,
          isNavigable: true,
        };
      }).filter(Boolean);
      const resolvedRelatedBranches = (conflict.lastRelatedBranches ?? []).map((entry) => {
        const branch = state.branches.find((item) => item.id === entry.branchId);
        return {
          id: branch?.id ?? entry.branchId ?? null,
          branchName: branch?.branchName ?? entry.branchName ?? "",
          status: branch?.status ?? entry.status ?? null,
          lastPushAt: branch?.lastPushAt ?? entry.lastPushAt ?? entry.lastSeenAt ?? null,
          changeType: entry.changeType ?? null,
          previousPath: entry.previousPath ?? null,
          isNavigable: Boolean(branch),
          isDeletedSnapshot: !branch,
        };
      });
      const relatedBranches = activeRelatedBranches.length > 0 ? activeRelatedBranches : resolvedRelatedBranches;
      const hasResolvedDetails = conflict.status === "resolved"
        && Boolean(conflict.resolvedAt || conflict.resolvedReason || (conflict.history?.length ?? 0) > 0);
      return {
        ...conflict,
        relatedBranches,
        relatedBranchCount: relatedBranches.length,
        hasResolvedDetails,
        canExpand: relatedBranches.length > 0 || hasResolvedDetails,
        isBranchListOpen: (relatedBranches.length > 0 || hasResolvedDetails)
          && (state.ui.expandedConflictIds ?? []).includes(conflict.id),
        notificationCount: notifications.length,
        lastNotificationType: notifications[0]?.notificationType ?? null,
      };
    });

  const warningCount = conflicts.filter((conflict) => conflict.status === "warning").length;
  const noticeCount = conflicts.filter((conflict) => conflict.status === "notice").length;
  const ignoredCount = conflicts.filter((conflict) => conflict.status === "conflict_ignored").length;
  const activeBranches = allBranches.filter((branch) => branch.status === "active").length;
  const staleBranches = allBranches.filter((branch) => branch.status === "stale").length;
  const longUnresolvedCount = conflicts.filter((conflict) => (
    (conflict.status === "warning" || conflict.status === "notice")
    && diffDays(conflict.firstDetectedAt, state.now) >= state.settings.longUnresolvedDays
  )).length;
  const recentNotifications = state.notifications
    .filter((notification) => {
      if (notification.repositoryId === repositoryId) {
        return true;
      }
      const conflict = state.conflicts.find((item) => item.conflictKey === notification.conflictKey);
      return conflict?.repositoryId === repositoryId;
    })
    .slice(0, 12);
  const fallbackNotifications = recentNotifications.length
    ? recentNotifications
    : conflicts
      .filter((conflict) => conflict.status === "warning" || conflict.status === "notice")
      .slice(0, 12)
      .map((conflict) => ({
        id: `synthetic-${conflict.id}`,
        repositoryId: conflict.repositoryId,
        conflictId: conflict.id,
        conflictKey: conflict.conflictKey,
        normalizedFilePath: conflict.normalizedFilePath,
        notificationType: "conflict_created",
        destinationType: "slack",
        destinationValue: state.settings.notificationDestination,
        sentAt: conflict.lastDetectedAt ?? conflict.updatedAt ?? state.now,
        status: "sent",
        errorMessage: null,
      }));

  return {
    repositories: state.repositories,
    selectedRepository,
    branches,
    allBranchCount: allBranches.length,
    conflicts,
    selectedConflict,
    selectedConflictBranches,
    selectedNotifications,
    selectedBranch,
    selectedBranchFiles,
    selectedBranchEvents,
    webhookEvents: state.webhookEvents
      .filter((event) => event.repositoryId === repositoryId || event.repositoryExternalId === selectedRepository?.externalRepoId)
      .slice(0, 12),
    recentNotifications: fallbackNotifications,
    securityLogs: state.securityLogs
      .filter((log) => log.repositoryExternalId === selectedRepository?.externalRepoId)
      .slice(0, 12),
    ignoreRules: state.ignoreRules.filter((rule) => rule.repositoryId === repositoryId),
    settings: state.settings,
    ui: state.ui,
    branchSummary: {
      displayedCount: branches.length,
      totalCount: allBranches.length,
      hasActiveFilters: Boolean(
        (state.ui.branchSearchQuery ?? "").trim()
        || state.ui.branchConflictOnly
        || (state.ui.branchStatusFilter ?? "all") !== "all",
      ),
    },
    dashboard: {
      repositories: state.repositories.filter((repository) => repository.isActive).length,
      activeBranches,
      staleBranches,
      warningCount,
      noticeCount,
      ignoredCount,
      longUnresolvedCount,
    },
  };
}

export function updateWebhookDraft(rawState, patch) {
  const state = deepClone(rawState);
  state.ui.webhookDraft = {
    ...state.ui.webhookDraft,
    ...patch,
  };
  return state;
}

export function loadWebhookPreset(rawState, draft) {
  const state = deepClone(rawState);
  state.ui.webhookDraft = {
    ...state.ui.webhookDraft,
    ...draft,
  };
  state.ui.feedbackMessage = "プリセット payload をフォームへ反映しました。";
  state.ui.feedbackTone = "info";
  return state;
}

export function updateConflictMemo(rawState, conflictKey, memo) {
  const state = deepClone(rawState);
  const conflict = state.conflicts.find((item) => item.conflictKey === conflictKey);
  if (!conflict) {
    return state;
  }
  conflict.memo = memo.trim();
  conflict.updatedAt = state.now;
  state.ui.feedbackMessage = "conflict memo を更新しました。";
  state.ui.feedbackTone = "success";
  return state;
}

export function updateConflictStatus(rawState, conflictKey, nextStatus) {
  const state = deepClone(rawState);
  const conflict = state.conflicts.find((item) => item.conflictKey === conflictKey);
  if (!conflict || !CONFLICT_STATUS_LABELS[nextStatus]) {
    return state;
  }
  if (nextStatus === "resolved" && (conflict.activeBranchIds?.length ?? 0) >= 2) {
    state.ui.feedbackMessage = "resolved は監視対象 branch が 2 未満になったときだけ確定します。branch 側の削除や除外で監視対象を減らしてください。";
    state.ui.feedbackTone = "warning";
    return state;
  }

  conflict.status = nextStatus;
  conflict.updatedAt = state.now;
  if (nextStatus === "conflict_ignored") {
    conflict.ignoredAt = state.now;
  }
  pushConflictHistory(conflict, nextStatus, `手動で ${nextStatus} へ変更`, state.now);
  state.ui.feedbackMessage = `conflict status を ${nextStatus} へ更新しました。`;
  state.ui.feedbackTone = "success";
  return reconcileState(state);
}

export function selectConflict(rawState, conflictKey) {
  const state = deepClone(rawState);
  state.ui.selectedConflictKey = conflictKey;
  return state;
}

export function selectBranch(rawState, branchId) {
  const state = deepClone(rawState);
  state.ui.selectedBranchId = branchId;
  return state;
}

export function selectRepository(rawState, repositoryId) {
  const state = deepClone(rawState);
  state.ui.selectedRepositoryId = repositoryId;
  state.ui.feedbackMessage = "";
  return reconcileState(state, { suppressNotifications: true });
}

export function updateSettings(rawState, patch) {
  const state = deepClone(rawState);
  state.settings = {
    ...state.settings,
    ...patch,
  };
  state.ui.feedbackMessage = "設定を更新しました。";
  state.ui.feedbackTone = "success";
  return reconcileState(state);
}

export function updateNewIgnorePattern(rawState, value) {
  const state = deepClone(rawState);
  state.ui.newIgnorePattern = value;
  return state;
}

export function addIgnoreRule(rawState) {
  const state = deepClone(rawState);
  const repository = getSelectedRepository(state);
  const pattern = normalizePath(state.ui.newIgnorePattern);
  if (!repository || !pattern) {
    state.ui.feedbackMessage = "ignore rule に追加する pattern を入力してください。";
    state.ui.feedbackTone = "warning";
    return state;
  }
  state.ignoreRules.unshift({
    id: nextId(state, "ignoreRule", "ignore-rule"),
    repositoryId: repository.id,
    ruleType: "path_pattern",
    pattern,
    isActive: true,
    createdAt: state.now,
    updatedAt: state.now,
  });
  state.ui.newIgnorePattern = "";
  state.ui.feedbackMessage = `ignore rule を追加しました: ${pattern}`;
  state.ui.feedbackTone = "success";
  return reconcileState(state);
}

export function toggleIgnoreRule(rawState, ruleId) {
  const state = deepClone(rawState);
  const rule = state.ignoreRules.find((item) => item.id === ruleId);
  if (!rule) {
    return state;
  }
  rule.isActive = !rule.isActive;
  rule.updatedAt = state.now;
  state.ui.feedbackMessage = `ignore rule を ${rule.isActive ? "有効" : "無効"} にしました。`;
  state.ui.feedbackTone = "success";
  return reconcileState(state, {
    resolutionReason: "other_observed_resolution",
  });
}

export function advanceClock(rawState, days) {
  const state = deepClone(rawState);
  state.now = addDays(state.now, days);
  state.ui.feedbackMessage = `仮想時刻を ${days > 0 ? "+" : ""}${days} 日進めました。`;
  state.ui.feedbackTone = "info";
  return reconcileState(state);
}

export function resetDemo(rawState, createState) {
  const nextState = initializeState(createState());
  nextState.ui.feedbackMessage = "デモ状態を初期スナップショットへ戻しました。";
  nextState.ui.feedbackTone = "success";
  return nextState;
}

export function updateBranchMemo(rawState, branchId, memo) {
  const state = deepClone(rawState);
  const branch = state.branches.find((item) => item.id === branchId);
  if (!branch) {
    return state;
  }
  branch.memo = memo.trim();
  branch.updatedAt = state.now;
  state.ui.feedbackMessage = `${branch.branchName} の memo を更新しました。`;
  state.ui.feedbackTone = "success";
  return state;
}

export function updateRepositoryDraft(rawState, patch) {
  const state = deepClone(rawState);
  state.ui = {
    ...state.ui,
    ...patch,
  };
  return state;
}

export function addRepository(rawState) {
  const state = deepClone(rawState);
  const repositoryName = state.ui.newRepositoryName.trim();
  const externalRepoId = state.ui.newRepositoryExternalId.trim();
  const providerType = state.ui.newRepositoryProvider;

  if (!repositoryName || !externalRepoId) {
    state.ui.feedbackMessage = "repository_name と external_repo_id を入力してください。";
    state.ui.feedbackTone = "warning";
    return state;
  }

  const exists = state.repositories.some((repository) => repository.externalRepoId === externalRepoId);
  if (exists) {
    state.ui.feedbackMessage = "同じ external_repo_id の repository は既に存在します。";
    state.ui.feedbackTone = "warning";
    return state;
  }

  const repository = {
    id: nextId(state, "repository", "repo"),
    providerType,
    externalRepoId,
    repositoryName,
    isActive: true,
    createdAt: state.now,
    updatedAt: state.now,
  };
  state.repositories.push(repository);
  state.ui.selectedRepositoryId = repository.id;
  state.ui.newRepositoryName = "";
  state.ui.newRepositoryExternalId = "";
  state.ui.newRepositoryProvider = providerType;
  state.ui.feedbackMessage = `repository を追加しました: ${repositoryName}`;
  state.ui.feedbackTone = "success";
  return reconcileState(state, { suppressNotifications: true });
}

export function toggleRepositoryActive(rawState, repositoryId) {
  const state = deepClone(rawState);
  const repository = state.repositories.find((item) => item.id === repositoryId);
  if (!repository) {
    return state;
  }
  repository.isActive = !repository.isActive;
  repository.updatedAt = state.now;
  state.ui.feedbackMessage = `${repository.repositoryName} を ${repository.isActive ? "有効" : "無効"} にしました。`;
  state.ui.feedbackTone = "success";
  return reconcileState(state, { suppressNotifications: true });
}

export function applyBranchAction(rawState, branchId, action) {
  const state = deepClone(rawState);
  const branch = state.branches.find((item) => item.id === branchId);
  if (!branch) {
    return state;
  }

  if (action === "toggle-excluded") {
    branch.isBranchExcluded = !branch.isBranchExcluded;
    branch.updatedAt = state.now;
    state.ui.feedbackMessage = `${branch.branchName} を ${branch.isBranchExcluded ? "branch_excluded" : "監視対象"} にしました。`;
    state.ui.feedbackTone = "success";
    return reconcileState(state, { resolutionReason: "branch_excluded" });
  }

  if (action === "merge") {
    branch.isMonitored = false;
    branch.monitoringClosedReason = "merged_to_main_or_master";
    branch.monitoringClosedAt = state.now;
    branch.updatedAt = state.now;
    state.branchFiles = state.branchFiles.filter((branchFile) => branchFile.branchId !== branch.id);
    state.ui.feedbackMessage = `${branch.branchName} を main/master マージ扱いでクローズしました。`;
    state.ui.feedbackTone = "success";
    return reconcileState(state, { resolutionReason: "merged_to_main_or_master" });
  }

  if (action === "delete") {
    removeBranchFromState(state, branch.id);
    state.ui.feedbackMessage = `${branch.branchName} を一覧から削除しました。`;
    state.ui.feedbackTone = "success";
    return reconcileState(state, { resolutionReason: "branch_deleted" });
  }

  if (action === "reset") {
    branch.possiblyInconsistent = false;
    branch.lastSeenAt = null;
    branch.updatedAt = state.now;
    state.branchFiles = state.branchFiles.filter((branchFile) => branchFile.branchId !== branch.id);
    state.ui.feedbackMessage = `${branch.branchName} の branch_files を手動リセットしました。`;
    state.ui.feedbackTone = "success";
    return reconcileState(state, { resolutionReason: "manual_reset" });
  }

  return state;
}

export function applyWebhookDraft(rawState) {
  const state = deepClone(rawState);
  const repository = getSelectedRepository(state);
  const draft = state.ui.webhookDraft;

  if (!repository || !repository.isActive) {
    state.ui.feedbackMessage = "Webhook を適用する前に active な repository を選択してください。";
    state.ui.feedbackTone = "warning";
    return state;
  }
  if (!draft.branchName.trim()) {
    state.ui.feedbackMessage = "Webhook を適用する branch 名を入力してください。";
    state.ui.feedbackTone = "warning";
    return state;
  }

  const desiredDeliveryId = draft.deliveryId.trim();
  if (draft.signatureStatus !== "valid") {
    recordSecurityLog(state, {
      providerType: draft.provider,
      deliveryId: desiredDeliveryId || nextDeliveryId(state, draft.provider),
      repositoryExternalId: repository.externalRepoId,
      branchName: draft.branchName.trim(),
      statusCode: 401,
      reason: "署名検証に失敗したため queue に積まず破棄",
    });
    state.ui.feedbackMessage = "署名検証に失敗したため security log に記録し、branch 状態は更新していません。";
    state.ui.feedbackTone = "warning";
    return reconcileState(state, { suppressNotifications: true });
  }

  if (desiredDeliveryId) {
    const duplicate = state.webhookEvents.find((event) => (
      event.providerType === draft.provider
      && event.deliveryId === desiredDeliveryId
    ));
    if (duplicate) {
      state.ui.feedbackMessage = `delivery_id ${desiredDeliveryId} は既に処理済みです。冪等性により再処理をスキップしました。`;
      state.ui.feedbackTone = "warning";
      return state;
    }
  }

  const event = buildEventFromDraft(state, repository);
  state.webhookEvents.unshift(event);

  if (draft.simulateFailure) {
    event.processStatus = "processing_failed";
    event.processedAt = state.now;
    event.errorMessage = "worker が provider 共通形式への正規化中に失敗しました。";
    state.ui.feedbackMessage = "Webhook は登録しましたが、非同期処理で failed にしました。イベント一覧から再処理できます。";
    state.ui.feedbackTone = "warning";
    return reconcileState(state, { suppressNotifications: true });
  }

  applyEventToBranches(state, event);
  event.processStatus = "processed";
  event.processedAt = state.now;
  state.ui.feedbackMessage = `${event.branchName} へ Webhook を適用しました。`;
  state.ui.feedbackTone = "success";
  return reconcileState(state, {
    resolutionReason: event.isDeleted === true ? "branch_deleted" : "other_observed_resolution",
  });
}

export function reprocessWebhookEvent(rawState, eventId) {
  const state = deepClone(rawState);
  const event = state.webhookEvents.find((item) => item.id === eventId);
  if (!event) {
    return state;
  }
  if (event.processStatus !== "processing_failed") {
    state.ui.feedbackMessage = "reprocess できるのは processing_failed の event のみです。";
    state.ui.feedbackTone = "warning";
    return state;
  }
  if (!event.rawPayloadRef) {
    state.ui.feedbackMessage = "raw payload の保持期限が切れているため再処理できません。";
    state.ui.feedbackTone = "warning";
    return state;
  }

  event.errorMessage = null;
  event.processStatus = "processed";
  event.processedAt = state.now;
  event.pushedAt = state.now;
  applyEventToBranches(state, event);
  state.ui.feedbackMessage = `${event.deliveryId} を raw payload から再処理しました。`;
  state.ui.feedbackTone = "success";
  return reconcileState(state, {
    resolutionReason: event.isDeleted === true ? "branch_deleted" : "other_observed_resolution",
  });
}

export const LABELS = {
  branch: BRANCH_STATUS_LABELS,
  conflict: CONFLICT_STATUS_LABELS,
  change: CHANGE_TYPE_LABELS,
};
