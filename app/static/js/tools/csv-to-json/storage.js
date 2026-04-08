import { STORAGE_KEY } from "./constants.js";

export function loadPresetCollection() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }

    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .filter((entry) => entry && typeof entry.name === "string" && entry.config)
      .sort((left, right) => left.name.localeCompare(right.name, "ja"));
  } catch {
    return [];
  }
}

export function savePresetCollection(presets) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(presets));
}

export function upsertPreset(presets, name, config) {
  const next = presets.filter((preset) => preset.name !== name);
  next.push({
    name,
    config,
    savedAt: new Date().toISOString(),
  });
  return next.sort((left, right) => left.name.localeCompare(right.name, "ja"));
}

export function deletePreset(presets, name) {
  return presets.filter((preset) => preset.name !== name);
}
