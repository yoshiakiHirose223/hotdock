function createId(prefix) {
  return `${prefix}-${crypto.randomUUID()}`;
}

export function createInitialState(presets = []) {
  return {
    headerEnabled: true,
    parsed: null,
    outputColumns: [],
    rules: [],
    presets,
    selectedPresetName: presets[0]?.name ?? "",
    loadMessages: [],
    presetMessage: null,
    presetMessageType: null,
    conversion: {
      status: "idle",
      errors: [],
      progressText: "",
      downloadUrl: "",
      downloadName: "",
      rowCount: 0,
    },
  };
}

export function createStore(initialState) {
  let state = initialState;
  const listeners = new Set();

  return {
    getState() {
      return state;
    },
    setState(updater) {
      state = typeof updater === "function" ? updater(state) : updater;
      listeners.forEach((listener) => listener(state));
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

export function createOutputColumnFromSource(sourceColumn) {
  return {
    id: createId("column"),
    key: sourceColumn.initialKey,
    sourceType: "csv",
    sourceColumnIndex: sourceColumn.index,
    fixedValue: "",
    fixedValueType: "string",
  };
}

export function createManualOutputColumn(sequence, sourceColumns) {
  return {
    id: createId("column"),
    key: `field${sequence + 1}`,
    sourceType: sourceColumns.length ? "csv" : "fixed",
    sourceColumnIndex: sourceColumns[0]?.index ?? 0,
    fixedValue: "",
    fixedValueType: "string",
  };
}

export function createConditionRule(sourceColumns, outputColumns) {
  return {
    id: createId("rule"),
    sourceColumnIndex: sourceColumns[0]?.index ?? 0,
    conditionType: "contains",
    compareValue: "",
    targetKey: outputColumns[0]?.key ?? "",
    value: "",
    valueType: "string",
  };
}

export function hydrateOutputColumns(savedColumns, sourceColumns) {
  if (!Array.isArray(savedColumns) || !savedColumns.length) {
    return sourceColumns.map(createOutputColumnFromSource);
  }

  return savedColumns.map((column, index) => ({
    id: createId("column"),
    key: typeof column.key === "string" ? column.key : `field${index + 1}`,
    sourceType: column.sourceType === "fixed" || column.sourceType === "custom" ? column.sourceType : "csv",
    sourceColumnIndex: Number.isInteger(column.sourceColumnIndex) ? column.sourceColumnIndex : 0,
    fixedValue: typeof column.fixedValue === "string" ? column.fixedValue : "",
    fixedValueType: typeof column.fixedValueType === "string" ? column.fixedValueType : "string",
  }));
}

export function hydrateRules(savedRules, sourceColumns, outputColumns) {
  if (!Array.isArray(savedRules) || !savedRules.length) {
    return [];
  }

  return savedRules.map((rule) => ({
    id: createId("rule"),
    sourceColumnIndex: Number.isInteger(rule.sourceColumnIndex) ? rule.sourceColumnIndex : (sourceColumns[0]?.index ?? 0),
    conditionType: typeof rule.conditionType === "string" ? rule.conditionType : "contains",
    compareValue: typeof rule.compareValue === "string" ? rule.compareValue : "",
    targetKey: typeof rule.targetKey === "string" ? rule.targetKey : (outputColumns[0]?.key ?? ""),
    value: typeof rule.value === "string" ? rule.value : "",
    valueType: typeof rule.valueType === "string" ? rule.valueType : "string",
  }));
}

export function serializePreset(state) {
  return {
    headerEnabled: state.headerEnabled,
    outputColumns: state.outputColumns.map((column) => ({
      key: column.key,
      sourceType: column.sourceType,
      sourceColumnIndex: column.sourceColumnIndex,
      fixedValue: column.fixedValue,
      fixedValueType: column.fixedValueType,
    })),
    rules: state.rules.map((rule) => ({
      sourceColumnIndex: rule.sourceColumnIndex,
      conditionType: rule.conditionType,
      compareValue: rule.compareValue,
      targetKey: rule.targetKey,
      value: rule.value,
      valueType: rule.valueType,
    })),
  };
}
