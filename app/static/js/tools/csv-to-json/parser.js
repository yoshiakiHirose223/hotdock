import { BLANK_ROW_POLICY } from "./constants.js";

function stripBom(text) {
  return text.replace(/^\uFEFF/, "");
}

function isBlankRow(row) {
  return row.every((cell) => cell === "");
}

export async function readCsvFile(file) {
  const text = await file.text();
  return stripBom(text);
}

export function parseCsvDocument(rawInput, options = {}) {
  const csvText = stripBom(rawInput ?? "");
  const headerEnabled = Boolean(options.headerEnabled);
  const inputMode = options.inputMode ?? "text";
  const fileName = options.fileName ?? "";

  if (!csvText.trim()) {
    return {
      ok: false,
      fatalError: {
        message: "CSV データが空です。テキスト入力またはファイル選択を見直してください。",
      },
    };
  }

  const parsed = parseCsvRows(csvText);
  if (parsed.fatalError) {
    return {
      ok: false,
      fatalError: parsed.fatalError,
    };
  }

  const { rows, rowMeta } = parsed;
  if (!rows.length) {
    return {
      ok: false,
      fatalError: {
        message: "有効なデータ行が見つかりませんでした。空行だけの CSV は変換できません。",
      },
    };
  }

  const expectedColumnCount = rows[0].length;
  const widthErrors = [];

  for (let index = 1; index < rows.length; index += 1) {
    if (rows[index].length !== expectedColumnCount) {
      widthErrors.push({
        rowIndex: index,
        lineNumber: rowMeta[index].startLine,
        expected: expectedColumnCount,
        actual: rows[index].length,
      });
    }
  }

  const headerRow = headerEnabled ? rows[0] : null;
  const sourceColumns = Array.from({ length: expectedColumnCount }, (_, index) => {
    const fallbackName = `column${index + 1}`;
    const headerValue = headerEnabled ? (headerRow[index] ?? "") : fallbackName;

    return {
      index,
      sourceName: fallbackName,
      label: headerEnabled ? (headerValue || `${fallbackName} (空ヘッダー)`) : fallbackName,
      initialKey: headerEnabled ? headerValue : fallbackName,
    };
  });

  const dataStartIndex = headerEnabled ? 1 : 0;
  const dataRows = rows.slice(dataStartIndex);
  const dataMeta = rowMeta.slice(dataStartIndex);

  return {
    ok: true,
    rawCsvText: csvText,
    inputMode,
    fileName,
    headerEnabled,
    rows,
    rowMeta,
    headerRow,
    sourceColumns,
    expectedColumnCount,
    dataRows,
    dataMeta,
    widthErrors,
  };
}

function parseCsvRows(csvText) {
  const rows = [];
  const rowMeta = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  let currentLine = 1;
  let rowStartLine = 1;

  function finalizeRow() {
    row.push(field);
    field = "";

    const candidate = [...row];
    const blank = isBlankRow(candidate);

    if (!(BLANK_ROW_POLICY === "skip" && blank)) {
      rowMeta.push({
        recordNumber: rowMeta.length + 1,
        startLine: rowStartLine,
      });
      rows.push(candidate);
    }

    row = [];
    rowStartLine = currentLine + 1;
  }

  for (let index = 0; index < csvText.length; index += 1) {
    const char = csvText[index];

    if (inQuotes) {
      if (char === '"') {
        if (csvText[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else if (char === "\r") {
        if (csvText[index + 1] === "\n") {
          index += 1;
        }
        field += "\n";
        currentLine += 1;
      } else {
        if (char === "\n") {
          currentLine += 1;
        }
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (char === "\r") {
      if (csvText[index + 1] === "\n") {
        index += 1;
      }
      finalizeRow();
      currentLine += 1;
      continue;
    }

    if (char === "\n") {
      finalizeRow();
      currentLine += 1;
      continue;
    }

    field += char;
  }

  if (inQuotes) {
    return {
      rows: [],
      rowMeta: [],
      fatalError: {
        message: `ダブルクォートが閉じられていません。開始行 ${rowStartLine} を確認してください。`,
      },
    };
  }

  if (field !== "" || row.length > 0) {
    finalizeRow();
  }

  return { rows, rowMeta };
}
