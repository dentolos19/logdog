import type { LogFile, LogProcess } from "#/lib/server";

export type TableColumn = {
  name: string;
  type: string;
  nullable: boolean;
  primary_key: boolean;
  description: string;
};

export type TableSummary = {
  id: string;
  name: string;
  rowCount: number;
  columns: TableColumn[];
  rows: Record<string, unknown>[];
  sourceFile: LogFile | null;
};

export function inferTablesFromProcesses(files: LogFile[], processes: LogProcess[]) {
  const fileById = new Map<string, LogFile>(files.map((file) => [file.id, file]));
  const tableMap = new Map<string, TableSummary>();

  for (const process of processes) {
    const result = asRecord(process.result);
    if (result === null) {
      continue;
    }

    const generatedTables = asArray(result.table_definitions);
    const recordsByTable = asRecord(result.records);

    for (const generatedTable of generatedTables) {
      const tableRecord = asRecord(generatedTable);
      if (tableRecord === null) {
        continue;
      }

      const tableName = asString(tableRecord.table_name, "unknown_table");
      const rawColumns = asArray(tableRecord.columns);
      const columns: TableColumn[] = rawColumns.map((column) => {
        const columnRecord = asRecord(column) ?? {};
        return {
          name: asString(columnRecord.name, "column"),
          type: asString(columnRecord.sql_type, "TEXT"),
          nullable: asBoolean(columnRecord.nullable, true),
          primary_key: asBoolean(columnRecord.primary_key, false),
          description: asString(columnRecord.description, ""),
        };
      });

      const recordRows = recordsByTable ? asArray(recordsByTable[tableName]) : [];
      const rows = recordRows.map((row) => asRecord(row)).filter((row): row is Record<string, unknown> => row !== null);

      const sourceFileByProcess = process.file_id !== null ? (fileById.get(process.file_id) ?? null) : null;
      const sourceFileByHint = resolveSourceFileByHint(tableName, files);

      tableMap.set(tableName, {
        id: tableName,
        name: tableName,
        rowCount: recordRows.length,
        columns,
        rows,
        sourceFile: sourceFileByProcess ?? sourceFileByHint,
      });
    }
  }

  return [...tableMap.values()].sort((leftTable, rightTable) => {
    const leftLabel = getTableDisplayName(leftTable).toLowerCase();
    const rightLabel = getTableDisplayName(rightTable).toLowerCase();
    return leftLabel.localeCompare(rightLabel);
  });
}

export function getTableDisplayName(table: TableSummary) {
  if (table.sourceFile !== null) {
    return table.sourceFile.name;
  }

  return table.name;
}

export function formatFileSize(bytes: number) {
  const bytesPerKilobyte = 1024;
  const bytesPerMegabyte = bytesPerKilobyte * 1024;

  if (bytes >= bytesPerMegabyte) {
    return `${(bytes / bytesPerMegabyte).toFixed(1)} MB`;
  }

  if (bytes >= bytesPerKilobyte) {
    return `${(bytes / bytesPerKilobyte).toFixed(1)} KB`;
  }

  return `${bytes} B`;
}

function resolveSourceFileByHint(tableName: string, files: LogFile[]) {
  const fileIdMatch = tableName.match(/[a-f0-9]{12}$/i);
  if (!fileIdMatch) {
    return null;
  }

  const hint = fileIdMatch[0].toLowerCase();
  return files.find((file) => file.id.replace(/-/g, "").toLowerCase().startsWith(hint)) ?? null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }

  return null;
}

function asArray(value: unknown) {
  return Array.isArray(value) ? value : [];
}

function asString(value: unknown, fallback: string) {
  return typeof value === "string" ? value : fallback;
}

function asBoolean(value: unknown, fallback: boolean) {
  return typeof value === "boolean" ? value : fallback;
}
