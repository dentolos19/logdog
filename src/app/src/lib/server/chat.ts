import { chat, type ModelMessage, toolDefinition, toServerSentEventsResponse } from "@tanstack/ai";
import { createOpenRouterText } from "@tanstack/ai-openrouter";
import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { getEnv } from "#/environment";

const TABLE_RESULT_LIMIT = 200;
const DEFAULT_SQL_TYPE = "TEXT";

type TableDefinition = {
  table_name?: string;
  columns?: Array<{
    name?: string;
    sql_type?: string;
  }>;
};

type LogProcessResult = {
  table_definitions?: TableDefinition[];
  records?: Record<string, unknown[]>;
};

type LogProcessResponse = {
  status: string;
  result: LogProcessResult | null;
};

type TableAccumulator = {
  rowCount: number;
  columns: Map<string, string>;
};

const streamLogChatInputSchema = z.object({
  entryId: z.string().min(1),
  authorizationHeader: z.string().min(1),
  origin: z.string().url(),
  messages: z.array(z.unknown()).min(1),
});

const listAvailableTablesInputSchema = z.object({
  include_columns: z.boolean().optional().default(true),
});

function getErrorMessage(error: unknown, fallbackMessage: string) {
  return error instanceof Error ? error.message : fallbackMessage;
}

function normalizeOrigin(origin: string) {
  return origin.endsWith("/") ? origin.slice(0, -1) : origin;
}

async function fetchLogProcesses(entryId: string, origin: string, authorizationHeader: string) {
  const processUrl = new URL(`/api/logs/${encodeURIComponent(entryId)}/processes`, normalizeOrigin(origin));
  const response = await fetch(processUrl, {
    headers: { Authorization: authorizationHeader },
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Failed to fetch log processes (${response.status}): ${payload}`);
  }

  return (await response.json()) as LogProcessResponse[];
}

function getOrCreateTableAccumulator(tableMetadata: Map<string, TableAccumulator>, tableName: string) {
  let accumulator = tableMetadata.get(tableName);
  if (accumulator === undefined) {
    accumulator = {
      rowCount: 0,
      columns: new Map<string, string>(),
    };
    tableMetadata.set(tableName, accumulator);
  }

  return accumulator;
}

function buildDiscoveredTables(processes: LogProcessResponse[]) {
  const completedProcesses = processes.filter((process) => process.status === "completed" && process.result !== null);
  if (completedProcesses.length === 0) {
    return [];
  }

  const tableMetadata = new Map<string, TableAccumulator>();

  for (const process of completedProcesses) {
    const result = process.result;
    if (result === null || typeof result !== "object") {
      continue;
    }

    const records =
      result.records !== null &&
      result.records !== undefined &&
      typeof result.records === "object" &&
      !Array.isArray(result.records)
        ? result.records
        : null;

    const tableDefinitions = Array.isArray(result.table_definitions) ? result.table_definitions : [];

    for (const definition of tableDefinitions) {
      const tableName = typeof definition.table_name === "string" ? definition.table_name.trim() : "";
      if (tableName.length === 0) {
        continue;
      }

      const accumulator = getOrCreateTableAccumulator(tableMetadata, tableName);

      const rows = records !== null && Array.isArray(records[tableName]) ? records[tableName] : [];
      accumulator.rowCount += rows.length;

      if (Array.isArray(definition.columns)) {
        for (const column of definition.columns) {
          const columnName = typeof column.name === "string" ? column.name.trim() : "";
          if (columnName.length === 0) {
            continue;
          }

          const sqlType =
            typeof column.sql_type === "string" && column.sql_type.trim().length > 0
              ? column.sql_type
              : DEFAULT_SQL_TYPE;
          if (!accumulator.columns.has(columnName)) {
            accumulator.columns.set(columnName, sqlType);
          }
        }
      }
    }

    if (records !== null) {
      for (const [tableName, rows] of Object.entries(records)) {
        const normalizedTableName = tableName.trim();
        if (normalizedTableName.length === 0 || !Array.isArray(rows)) {
          continue;
        }

        const accumulator = getOrCreateTableAccumulator(tableMetadata, normalizedTableName);
        accumulator.rowCount += rows.length;
      }
    }
  }

  return [...tableMetadata.entries()]
    .map(([tableName, details]) => ({
      table_name: tableName,
      row_count: details.rowCount,
      columns: [...details.columns.entries()]
        .map(([columnName, sqlType]) => ({
          name: columnName,
          sql_type: sqlType,
        }))
        .sort((leftColumn, rightColumn) => leftColumn.name.localeCompare(rightColumn.name)),
    }))
    .sort((leftTable, rightTable) => leftTable.table_name.localeCompare(rightTable.table_name))
    .slice(0, TABLE_RESULT_LIMIT);
}

function extractTextFromParts(parts: unknown[]) {
  return parts
    .map((part) => {
      if (typeof part !== "object" || part === null) {
        return "";
      }

      const typedPart = part as { type?: unknown; content?: unknown };
      if (typedPart.type !== "text" || typeof typedPart.content !== "string") {
        return "";
      }

      return typedPart.content;
    })
    .filter((value) => value.length > 0)
    .join("\n");
}

function toTextModelMessages(messages: unknown[]) {
  const modelMessages: Array<ModelMessage<string>> = [];

  for (const message of messages) {
    if (typeof message !== "object" || message === null) {
      continue;
    }

    const typedMessage = message as {
      role?: unknown;
      content?: unknown;
      parts?: unknown;
    };
    const role = typedMessage.role === "user" || typedMessage.role === "assistant" ? typedMessage.role : null;
    if (role === null) {
      continue;
    }

    const content =
      typeof typedMessage.content === "string"
        ? typedMessage.content
        : Array.isArray(typedMessage.parts)
          ? extractTextFromParts(typedMessage.parts)
          : "";

    if (content.trim().length === 0) {
      continue;
    }

    modelMessages.push({
      role,
      content,
    });
  }

  return modelMessages;
}

function buildSystemPrompt(entryId: string) {
  return [
    "You are Logdog's data analyst assistant for a specific log group.",
    `Log group id: ${entryId}.`,
    "Use the list_available_tables tool to discover table availability and schemas before making assumptions.",
    "Rely only on user-provided information and tool outputs.",
    "If the available data is insufficient, explicitly say what is missing and suggest the next query or upload.",
    "Keep answers concise, actionable, and focused on insights from the log data.",
    "Do not invent columns, tables, or values that are not present in tool outputs.",
  ].join("\n");
}

function createLogChatServerTools(options: { entryId: string; origin: string; authorizationHeader: string }) {
  const listAvailableTables = toolDefinition({
    name: "list_available_tables",
    description: "List parsed tables available for the current log group, including row counts and schemas.",
    inputSchema: listAvailableTablesInputSchema,
    outputSchema: z.object({
      status: z.enum(["ok", "no_data", "error"]),
      message: z.string(),
      tables: z.array(
        z.object({
          table_name: z.string(),
          row_count: z.number().int().nonnegative(),
          columns: z.array(
            z.object({
              name: z.string(),
              sql_type: z.string(),
            }),
          ),
        }),
      ),
    }),
  }).server(async ({ include_columns }) => {
    try {
      const processes = await fetchLogProcesses(options.entryId, options.origin, options.authorizationHeader);
      const tables = buildDiscoveredTables(processes);

      if (tables.length === 0) {
        return {
          status: "no_data" as const,
          message: "No parsed table data is currently available for this log group.",
          tables: [],
        };
      }

      const includeColumns = include_columns ?? true;
      const responseTables = includeColumns
        ? tables
        : tables.map((table) => ({
            ...table,
            columns: [],
          }));

      return {
        status: "ok" as const,
        message: `Found ${responseTables.length} parsed table${responseTables.length === 1 ? "" : "s"}.`,
        tables: responseTables,
      };
    } catch (error) {
      return {
        status: "error" as const,
        message: `Failed to list tables: ${getErrorMessage(error, "Unknown error.")}`,
        tables: [],
      };
    }
  });

  return [listAvailableTables];
}

export const streamLogChat = createServerFn({ method: "POST" })
  .inputValidator((data: unknown) => streamLogChatInputSchema.parse(data))
  .handler(async ({ data }) => {
    const modelMessages = toTextModelMessages(data.messages);
    if (modelMessages.length === 0) {
      throw new Error("No text messages were provided.");
    }

    const tools = createLogChatServerTools({
      entryId: data.entryId,
      origin: data.origin,
      authorizationHeader: data.authorizationHeader,
    });

    const {
      openRouterApiKey: orApiKey,
      openRouterModel: orModel,
      openRouterTitle: orTitle,
      openRouterReferer: orReferer,
    } = getEnv();

    const chatStream = chat({
      adapter: createOpenRouterText(orModel as any, orApiKey, {
        xTitle: orTitle,
        httpReferer: orReferer,
      }),
      messages: modelMessages,
      systemPrompts: [buildSystemPrompt(data.entryId)],
      tools,
    });

    return toServerSentEventsResponse(chatStream);
  });
