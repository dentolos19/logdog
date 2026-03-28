import {
  chat,
  convertMessagesToModelMessages,
  type ModelMessage,
  toServerSentEventsResponse,
  type UIMessage,
} from "@tanstack/ai";
import { createOpenRouterText } from "@tanstack/ai-openrouter";
import { createFileRoute } from "@tanstack/react-router";

const TABLE_PREVIEW_LIMIT = 4;
const ROW_PREVIEW_LIMIT = 3;
const MAX_PREVIEW_JSON_LENGTH = 600;
const OPENROUTER_MODEL = "openai/gpt-5.4-mini";

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

function truncateJson(value: unknown, maxLength: number) {
  const serialized = JSON.stringify(value);
  if (serialized.length <= maxLength) {
    return serialized;
  }

  return `${serialized.slice(0, maxLength)}...`;
}

function buildSystemPrompt(contextBlock: string) {
  return [
    "You are Logdog's data analyst assistant for a specific log group.",
    "Use the provided table metadata and previews as your source of truth.",
    "If the available data is insufficient, explicitly say what is missing and suggest the next query or upload.",
    "Keep answers concise, actionable, and focused on insights from the log data.",
    "Do not invent columns, tables, or values that are not present in the context.",
    "",
    "Log Group Context:",
    contextBlock,
  ].join("\n");
}

function getErrorMessage(error: unknown, fallbackMessage: string) {
  return error instanceof Error ? error.message : fallbackMessage;
}

async function fetchLogProcesses(entryId: string, request: Request, authorizationHeader: string) {
  const processUrl = new URL(`/api/logs/${encodeURIComponent(entryId)}/processes`, request.url);
  const response = await fetch(processUrl, {
    headers: { Authorization: authorizationHeader },
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Failed to fetch log processes (${response.status}): ${payload}`);
  }

  return (await response.json()) as LogProcessResponse[];
}

async function buildLogDataContext(entryId: string, request: Request, authorizationHeader: string) {
  const processes = await fetchLogProcesses(entryId, request, authorizationHeader);
  const completedProcesses = processes.filter((process) => process.status === "completed" && process.result !== null);

  if (completedProcesses.length === 0) {
    return "No parsed table data is currently available for this log group.";
  }

  const tableMetadata = new Map<string, { columns: string[]; rowCount: number; sampleRows: unknown[] }>();

  for (const process of completedProcesses) {
    const result = process.result;
    if (result === null || typeof result !== "object") {
      continue;
    }

    const records = result.records;
    const tableDefinitions = Array.isArray(result.table_definitions) ? result.table_definitions : [];

    for (const definition of tableDefinitions) {
      const tableName = typeof definition.table_name === "string" ? definition.table_name : null;
      if (tableName === null) {
        continue;
      }

      const rows =
        records && typeof records === "object" && Array.isArray(records[tableName]) ? records[tableName] : [];
      const columns = Array.isArray(definition.columns)
        ? definition.columns
            .map((column) => {
              if (typeof column.name !== "string" || column.name.length === 0) {
                return null;
              }

              const sqlType =
                typeof column.sql_type === "string" && column.sql_type.length > 0 ? column.sql_type : "TEXT";
              return `${column.name}:${sqlType}`;
            })
            .filter((column): column is string => column !== null)
        : [];

      const existing = tableMetadata.get(tableName);
      if (existing) {
        if (existing.columns.length === 0 && columns.length > 0) {
          existing.columns = columns;
        }
        existing.rowCount += rows.length;
        if (existing.sampleRows.length === 0 && rows.length > 0) {
          existing.sampleRows = rows.slice(0, ROW_PREVIEW_LIMIT);
        }
      } else {
        tableMetadata.set(tableName, {
          columns,
          rowCount: rows.length,
          sampleRows: rows.slice(0, ROW_PREVIEW_LIMIT),
        });
      }
    }
  }

  if (tableMetadata.size === 0) {
    return "No parsed table definitions are currently available for this log group.";
  }

  const selectedTables = [...tableMetadata.entries()].slice(0, TABLE_PREVIEW_LIMIT);
  const tableSummary = selectedTables
    .map(([tableName, details]) => `- ${tableName} (rows=${details.rowCount}) columns=[${details.columns.join(", ")}]`)
    .join("\n");
  const previewSummary = selectedTables
    .map(
      ([tableName, details]) =>
        `Table ${tableName}\nRows: ${truncateJson(details.sampleRows, MAX_PREVIEW_JSON_LENGTH)}`,
    )
    .join("\n\n");

  return ["Known tables:", tableSummary, "", "Table previews:", previewSummary].join("\n");
}

export const Route = createFileRoute("/api/logs/$entryId/chat")({
  server: {
    handlers: {
      POST: async ({ params, request }) => {
        const authorizationHeader = request.headers.get("authorization") ?? request.headers.get("Authorization") ?? "";
        if (authorizationHeader === "") {
          return Response.json({ message: "Unauthorized." }, { status: 401 });
        }

        let body: { messages?: unknown[] };
        try {
          body = (await request.json()) as { messages?: unknown[] };
        } catch {
          return Response.json({ message: "Invalid JSON payload." }, { status: 400 });
        }

        if (!Array.isArray(body.messages) || body.messages.length === 0) {
          return Response.json({ message: "A non-empty messages array is required." }, { status: 400 });
        }

        const modelMessages = convertMessagesToModelMessages(body.messages as Array<UIMessage | ModelMessage>);
        if (modelMessages.length === 0) {
          return Response.json({ message: "No text messages were provided." }, { status: 400 });
        }

        let contextBlock = "No parsed table data is currently available for this log group.";
        try {
          contextBlock = await buildLogDataContext(params.entryId, request, authorizationHeader);
        } catch (error) {
          contextBlock = `Failed to build table context: ${getErrorMessage(error, "Unknown error.")}`;
        }

        const openRouterApiKey = process.env.OPENROUTER_API_KEY;
        if (!openRouterApiKey) {
          return Response.json({ message: "OPENROUTER_API_KEY is not configured." }, { status: 500 });
        }

        const stream = chat({
          adapter: createOpenRouterText(OPENROUTER_MODEL, openRouterApiKey, {
            httpReferer: process.env.OPENROUTER_REFERER,
            xTitle: process.env.OPENROUTER_TITLE,
          }),
          messages: modelMessages,
          systemPrompts: [buildSystemPrompt(contextBlock)],
        });

        return toServerSentEventsResponse(stream);
      },
    },
  },
});
