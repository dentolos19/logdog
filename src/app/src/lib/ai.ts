import { createOpenRouter } from "@openrouter/ai-sdk-provider";
import type { ModelMessage, UIMessage } from "ai";
import { API_URL } from "@/environment";

const TABLE_PREVIEW_LIMIT = 4;
const ROW_PREVIEW_LIMIT = 3;
const MAX_PREVIEW_JSON_LENGTH = 600;

interface LogGroupTableSummary {
  name: string;
  row_count: number;
  columns: Array<{
    name: string;
    type: string;
    description: string;
  }>;
}

interface LogGroupResponse {
  id: string;
  name: string;
  tables: LogGroupTableSummary[];
}

interface TableRowsResponse {
  columns: string[];
  rows: Record<string, unknown>[];
}

export function getOpenRouterModel() {
  return process.env.OPENROUTER_MODEL ?? "openai/gpt-5.4-mini";
}

export function getOpenRouterClient() {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    throw new Error("Missing OPENROUTER_API_KEY environment variable.");
  }

  return createOpenRouter({
    apiKey,
    headers: {
      "X-Title": "Logdog",
      "X-Referer": "https://logdog.dennise.me",
    },
  });
}

export async function fetchBackendJson<T>(
  path: string,
  authorizationHeader: string,
  init: Omit<RequestInit, "headers"> = {},
): Promise<T> {
  const headers = new Headers(init.body ? { "Content-Type": "application/json" } : undefined);
  if (authorizationHeader.length > 0) {
    headers.set("authorization", authorizationHeader);
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...init,
    headers,
    cache: "no-store",
  });

  if (!response.ok) {
    const payload = (await response.json().catch(() => ({ message: "Request failed." }))) as {
      message?: string;
    };
    throw new Error(payload.message ?? "Request failed.");
  }

  return (await response.json()) as T;
}

function truncateJson(value: unknown, maxLength: number) {
  const serialized = JSON.stringify(value);
  if (serialized.length <= maxLength) {
    return serialized;
  }
  return `${serialized.slice(0, maxLength)}...`;
}

export async function buildLogDataContext(logGroupId: string, authorizationHeader: string) {
  const logGroup = await fetchBackendJson<LogGroupResponse>(
    `/logs/${encodeURIComponent(logGroupId)}`,
    authorizationHeader,
  );

  if (logGroup.tables.length === 0) {
    return `Log group name: ${logGroup.name}\nNo parsed tables are available yet.`;
  }

  const selectedTables = logGroup.tables.slice(0, TABLE_PREVIEW_LIMIT);
  const previewResults = await Promise.all(
    selectedTables.map(async (table) => {
      try {
        const preview = await fetchBackendJson<TableRowsResponse>(
          `/logs/${encodeURIComponent(logGroupId)}/tables/${encodeURIComponent(table.name)}/rows?page=1&page_size=${ROW_PREVIEW_LIMIT}`,
          authorizationHeader,
        );
        return {
          tableName: table.name,
          columns: preview.columns,
          rows: preview.rows,
        };
      } catch {
        return {
          tableName: table.name,
          columns: [] as string[],
          rows: [] as Record<string, unknown>[],
        };
      }
    }),
  );

  const tableSummaryText = selectedTables
    .map((table) => {
      const columns = table.columns.map((column) => `${column.name}:${column.type}`).join(", ");
      return `- ${table.name} (rows=${table.row_count}) columns=[${columns}]`;
    })
    .join("\n");

  const previewText = previewResults
    .map((preview) => {
      const rowsText = truncateJson(preview.rows, MAX_PREVIEW_JSON_LENGTH);
      const columnsText = preview.columns.join(", ");
      return `Table ${preview.tableName}\nColumns: ${columnsText}\nRows: ${rowsText}`;
    })
    .join("\n\n");

  return [`Log group name: ${logGroup.name}`, "Known tables:", tableSummaryText, "Table previews:", previewText].join(
    "\n",
  );
}

export function buildSystemPrompt(contextBlock: string) {
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

export async function persistMessages(logGroupId: string, messages: UIMessage[], authorizationHeader: string) {
  await fetchBackendJson<{ saved_messages: number }>(
    `/logs/${encodeURIComponent(logGroupId)}/chat/messages`,
    authorizationHeader,
    {
      method: "PUT",
      body: JSON.stringify({ messages }),
    },
  );
}

function extractTextFromUiMessage(message: UIMessage) {
  if (Array.isArray(message.parts)) {
    const text = message.parts
      .map((part) => {
        if (part.type === "text" && typeof part.text === "string") {
          return part.text;
        }
        return "";
      })
      .filter((partText) => partText.length > 0)
      .join("\n");

    if (text.length > 0) {
      return text;
    }
  }

  return "";
}

export function toModelMessages(messages: UIMessage[]) {
  const supportedRoles = new Set(["user", "assistant", "system"]);
  const modelMessages: ModelMessage[] = [];

  for (const message of messages) {
    if (!supportedRoles.has(message.role)) {
      continue;
    }

    const text = extractTextFromUiMessage(message);
    if (text.length === 0) {
      continue;
    }

    modelMessages.push({
      role: message.role,
      content: text,
    });
  }

  return modelMessages;
}
