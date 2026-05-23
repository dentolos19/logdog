import {
  chat,
  convertMessagesToModelMessages,
  type MessagePart,
  type ModelMessage,
  maxIterations,
  toolDefinition,
  toServerSentEventsResponse,
  type UIMessage,
} from "@tanstack/ai";
import { createOpenRouterText } from "@tanstack/ai-openrouter";
import { createServerFn } from "@tanstack/react-start";
import { z } from "zod";
import { getEnv } from "#/environment";

const ACCESS_TOKEN_COOKIE = "logdog-access-token";

const TABLE_RESULT_LIMIT = 200;
const DEFAULT_SQL_TYPE = "TEXT";

type TableDefinition = {
  table_name?: string;
  display_name?: string;
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
  displayName: string;
};

const MAX_PERSISTED_MESSAGES = 500;
const MAX_MESSAGE_CONTENT_LENGTH = 100000;

const streamLogChatInputSchema = z.object({
  entryId: z.string().min(1),
  messages: z.array(z.unknown()).min(1).max(MAX_PERSISTED_MESSAGES),
});

const listAvailableTablesInputSchema = z.object({
  include_columns: z.boolean().optional().default(true),
});

const executeSqlQueryInputSchema = z.object({
  sql: z.string().min(1).describe("A SELECT SQL query to execute against the parsed log tables."),
});

const getSqlCommandTemplatesInputSchema = z.object({
  table_name: z
    .string()
    .optional()
    .describe("Optional table name to substitute into SQL templates. Double quotes are applied automatically."),
});

const widgetStatsItemSchema = z.object({
  label: z.string(),
  value: z.union([z.string(), z.number()]),
  description: z.string().optional(),
});

const renderWidgetInputSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("data_table"),
    columns: z.array(z.string()).min(1),
    rows: z.array(z.array(z.unknown())),
    title: z.string().optional(),
  }),
  z.object({
    type: z.literal("chart"),
    chart_type: z.enum(["bar", "line", "pie"]),
    data: z.array(z.record(z.string(), z.unknown())).min(1),
    x_key: z.string(),
    y_key: z.string(),
    title: z.string().optional(),
  }),
  z.object({
    type: z.literal("stats"),
    stats: z.array(widgetStatsItemSchema).min(1),
  }),
]);

function getErrorMessage(error: unknown, fallbackMessage: string) {
  return error instanceof Error ? error.message : fallbackMessage;
}

async function fetchBackendPost<T>(
  origin: string,
  authorizationHeader: string,
  path: string,
  body: unknown,
): Promise<T> {
  const url = new URL(`/api${path}`, normalizeOrigin(origin));
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: authorizationHeader,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Backend request failed (${response.status}): ${payload}`);
  }

  return (await response.json()) as T;
}

function normalizeOrigin(origin: string) {
  return origin.endsWith("/") ? origin.slice(0, -1) : origin;
}

type LogGroupMetadata = {
  id: string;
  name: string;
};

async function fetchLogGroupMetadata(entryId: string, origin: string, authorizationHeader: string) {
  const url = new URL(`/api/logs/${encodeURIComponent(entryId)}`, normalizeOrigin(origin));
  const response = await fetch(url, {
    headers: { Authorization: authorizationHeader },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch log group metadata (${response.status})`);
  }

  return (await response.json()) as LogGroupMetadata;
}

function normalizeGroupName(name: string): string {
  return name.trim().replace(/\s+/g, " ").slice(0, 120);
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

function getOrCreateTableAccumulator(
  tableMetadata: Map<string, TableAccumulator>,
  tableName: string,
  displayName?: string,
) {
  let accumulator = tableMetadata.get(tableName);
  if (accumulator === undefined) {
    accumulator = {
      rowCount: 0,
      columns: new Map<string, string>(),
      displayName: displayName ?? "",
    };
    tableMetadata.set(tableName, accumulator);
  } else if (displayName !== undefined && displayName.length > 0 && accumulator.displayName.length === 0) {
    accumulator.displayName = displayName;
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

      const defDisplayName = typeof definition.display_name === "string" ? definition.display_name.trim() : "";
      const accumulator = getOrCreateTableAccumulator(tableMetadata, tableName, defDisplayName);

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
      display_name: details.displayName.length > 0 ? details.displayName : tableName,
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

type IncomingChatMessage = UIMessage | ModelMessage;
type TextModelMessage = ModelMessage<string | null>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeMessagePart(part: unknown): MessagePart | null {
  if (!isRecord(part)) {
    return null;
  }

  if (part.type === "text" && typeof part.content === "string") {
    return { type: "text", content: part.content.slice(0, MAX_MESSAGE_CONTENT_LENGTH) };
  }

  if (
    part.type === "tool-call" &&
    typeof part.id === "string" &&
    typeof part.name === "string" &&
    typeof part.arguments === "string"
  ) {
    const state =
      part.state === "awaiting-input" ||
      part.state === "input-streaming" ||
      part.state === "input-complete" ||
      part.state === "approval-requested" ||
      part.state === "approval-responded"
        ? part.state
        : "input-complete";

    return {
      type: "tool-call",
      id: part.id,
      name: part.name,
      arguments: part.arguments,
      state,
      ...("output" in part ? { output: part.output } : {}),
      ...(isRecord(part.approval) ? { approval: part.approval as never } : {}),
    };
  }

  if (part.type === "tool-result" && typeof part.toolCallId === "string" && typeof part.content === "string") {
    return {
      type: "tool-result",
      toolCallId: part.toolCallId,
      content: part.content.slice(0, MAX_MESSAGE_CONTENT_LENGTH),
      state: part.state === "error" ? "error" : "complete",
      ...(typeof part.error === "string" ? { error: part.error } : {}),
    };
  }

  return null;
}

function normalizeMessageParts(parts: unknown[]): MessagePart[] {
  return parts.map(normalizeMessagePart).filter((part): part is MessagePart => part !== null);
}

function normalizeIncomingMessages(messages: unknown[]): IncomingChatMessage[] {
  const normalizedMessages: IncomingChatMessage[] = [];

  for (const message of messages) {
    if (!isRecord(message)) {
      continue;
    }

    const role = message.role === "user" || message.role === "assistant" ? message.role : null;
    if (role === null) {
      continue;
    }

    if (Array.isArray(message.parts)) {
      normalizedMessages.push({
        id: typeof message.id === "string" ? message.id : crypto.randomUUID(),
        role,
        parts: normalizeMessageParts(message.parts),
      });
      continue;
    }

    if (typeof message.content === "string") {
      normalizedMessages.push({
        role,
        content: message.content.slice(0, MAX_MESSAGE_CONTENT_LENGTH),
      });
    }
  }

  return normalizedMessages;
}

function truncateModelContent(content: ModelMessage["content"]): string | null {
  if (typeof content === "string") {
    return content.slice(0, MAX_MESSAGE_CONTENT_LENGTH);
  }

  if (Array.isArray(content)) {
    const textContent = content
      .filter((part) => part.type === "text")
      .map((part) => part.content)
      .join("\n")
      .slice(0, MAX_MESSAGE_CONTENT_LENGTH);
    return textContent.length > 0 ? textContent : null;
  }

  return null;
}

function hasModelMessageContent(message: TextModelMessage) {
  const hasTextContent = typeof message.content === "string" && message.content.trim().length > 0;

  return (
    hasTextContent || (Array.isArray(message.toolCalls) && message.toolCalls.length > 0) || message.role === "tool"
  );
}

function toModelMessages(messages: unknown[]) {
  const incomingMessages = normalizeIncomingMessages(messages);

  return convertMessagesToModelMessages(incomingMessages)
    .map(
      (message): TextModelMessage => ({
        ...message,
        content: truncateModelContent(message.content),
      }),
    )
    .filter(hasModelMessageContent);
}

function hasUserTextMessage(messages: TextModelMessage[]) {
  return messages.some((message) => {
    if (message.role !== "user") {
      return false;
    }

    return typeof message.content === "string" && message.content.trim().length > 0;
  });
}

function redactText(text: string, entryId: string, groupName: string, tableNameMap?: Map<string, string>) {
  let content = text.replaceAll(entryId, `"${groupName}"`);
  if (tableNameMap !== undefined) {
    for (const [rawName, displayName] of tableNameMap) {
      if (rawName !== displayName) {
        content = content.replaceAll(rawName, `"${displayName}"`);
      }
    }
  }
  return content;
}

/**
 * Redacts raw identifiers from conversational text while leaving tool calls and
 * tool results intact. Tool payloads are needed as follow-up context; stripping
 * them causes the client to auto-continue the same tool call forever.
 */
function sanitizeModelMessages(
  messages: TextModelMessage[],
  entryId: string,
  groupName: string,
  tableNameMap?: Map<string, string>,
): TextModelMessage[] {
  if (entryId.length === 0 || entryId === groupName) {
    return messages;
  }
  return messages.map((msg) => {
    // Keep tool result payloads and tool-call arguments intact. They are the
    // model's evidence trail for follow-up questions and may contain exact
    // table identifiers required for subsequent SQL calls.
    if (msg.role === "tool") {
      return msg;
    }

    if (typeof msg.content === "string") {
      return { ...msg, content: redactText(msg.content, entryId, groupName, tableNameMap) };
    }

    return msg;
  });
}

function buildSystemPrompt(logGroupName: string) {
  return [
    "You are Logdog's data analyst assistant for a specific log group.",
    `Current log group display name: "${logGroupName}".`,
    "Refer to this log group by its display name. Do not mention internal UUIDs or raw identifiers.",
    "",
    "## Scope rules",
    "Your default analytical scope is the entire log group. When asked broad questions such as summary, anomalies, trends,",
    "analysis, charts, or anything that does not explicitly name specific tables, you MUST inspect EVERY available table.",
    "Only narrow your analysis to specific tables if the user explicitly requests it.",
    "",
    "## Autonomy rules (CRITICAL)",
    "Do NOT ask clarifying questions for broad analysis requests. Choose sensible defaults and proceed with exploratory queries.",
    "After list_available_tables returns tables, continue querying immediately. Do not stop to ask the user what to inspect.",
    "State your assumptions briefly and move forward.",
    "The only acceptable reason to stop and ask is if every table returned zero rows or no useful columns exist.",
    "",
    "## Discovery protocol",
    "Before any analysis or SQL assumptions, call list_available_tables(include_columns=true) to discover available data.",
    "If no tables are available, stop querying and tell the user to upload/process logs first.",
    "Use get_sql_command_templates to get reusable SQL patterns before writing custom SQL.",
    "Use execute_sql_query to run SELECT queries against the parsed log tables.",
    "Always use double quotes around table and column identifiers in SQL.",
    "Avoid ROUND(); when you need integer/decimal averages, prefer CAST(AVG(...) AS INTEGER/REAL).",
    "If a query fails, immediately retry with a simpler inspection query (for example SELECT * ... LIMIT 5) before further analysis.",
    "",
    "## Summary playbook",
    "For a summary request, do ALL of the following for EACH table:",
    '1) Count total rows: SELECT COUNT(*) AS row_count FROM "<table_name>";',
    '2) Preview sample rows: SELECT * FROM "<table_name>" LIMIT 5;',
    "3) If a timestamp/date column exists (check the column names from list_available_tables), get the time range.",
    '4) For string/category columns, get top values: SELECT "<col>", COUNT(*) AS cnt FROM "<table_name>" GROUP BY "<col>" ORDER BY cnt DESC LIMIT 10;',
    "5) Synthesize observations about each table's structure, content, and notable fields.",
    "6) Present a concise summary per table and an overall picture across all tables.",
    "",
    "## Anomaly detection playbook",
    "For anomaly/error detection, do ALL of the following:",
    "1) Count total rows per table.",
    '2) Look for columns with names like "error", "status", "level", "code", "type", "severity". Group by those columns to find distributions.',
    '3) Check for null-heavy columns: SELECT COUNT(*) AS total, COUNT("<col>") AS populated FROM "<table_name>";',
    "4) If a timestamp column exists, group by date to find spikes and drops.",
    "5) Highlight status failures, error counts, null-heavy fields, rare categorical values, and unusual numeric values.",
    "6) Use render_widget with type 'stats' or 'chart' to present key anomaly metrics.",
    "",
    "## Trend chart playbook",
    "For a trend/chart request, do ALL of the following:",
    "1) Inspect each table's schema (already available from list_available_tables) to find a timestamp or date column.",
    "2) If a timestamp column exists, group by it with row count.",
    "3) If no timestamp column exists, chart row counts by a categorical column or explain that no temporal data is available.",
    "4) Use render_widget with type 'chart' (bar/line/pie as appropriate). Explain which table and metric you chose.",
    "",
    "## Widgets",
    "Use render_widget to present results visually: type 'data_table' for tabular data, 'chart' for bar/line/pie charts, 'stats' for key metrics.",
    "",
    "## Constraints",
    "Rely only on user-provided information and tool outputs.",
    "Keep answers concise, actionable, and focused on insights from the log data.",
    "Do not invent columns, tables, or values that are not present in tool outputs.",
    "",
    "## User-friendly language",
    "Write responses in plain, conversational English. Avoid technical jargon, database terminology (like table names, column names, SQL types), internal identifiers, UUIDs, and long alphanumeric strings.",
    "Use short sentences and simple words. Pretend you are explaining findings to someone who is not a database expert.",
    "Do not include raw SQL queries or code snippets in your text responses unless the user explicitly asks for them.",
    "When referring to tables, use the display name (e.g. 'nginx access logs' instead of 'nginx_access_log_2024').",
    "When showing numbers, format them in a readable way (e.g. '1,234' instead of '1234').",
    "Never mention table_name, column_name, sql_type, or other internal schema details in your conversational replies.",
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
          table_name: z.string().describe("The exact table identifier. Use this in all SQL queries."),
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

  const getSqlCommandTemplates = toolDefinition({
    name: "get_sql_command_templates",
    description:
      "Return SQL exploration command templates that are safe for this environment. Use this before writing custom SQL.",
    inputSchema: getSqlCommandTemplatesInputSchema,
    outputSchema: z.object({
      status: z.literal("ok"),
      dialect: z.string(),
      notes: z.array(z.string()),
      commands: z.array(
        z.object({
          name: z.string(),
          purpose: z.string(),
          sql: z.string(),
        }),
      ),
    }),
  }).server(async ({ table_name }) => {
    const normalizedTableName = typeof table_name === "string" ? table_name.trim() : "";
    const escapedTableName = normalizedTableName.replaceAll('"', '""');
    const tableTarget = escapedTableName.length > 0 ? `"${escapedTableName}"` : '"<table_name>"';

    return {
      status: "ok" as const,
      dialect: "SQLAlchemy backend SQL dialect",
      notes: [
        "Use double quotes around table and column identifiers.",
        "Use SELECT-only queries.",
        "Prefer CAST(AVG(...)) over ROUND(...) for compatibility.",
      ],
      commands: [
        {
          name: "preview_rows",
          purpose: "Inspect sample rows before analysis.",
          sql: `SELECT * FROM ${tableTarget} LIMIT 5;`,
        },
        {
          name: "row_count",
          purpose: "Check table size.",
          sql: `SELECT COUNT(*) AS row_count FROM ${tableTarget};`,
        },
        {
          name: "column_types",
          purpose: "Inspect schema metadata from information_schema.",
          sql: 'SELECT "column_name", "data_type" FROM information_schema.columns WHERE "table_name" = \'<table_name>\' ORDER BY "ordinal_position";',
        },
        {
          name: "value_distribution",
          purpose: "Get top grouped values with frequencies.",
          sql: `SELECT "<group_col>", COUNT(*) AS count_rows FROM ${tableTarget} GROUP BY "<group_col>" ORDER BY count_rows DESC LIMIT 20;`,
        },
        {
          name: "time_range",
          purpose: "Get observed timestamp boundaries.",
          sql: `SELECT MIN("<timestamp_col>") AS min_timestamp, MAX("<timestamp_col>") AS max_timestamp FROM ${tableTarget};`,
        },
        {
          name: "casted_averages",
          purpose: "Compute averages without ROUND().",
          sql: `SELECT "<group_col>", CAST(AVG("<volume_col>") AS INTEGER) AS avg_volume, CAST(AVG("<metric_col>") AS REAL) AS avg_metric FROM ${tableTarget} GROUP BY "<group_col>" ORDER BY avg_volume DESC LIMIT 20;`,
        },
      ],
    };
  });

  const executeSqlQuery = toolDefinition({
    name: "execute_sql_query",
    description:
      "Run a read-only SELECT SQL query against the parsed log tables. Use this to explore data, compute aggregations, find anomalies, and answer analytical questions.",
    inputSchema: executeSqlQueryInputSchema,
    outputSchema: z.object({
      status: z.enum(["ok", "error"]),
      columns: z.array(z.string()),
      rows: z.array(z.array(z.unknown())),
      row_count: z.number(),
      execution_time_ms: z.number(),
      message: z.string(),
    }),
  }).server(async ({ sql }) => {
    try {
      const result = await fetchBackendPost<{
        status: string;
        columns: string[];
        rows: unknown[][];
        row_count: number;
        execution_time_ms: number;
        message: string;
      }>(options.origin, options.authorizationHeader, `/logs/${encodeURIComponent(options.entryId)}/query`, { sql });

      return {
        status: result.status === "ok" ? ("ok" as const) : ("error" as const),
        columns: result.columns ?? [],
        rows: (result.rows ?? []).map((row) => row.map((v) => (typeof v === "object" ? JSON.stringify(v) : v))),
        row_count: result.row_count ?? 0,
        execution_time_ms: result.execution_time_ms ?? 0,
        message: result.message ?? "",
      };
    } catch (error) {
      return {
        status: "error" as const,
        columns: [],
        rows: [],
        row_count: 0,
        execution_time_ms: 0,
        message: `Query failed: ${getErrorMessage(error, "Unknown error.")}`,
      };
    }
  });

  const renderWidget = toolDefinition({
    name: "render_widget",
    description:
      "Render a visual widget in the chat. Use type 'data_table' for tabular results, 'chart' for bar/line/pie charts, or 'stats' for key metric cards.",
    inputSchema: renderWidgetInputSchema,
    outputSchema: z.object({
      type: z.string(),
      message: z.string(),
    }),
  }).server(async (input) => {
    return {
      type: input.type,
      message: `Rendered ${input.type} widget.`,
    };
  });

  return [listAvailableTables, getSqlCommandTemplates, executeSqlQuery, renderWidget];
}

export const streamLogChat = createServerFn({ method: "POST" })
  .inputValidator((data: unknown) => streamLogChatInputSchema.parse(data))
  .handler(async ({ data }) => {
    const modelMessages = toModelMessages(data.messages);
    if (!hasUserTextMessage(modelMessages)) {
      throw new Error("No text messages were provided.");
    }

    // Derive backend origin and auth token server-side from the incoming
    // request instead of accepting them from the client. This prevents SSRF
    // and token-forwarding attacks.
    const request = getRequest();
    const requestUrl = new URL(request.url);
    const backendOrigin = `${requestUrl.protocol}//${requestUrl.host}`;

    const cookies = request.headers.get("cookie") ?? "";
    const accessToken = parseCookieValue(cookies, ACCESS_TOKEN_COOKIE);
    const authorizationHeader = accessToken ? `Bearer ${accessToken}` : "";

    const tools = createLogChatServerTools({
      entryId: data.entryId,
      origin: backendOrigin,
      authorizationHeader,
    });

    // Fetch the log group's friendly name so we never expose the raw UUID
    // to the model or in the system prompt.
    const groupMetadata = await fetchLogGroupMetadata(data.entryId, backendOrigin, authorizationHeader);
    const logGroupName = normalizeGroupName(groupMetadata.name);

    // Fetch processes to build a table name map for conversational text
    // redaction. Tool outputs are not redacted because the model needs exact
    // identifiers from prior tool calls to answer follow-up questions.
    const processes = await fetchLogProcesses(data.entryId, backendOrigin, authorizationHeader);
    const discoveredTables = buildDiscoveredTables(processes);
    const tableNameMap = new Map<string, string>();
    for (const table of discoveredTables) {
      if (table.table_name !== table.display_name) {
        tableNameMap.set(table.table_name, table.display_name);
      }
    }

    // Sanitize visible chat history but preserve tool-call/tool-result history.
    const sanitizedMessages = sanitizeModelMessages(modelMessages, data.entryId, logGroupName, tableNameMap);

    const {
      openRouterApiKey: orApiKey,
      openRouterModel: orModel,
      openRouterTitle: orTitle,
      openRouterReferer: orReferer,
    } = getEnv();

    const chatStream = chat({
      adapter: createOpenRouterText(orModel as Parameters<typeof createOpenRouterText>[0], orApiKey, {
        xTitle: orTitle,
        httpReferer: orReferer,
      } as Parameters<typeof createOpenRouterText>[2]),
      messages: sanitizedMessages,
      systemPrompts: [buildSystemPrompt(logGroupName)],
      agentLoopStrategy: maxIterations(8),
      tools,
    });

    return toServerSentEventsResponse(chatStream);
  });

const generateChatSuggestionsInputSchema = z.object({
  entryId: z.string().min(1),
  recentMessages: z.array(
    z.object({
      role: z.enum(["user", "assistant"]),
      content: z.string().max(2000),
    }),
  ),
  hasTables: z.boolean(),
});

export const generateChatSuggestions = createServerFn({ method: "POST" })
  .inputValidator((data: unknown) => generateChatSuggestionsInputSchema.parse(data))
  .handler(async ({ data }) => {
    const request = getRequest();
    const requestUrl = new URL(request.url);
    const backendOrigin = `${requestUrl.protocol}//${requestUrl.host}`;

    const cookies = request.headers.get("cookie") ?? "";
    const accessToken = parseCookieValue(cookies, ACCESS_TOKEN_COOKIE);
    const authorizationHeader = accessToken ? `Bearer ${accessToken}` : "";

    const groupMetadata = await fetchLogGroupMetadata(data.entryId, backendOrigin, authorizationHeader);
    const logGroupName = normalizeGroupName(groupMetadata.name);

    const {
      openRouterApiKey: orApiKey,
      openRouterModel: orModel,
      openRouterTitle: orTitle,
      openRouterReferer: orReferer,
    } = getEnv();

    const suggestionSystemPrompt = [
      "You are a helpful assistant that suggests follow-up questions for a log analysis chatbot.",
      `The current log group is "${logGroupName}".`,
      data.hasTables
        ? "The user has uploaded log data that is ready for analysis."
        : "The user has not uploaded any processed log data yet.",
      "",
      "Based on the conversation so far, suggest 3 short follow-up questions the user might want to ask next.",
      "Each suggestion MUST be under 50 characters, user-friendly, and directly relevant to the conversation.",
      "Use plain language - no jargon, no technical terms.",
      "",
      "Return ONLY a JSON array of 3 strings, no other text or explanation.",
      'Example: ["Show me error trends", "Find unusual activity", "Summarize recent logs"]',
    ].join("\n");

    try {
      const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${orApiKey}`,
          "Content-Type": "application/json",
          ...(orTitle ? { "X-Title": orTitle } : {}),
          ...(orReferer ? { "HTTP-Referer": orReferer } : {}),
        },
        body: JSON.stringify({
          model: orModel,
          messages: [{ role: "system", content: suggestionSystemPrompt }, ...data.recentMessages],
          max_tokens: 150,
          temperature: 0.3,
        }),
      });

      if (!response.ok) {
        throw new Error(`OpenRouter API returned ${response.status}`);
      }

      const body = (await response.json()) as {
        choices?: Array<{ message?: { content?: string } }>;
      };
      const text = body.choices?.[0]?.message?.content ?? "";

      // Try to parse JSON array from the response
      const jsonMatch = text.match(/\[[\s\S]*\]/);
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0]);
        if (Array.isArray(parsed)) {
          return parsed
            .map((s) => (typeof s === "string" ? s.trim() : ""))
            .filter((s) => s.length > 0)
            .slice(0, 4);
        }
      }
    } catch {
      // Fall through to fallback suggestions
    }

    // Fallback suggestions
    if (data.hasTables) {
      return ["Explore key trends", "Find top anomalies", "Show a quick summary"];
    }
    return ["How do I upload logs?", "What kind of data works?", "Show me the basics"];
  });

function parseCookieValue(cookieHeader: string, name: string): string | null {
  for (const cookie of cookieHeader.split(";")) {
    const trimmed = cookie.trim();
    const eqIndex = trimmed.indexOf("=");
    if (eqIndex === -1) {
      continue;
    }
    const key = trimmed.slice(0, eqIndex).trim();
    if (key === name) {
      return decodeURIComponent(trimmed.slice(eqIndex + 1).trim());
    }
  }
  return null;
}
