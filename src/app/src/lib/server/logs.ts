import { $fetch } from "#/lib/server/utils";

export type LogEntry = {
  id: string;
  user_id: string;
  name: string;
  profile_name?: string | null;
  created_at: string;
};

export type LogFile = {
  id: string;
  entry_id: string;
  asset_id: string;
  name: string;
  size: number;
  content_type: string;
  created_at: string;
};

export type LogProcess = {
  id: string;
  entry_id: string;
  file_id: string | null;
  status: LogProcessStatus;
  classification: LogProcessClassification | Record<string, unknown> | null;
  result: LogProcessResult | Record<string, unknown> | null;
  error: string | null;
  created_at: string;
  updated_at: string;
};

export type LogProcessStatus = "queued" | "processing" | "completed" | "failed";

export type LogProcessClassification = {
  dominant_format?: string;
  structural_class?: string;
  selected_parser_key?: string;
  file_classifications?: Array<Record<string, unknown>>;
  warnings?: string[];
  confidence?: number;
};

export type LogProcessResult = {
  table_definitions?: Array<Record<string, unknown>>;
  records?: Record<string, unknown>;
  parser_key?: string;
  warnings?: string[];
  confidence?: number;
};

export type UploadFilesResponse = {
  process_ids: string[];
  status: string;
  files: LogFile[];
  outcomes: UploadFileOutcome[];
};

export type UploadFileOutcome = {
  file_id: string;
  filename: string;
  process_id: string | null;
  status: string;
  error: string | null;
};

export type ProcessEnqueuedResponse = {
  process_ids: string[];
  status: string;
  errors: string[];
};

export type FilteredExportPayload = {
  format: "csv" | "json";
  search?: string;
  levels?: string[];
  field_filters?: Record<string, string>;
  timestamp_from?: string;
  timestamp_to?: string;
};

export type FormatDistributionItem = {
  format: string;
  count: number;
};

export type DashboardStats = {
  log_group_count: number;
  total_files: number;
  total_rows: number;
  processes: {
    queued: number;
    processing: number;
    completed: number;
    failed: number;
  };
  format_distribution: FormatDistributionItem[];
};

export type LogInsightReport = {
  summary: string;
  severity: string;
  top_errors: string[];
  root_cause_hypothesis: string;
  log_sequence_narrative: string;
  recommendations: string[];
  anomalies: string[];
};

export type ChatMessage = {
  id: string;
  role: string;
  content?: string;
  parts?: Array<Record<string, unknown>>;
  [key: string]: unknown;
};

type ChatMessagesResponse = {
  messages: ChatMessage[];
};

type ReplaceChatMessagesPayload = {
  messages: ChatMessage[];
};

type ReplaceChatMessagesResponse = {
  saved_messages: number;
};

type MessageResponse = {
  message: string;
};

type CreateLogEntryPayload = {
  name: string;
  profile_name?: string;
};

type UpdateLogEntryPayload = {
  name: string;
  profile_name?: string;
};

type CreateProcessPayload = {
  file_ids?: string[];
};

async function parseJsonResponse<T>(response: Response) {
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }

  return (await response.json()) as T;
}

export async function listLogEntries() {
  const response = await $fetch("/logs");
  return parseJsonResponse<LogEntry[]>(response);
}

export async function createLogEntry(payload: CreateLogEntryPayload) {
  const response = await $fetch("/logs", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return parseJsonResponse<LogEntry>(response);
}

export async function getLogEntry(entryId: string) {
  const response = await $fetch(`/logs/${entryId}`);
  return parseJsonResponse<LogEntry>(response);
}

export async function updateLogEntry(entryId: string, payload: UpdateLogEntryPayload) {
  const response = await $fetch(`/logs/${entryId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return parseJsonResponse<LogEntry>(response);
}

export async function deleteLogEntry(entryId: string) {
  const response = await $fetch(`/logs/${entryId}`, {
    method: "DELETE",
  });

  return parseJsonResponse<MessageResponse>(response);
}

export async function uploadLogFiles(entryId: string, files: File[]) {
  if (!files.length) {
    throw new Error("At least one file is required.");
  }

  const formData = new FormData();
  for (const file of files) {
    formData.append("files", file);
  }

  const response = await $fetch(`/logs/${entryId}/files/upload`, {
    method: "POST",
    body: formData,
  });

  return parseJsonResponse<UploadFilesResponse>(response);
}

export async function listLogFiles(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/files`);
  return parseJsonResponse<LogFile[]>(response);
}

export async function getLogFile(entryId: string, fileId: string) {
  const response = await $fetch(`/logs/${entryId}/files/${fileId}`);
  return parseJsonResponse<LogFile>(response);
}

export async function downloadLogFile(entryId: string, fileId: string) {
  const response = await $fetch(`/logs/${entryId}/files/${fileId}/download`);
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }

  return response.blob();
}

export async function downloadTableCsv(entryId: string, tableName: string) {
  const response = await $fetch(`/logs/${entryId}/tables/${tableName}/download/csv`);
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }

  return response.blob();
}

export async function downloadTableXlsx(entryId: string, tableName: string) {
  const response = await $fetch(`/logs/${entryId}/tables/${tableName}/download/xlsx`);
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }

  return response.blob();
}

export async function downloadFilteredTable(entryId: string, tableName: string, payload: FilteredExportPayload) {
  const response = await $fetch(`/logs/${entryId}/tables/${tableName}/download/filtered`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const rawPayload = await response.text();
    throw new Error(`Request failed (${response.status}): ${rawPayload}`);
  }

  return response.blob();
}

export async function deleteLogFile(entryId: string, fileId: string) {
  const response = await $fetch(`/logs/${entryId}/files/${fileId}`, {
    method: "DELETE",
  });

  return parseJsonResponse<MessageResponse>(response);
}

export async function listLogProcesses(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/processes`);
  return parseJsonResponse<LogProcess[]>(response);
}

export async function getLogProcess(entryId: string, processId: string) {
  const response = await $fetch(`/logs/${entryId}/processes/${processId}`);
  return parseJsonResponse<LogProcess>(response);
}

export async function createLogProcess(entryId: string, payload?: CreateProcessPayload) {
  const response = await $fetch(`/logs/${entryId}/processes`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload ?? {}),
  });

  return parseJsonResponse<ProcessEnqueuedResponse>(response);
}

export async function getDashboardStats() {
  const response = await $fetch("/stats");
  return parseJsonResponse<DashboardStats>(response);
}

export async function getLogReport(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/insights`);
  if (response.status === 404) {
    return null;
  }
  return parseJsonResponse<LogInsightReport | null>(response);
}

export async function downloadWorkbookReport(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/workbook-report`, {
    method: "POST",
  });
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }
  return response.blob();
}

export async function generateLogReport(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/insights`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({}),
  });
  return parseJsonResponse<LogInsightReport>(response);
}

export type NlQueryResult = {
  sql: string;
  results: Array<Record<string, unknown>>;
  columns: string[];
};

export async function executeNlQuery(entryId: string, question: string) {
  const response = await $fetch(`/logs/${entryId}/nl-query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ question }),
  });
  return parseJsonResponse<NlQueryResult>(response);
}

export async function getLogChatMessages(entryId: string) {
  const response = await $fetch(`/logs/${entryId}/chat/messages`);
  const payload = await parseJsonResponse<ChatMessagesResponse>(response);
  return payload.messages;
}

export async function replaceLogChatMessages(entryId: string, payload: ReplaceChatMessagesPayload) {
  const response = await $fetch(`/logs/${entryId}/chat/messages`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return parseJsonResponse<ReplaceChatMessagesResponse>(response);
}

export type ChatHistoryMessage = {
  role: string;
  content: string;
};

export async function* streamChatWithLogs(
  entryId: string,
  message: string,
  history: ChatHistoryMessage[],
  signal?: AbortSignal,
): AsyncGenerator<string, void, unknown> {
  const response = await $fetch(`/logs/${entryId}/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message, history }),
    signal,
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Chat request failed (${response.status}): ${payload}`);
  }

  if (response.body === null) {
    throw new Error("Chat response body is empty.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("data: ")) {
          continue;
        }

        const data = trimmed.slice(6);
        if (data === "[DONE]") {
          return;
        }

        try {
          const parsed = JSON.parse(data) as { token?: string };
          if (typeof parsed.token === "string") {
            yield parsed.token;
          }
        } catch {
          // Ignore malformed SSE lines.
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
