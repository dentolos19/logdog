import { $fetch } from "#/lib/server/utils";

export type LogEntry = {
  id: string;
  user_id: string;
  name: string;
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
  status: string;
  classification: Record<string, unknown> | null;
  result: Record<string, unknown> | null;
  error: string | null;
  created_at: string;
  updated_at: string;
};

export type UploadFilesResponse = {
  process_id: string;
  status: string;
  files: LogFile[];
};

export type ProcessEnqueuedResponse = {
  process_id: string;
  status: string;
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
};

type UpdateLogEntryPayload = {
  name: string;
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
