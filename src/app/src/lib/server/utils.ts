import { getAccessToken } from "#/lib/server/store";

const API_BASE_PATH = "/api";

function normalizePath(path: string) {
  if (path.startsWith("/")) {
    return path;
  }

  throw new Error("API path must start with '/'.");
}

export const $fetch = (path: string, init?: RequestInit) => {
  const normalizedPath = normalizePath(path);

  const headers = new Headers(init?.headers);
  const accessToken = getAccessToken();
  if (accessToken && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${accessToken}`);
  }

  return fetch(`${API_BASE_PATH}${normalizedPath}`, {
    ...init,
    headers,
  });
};
