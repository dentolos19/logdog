import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

import { clearAuthTokens, getAccessToken, getRefreshToken, setAuthTokens } from "#/lib/auth";

type TokenResponse = {
  access_token: string;
  refresh_token: string;
};

let refreshPromise: Promise<void> | null = null;

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

async function refreshAccessToken() {
  const refreshToken = getRefreshToken();
  if (!refreshToken) {
    throw new Error("Refresh token is missing.");
  }

  const response = await fetch("/api/auth/refresh", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: refreshToken }),
  });

  if (!response.ok) {
    clearAuthTokens();
    throw new Error(`Token refresh failed (${response.status}).`);
  }

  const tokens = (await response.json()) as TokenResponse;
  setAuthTokens({ accessToken: tokens.access_token, refreshToken: tokens.refresh_token });
}

function shouldRetryWithRefresh(path: string, response: Response) {
  return response.status === 401 && !path.startsWith("/auth/") && getRefreshToken() !== null;
}

function withAuthHeaders(init?: RequestInit) {
  const headers = new Headers(init?.headers);
  const accessToken = getAccessToken();

  if (accessToken && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${accessToken}`);
  }

  return headers;
}

export const $fetch = async (path: string, init?: RequestInit) => {
  const headers = withAuthHeaders(init);

  const response = await fetch(`/api${path}`, {
    ...init,
    headers,
  });

  if (!shouldRetryWithRefresh(path, response)) {
    return response;
  }

  refreshPromise ??= refreshAccessToken().finally(() => {
    refreshPromise = null;
  });
  await refreshPromise;

  return fetch(`/api${path}`, {
    ...init,
    headers: withAuthHeaders(init),
  });
};
