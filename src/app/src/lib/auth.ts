import { $fetch } from "#/lib/utils";

const ACCESS_TOKEN_COOKIE_NAME = "logdog-access-token";
const REFRESH_TOKEN_COOKIE_NAME = "logdog-refresh-token";

type RegisterPayload = {
  email: string;
  password: string;
};

type LoginPayload = {
  email: string;
  password: string;
};

type User = {
  id: string;
  email: string;
  created_at: string;
};

type TokenResponse = {
  access_token: string;
  refresh_token: string;
  token_type: string;
};

type MessageResponse = {
  message: string;
};

export type AuthTokens = {
  accessToken: string;
  refreshToken: string;
};

function getCookieValue(name: string) {
  if (typeof document === "undefined") {
    return null;
  }

  const cookies = document.cookie.split(";");
  for (const cookie of cookies) {
    const [rawKey, ...rawValue] = cookie.trim().split("=");
    if (rawKey === name) {
      return decodeURIComponent(rawValue.join("="));
    }
  }

  return null;
}

function setCookieValue(name: string, value: string, maxAgeSeconds: number) {
  if (typeof document === "undefined") {
    return;
  }

  const secureSegment = window.location.protocol === "https:" ? "; Secure" : "";
  document.cookie = `${name}=${encodeURIComponent(value)}; path=/; max-age=${maxAgeSeconds}; SameSite=Lax${secureSegment}`;
}

function clearCookieValue(name: string) {
  if (typeof document === "undefined") {
    return;
  }

  document.cookie = `${name}=; path=/; max-age=0; SameSite=Lax`;
}

export function getAccessToken() {
  return getCookieValue(ACCESS_TOKEN_COOKIE_NAME);
}

export function getRefreshToken() {
  return getCookieValue(REFRESH_TOKEN_COOKIE_NAME);
}

export function setAuthTokens(tokens: AuthTokens) {
  setCookieValue(ACCESS_TOKEN_COOKIE_NAME, tokens.accessToken, 60 * 30);
  setCookieValue(REFRESH_TOKEN_COOKIE_NAME, tokens.refreshToken, 60 * 60 * 24 * 7);
}

export function clearAuthTokens() {
  clearCookieValue(ACCESS_TOKEN_COOKIE_NAME);
  clearCookieValue(REFRESH_TOKEN_COOKIE_NAME);
}

async function parseJsonResponse<T>(response: Response) {
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }

  return (await response.json()) as T;
}

export async function register(payload: RegisterPayload) {
  const response = await $fetch("/auth/register", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  return parseJsonResponse<User>(response);
}

export async function login(payload: LoginPayload) {
  const response = await $fetch("/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const tokens = await parseJsonResponse<TokenResponse>(response);
  setAuthTokens({
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token,
  });

  return tokens;
}

export async function refreshTokens(refreshToken?: string) {
  const tokenToUse = refreshToken ?? getRefreshToken();
  if (!tokenToUse) {
    throw new Error("Refresh token is missing.");
  }

  const response = await $fetch("/auth/refresh", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ refresh_token: tokenToUse }),
  });

  const tokens = await parseJsonResponse<TokenResponse>(response);
  setAuthTokens({
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token,
  });

  return tokens;
}

export async function logout() {
  const refreshToken = getRefreshToken();
  const response = await $fetch("/auth/logout", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ refresh_token: refreshToken }),
  });

  const payload = await parseJsonResponse<MessageResponse>(response);
  clearAuthTokens();
  return payload;
}

export async function getMe() {
  const response = await $fetch("/auth/me");
  return parseJsonResponse<User>(response);
}
