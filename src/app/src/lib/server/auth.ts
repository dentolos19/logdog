import { clearAuthTokens, getRefreshToken, setAuthTokens } from "#/lib/server/store";
import { $fetch } from "#/lib/server/utils";

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
  const response = await $fetch("/auth/logout", {
    method: "POST",
  });

  const payload = await parseJsonResponse<MessageResponse>(response);
  clearAuthTokens();
  return payload;
}

export async function getMe() {
  const response = await $fetch("/auth/me");
  return parseJsonResponse<User>(response);
}
