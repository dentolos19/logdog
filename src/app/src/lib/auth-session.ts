const ACCESS_TOKEN_KEY = "logdog.access_token";
const REFRESH_TOKEN_KEY = "logdog.refresh_token";

export interface AuthSessionTokens {
  accessToken: string;
  refreshToken: string;
}

function readStorageValue(key: string) {
  if (typeof window === "undefined") {
    return null;
  }

  return window.localStorage.getItem(key);
}

function writeStorageValue(key: string, value: string) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(key, value);
}

function removeStorageValue(key: string) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.removeItem(key);
}

export function getAccessToken() {
  return readStorageValue(ACCESS_TOKEN_KEY);
}

export function getRefreshToken() {
  return readStorageValue(REFRESH_TOKEN_KEY);
}

export function setAuthSession(tokens: AuthSessionTokens) {
  writeStorageValue(ACCESS_TOKEN_KEY, tokens.accessToken);
  writeStorageValue(REFRESH_TOKEN_KEY, tokens.refreshToken);
}

export function clearAuthSession() {
  removeStorageValue(ACCESS_TOKEN_KEY);
  removeStorageValue(REFRESH_TOKEN_KEY);
}

export function getBearerAuthHeaders() {
  const headers = new Headers();
  const accessToken = getAccessToken();
  if (accessToken !== null) {
    headers.set("authorization", `Bearer ${accessToken}`);
  }

  return headers;
}
