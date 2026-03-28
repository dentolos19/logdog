const ACCESS_TOKEN_COOKIE_NAME = "logdog-access-token";
const REFRESH_TOKEN_COOKIE_NAME = "logdog-refresh-token";

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
