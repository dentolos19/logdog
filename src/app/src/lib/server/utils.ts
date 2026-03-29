import { getAccessToken } from "#/lib/server/auth-tokens";

export const $fetch = (path: string, init?: RequestInit) => {
  const headers = new Headers(init?.headers);
  const accessToken = getAccessToken();

  if (accessToken && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${accessToken}`);
  }

  return fetch(`/api${path}`, {
    ...init,
    headers,
  });
};
