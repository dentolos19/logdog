import { env } from "cloudflare:workers";
import { getContainer } from "@cloudflare/containers";
import { getAccessToken } from "#/lib/server/token-store";

export const $fetch = (path: string, init?: RequestInit) => {
  const instance = getContainer(env.SERVER, "singleton");
  const headers = new Headers(init?.headers);
  const accessToken = getAccessToken();
  if (accessToken && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${accessToken}`);
  }

  return instance.fetch(`http://cloudflare.container${path}`, {
    ...init,
    headers,
  });
};
