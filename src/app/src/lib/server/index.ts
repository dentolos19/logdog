import { env } from "cloudflare:workers";
import { getContainer } from "@cloudflare/containers";

export const $fetch = (path: string, init?: RequestInit) => {
  const instance = getContainer(env.SERVER, "singleton");
  return instance.fetch(`http://cloudflare.container${path}`, init);
};

export * from "./root";
