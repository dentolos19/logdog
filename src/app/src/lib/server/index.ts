import { env } from "cloudflare:workers";
import { getContainer } from "@cloudflare/containers";

export const $fetch = (path: string, init?: RequestInit) => {
  const instance = getContainer(env.SERVER, "singleton");
  return instance.fetch(`http://cloudflare.container${path}`, init);
};

export const getRoot = () => {
  return $fetch("/").then((res) => res.text());
};

export const getEnv = () => {
  return $fetch("/env").then((res) => res.json());
};
