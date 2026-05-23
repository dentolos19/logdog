import { $fetch } from "#/lib/utils";

export const getRoot = () => {
  return $fetch("/").then((res) => res.text());
};

export const getEnv = () => {
  return $fetch("/env").then((res) => res.json());
};
