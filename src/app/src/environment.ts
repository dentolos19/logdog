function getEnvVar<T>(key: string, defaultValue?: T): T {
  let value = process.env[key] as T | undefined;

  if (typeof value === "undefined") {
    value = import.meta.env[key] as T | undefined;
  }

  if (typeof value === "undefined" && typeof defaultValue !== "undefined") {
    value = defaultValue;
  }

  if (typeof value === "undefined") {
    throw new Error(`${key} is not defined.`);
  }

  return value as T;
}

export function getEnv() {
  return {
    openRouterApiKey: getEnvVar<string>("OPENROUTER_API_KEY"),
    openRouterTitle: getEnvVar<string>("OPENROUTER_TITLE", "Logdog"),
    openRouterReferer: getEnvVar<string>("OPENROUTER_REFERER", "https://dennise.me"),
    openRouterModel: getEnvVar<string>("OPENROUTER_MODEL", "moonshotai/kimi-k2.5"),
  };
}

export const ENVIRONMENT = getEnvVar<"production" | "development">("NODE_ENV", "development");
export const isProduction = ENVIRONMENT === "production";
export const isDevelopment = ENVIRONMENT === "development";
