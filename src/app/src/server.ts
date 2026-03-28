import { Container } from "@cloudflare/containers";
import handler from "@tanstack/react-start/server-entry";

export class Server extends Container<Env> {
  defaultPort = 8000;
  sleepAfter = "10m";
  envVars = Object.fromEntries(
    Object.entries(this.env).filter(([, value]) => typeof value === "string" && !!value),
  ) as Record<string, string>;
}

export default {
  fetch: handler.fetch,
};
