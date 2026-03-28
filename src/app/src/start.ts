import { env } from "cloudflare:workers";
import { Container as CloudflareContainer, getContainer } from "@cloudflare/containers";
import handler from "@tanstack/react-start/server-entry";

export class Container extends CloudflareContainer<Env> {
  defaultPort = 8000;
  sleepAfter = "10m";
  envVars = Object.fromEntries(
    Object.entries(this.env).filter(([, value]) => typeof value === "string" && !!value),
  ) as Record<string, string>;
}

export default {
  async fetch(request: Request) {
    if (request.url.startsWith("/apix")) {
      const instance = getContainer<Container>(
        env.CONTAINER as unknown as DurableObjectNamespace<Container>,
        "singleton",
      );
      return instance.fetch(request);
    } else {
      return handler.fetch(request);
    }
  },
};
