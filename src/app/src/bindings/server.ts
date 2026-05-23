import { env } from "cloudflare:workers";
import { Container, getContainer } from "@cloudflare/containers";

const PORT = 3001;

export class Server extends Container<Env> {
  defaultPort = PORT;
  sleepAfter = "10m";
  envVars = Object.fromEntries(
    Object.entries(this.env).filter(([, value]) => typeof value === "string" && !!value),
  ) as Record<string, string>;
}

export const handler = async (ctx: { request: Request }) => {
  const request = ctx instanceof Request ? ctx : ctx.request;
  const url = new URL(request.url, "http://localhost");
  url.pathname = url.pathname.replace(/^\/api/, "") || "/";

  if (env.SERVER) {
    const instance = getContainer(env.SERVER, "singleton");
    return await instance.fetch(new Request(url.toString(), request));
  } else {
    url.protocol = "http";
    url.host = `localhost:${PORT}`;
    return await fetch(new Request(url.toString(), request));
  }
};
