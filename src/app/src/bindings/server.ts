import { Container, getContainer } from "@cloudflare/containers";
import { env } from "cloudflare:workers";

const PORT = 3001;
const STORAGE_HOST = "r2.internal";

export const handleStorage = async (request: Request, env: Env) => {
  const key = decodeURIComponent(new URL(request.url).pathname.slice(1));
  if (!key) {
    return new Response("Missing object key.", { status: 400 });
  }

  switch (request.method) {
    case "PUT":
      await env.BUCKET.put(key, request.body, {
        httpMetadata: { contentType: request.headers.get("content-type") ?? undefined },
      });
      return new Response(null, { status: 204 });
    case "GET": {
      const object = await env.BUCKET.get(key);
      return object ? new Response(object.body) : new Response(null, { status: 404 });
    }
    case "DELETE":
      await env.BUCKET.delete(key);
      return new Response(null, { status: 204 });
    default:
      return new Response("Method not allowed.", { headers: { Allow: "DELETE, GET, PUT" }, status: 405 });
  }
};

export class Server extends Container<Env> {
  static outboundByHost = { [STORAGE_HOST]: handleStorage };

  defaultPort = PORT;
  sleepAfter = "10m";
  envVars = {
    ...(Object.fromEntries(
      Object.entries(this.env).filter(([, value]) => typeof value === "string" && !!value),
    ) as Record<string, string>),
    STORAGE_URL: import.meta.env.DEV ? "http://localhost:3000/assets" : `http://${STORAGE_HOST}`,
  };
}

export const handler = async (ctx: { request: Request }) => {
  const request = ctx instanceof Request ? ctx : ctx.request;
  const url = new URL(request.url, "http://localhost");
  url.pathname = url.pathname.replace(/^\/api/, "") || "/";

  if (import.meta.env.DEV) {
    url.protocol = "http";
    url.host = `localhost:${PORT}`;
    return await fetch(new Request(url.toString(), request));
  }

  const instance = getContainer(env.SERVER as DurableObjectNamespace<Server>, "singleton");
  return await instance.fetch(new Request(url.toString(), request));
};
