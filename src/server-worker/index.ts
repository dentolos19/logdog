import { Container as CloudflareContainer } from "@cloudflare/containers";
import { Hono } from "hono";

export class Container extends CloudflareContainer<CloudflareEnv> {
  defaultPort = 8000;
  sleepAfter = "10m";
}

const app = new Hono<{ Bindings: CloudflareEnv }>();
const variables = Object.fromEntries(Object.entries(process.env)) as Record<string, string>;

app.all("*", async (c) => {
  const instance = c.env.CONTAINER.getByName("singleton");
  const state = await instance.getState();

  if (state.status !== "running") {
    await instance.startAndWaitForPorts({
      startOptions: {
        envVars: variables,
      },
    });
  }

  return await instance.fetch(c.req.raw);
});

export default app;
