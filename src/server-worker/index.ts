import { Container as CloudflareContainer, getContainer } from "@cloudflare/containers";
import { Hono } from "hono";

export class Container extends CloudflareContainer<Env> {
	defaultPort = 8000;
	sleepAfter = "10m";
	envVars = Object.fromEntries(
		Object.entries(this.env).filter(([, value]) => typeof value === "string" && !!value),
	) as Record<string, string>;
}

const app = new Hono<{ Bindings: Env }>();

app.all("*", async (c) => {
	const instance = getContainer<Container>(c.env.CONTAINER, "singleton");
	return instance.fetch(c.req.raw);
});

export default app;
