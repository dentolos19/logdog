import { createFileRoute } from "@tanstack/react-router";
import { env } from "cloudflare:workers";

import { handleStorage } from "#/bindings/server";

export const Route = createFileRoute("/assets/$")({
  server: {
    handlers: {
      ANY: ({ request }) => {
        if (!import.meta.env.DEV) {
          return new Response(null, { status: 404 });
        }

        const url = new URL(request.url);
        url.pathname = url.pathname.replace(/^\/assets/, "");
        return handleStorage(new Request(url, request), env);
      },
    },
  },
});
