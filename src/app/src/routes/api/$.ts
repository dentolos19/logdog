import { createFileRoute } from "@tanstack/react-router";

import { handler } from "#/bindings/server";

export const Route = createFileRoute("/api/$")({
  server: {
    handlers: {
      ANY: handler,
    },
  },
});
