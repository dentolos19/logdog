import { createFileRoute } from "@tanstack/react-router";
import { getRoot } from "#/lib/server";

export const Route = createFileRoute("/(public)/damn")({
  server: {
    handlers: {
      GET: async () => {
        const value = await getRoot();
        return Response.json({ value });
      },
    },
  },
});
