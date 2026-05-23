import handler, { createServerEntry } from "@tanstack/react-start/server-entry";

export * from "#/bindings/server";

export default createServerEntry({
  fetch(request) {
    return handler.fetch(request);
  },
});
