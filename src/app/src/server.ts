import handler, { createServerEntry } from "@tanstack/react-start/server-entry";

export { ContainerProxy } from "@cloudflare/containers";
export * from "#/bindings/server";

export default createServerEntry({
  fetch(request) {
    return handler.fetch(request);
  },
});
