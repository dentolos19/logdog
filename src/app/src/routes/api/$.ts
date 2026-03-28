import { env } from "cloudflare:workers";
import { getContainer } from "@cloudflare/containers";
import { createFileRoute } from "@tanstack/react-router";

function buildTargetPath(splatPath: string | undefined, searchParams: string) {
  const pathname = splatPath ? `/${splatPath}` : "/";
  if (searchParams.length === 0) {
    return pathname;
  }

  return `${pathname}?${searchParams}`;
}

export const Route = createFileRoute("/api/$")({
  server: {
    handlers: {
      GET: proxyToBackend,
      POST: proxyToBackend,
      PUT: proxyToBackend,
      PATCH: proxyToBackend,
      DELETE: proxyToBackend,
      HEAD: proxyToBackend,
      OPTIONS: proxyToBackend,
    },
  },
});

async function proxyToBackend({ params, request }: { params: { _splat?: string }; request: Request }) {
  const instance = getContainer(env.SERVER, "singleton");
  const requestUrl = new URL(request.url);
  const targetPath = buildTargetPath(params._splat, requestUrl.searchParams.toString());
  const targetUrl = `http://cloudflare.container${targetPath}`;

  const response = await instance.fetch(new Request(targetUrl, request));
  return response;
}
