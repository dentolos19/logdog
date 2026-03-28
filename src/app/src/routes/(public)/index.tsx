import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/(public)/")({
  component: Page,
});

function Page() {
  return <div>Hello "/"!</div>;
}
