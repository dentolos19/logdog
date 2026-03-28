import { createFileRoute, notFound } from "@tanstack/react-router";
import { isDevelopment } from "#/environment";
import { PageHeader } from "#/routes/(platform)/_components/-page-header";
import { ParserTesterButton } from "#/routes/(platform)/dashboard/_components/-parser-tester";

export const Route = createFileRoute("/(platform)/development/")({
  component: DevelopmentPage,
});

function DevelopmentPage() {
  if (!isDevelopment) {
    notFound();
  }

  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Development" }]} />
      <div className={"flex flex-1 flex-col gap-6 p-6"}>
        <div className={"flex flex-col gap-4 rounded-lg border p-5"}>
          <div className={"flex flex-col gap-1"}>
            <h2 className={"font-semibold text-sm"}>Semi-Structured Parser</h2>
            <p className={"text-muted-foreground text-sm"}>
              Test the parsing pipeline preview with local files. This migration keeps the design while backend parser
              test routes are still pending.
            </p>
          </div>
          <div>
            <ParserTesterButton />
          </div>
        </div>
      </div>
    </>
  );
}
