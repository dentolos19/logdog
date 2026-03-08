import { PageHeader } from "@/app/(platform)/_components/page-header";
import { ParserTesterButton } from "./_components/parser-tester";

export default function DashboardPage() {
  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Dashboard" }]} />
      <div className={"flex flex-1 flex-col gap-6 p-6"}>
        <div className={"flex flex-col gap-4 rounded-lg border p-5"}>
          <div className={"flex flex-col gap-1"}>
            <h2 className={"text-sm font-semibold"}>Semi-Structured Parser</h2>
            <p className={"text-sm text-muted-foreground"}>
              Test the parsing pipeline against your log files. Upload any plaintext, JSON, CSV, syslog,
              or key=value log and inspect the extracted fields, confidence score, and pipeline stages.
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
