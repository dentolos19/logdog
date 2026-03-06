import { PageHeader } from "@/app/(platform)/_components/page-header";

export default function DashboardPage() {
  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Dashboard" }]} />
      <div className={"flex flex-1 items-center justify-center"}>
        <p className={"text-muted-foreground"}>{"Dashboard"}</p>
      </div>
    </>
  );
}
