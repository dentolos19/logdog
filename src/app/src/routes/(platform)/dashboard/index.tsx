import { createFileRoute } from "@tanstack/react-router";
import { ActivityIcon, FileIcon, FolderOpenIcon, RowsIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { listLogEntries, listLogFiles, listLogProcesses } from "#/lib/server";
import { PageHeader } from "#/routes/(platform)/_components/-page-header";
import { StatCard } from "#/routes/(platform)/dashboard/_components/-stat-card";

export const Route = createFileRoute("/(platform)/dashboard/")({
  component: DashboardPage,
});

type DashboardStats = {
  logGroupCount: number;
  totalFiles: number;
  totalRows: number;
  processes: {
    queued: number;
    processing: number;
    completed: number;
    failed: number;
  };
};

function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true);
      try {
        const entries = await listLogEntries();
        const processCollections = await Promise.all(
          entries.map((entry) => listLogProcesses(entry.id).catch(() => [])),
        );
        const fileCollections = await Promise.all(entries.map((entry) => listLogFiles(entry.id).catch(() => [])));

        let queued = 0;
        let processing = 0;
        let completed = 0;
        let failed = 0;
        let totalRows = 0;

        for (const processes of processCollections) {
          for (const process of processes) {
            if (process.status === "queued") {
              queued += 1;
            } else if (process.status === "processing") {
              processing += 1;
            } else if (process.status === "completed") {
              completed += 1;
            } else if (process.status === "failed") {
              failed += 1;
            }

            const result = process.result;
            if (result !== null && typeof result === "object") {
              const records = (result as Record<string, unknown>).records;
              if (records !== null && typeof records === "object" && !Array.isArray(records)) {
                for (const tableRows of Object.values(records)) {
                  if (Array.isArray(tableRows)) {
                    totalRows += tableRows.length;
                  }
                }
              }
            }
          }
        }

        const totalFiles = fileCollections.reduce((accumulator, files) => accumulator + files.length, 0);

        setStats({
          logGroupCount: entries.length,
          totalFiles,
          totalRows,
          processes: {
            queued,
            processing,
            completed,
            failed,
          },
        });
      } catch {
        setStats(null);
      } finally {
        setLoading(false);
      }
    };

    void fetchStats();
  }, []);

  const processingCount = useMemo(() => {
    return (stats?.processes.queued ?? 0) + (stats?.processes.processing ?? 0);
  }, [stats]);

  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Dashboard" }]} />
      <div className={"flex flex-1 flex-col gap-6 p-6"}>
        <div className={"grid gap-4 md:grid-cols-2 lg:grid-cols-4"}>
          <StatCard
            description={"Total log groups created"}
            icon={FolderOpenIcon}
            loading={loading}
            title={"Log Groups"}
            value={stats?.logGroupCount ?? 0}
          />
          <StatCard
            description={"Files uploaded across all groups"}
            icon={FileIcon}
            loading={loading}
            title={"Total Files"}
            value={stats?.totalFiles ?? 0}
          />
          <StatCard
            description={"Rows inferred from process results"}
            icon={RowsIcon}
            loading={loading}
            title={"Total Rows"}
            value={stats?.totalRows ?? 0}
          />
          <StatCard
            description={`${stats?.processes.completed ?? 0} completed, ${stats?.processes.failed ?? 0} failed`}
            icon={ActivityIcon}
            loading={loading}
            title={"Processing"}
            value={processingCount}
          />
        </div>
      </div>
    </>
  );
}
