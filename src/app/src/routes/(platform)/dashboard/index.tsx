import { createFileRoute } from "@tanstack/react-router";
import { ActivityIcon, FileIcon, FolderOpenIcon, RowsIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { type DashboardStats, getDashboardStats } from "#/lib/server";
import { PageHeader } from "#/routes/(platform)/-components/page-header";
import { StatCard } from "#/routes/(platform)/dashboard/-components/stat-card";

export const Route = createFileRoute("/(platform)/dashboard/")({
  component: DashboardPage,
});

function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true);
      try {
        const data = await getDashboardStats();
        setStats(data);
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
            value={stats?.log_group_count ?? 0}
          />
          <StatCard
            description={"Files uploaded across all groups"}
            icon={FileIcon}
            loading={loading}
            title={"Total Files"}
            value={stats?.total_files ?? 0}
          />
          <StatCard
            description={"Rows inferred from process results"}
            icon={RowsIcon}
            loading={loading}
            title={"Total Rows"}
            value={stats?.total_rows ?? 0}
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
