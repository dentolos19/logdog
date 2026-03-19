"use client";

import { useEffect, useState } from "react";
import { FolderOpenIcon, FileIcon, RowsIcon, ActivityIcon } from "lucide-react";
import { PageHeader } from "@/app/(platform)/_components/page-header";
import { StatCard } from "./_components/stat-card";
import { getStats } from "@/lib/api";
import type { DashboardStats } from "@/lib/api";

export default function DashboardPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const data = await getStats();
        setStats(data);
      } catch {
        setStats(null);
      } finally {
        setLoading(false);
      }
    };
    fetchStats();
  }, []);

  const processingCount =
    (stats?.processes.pending ?? 0) +
    (stats?.processes.classified ?? 0) +
    (stats?.processes.processing ?? 0);

  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Dashboard" }]} />
      <div className={"flex flex-1 flex-col gap-6 p-6"}>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <StatCard
            title="Log Groups"
            value={stats?.log_group_count ?? 0}
            description="Total log groups created"
            icon={FolderOpenIcon}
            loading={loading}
          />
          <StatCard
            title="Total Files"
            value={stats?.total_files ?? 0}
            description="Files uploaded across all groups"
            icon={FileIcon}
            loading={loading}
          />
          <StatCard
            title="Total Rows"
            value={stats?.total_rows ?? 0}
            description="Data rows parsed and stored"
            icon={RowsIcon}
            loading={loading}
          />
          <StatCard
            title="Processing"
            value={processingCount}
            description={`${stats?.processes.completed ?? 0} completed, ${stats?.processes.failed ?? 0} failed`}
            icon={ActivityIcon}
            loading={loading}
          />
        </div>
      </div>
    </>
  );
}