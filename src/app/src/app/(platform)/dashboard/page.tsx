"use client";

import { ActivityIcon, FileIcon, FolderOpenIcon, RowsIcon } from "lucide-react";
import { useEffect, useState } from "react";
import { PageHeader } from "@/app/(platform)/_components/page-header";
import type { DashboardStats } from "@/lib/api";
import { getStats } from "@/lib/api";
import { StatCard } from "./_components/stat-card";

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
    (stats?.processes.pending ?? 0) + (stats?.processes.classified ?? 0) + (stats?.processes.processing ?? 0);

  return (
    <>
      <PageHeader breadcrumbs={[{ label: "Dashboard" }]} />
      <div className={"flex flex-1 flex-col gap-6 p-6"}>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <StatCard
            description="Total log groups created"
            icon={FolderOpenIcon}
            loading={loading}
            title="Log Groups"
            value={stats?.log_group_count ?? 0}
          />
          <StatCard
            description="Files uploaded across all groups"
            icon={FileIcon}
            loading={loading}
            title="Total Files"
            value={stats?.total_files ?? 0}
          />
          <StatCard
            description="Data rows parsed and stored"
            icon={RowsIcon}
            loading={loading}
            title="Total Rows"
            value={stats?.total_rows ?? 0}
          />
          <StatCard
            description={`${stats?.processes.completed ?? 0} completed, ${stats?.processes.failed ?? 0} failed`}
            icon={ActivityIcon}
            loading={loading}
            title="Processing"
            value={processingCount}
          />
        </div>
      </div>
    </>
  );
}
