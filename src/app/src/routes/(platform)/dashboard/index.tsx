import { createFileRoute } from "@tanstack/react-router";
import { BarChart3Icon, FileIcon, FolderOpenIcon, GaugeIcon, PieChartIcon, RowsIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, Cell, Pie, PieChart, XAxis } from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "#/components/ui/card";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "#/components/ui/chart";
import { Skeleton } from "#/components/ui/skeleton";
import { type DashboardStats, getDashboardStats } from "#/lib/server";
import { PageHeader } from "#/routes/(platform)/-components/page-header";
import { StatCard } from "#/routes/(platform)/dashboard/-components/stat-card";

const CHART_COLORS = ["#F9F618", "#F4F4F4", "#9A9A9A", "#404040", "#D9D9D9", "#6B6B6B", "#C8C8C8", "#141414"];

const processStatusChartConfig = {
  queued: { label: "Queued", color: "#3B82F6" },
  processing: { label: "Processing", color: "#F59E0B" },
  completed: { label: "Completed", color: "#22C55E" },
  failed: { label: "Failed", color: "#EF4444" },
} satisfies import("#/components/ui/chart").ChartConfig;

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

  const processStatusData = useMemo(() => {
    if (!stats) return [];
    return [
      { status: "queued", value: stats.processes.queued },
      { status: "processing", value: stats.processes.processing },
      { status: "completed", value: stats.processes.completed },
      { status: "failed", value: stats.processes.failed },
    ];
  }, [stats]);

  const fileFormatData = useMemo(() => {
    if (!stats) return [];
    return stats.format_distribution.map((item) => ({
      format: item.format,
      value: item.count,
    }));
  }, [stats]);

  const fileFormatChartConfig = useMemo(() => {
    const config: Record<string, { label: string; color: string }> = {};
    fileFormatData.forEach((item, index) => {
      const label = item.format.replace(/^\./, "").toUpperCase() || "UNKNOWN";
      config[item.format] = { label, color: CHART_COLORS[index % CHART_COLORS.length] };
    });
    return config;
  }, [fileFormatData]);

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
            description={"Across all completed processes"}
            icon={GaugeIcon}
            loading={loading}
            title={"Avg Parser Confidence"}
            value={
              stats?.avg_parser_confidence != null
                ? `${Math.round(stats.avg_parser_confidence * 100)}%`
                : "—"
            }
          />
        </div>

        <div className={"grid gap-4 md:grid-cols-2"}>
          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"flex items-center gap-2 font-medium text-sm"}>
                <BarChart3Icon className={"size-4 text-muted-foreground"} />
                Process Status
              </CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <Skeleton className={"h-[200px] w-full"} />
              ) : processStatusData.length === 0 || processStatusData.every((d) => d.value === 0) ? (
                <p className={"text-muted-foreground py-8 text-center text-sm"}>No process data yet.</p>
              ) : (
                <ChartContainer className={"h-[200px] w-full"} config={processStatusChartConfig}>
                  <BarChart data={processStatusData}>
                    <XAxis dataKey="status" tickLine={false} axisLine={false} />
                    <ChartTooltip content={<ChartTooltipContent nameKey="status" />} />
                    <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                      {processStatusData.map((entry) => (
                        <Cell fill={`var(--color-${entry.status})`} key={`cell-${entry.status}`} />
                      ))}
                    </Bar>
                  </BarChart>
                </ChartContainer>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"flex items-center gap-2 font-medium text-sm"}>
                <PieChartIcon className={"size-4 text-muted-foreground"} />
                File Format Distribution
              </CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <Skeleton className={"h-[200px] w-full"} />
              ) : fileFormatData.length === 0 ? (
                <p className={"text-muted-foreground py-8 text-center text-sm"}>No file format data yet.</p>
              ) : (
                <ChartContainer className={"h-[200px] w-full"} config={fileFormatChartConfig}>
                  <PieChart>
                    <Pie
                      cx="50%"
                      cy="50%"
                      data={fileFormatData}
                      dataKey="value"
                      nameKey="format"
                      outerRadius={80}
                      paddingAngle={2}
                    >
                      {fileFormatData.map((entry, index) => (
                        <Cell fill={CHART_COLORS[index % CHART_COLORS.length]} key={`cell-${entry.format}`} />
                      ))}
                    </Pie>
                    <ChartTooltip content={<ChartTooltipContent nameKey="format" />} />
                    <ChartLegend content={<ChartLegendContent nameKey="format" />} />
                  </PieChart>
                </ChartContainer>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </>
  );
}
