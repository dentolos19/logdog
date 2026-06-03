import { Link } from "@tanstack/react-router";
import {
  AlertTriangleIcon,
  BarChart3Icon,
  DatabaseZapIcon,
  FileIcon,
  FileTextIcon,
  GaugeIcon,
  LightbulbIcon,
  RefreshCwIcon,
  RowsIcon,
  ShieldAlertIcon,
  SparklesIcon,
  TableIcon,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "#/components/ui/card";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import {
  type GroupStats,
  generateLogReport,
  generateTableSummary,
  getGroupStats,
  getLogReport,
  getTableSummary,
  type LogInsightReport,
  type TableSummaryResponse,
} from "#/lib/logs";
import { StatCard } from "#/routes/(platform)/dashboard/-components/stat-card";

// ── Types ──────────────────────────────────────────────────────────────────────

type ReportTabProps = {
  logGroupId: string;
  tableNames: { id: string; name: string }[];
};

type TableReportState = {
  tableName: string;
  displayName: string;
  report: TableSummaryResponse | null;
  loading: boolean;
  generating: boolean;
};

// ── Main Component ─────────────────────────────────────────────────────────────

export function ReportTab({ logGroupId, tableNames }: ReportTabProps) {
  // Report state
  const [report, setReport] = useState<LogInsightReport | null>(null);
  const [loadingReport, setLoadingReport] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Group stats state
  const [stats, setStats] = useState<GroupStats | null>(null);

  // Per-table reports state
  const [tableReports, setTableReports] = useState<Map<string, TableReportState>>(new Map());
  const loadingTableIdsRef = useRef<Set<string>>(new Set());

  // ── Fetch Report ──────────────────────────────────────────────────────────

  const fetchReport = useCallback(async () => {
    setLoadingReport(true);
    setError(null);
    try {
      const data = await getLogReport(logGroupId);
      setReport(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to load report.";
      setError(message);
    } finally {
      setLoadingReport(false);
    }
  }, [logGroupId]);

  const fetchStats = useCallback(async () => {
    try {
      const data = await getGroupStats(logGroupId);
      setStats(data);
    } catch {
      // Stats are non-critical; silently fail
    }
  }, [logGroupId]);

  useEffect(() => {
    void fetchReport();
    void fetchStats();
  }, [fetchReport, fetchStats]);

  // ── Generate / Regenerate Report ─────────────────────────────────────────

  const handleGenerate = useCallback(async () => {
    setGenerating(true);
    try {
      const data = await generateLogReport(logGroupId);
      setReport(data);
      toast.success("Report generated.");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to generate report.";
      toast.error(message);
    } finally {
      setGenerating(false);
    }
  }, [logGroupId]);

  // ── Per-Table Reports ──────────────────────────────────────────────────

  useEffect(() => {
    for (const table of tableNames) {
      const state = tableReports.get(table.id);
      if (state === undefined && !loadingTableIdsRef.current.has(table.id)) {
        loadingTableIdsRef.current.add(table.id);
        setTableReports((prev) => {
          if (prev.has(table.id)) {
            return prev;
          }

          const next = new Map(prev);
          next.set(table.id, {
            tableName: table.id,
            displayName: table.name,
            report: null,
            loading: true,
            generating: false,
          });
          return next;
        });

        void loadTableSummary(logGroupId, table.id, table.name, setTableReports).finally(() => {
          loadingTableIdsRef.current.delete(table.id);
        });
      }
    }
  }, [tableNames, logGroupId, tableReports]);

  const handleGenerateTableSummary = useCallback(
    async (tableId: string, displayName: string) => {
      setTableReports((prev) => {
        const next = new Map(prev);
        const existing = next.get(tableId);
        next.set(tableId, {
          tableName: tableId,
          displayName,
          report: existing?.report ?? null,
          loading: false,
          generating: true,
        });
        return next;
      });

      try {
        const data = await generateTableSummary(logGroupId, tableId);
        setTableReports((prev) => {
          const next = new Map(prev);
          next.set(tableId, {
            tableName: tableId,
            displayName,
            report: data,
            loading: false,
            generating: false,
          });
          return next;
        });
        toast.success(`Summary generated for "${displayName}".`);
      } catch (err) {
        const message = err instanceof Error ? err.message : "Failed to generate table summary.";
        toast.error(message);
        setTableReports((prev) => {
          const next = new Map(prev);
          const existing = next.get(tableId);
          if (existing !== undefined) {
            next.set(tableId, { ...existing, generating: false });
          }
          return next;
        });
      }
    },
    [logGroupId],
  );

  // ── Derived ─────────────────────────────────────────────────────────────

  const hasReport = report !== null;
  const isLoading = loadingReport;
  const severityAttrs = getSeverityAttrs(report?.severity ?? "");

  const statCards = useMemo(() => {
    if (!stats) return [];
    return [
      { title: "Files", value: stats.file_count, description: "Uploaded log files", icon: FileIcon },
      { title: "Tables", value: stats.table_count, description: "Parsed data tables", icon: TableIcon },
      { title: "Processes", value: stats.process_count, description: "Total processing runs", icon: DatabaseZapIcon },
      {
        title: "Total Rows",
        value: stats.total_rows.toLocaleString(),
        description: "Across all tables",
        icon: RowsIcon,
      },
      {
        title: "Confidence",
        value: stats.parser_confidence != null ? `${Math.round(stats.parser_confidence * 100)}%` : "—",
        description: "Avg parser confidence",
        icon: GaugeIcon,
      },
    ];
  }, [stats]);

  // ── Loading ─────────────────────────────────────────────────────────────

  if (isLoading) {
    return (
      <div className={"flex flex-col gap-6"}>
        <div className={"grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5"}>
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton className={"h-[120px] w-full rounded-xl"} key={i} />
          ))}
        </div>
        <Skeleton className={"h-[200px] w-full rounded-xl"} />
        <Skeleton className={"h-[200px] w-full rounded-xl"} />
      </div>
    );
  }

  // ── Error ───────────────────────────────────────────────────────────────

  if (error !== null) {
    return (
      <Alert className={"border-destructive/30"} variant={"destructive"}>
        <AlertTriangleIcon className={"size-4"} />
        <AlertTitle>Error Loading Report</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  // ── Empty ───────────────────────────────────────────────────────────────

  if (!hasReport) {
    return (
      <div className={"flex flex-col gap-6"}>
        {/* Stats cards */}
        {statCards.length > 0 && (
          <div className={"grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5"}>
            {statCards.map((card) => (
              <StatCard
                description={card.description}
                icon={card.icon}
                key={card.title}
                title={card.title}
                value={card.value}
              />
            ))}
          </div>
        )}

        {/* No AI Report yet */}
        <Card className={"border-dashed"}>
          <CardContent className={"flex flex-col items-center gap-4 py-12"}>
            <div className={"bg-muted rounded-lg p-2"}>
              <FileTextIcon className={"text-muted-foreground size-5"} />
            </div>
            <div className={"text-center"}>
              <p className={"font-semibold"}>No AI Report Yet</p>
              <p className={"text-muted-foreground mt-1 max-w-sm text-sm"}>
                Generate an AI-powered insight report to see summaries, severity analysis, anomalies, and
                recommendations for this log group.
              </p>
            </div>
            <Button className={"mt-2"} disabled={generating} onClick={() => void handleGenerate()}>
              {generating ? <Spinner className={"size-4"} /> : <SparklesIcon className={"size-4"} />}
              {generating ? "Generating..." : "Generate Report"}
            </Button>
          </CardContent>
        </Card>

        {/* Per-Table Reports */}
        {tableNames.length > 0 && (
          <div className={"flex flex-col gap-3"}>
            <PerTableReportsGrid
              handleGenerateTableSummary={handleGenerateTableSummary}
              logGroupId={logGroupId}
              tableNames={tableNames}
              tableReports={tableReports}
            />
          </div>
        )}
      </div>
    );
  }

  // ── Report Render ──────────────────────────────────────────────────────

  return (
    <div className={"flex flex-col gap-6"}>
      {/* Stats Cards */}
      {statCards.length > 0 && (
        <div className={"grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5"}>
          {statCards.map((card) => (
            <StatCard
              description={card.description}
              icon={card.icon}
              key={card.title}
              title={card.title}
              value={card.value}
            />
          ))}
        </div>
      )}

      {/* AI Insight Report */}
      <div className={"flex flex-col gap-4"}>
        <h2 className={"text-muted-foreground flex items-center gap-2 text-sm font-medium"}>
          <SparklesIcon className={"size-4"} />
          AI Insight Report
        </h2>
        <div
          className={"bg-muted/20 flex items-center gap-4 rounded-lg border p-4"}
          style={{ borderColor: `${severityAttrs.hex}25` }}
        >
          <div
            className={"flex size-10 shrink-0 items-center justify-center rounded-lg"}
            style={{ background: severityAttrs.hex, color: severityAttrs.textColor }}
          >
            {severityAttrs.icon}
          </div>
          <div className={"min-w-0 flex-1"}>
            <div className={"flex items-center gap-2"}>
              <span className={"text-sm font-semibold uppercase"} style={{ color: severityAttrs.hex }}>
                {report.severity}
              </span>
              <span className={"text-muted-foreground text-xs"}>Severity</span>
            </div>
            <p className={"text-muted-foreground mt-0.5 text-sm leading-relaxed"}>{report.summary}</p>
          </div>
        </div>

        {/* Root Cause + Top Errors */}
        <div className={"grid gap-4 md:grid-cols-2"}>
          <Card className={"border-l-4"} style={{ borderLeftColor: severityAttrs.hex }}>
            <CardHeader className={"pb-2"}>
              <CardDescription>Root Cause Hypothesis</CardDescription>
            </CardHeader>
            <CardContent>
              <p className={"text-sm leading-relaxed"}>{report.root_cause_hypothesis}</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className={"pb-2"}>
              <CardDescription>Top Errors</CardDescription>
            </CardHeader>
            <CardContent>
              {report.top_errors.length === 0 ? (
                <p className={"text-muted-foreground text-sm"}>No top errors identified.</p>
              ) : (
                <ul className={"space-y-1.5"}>
                  {report.top_errors.map((error, index) => (
                    <li className={"flex items-start gap-2 text-sm"} key={index}>
                      <span
                        className={"mt-1.5 block size-1.5 shrink-0 rounded-full"}
                        style={{ background: severityAttrs.hex }}
                      />
                      <span>{error}</span>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Log Sequence Narrative */}
        <Card>
          <CardHeader className={"pb-2"}>
            <CardTitle className={"flex items-center gap-2 text-sm font-medium"}>
              <FileTextIcon className={"text-muted-foreground size-4"} />
              Log Sequence Narrative
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div
              className={"bg-muted/30 rounded-lg border-l-4 p-4 text-sm leading-relaxed"}
              style={{ borderLeftColor: `${severityAttrs.hex}60` }}
            >
              {report.log_sequence_narrative}
            </div>
          </CardContent>
        </Card>

        {/* Recommendations + Anomalies */}
        <div className={"grid gap-4 md:grid-cols-2"}>
          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"flex items-center gap-2 text-sm font-medium"}>
                <LightbulbIcon className={"text-muted-foreground size-4"} />
                Recommendations
              </CardTitle>
            </CardHeader>
            <CardContent>
              {report.recommendations.length === 0 ? (
                <p className={"text-muted-foreground text-sm"}>No recommendations.</p>
              ) : (
                <ul className={"space-y-2"}>
                  {report.recommendations.map((rec, index) => (
                    <li className={"flex items-start gap-2.5 text-sm"} key={index}>
                      <span
                        className={
                          "flex size-5 shrink-0 items-center justify-center rounded-full text-[11px] font-bold"
                        }
                        style={{ background: `${severityAttrs.hex}20`, color: severityAttrs.hex }}
                      >
                        {index + 1}
                      </span>
                      <span className={"mt-0.5"}>{rec}</span>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"flex items-center gap-2 text-sm font-medium"}>
                <AlertTriangleIcon className={"text-muted-foreground size-4"} />
                Anomalies
              </CardTitle>
            </CardHeader>
            <CardContent>
              {report.anomalies.length === 0 ? (
                <p className={"text-muted-foreground text-sm"}>No anomalies detected.</p>
              ) : (
                <ul className={"space-y-2"}>
                  {report.anomalies.map((anomaly, index) => (
                    <li
                      className={
                        "flex items-start gap-2.5 rounded-lg border border-rose-500/15 bg-rose-500/5 p-3 text-sm"
                      }
                      key={index}
                    >
                      <ShieldAlertIcon className={"mt-0.5 size-4 shrink-0 text-rose-400"} />
                      <div>
                        <span className={"text-xs font-medium text-rose-400 uppercase"}>Anomaly {index + 1}</span>
                        <p className={"text-muted-foreground mt-0.5"}>{anomaly}</p>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Action Row */}
        <div className={"bg-muted/20 flex items-center justify-between gap-4 rounded-lg border px-4 py-3"}>
          <p className={"text-muted-foreground text-xs"}>Run the analysis again with fresh data.</p>
          <Button disabled={generating} onClick={() => void handleGenerate()} size={"sm"} variant={"outline"}>
            {generating ? <Spinner className={"size-3"} /> : <RefreshCwIcon />}
            {generating ? "Generating..." : "Regenerate Report"}
          </Button>
        </div>
      </div>

      {/* Per-Table Reports */}
      <div className={"flex flex-col gap-4"}>
        <h2 className={"text-muted-foreground flex items-center gap-2 text-sm font-medium"}>
          <TableIcon className={"size-4"} />
          Per-Table Reports
        </h2>
        <PerTableReportsGrid
          handleGenerateTableSummary={handleGenerateTableSummary}
          logGroupId={logGroupId}
          tableNames={tableNames}
          tableReports={tableReports}
        />
      </div>
    </div>
  );
}

// ── PerTableReportsGrid ────────────────────────────────────────────────────────

function PerTableReportsGrid({
  logGroupId,
  tableNames,
  tableReports,
  handleGenerateTableSummary,
}: {
  logGroupId: string;
  tableNames: { id: string; name: string }[];
  tableReports: Map<string, TableReportState>;
  handleGenerateTableSummary: (tableId: string, displayName: string) => void;
}) {
  if (tableNames.length === 0) {
    return (
      <Card className={"border-dashed"}>
        <CardContent className={"flex flex-col items-center gap-3 py-10"}>
          <div className={"bg-muted rounded-lg p-2"}>
            <TableIcon className={"text-muted-foreground size-5"} />
          </div>
          <div className={"text-center"}>
            <p className={"text-sm font-medium"}>No tables available</p>
            <p className={"text-muted-foreground text-xs"}>Upload and parse log files to generate per-table reports.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className={"grid gap-4 md:grid-cols-2"}>
      {tableNames.map((table) => {
        const state = tableReports.get(table.id);
        const report = state?.report ?? null;
        const loading = state?.loading ?? false;
        const generating = state?.generating ?? false;
        const sevAttrs = report !== null ? getSeverityAttrs(report.severity) : null;

        return (
          <Card key={table.id}>
            <CardHeader className={"pb-3"}>
              <div className={"flex items-center justify-between gap-2"}>
                <CardTitle className={"flex items-center gap-2 font-mono text-sm"}>
                  <div className={"bg-muted rounded-lg p-1.5"}>
                    <TableIcon className={"text-muted-foreground size-3.5 shrink-0"} />
                  </div>
                  <Link
                    className={"truncate underline-offset-2 hover:underline"}
                    params={{ id: logGroupId, tableId: table.id }}
                    to={"/logs/$id/$tableId"}
                  >
                    {table.name}
                  </Link>
                </CardTitle>
                {report !== null && (
                  <Badge
                    className={"shrink-0 border-0 font-medium"}
                    style={{
                      background: `${sevAttrs?.hex ?? "var(--secondary)"}20`,
                      color: sevAttrs?.hex ?? "var(--secondary-foreground)",
                    }}
                  >
                    {report.severity.toUpperCase()}
                  </Badge>
                )}
              </div>
            </CardHeader>
            <CardContent className={"flex flex-col gap-3"}>
              {loading ? (
                <div className={"space-y-2"}>
                  <Skeleton className={"h-4 w-full"} />
                  <Skeleton className={"h-4 w-3/4"} />
                  <Skeleton className={"h-4 w-1/2"} />
                </div>
              ) : report !== null ? (
                <>
                  <p className={"text-muted-foreground text-sm leading-relaxed"}>{report.summary}</p>

                  {report.key_observations.length > 0 && (
                    <div>
                      <p className={"text-muted-foreground mb-1.5 flex items-center gap-1.5 text-xs font-medium"}>
                        <BarChart3Icon className={"size-3"} />
                        Key Observations
                      </p>
                      <ul className={"space-y-1"}>
                        {report.key_observations.map((obs, i) => (
                          <li className={"text-muted-foreground flex items-start gap-2 text-xs"} key={i}>
                            <span className={"bg-muted-foreground/40 mt-1.5 block size-1 shrink-0 rounded-full"} />
                            <span>{obs}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {report.next_actions.length > 0 && (
                    <div>
                      <p className={"text-muted-foreground mb-1.5 flex items-center gap-1.5 text-xs font-medium"}>
                        <LightbulbIcon className={"size-3"} />
                        Next Actions
                      </p>
                      <ul className={"space-y-1"}>
                        {report.next_actions.map((action, i) => (
                          <li className={"text-muted-foreground flex items-start gap-2 text-xs"} key={i}>
                            <span className={"bg-muted-foreground/40 mt-1.5 block size-1 shrink-0 rounded-full"} />
                            <span>{action}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {report.errors_or_anomalies.length > 0 && (
                    <div className={"rounded-lg border border-rose-500/15 bg-rose-500/5 p-3"}>
                      <p className={"mb-1.5 flex items-center gap-1.5 text-xs font-medium text-rose-400"}>
                        <AlertTriangleIcon className={"size-3"} />
                        Errors & Anomalies
                      </p>
                      <ul className={"space-y-1"}>
                        {report.errors_or_anomalies.map((err, i) => (
                          <li className={"text-muted-foreground flex items-start gap-2 text-xs"} key={i}>
                            <span className={"mt-1.5 block size-1 shrink-0 rounded-full bg-rose-400/60"} />
                            <span>{err}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  <div className={"mt-1 flex"}>
                    <Button
                      className={"h-7 text-xs"}
                      disabled={generating}
                      onClick={() => handleGenerateTableSummary(table.id, table.name)}
                      size={"sm"}
                      variant={"ghost"}
                    >
                      {generating ? <Spinner className={"size-3"} /> : <RefreshCwIcon />}
                      {generating ? "Regenerating..." : "Regenerate"}
                    </Button>
                  </div>
                </>
              ) : (
                <div className={"flex flex-col items-center gap-3 py-4"}>
                  <p className={"text-muted-foreground text-sm"}>No summary yet.</p>
                  <Button
                    className={"h-7 text-xs"}
                    disabled={generating}
                    onClick={() => handleGenerateTableSummary(table.id, table.name)}
                    size={"sm"}
                  >
                    {generating ? <Spinner className={"size-3"} /> : <SparklesIcon className={"size-3"} />}
                    {generating ? "Generating..." : "Generate Summary"}
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}

// ── Helpers ────────────────────────────────────────────────────────────────────

async function loadTableSummary(
  logGroupId: string,
  tableId: string,
  displayName: string,
  setTableReports: React.Dispatch<React.SetStateAction<Map<string, TableReportState>>>,
) {
  try {
    const data = await getTableSummary(logGroupId, tableId);
    setTableReports((prev) => {
      const next = new Map(prev);
      next.set(tableId, {
        tableName: tableId,
        displayName,
        report: data,
        loading: false,
        generating: false,
      });
      return next;
    });
  } catch {
    setTableReports((prev) => {
      const next = new Map(prev);
      next.set(tableId, {
        tableName: tableId,
        displayName,
        report: null,
        loading: false,
        generating: false,
      });
      return next;
    });
  }
}

function getSeverityAttrs(severity: string): {
  className: string;
  variant: "default" | "secondary";
  hex: string;
  textColor: string;
  icon: React.ReactNode;
} {
  const normalized = severity.trim().toLowerCase();
  if (normalized === "critical") {
    return {
      className: "bg-red-600 text-white",
      variant: "default",
      hex: "#DC2626",
      textColor: "#FFFFFF",
      icon: <ShieldAlertIcon className={"size-5"} />,
    };
  }
  if (normalized === "high") {
    return {
      className: "bg-orange-500 text-white",
      variant: "default",
      hex: "#F97316",
      textColor: "#FFFFFF",
      icon: <AlertTriangleIcon className={"size-5"} />,
    };
  }
  if (normalized === "medium") {
    return {
      className: "bg-yellow-500 text-black",
      variant: "default",
      hex: "#EAB308",
      textColor: "#000000",
      icon: <AlertTriangleIcon className={"size-5"} />,
    };
  }
  return {
    className: "",
    variant: "secondary",
    hex: "var(--muted-foreground)",
    textColor: "var(--foreground)",
    icon: <BarChart3Icon className={"size-5"} />,
  };
}
