import { createFileRoute, Link } from "@tanstack/react-router";
import type { ColumnDef, PaginationState, SortingState, VisibilityState } from "@tanstack/react-table";
import { format } from "date-fns";
import { ChevronDownIcon, DownloadIcon, FileSpreadsheetIcon, FileTextIcon, InfoIcon, TableIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { DataTable, DataTableColumnHeader, DataTableViewOptions } from "#/components/ui/data-table";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "#/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "#/components/ui/dropdown-menu";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import {
  downloadLogFile,
  downloadTableCsv,
  downloadTableXlsx,
  getLogEntry,
  type LogEntry,
  type LogFile,
  type LogProcess,
  listLogFiles,
  listLogProcesses,
} from "#/lib/server";
import { PageHeader } from "#/routes/(platform)/-components/page-header";
import {
  formatFileSize,
  getTableDisplayName,
  inferTablesFromProcesses,
  type TableColumn,
  type TableSummary,
} from "#/routes/(platform)/logs/-components/table-summaries";

export const Route = createFileRoute("/(platform)/logs/$id/$tableId")({
  component: LogTablePage,
});

function LogTablePage() {
  const { id, tableId } = Route.useParams();

  const [entry, setEntry] = useState<LogEntry | null>(null);
  const [files, setFiles] = useState<LogFile[]>([]);
  const [processes, setProcesses] = useState<LogProcess[]>([]);

  const [entryLoading, setEntryLoading] = useState(false);
  const [filesLoading, setFilesLoading] = useState(false);
  const [processesLoading, setProcessesLoading] = useState(false);

  const [entryError, setEntryError] = useState<string | null>(null);
  const [filesError, setFilesError] = useState<string | null>(null);
  const [processesError, setProcessesError] = useState<string | null>(null);

  const [isDetailsOpen, setIsDetailsOpen] = useState(false);
  const [downloadingFormat, setDownloadingFormat] = useState<string | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  const fetchEntry = useCallback(async () => {
    setEntryLoading(true);
    setEntryError(null);
    try {
      const nextEntry = await getLogEntry(id);
      setEntry(nextEntry);
    } catch (error) {
      setEntryError(error instanceof Error ? error.message : "Failed to load log entry.");
    } finally {
      setEntryLoading(false);
    }
  }, [id]);

  const fetchFiles = useCallback(async () => {
    setFilesLoading(true);
    setFilesError(null);
    try {
      const nextFiles = await listLogFiles(id);
      setFiles(nextFiles);
    } catch (error) {
      setFilesError(error instanceof Error ? error.message : "Failed to load files.");
    } finally {
      setFilesLoading(false);
    }
  }, [id]);

  const fetchProcesses = useCallback(async () => {
    setProcessesLoading(true);
    setProcessesError(null);
    try {
      const nextProcesses = await listLogProcesses(id);
      setProcesses(nextProcesses);
    } catch (error) {
      setProcessesError(error instanceof Error ? error.message : "Failed to load table data.");
    } finally {
      setProcessesLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void Promise.all([fetchEntry(), fetchFiles(), fetchProcesses()]);
  }, [fetchEntry, fetchFiles, fetchProcesses]);

  const tables = useMemo(() => inferTablesFromProcesses(files, processes), [files, processes]);

  const table = useMemo(() => tables.find((tableItem) => tableItem.id === tableId) ?? null, [tableId, tables]);

  const onDownload = useCallback(
    async (format: "original" | "csv" | "xlsx") => {
      if (table === null) {
        return;
      }

      if (format === "original" && table.sourceFile === null) {
        return;
      }

      setDownloadingFormat(format);
      setDownloadError(null);
      try {
        let blob: Blob;
        let filename: string;

        if (format === "original" && table.sourceFile !== null) {
          blob = await downloadLogFile(id, table.sourceFile.id);
          filename = table.sourceFile.name;
        } else if (format === "csv") {
          blob = await downloadTableCsv(id, table.name);
          filename = `${table.name}.csv`;
        } else {
          blob = await downloadTableXlsx(id, table.name);
          filename = `${table.name}.xlsx`;
        }

        const blobUrl = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = blobUrl;
        anchor.download = filename;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(blobUrl);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Download failed. Please try again.";
        setDownloadError(message);
        toast.error(message);
      } finally {
        setDownloadingFormat(null);
      }
    },
    [id, table],
  );

  const hasLoadingState = entryLoading || filesLoading || processesLoading;
  const hasRequestError = entryError !== null || filesError !== null || processesError !== null;

  return (
    <div className={"flex h-full flex-col"}>
      <PageHeader
        breadcrumbs={
          entry !== null
            ? [
                { label: "Logs", href: "/logs" },
                { label: entry.name, href: `/logs/${id}` },
                ...(table !== null ? [{ label: getTableDisplayName(table) }] : []),
              ]
            : [{ label: "Logs", href: "/logs" }]
        }
        loading={entryLoading}
      />

      <main className={"flex flex-1 flex-col gap-4 p-4"}>
        {hasLoadingState && (
          <div className={"flex flex-col gap-3"}>
            <Skeleton className={"h-24 w-full rounded-lg"} />
            <Skeleton className={"h-72 w-full rounded-lg"} />
          </div>
        )}

        {!hasLoadingState && hasRequestError && (
          <div className={"flex flex-col gap-2"}>
            {entryError !== null && (
              <Alert variant={"destructive"}>
                <AlertTitle>Failed to load log entry</AlertTitle>
                <AlertDescription>{entryError}</AlertDescription>
              </Alert>
            )}
            {filesError !== null && (
              <Alert variant={"destructive"}>
                <AlertTitle>Failed to load files</AlertTitle>
                <AlertDescription>{filesError}</AlertDescription>
              </Alert>
            )}
            {processesError !== null && (
              <Alert variant={"destructive"}>
                <AlertTitle>Failed to load table data</AlertTitle>
                <AlertDescription>{processesError}</AlertDescription>
              </Alert>
            )}
            <Button
              onClick={() => void Promise.all([fetchEntry(), fetchFiles(), fetchProcesses()])}
              size={"sm"}
              variant={"outline"}
            >
              Try again
            </Button>
          </div>
        )}

        {!hasLoadingState && !hasRequestError && table === null && (
          <Empty className={"border"}>
            <EmptyHeader>
              <EmptyMedia variant={"icon"}>
                <TableIcon />
              </EmptyMedia>
              <EmptyTitle>Table not found</EmptyTitle>
              <EmptyDescription>The selected table is no longer available for this log group.</EmptyDescription>
            </EmptyHeader>
            <Button asChild size={"sm"} variant={"outline"}>
              <Link params={{ id }} to={"/logs/$id"}>
                Back to data tab
              </Link>
            </Button>
          </Empty>
        )}

        {!hasLoadingState && !hasRequestError && table !== null && (
          <>
            <section className={"flex flex-col gap-3 rounded-md border p-4"}>
              <div className={"flex items-start gap-3"}>
                <FileTextIcon className={"mt-0.5 size-4 shrink-0 text-muted-foreground"} />
                <div className={"flex flex-1 flex-col gap-1"}>
                  <h2 className={"font-medium font-mono text-sm"}>{getTableDisplayName(table)}</h2>
                  <p className={"text-muted-foreground text-xs"}>Table: {table.name}</p>
                </div>
                <div className={"ml-auto flex flex-wrap items-center justify-end gap-1.5"}>
                  <Badge variant={"secondary"}>
                    {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
                  </Badge>
                  <Badge variant={"secondary"}>
                    {table.rowCount.toLocaleString()} {table.rowCount === 1 ? "row" : "rows"}
                  </Badge>
                </div>
              </div>

              <div className={"flex flex-wrap items-center gap-x-3 gap-y-1 text-muted-foreground text-xs"}>
                <span>
                  Uploaded:{" "}
                  <span className={"text-foreground"}>
                    {table.sourceFile !== null
                      ? format(new Date(table.sourceFile.created_at), "MMM d, yyyy 'at' h:mm a")
                      : "Unknown"}
                  </span>
                </span>
                <span>
                  Size:{" "}
                  <span className={"text-foreground"}>
                    {table.sourceFile !== null ? formatFileSize(table.sourceFile.size) : "Unknown"}
                  </span>
                </span>
              </div>

              <div className={"flex flex-wrap items-center gap-2"}>
                <Button onClick={() => setIsDetailsOpen(true)} size={"sm"} variant={"ghost"}>
                  <InfoIcon />
                  Details
                </Button>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button disabled={downloadingFormat !== null} size={"sm"} variant={"ghost"}>
                      {downloadingFormat !== null ? <Spinner /> : <DownloadIcon />}
                      Download
                      <ChevronDownIcon />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align={"end"}>
                    {table.sourceFile !== null && (
                      <DropdownMenuItem onClick={() => void onDownload("original")}>
                        <FileTextIcon />
                        Original File
                      </DropdownMenuItem>
                    )}
                    <DropdownMenuItem onClick={() => void onDownload("csv")}>
                      <TableIcon />
                      CSV Spreadsheet
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={() => void onDownload("xlsx")}>
                      <FileSpreadsheetIcon />
                      Excel Spreadsheet
                    </DropdownMenuItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </div>

              {downloadError !== null && <p className={"text-destructive text-xs"}>{downloadError}</p>}
            </section>

            <TableRowsDataTable table={table} />

            {isDetailsOpen && <TableDetailsDialog onClose={() => setIsDetailsOpen(false)} table={table} />}
          </>
        )}
      </main>
    </div>
  );
}

function TableRowsDataTable({ table }: { table: TableSummary }) {
  const columnKeys = useMemo(() => {
    const keys = new Set(table.columns.map((column) => column.name));
    for (const row of table.rows) {
      for (const key of Object.keys(row)) {
        keys.add(key);
      }
    }
    return [...keys];
  }, [table.columns, table.rows]);

  const [sorting, setSorting] = useState<SortingState>([]);
  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 20 });
  const defaultColumnVisibility = useMemo(
    () => getDefaultColumnVisibility(columnKeys, table.rows),
    [columnKeys, table.rows],
  );
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>(defaultColumnVisibility);
  const [jsonPreview, setJsonPreview] = useState<{ title: string; value: unknown } | null>(null);

  useEffect(() => {
    setColumnVisibility(defaultColumnVisibility);
  }, [defaultColumnVisibility, table.id]);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    return columnKeys.map((key) => ({
      id: key,
      accessorKey: key,
      meta: { label: key },
      header: ({ column }) => <DataTableColumnHeader column={column} title={key} />,
      cell: ({ getValue, row }) => {
        const value = getValue();
        const previewValue = getPreviewableJsonValue(value);
        if (previewValue !== null) {
          return (
            <Button
              className={"h-7 px-2 font-mono text-xs"}
              onClick={() =>
                setJsonPreview({
                  title: `${key} · row ${row.index + 1}`,
                  value: previewValue,
                })
              }
              size={"sm"}
              variant={"outline"}
            >
              View
            </Button>
          );
        }

        if (value === null || value === undefined) {
          return <span className={"text-muted-foreground"}>null</span>;
        }

        if (typeof value === "object") {
          return <span className={"font-mono text-xs"}>{safeSerialize(value)}</span>;
        }

        return <span className={"font-mono text-xs"}>{String(value)}</span>;
      },
      enableHiding: true,
      enableSorting: true,
    }));
  }, [columnKeys]);

  if (table.rows.length === 0) {
    return (
      <Empty className={"border"}>
        <EmptyHeader>
          <EmptyMedia variant={"icon"}>
            <TableIcon />
          </EmptyMedia>
          <EmptyTitle>No table rows available</EmptyTitle>
          <EmptyDescription>
            This table currently has no persisted rows from available process outputs.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }

  return (
    <>
      <DataTable
        columns={columns}
        data={table.rows}
        onColumnVisibilityChange={setColumnVisibility}
        onPaginationChange={setPagination}
        onSortingChange={setSorting}
        state={{ sorting, columnVisibility, pagination }}
        toolbar={(reactTable) => (
          <div className={"flex items-center justify-end"}>
            <DataTableViewOptions table={reactTable} />
          </div>
        )}
      />

      {jsonPreview !== null && (
        <JsonPreviewDialog onClose={() => setJsonPreview(null)} title={jsonPreview.title} value={jsonPreview.value} />
      )}
    </>
  );
}

function JsonPreviewDialog({ onClose, title, value }: { onClose: () => void; title: string; value: unknown }) {
  return (
    <Dialog
      onOpenChange={(open) => {
        if (!open) {
          onClose();
        }
      }}
      open
    >
      <DialogContent className={"flex max-h-[90vh] flex-col overflow-hidden sm:max-w-3xl"}>
        <DialogHeader className={"shrink-0"}>
          <DialogTitle>JSON preview</DialogTitle>
          <DialogDescription>{title}</DialogDescription>
        </DialogHeader>

        <div className={"min-h-0 flex-1 overflow-auto rounded-md border bg-muted/20 p-3"}>
          <JsonTreeNode depth={0} label={null} value={value} />
        </div>
      </DialogContent>
    </Dialog>
  );
}

function JsonTreeNode({ label, value, depth }: { label: string | null; value: unknown; depth: number }) {
  const indentation = { paddingLeft: `${depth * 0.9}rem` };

  if (Array.isArray(value)) {
    return (
      <div className={"space-y-1"}>
        <div className={"font-mono text-xs"} style={indentation}>
          {label !== null && <span className={"text-muted-foreground"}>{label}: </span>}
          <span>[{value.length}]</span>
        </div>

        {value.length === 0 ? (
          <div className={"font-mono text-xs text-muted-foreground"} style={{ paddingLeft: `${(depth + 1) * 0.9}rem` }}>
            Empty array.
          </div>
        ) : (
          value.map((item, index) => (
            <JsonTreeNode depth={depth + 1} key={`${depth}-${index}`} label={String(index)} value={item} />
          ))
        )}
      </div>
    );
  }

  if (isJsonRecord(value)) {
    const entries = Object.entries(value);

    return (
      <div className={"space-y-1"}>
        <div className={"font-mono text-xs"} style={indentation}>
          {label !== null && <span className={"text-muted-foreground"}>{label}: </span>}
          <span>{`{${entries.length}}`}</span>
        </div>

        {entries.length === 0 ? (
          <div className={"font-mono text-xs text-muted-foreground"} style={{ paddingLeft: `${(depth + 1) * 0.9}rem` }}>
            Empty object.
          </div>
        ) : (
          entries.map(([entryKey, entryValue]) => (
            <JsonTreeNode depth={depth + 1} key={`${depth}-${entryKey}`} label={entryKey} value={entryValue} />
          ))
        )}
      </div>
    );
  }

  return (
    <div className={"font-mono text-xs"} style={indentation}>
      {label !== null && <span className={"text-muted-foreground"}>{label}: </span>}
      <span className={getJsonValueClassName(value)}>{formatJsonPrimitive(value)}</span>
    </div>
  );
}

function TableDetailsDialog({ table, onClose }: { table: TableSummary; onClose: () => void }) {
  return (
    <Dialog
      onOpenChange={(open) => {
        if (!open) {
          onClose();
        }
      }}
      open
    >
      <DialogContent className={"flex max-h-[90vh] flex-col overflow-hidden sm:max-w-2xl"}>
        <DialogHeader className={"shrink-0"}>
          <DialogTitle>{getTableDisplayName(table)}</DialogTitle>
          <DialogDescription>
            {table.name} · {table.columns.length} {table.columns.length === 1 ? "column" : "columns"} ·{" "}
            {table.rowCount.toLocaleString()} {table.rowCount === 1 ? "row" : "rows"}
          </DialogDescription>
        </DialogHeader>

        <div className={"min-h-0 flex-1 overflow-y-auto"}>
          <div className={"flex flex-col divide-y rounded-md border"}>
            {table.columns.map((column) => (
              <ColumnDetailRow column={column} key={column.name} />
            ))}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function ColumnDetailRow({ column }: { column: TableColumn }) {
  return (
    <div className={"flex flex-col gap-1 px-3 py-2.5"}>
      <div className={"flex items-center gap-2"}>
        <span className={"font-medium font-mono text-sm"}>{column.name}</span>
        <div className={"ml-auto flex shrink-0 items-center gap-1"}>
          <Badge className={"font-mono text-xs"} variant={"outline"}>
            {column.type}
          </Badge>
          {!column.nullable && (
            <Badge className={"text-xs"} variant={"secondary"}>
              NOT NULL
            </Badge>
          )}
          {column.primary_key && <Badge className={"text-xs"}>PK</Badge>}
        </div>
      </div>
      {column.description !== "" && <p className={"text-muted-foreground text-xs"}>{column.description}</p>}
    </div>
  );
}

function safeSerialize(value: unknown) {
  try {
    return JSON.stringify(value);
  } catch {
    return "[unserializable value]";
  }
}

function getDefaultColumnVisibility(columnKeys: string[], rows: Record<string, unknown>[]) {
  const visibility: VisibilityState = {};

  for (const key of columnKeys) {
    if (isDefaultHiddenColumn(key) || isAllNullColumn(key, rows)) {
      visibility[key] = false;
    }
  }

  return visibility;
}

function isDefaultHiddenColumn(key: string) {
  const normalizedKey = key.trim().toLowerCase();
  return normalizedKey === "raw" || normalizedKey === "id" || normalizedKey.endsWith("_id");
}

function isAllNullColumn(key: string, rows: Record<string, unknown>[]) {
  if (rows.length === 0) {
    return false;
  }

  return rows.every((row) => row[key] === null || row[key] === undefined);
}

function getPreviewableJsonValue(value: unknown) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "object") {
    return value;
  }

  if (typeof value !== "string") {
    return null;
  }

  const normalizedValue = value.trim();
  if (normalizedValue === "") {
    return null;
  }

  try {
    const parsed = JSON.parse(normalizedValue);
    return parsed !== null && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function isJsonRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function formatJsonPrimitive(value: unknown) {
  if (value === null) {
    return "null";
  }

  if (typeof value === "string") {
    return `"${value}"`;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return "[unsupported value]";
}

function getJsonValueClassName(value: unknown) {
  if (value === null) {
    return "text-muted-foreground";
  }

  if (typeof value === "string") {
    return "text-blue-600";
  }

  if (typeof value === "number") {
    return "text-violet-600";
  }

  if (typeof value === "boolean") {
    return "text-emerald-600";
  }

  return "text-muted-foreground";
}
