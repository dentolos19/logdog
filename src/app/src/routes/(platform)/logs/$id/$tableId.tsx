import { createFileRoute, Link } from "@tanstack/react-router";
import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
  type VisibilityState,
} from "@tanstack/react-table";
import { format } from "date-fns";
import {
  ArrowDownIcon,
  ArrowUpDownIcon,
  ArrowUpIcon,
  ChevronDownIcon,
  DownloadIcon,
  FileTextIcon,
  InfoIcon,
  TableIcon,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "#/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "#/components/ui/dropdown-menu";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "#/components/ui/table";
import {
  downloadLogFile,
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
  const [isDownloading, setIsDownloading] = useState(false);
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

  const onDownload = useCallback(async () => {
    if (table?.sourceFile === null || table === null) {
      return;
    }

    setIsDownloading(true);
    setDownloadError(null);
    try {
      const blob = await downloadLogFile(id, table.sourceFile.id);
      const blobUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = blobUrl;
      anchor.download = table.sourceFile.name;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(blobUrl);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Download failed. Please try again.";
      setDownloadError(message);
      toast.error(message);
    } finally {
      setIsDownloading(false);
    }
  }, [id, table]);

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
                <Button asChild size={"sm"} variant={"outline"}>
                  <Link params={{ id }} to={"/logs/$id"}>
                    Back
                  </Link>
                </Button>
                <Button onClick={() => setIsDetailsOpen(true)} size={"sm"} variant={"ghost"}>
                  <InfoIcon />
                  Details
                </Button>
                <Button
                  disabled={isDownloading || table.sourceFile === null}
                  onClick={() => void onDownload()}
                  size={"sm"}
                  variant={"ghost"}
                >
                  {isDownloading ? <Spinner /> : <DownloadIcon />}
                  Download
                </Button>
              </div>

              {downloadError !== null && <p className={"text-destructive text-xs"}>{downloadError}</p>}
            </section>

            <section className={"flex flex-col gap-3"}>
              <div className={"flex items-center gap-2"}>
                <h3 className={"font-semibold text-sm"}>Table Data</h3>
              </div>
              <TableRowsDataTable table={table} />
            </section>

            {isDetailsOpen && <TableDetailsDialog onClose={() => setIsDetailsOpen(false)} table={table} />}
          </>
        )}
      </main>
    </div>
  );
}

function TableRowsDataTable({ table }: { table: TableSummary }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});

  const columnKeys = useMemo(() => {
    const keys = new Set(table.columns.map((column) => column.name));
    for (const row of table.rows) {
      for (const key of Object.keys(row)) {
        keys.add(key);
      }
    }
    return [...keys];
  }, [table.columns, table.rows]);

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    return columnKeys.map((key) => ({
      id: key,
      accessorKey: key,
      header: ({ column }) => {
        const sorted = column.getIsSorted();
        return (
          <Button
            className={"-ml-3 h-7 gap-1 font-medium font-mono text-xs"}
            onClick={() => column.toggleSorting(sorted === "asc")}
            size={"sm"}
            variant={"ghost"}
          >
            {key}
            {sorted === "asc" ? (
              <ArrowUpIcon className={"size-3"} />
            ) : sorted === "desc" ? (
              <ArrowDownIcon className={"size-3"} />
            ) : (
              <ArrowUpDownIcon className={"size-3 opacity-40"} />
            )}
          </Button>
        );
      },
      cell: ({ getValue }) => {
        const value = getValue();
        if (value === null || value === undefined) {
          return <span className={"text-muted-foreground"}>null</span>;
        }

        if (typeof value === "object") {
          return <span className={"font-mono text-xs"}>{safeSerialize(value)}</span>;
        }

        return <span className={"font-mono text-xs"}>{String(value)}</span>;
      },
    }));
  }, [columnKeys]);

  const reactTable = useReactTable({
    data: table.rows,
    columns,
    state: { sorting, columnVisibility },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

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
    <div className={"flex flex-col gap-3"}>
      <div className={"flex justify-end"}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button className={"gap-1.5"} size={"sm"} variant={"outline"}>
              Columns
              <ChevronDownIcon className={"size-3.5"} />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align={"end"}>
            <DropdownMenuLabel>Toggle columns</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {reactTable
              .getAllColumns()
              .filter((column) => column.getCanHide())
              .map((column) => (
                <DropdownMenuCheckboxItem
                  checked={column.getIsVisible()}
                  className={"font-mono text-xs"}
                  key={column.id}
                  onCheckedChange={(value) => column.toggleVisibility(value)}
                >
                  {column.id}
                </DropdownMenuCheckboxItem>
              ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className={"rounded-md border"}>
        <Table>
          <TableHeader>
            {reactTable.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead className={"whitespace-nowrap"} key={header.id}>
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {reactTable.getRowModel().rows.map((row) => (
              <TableRow key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <TableCell className={"max-w-64 truncate"} key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
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
