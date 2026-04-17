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
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "#/components/ui/dropdown-menu";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { Input } from "#/components/ui/input";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import {
  downloadFilteredTable,
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

            <TableRowsDataTable entryId={id} table={table} />

            {isDetailsOpen && <TableDetailsDialog onClose={() => setIsDetailsOpen(false)} table={table} />}
          </>
        )}
      </main>
    </div>
  );
}

function TableRowsDataTable({ table, entryId }: { table: TableSummary; entryId: string }) {
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
  const [cellPreview, setCellPreview] = useState<{ title: string; value: unknown } | null>(null);
  const [searchText, setSearchText] = useState("");
  const [selectedLevels, setSelectedLevels] = useState<string[]>([]);
  const [timestampFrom, setTimestampFrom] = useState("");
  const [timestampTo, setTimestampTo] = useState("");
  const [fieldFilters, setFieldFilters] = useState<Record<string, string>>({});
  const [isExportingFiltered, setIsExportingFiltered] = useState(false);

  useEffect(() => {
    setColumnVisibility(defaultColumnVisibility);
  }, [defaultColumnVisibility]);

  useEffect(() => {
    const currentTableId = table.id;
    if (currentTableId === "") {
      return;
    }
    setSearchText("");
    setSelectedLevels([]);
    setTimestampFrom("");
    setTimestampTo("");
    setFieldFilters({});
    setPagination({ pageIndex: 0, pageSize: 20 });
  }, [table.id]);

  const availableLevelOptions = useMemo(() => {
    const levels = new Set<string>();
    for (const row of table.rows) {
      const level = getRowLevel(row);
      if (level !== "") {
        levels.add(level);
      }
    }
    return [...levels].sort();
  }, [table.rows]);

  const filterableFieldKeys = useMemo(() => {
    const preferred = ["tool_id", "tool", "chamber_id", "wafer_id", "lot_id"];
    return preferred.filter((key) => columnKeys.includes(key));
  }, [columnKeys]);

  const filteredRows = useMemo(() => {
    const normalizedSearch = searchText.trim().toLowerCase();
    const fromTime = parseTimestampInput(timestampFrom);
    const toTime = parseTimestampInput(timestampTo);
    const levelSet = new Set(selectedLevels.map((level) => normalizeLevel(level)));

    return table.rows.filter((row) => {
      if (normalizedSearch !== "") {
        const serialized = safeSerialize(row).toLowerCase();
        if (!serialized.includes(normalizedSearch)) {
          return false;
        }
      }

      if (levelSet.size > 0) {
        const level = getRowLevel(row);
        if (!levelSet.has(level)) {
          return false;
        }
      }

      for (const [key, value] of Object.entries(fieldFilters)) {
        const normalizedFilter = value.trim().toLowerCase();
        if (normalizedFilter === "") {
          continue;
        }
        const actualValue = row[key];
        const normalizedValue =
          actualValue === null || actualValue === undefined ? "" : String(actualValue).toLowerCase();
        if (!normalizedValue.includes(normalizedFilter)) {
          return false;
        }
      }

      if (fromTime !== null || toTime !== null) {
        const rowTime = getRowTimestamp(row);
        if (rowTime === null) {
          return false;
        }
        if (fromTime !== null && rowTime < fromTime) {
          return false;
        }
        if (toTime !== null && rowTime > toTime) {
          return false;
        }
      }

      return true;
    });
  }, [fieldFilters, searchText, selectedLevels, table.rows, timestampFrom, timestampTo]);

  const onToggleLevel = useCallback((level: string, checked: boolean) => {
    setSelectedLevels((previous) => {
      if (checked) {
        if (previous.includes(level)) {
          return previous;
        }
        return [...previous, level];
      }
      return previous.filter((item) => item !== level);
    });
  }, []);

  const onChangeFieldFilter = useCallback((field: string, value: string) => {
    setFieldFilters((previous) => ({
      ...previous,
      [field]: value,
    }));
  }, []);

  const exportFiltered = useCallback(
    async (format: "csv" | "json") => {
      setIsExportingFiltered(true);
      try {
        const blob = await downloadFilteredTable(entryId, table.name, {
          format,
          search: searchText,
          levels: selectedLevels,
          field_filters: fieldFilters,
          timestamp_from: timestampFrom === "" ? undefined : timestampFrom,
          timestamp_to: timestampTo === "" ? undefined : timestampTo,
        });
        const filename = `${table.name}.filtered.${format}`;
        const blobUrl = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = blobUrl;
        anchor.download = filename;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(blobUrl);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to export filtered rows.";
        toast.error(message);
      } finally {
        setIsExportingFiltered(false);
      }
    },
    [entryId, fieldFilters, searchText, selectedLevels, table.name, timestampFrom, timestampTo],
  );

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    return columnKeys.map((key) => ({
      id: key,
      accessorKey: key,
      meta: { label: key },
      header: ({ column }) => <DataTableColumnHeader column={column} title={key} />,
      cell: ({ getValue, row }) => {
        const value = getValue();
        const displayValue = formatTableCellValue(value);

        return (
          <button
            className={
              "block max-w-[36ch] cursor-pointer truncate text-left font-mono text-xs hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
            }
            onClick={() =>
              setCellPreview({
                title: `${key} · row ${row.index + 1}`,
                value,
              })
            }
            title={displayValue}
            type={"button"}
          >
            <span className={value === null || value === undefined ? "text-muted-foreground" : undefined}>
              {displayValue}
            </span>
          </button>
        );
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
      <div className={"flex w-full flex-wrap items-center gap-2 rounded-md border p-2"}>
        <Input
          className={"h-8 w-56"}
          onChange={(event) => setSearchText(event.target.value)}
          placeholder={"Search table..."}
          value={searchText}
        />

        {filterableFieldKeys.map((fieldKey) => (
          <Input
            className={"h-8 w-40"}
            key={fieldKey}
            onChange={(event) => onChangeFieldFilter(fieldKey, event.target.value)}
            placeholder={`Filter ${fieldKey}`}
            value={fieldFilters[fieldKey] ?? ""}
          />
        ))}

        <Input
          className={"h-8 w-52"}
          onChange={(event) => setTimestampFrom(event.target.value)}
          type={"datetime-local"}
          value={timestampFrom}
        />
        <Input
          className={"h-8 w-52"}
          onChange={(event) => setTimestampTo(event.target.value)}
          type={"datetime-local"}
          value={timestampTo}
        />

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button size={"sm"} variant={"outline"}>
              Levels {selectedLevels.length > 0 ? `(${selectedLevels.length})` : ""}
              <ChevronDownIcon />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align={"start"}>
            {availableLevelOptions.length === 0 ? (
              <DropdownMenuItem disabled>No level values</DropdownMenuItem>
            ) : (
              availableLevelOptions.map((level) => (
                <DropdownMenuCheckboxItem
                  checked={selectedLevels.includes(level)}
                  key={level}
                  onCheckedChange={(checked) => onToggleLevel(level, checked === true)}
                >
                  {level}
                </DropdownMenuCheckboxItem>
              ))
            )}
          </DropdownMenuContent>
        </DropdownMenu>

        <Badge className={"ml-auto"} variant={"secondary"}>
          {filteredRows.length.toLocaleString()} of {table.rows.length.toLocaleString()} rows
        </Badge>
      </div>

      <DataTable
        columns={columns}
        data={filteredRows}
        onColumnVisibilityChange={setColumnVisibility}
        onPaginationChange={setPagination}
        onSortingChange={setSorting}
        state={{ sorting, columnVisibility, pagination }}
        toolbar={(reactTable) => (
          <div className={"flex flex-wrap items-center gap-2"}>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button disabled={isExportingFiltered} size={"sm"} variant={"outline"}>
                  {isExportingFiltered ? <Spinner /> : <DownloadIcon />}
                  Export Filtered
                  <ChevronDownIcon />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align={"start"}>
                <DropdownMenuItem onClick={() => void exportFiltered("csv")}>CSV</DropdownMenuItem>
                <DropdownMenuItem onClick={() => void exportFiltered("json")}>JSON</DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>

            <DataTableViewOptions className={"ml-0"} label={"Columns"} table={reactTable} />
          </div>
        )}
      />

      {cellPreview !== null && (
        <CellValuePreviewDialog
          onClose={() => setCellPreview(null)}
          title={cellPreview.title}
          value={cellPreview.value}
        />
      )}
    </>
  );
}

function CellValuePreviewDialog({ onClose, title, value }: { onClose: () => void; title: string; value: unknown }) {
  const previewValue = getPreviewableJsonValue(value);

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
          <DialogTitle>Cell value</DialogTitle>
          <DialogDescription>{title}</DialogDescription>
        </DialogHeader>

        <div className={"min-h-0 flex-1 overflow-auto rounded-md border bg-muted/20 p-3"}>
          {previewValue !== null ? (
            <JsonTreeNode depth={0} label={null} value={previewValue} />
          ) : (
            <pre className={"whitespace-pre-wrap break-all font-mono text-xs"}>{formatTableCellValue(value)}</pre>
          )}
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
          <div className={"font-mono text-muted-foreground text-xs"} style={{ paddingLeft: `${(depth + 1) * 0.9}rem` }}>
            Empty array.
          </div>
        ) : (
          value.map((item) => (
            <JsonTreeNode depth={depth + 1} key={`${depth}-${safeSerialize(item)}`} label={null} value={item} />
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
          <div className={"font-mono text-muted-foreground text-xs"} style={{ paddingLeft: `${(depth + 1) * 0.9}rem` }}>
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

function normalizeLevel(value: string) {
  const normalized = value.trim().toUpperCase();
  if (normalized === "WARNING") {
    return "WARN";
  }
  return normalized;
}

function getRowLevel(row: Record<string, unknown>) {
  for (const key of ["log_level", "level", "severity"]) {
    const value = row[key];
    if (typeof value === "string" && value.trim() !== "") {
      return normalizeLevel(value);
    }
  }
  return "";
}

function parseTimestampInput(value: string) {
  const trimmed = value.trim();
  if (trimmed === "") {
    return null;
  }

  const date = new Date(trimmed);
  if (Number.isNaN(date.valueOf())) {
    return null;
  }
  return date;
}

function getRowTimestamp(row: Record<string, unknown>) {
  for (const key of ["timestamp", "ts", "time"]) {
    const value = row[key];
    if (typeof value !== "string") {
      continue;
    }

    const parsed = new Date(value);
    if (!Number.isNaN(parsed.valueOf())) {
      return parsed;
    }
  }
  return null;
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
  return (
    normalizedKey === "raw" ||
    normalizedKey === "id" ||
    normalizedKey === "source" ||
    normalizedKey === "parse_confidence" ||
    normalizedKey === "message" ||
    normalizedKey === "extra" ||
    normalizedKey.endsWith("_id")
  );
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

function formatTableCellValue(value: unknown) {
  if (value === null || value === undefined) {
    return "null";
  }

  if (typeof value === "object") {
    return safeSerialize(value);
  }

  return String(value);
}
