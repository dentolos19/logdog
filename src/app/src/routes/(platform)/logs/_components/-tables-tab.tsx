import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
  type VisibilityState,
} from "@tanstack/react-table";
import {
  ArrowDownIcon,
  ArrowUpDownIcon,
  ArrowUpIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  DatabaseZapIcon,
  InfoIcon,
} from "lucide-react";
import { useMemo, useState } from "react";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Collapsible, CollapsibleContent } from "#/components/ui/collapsible";
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "#/components/ui/table";
import type { LogFile, LogProcess } from "#/lib/server";

type TablesTabProps = {
  files: LogFile[];
  processes: LogProcess[];
};

type TableColumn = {
  name: string;
  type: string;
  nullable: boolean;
  primary_key: boolean;
  description: string;
};

type TableSummary = {
  id: string;
  name: string;
  rowCount: number;
  isNormalized: boolean;
  sourceFileName: string | null;
  columns: TableColumn[];
  sampleRows: Record<string, unknown>[];
};

export function TablesTab({ files, processes }: TablesTabProps) {
  const tables = useMemo(() => inferTablesFromProcesses(files, processes), [files, processes]);
  const [selectedTable, setSelectedTable] = useState<TableSummary | null>(null);

  return (
    <>
      {tables.length === 0 ? (
        <Empty className={"border"}>
          <EmptyHeader>
            <EmptyMedia variant={"icon"}>
              <DatabaseZapIcon />
            </EmptyMedia>
            <EmptyTitle>No tables yet</EmptyTitle>
            <EmptyDescription>
              Upload log files and complete parsing processes to inspect inferred tables and columns.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className={"flex flex-col gap-2"}>
          {tables.map((table) => (
            <TableItem key={table.id} onInfoClick={() => setSelectedTable(table)} table={table} />
          ))}
        </div>
      )}

      {selectedTable !== null && <TableDetailsDialog onClose={() => setSelectedTable(null)} table={selectedTable} />}
    </>
  );
}

function TableItem({ table, onInfoClick }: { table: TableSummary; onInfoClick: () => void }) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <Collapsible onOpenChange={setIsOpen} open={isOpen}>
      <div className={"flex items-center gap-3 rounded-md border p-4"}>
        <Button
          aria-label={isOpen ? "Collapse rows" : "Expand rows"}
          className={"size-7 shrink-0"}
          onClick={() => setIsOpen(!isOpen)}
          size={"icon-sm"}
          variant={"ghost"}
        >
          {isOpen ? <ChevronDownIcon className={"size-3.5"} /> : <ChevronRightIcon className={"size-3.5"} />}
        </Button>

        <span className={"font-medium text-sm"}>{getTableDisplayName(table)}</span>
        <div className={"ml-auto flex items-center gap-1.5"}>
          {table.isNormalized && <Badge variant={"outline"}>Normalized</Badge>}
          <Badge variant={"secondary"}>
            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
          </Badge>
          <Badge variant={"secondary"}>
            {table.rowCount.toLocaleString()} {table.rowCount === 1 ? "row" : "rows"}
          </Badge>
          <Button
            aria-label={`View details for ${table.name}`}
            className={"size-7"}
            onClick={(event) => {
              event.stopPropagation();
              onInfoClick();
            }}
            size={"icon-sm"}
            variant={"ghost"}
          >
            <InfoIcon className={"size-3.5"} />
          </Button>
        </div>
      </div>

      <CollapsibleContent>
        <div className={"rounded-b-md border border-t-0 p-4"}>
          {table.sampleRows.length === 0 ? (
            <p className={"py-4 text-center text-muted-foreground text-sm"}>
              Row preview is not available from current APIs.
            </p>
          ) : (
            <RowsDataTable rows={table.sampleRows} />
          )}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

function RowsDataTable({ rows }: { rows: Record<string, unknown>[] }) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    const firstRow = rows[0] ?? {};
    return Object.keys(firstRow).map((key) => ({
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
        return <span className={"font-mono text-xs"}>{String(value)}</span>;
      },
    }));
  }, [rows]);

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting, columnVisibility },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

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
            {table
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
            {table.getHeaderGroups().map((headerGroup) => (
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
            {table.getRowModel().rows.map((row) => (
              <TableRow key={row.id}>
                {row.getVisibleCells().map((cell) => (
                  <TableCell className={"max-w-48 truncate"} key={cell.id}>
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
            {table.isNormalized ? " · Normalized" : ""}
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

function inferTablesFromProcesses(files: LogFile[], processes: LogProcess[]) {
  const tableMap = new Map<string, TableSummary>();

  for (const process of processes) {
    const result = asRecord(process.result);
    if (result === null) {
      continue;
    }

    const generatedTables = asArray(result.table_definitions);
    const recordsByTable = asRecord(result.records);

    for (const generatedTable of generatedTables) {
      const tableRecord = asRecord(generatedTable);
      if (tableRecord === null) {
        continue;
      }

      const tableName = asString(tableRecord.table_name, "unknown_table");
      const rawColumns = asArray(tableRecord.columns);
      const columns: TableColumn[] = rawColumns.map((column) => {
        const columnRecord = asRecord(column) ?? {};
        return {
          name: asString(columnRecord.name, "column"),
          type: asString(columnRecord.sql_type, "TEXT"),
          nullable: asBoolean(columnRecord.nullable, true),
          primary_key: asBoolean(columnRecord.primary_key, false),
          description: asString(columnRecord.description, ""),
        };
      });

      const recordRows = recordsByTable ? asArray(recordsByTable[tableName]) : [];
      const sampleRows = recordRows
        .map((row) => asRecord(row))
        .filter((row): row is Record<string, unknown> => row !== null)
        .slice(0, 20);

      const sourceFileName = resolveSourceFileName(tableName, files);

      tableMap.set(tableName, {
        id: `${process.id}-${tableName}`,
        name: tableName,
        rowCount: recordRows.length,
        isNormalized: tableName === "logs",
        sourceFileName,
        columns,
        sampleRows,
      });
    }
  }

  return [...tableMap.values()];
}

function resolveSourceFileName(tableName: string, files: LogFile[]) {
  const fileIdMatch = tableName.match(/[a-f0-9]{12}$/i);
  if (!fileIdMatch) {
    return null;
  }
  const hint = fileIdMatch[0].toLowerCase();
  const match = files.find((file) => file.id.replace(/-/g, "").toLowerCase().startsWith(hint));
  return match?.name ?? null;
}

function getTableDisplayName(table: TableSummary) {
  if (table.isNormalized) {
    return "Normalized Logs";
  }
  if (table.sourceFileName !== null) {
    return `Logs for ${table.sourceFileName}`;
  }
  return table.name;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asString(value: unknown, fallback: string) {
  return typeof value === "string" ? value : fallback;
}

function asBoolean(value: unknown, fallback: boolean) {
  return typeof value === "boolean" ? value : fallback;
}
