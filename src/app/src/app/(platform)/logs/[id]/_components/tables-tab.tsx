"use client";

import {
  type ColumnDef,
  type SortingState,
  type VisibilityState,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
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

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent } from "@/components/ui/collapsible";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getTableRows } from "@/lib/api";
import type { LogGroupFile, LogGroupTable, LogTableColumn, TableRowsResponse } from "@/lib/api/types";

const PAGE_SIZE = 50;

interface TablesTabProps {
  tables: LogGroupTable[];
  files: LogGroupFile[];
  logGroupId: string;
}

export function TablesTab({ tables, files, logGroupId }: TablesTabProps) {
  const [selectedTable, setSelectedTable] = useState<LogGroupTable | null>(null);

  return (
    <>
      {tables.length === 0 ? (
        <Empty className={"border"}>
          <EmptyHeader>
            <EmptyMedia variant={"icon"}>
              <DatabaseZapIcon />
            </EmptyMedia>
            <EmptyTitle>No tables yet</EmptyTitle>
            <EmptyDescription>Upload log files to automatically generate queryable tables.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className={"flex flex-col gap-2"}>
          {tables.map((table) => {
            const presentation = getTablePresentation(table, files);

            return (
              <TableItem
                key={table.id}
                table={table}
                presentation={presentation}
                logGroupId={logGroupId}
                onInfoClick={() => setSelectedTable(table)}
              />
            );
          })}
        </div>
      )}

      {selectedTable !== null && (
        <TableDetailsDialog table={selectedTable} files={files} onClose={() => setSelectedTable(null)} />
      )}
    </>
  );
}

interface TableItemProps {
  table: LogGroupTable;
  presentation: TablePresentation;
  logGroupId: string;
  onInfoClick: () => void;
}

function TableItem({ table, presentation, logGroupId, onInfoClick }: TableItemProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [rowsData, setRowsData] = useState<TableRowsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);

  const loadRows = async (targetPage: number) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await getTableRows(logGroupId, table.name, targetPage, PAGE_SIZE);
      setRowsData(data);
      setPage(targetPage);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load rows.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleToggle = () => {
    const next = !isOpen;
    setIsOpen(next);
    if (next && rowsData === null && !isLoading) {
      void loadRows(1);
    }
  };

  return (
    <Collapsible open={isOpen} onOpenChange={handleToggle}>
      <div className={"flex items-center gap-3 rounded-md border p-4"}>
        <Button
          variant={"ghost"}
          size={"icon"}
          className={"size-7 shrink-0"}
          aria-label={isOpen ? "Collapse rows" : "Expand rows"}
          onClick={handleToggle}
        >
          {isOpen ? <ChevronDownIcon className={"size-3.5"} /> : <ChevronRightIcon className={"size-3.5"} />}
        </Button>
        <span className={"text-sm font-medium"}>{presentation.displayName}</span>
        <div className={"ml-auto flex items-center gap-1.5"}>
          {table.is_normalized && <Badge variant={"outline"}>Normalized</Badge>}
          {presentation.fileBadge !== null && <Badge variant={"outline"}>{presentation.fileBadge}</Badge>}
          <Badge variant={"secondary"}>
            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
          </Badge>
          <Badge variant={"secondary"}>
            {table.row_count.toLocaleString()} {table.row_count === 1 ? "row" : "rows"}
          </Badge>
          <Button
            variant={"ghost"}
            size={"icon"}
            onClick={(e) => {
              e.stopPropagation();
              onInfoClick();
            }}
            aria-label={`View details for ${presentation.displayName}`}
            className={"size-7"}
          >
            <InfoIcon className={"size-3.5"} />
          </Button>
        </div>
      </div>

      <CollapsibleContent>
        <div className={"rounded-b-md border border-t-0 p-4"}>
          {isLoading && rowsData === null && (
            <div className={"flex flex-col gap-2"}>
              <Skeleton className={"h-8 w-full"} />
              <Skeleton className={"h-8 w-full"} />
              <Skeleton className={"h-8 w-full"} />
            </div>
          )}

          {error !== null && !isLoading && (
            <div className={"flex flex-col items-center gap-3 py-6 text-center"}>
              <p className={"text-sm text-destructive"}>{error}</p>
              <Button variant={"outline"} size={"sm"} onClick={() => void loadRows(page)}>
                Try again
              </Button>
            </div>
          )}

          {rowsData !== null && error === null && (
            <>
              {rowsData.rows.length === 0 ? (
                <p className={"py-4 text-center text-sm text-muted-foreground"}>No rows in this table.</p>
              ) : (
                <RowsDataTable rowsData={rowsData} isLoading={isLoading} onPageChange={(p) => void loadRows(p)} />
              )}
            </>
          )}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

interface RowsDataTableProps {
  rowsData: TableRowsResponse;
  isLoading: boolean;
  onPageChange: (page: number) => void;
}

function RowsDataTable({ rowsData, isLoading, onPageChange }: RowsDataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      rowsData.columns.map((col) => ({
        id: col,
        accessorKey: col,
        header: ({ column }) => {
          const sorted = column.getIsSorted();
          return (
            <Button
              variant={"ghost"}
              size={"sm"}
              className={"-ml-3 h-7 gap-1 font-mono text-xs font-medium"}
              onClick={() => column.toggleSorting(sorted === "asc")}
            >
              {col}
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
      })),
    [rowsData.columns],
  );

  const table = useReactTable({
    data: rowsData.rows,
    columns,
    state: { sorting, columnVisibility },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualPagination: true,
  });

  return (
    <div className={"flex flex-col gap-3"}>
      <div className={"flex justify-end"}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant={"outline"} size={"sm"} className={"gap-1.5"}>
              Columns
              <ChevronDownIcon className={"size-3.5"} />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align={"end"} className={"max-h-72 overflow-y-auto"}>
            <DropdownMenuLabel>Toggle columns</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {table
              .getAllColumns()
              .filter((col) => col.getCanHide())
              .map((col) => (
                <DropdownMenuCheckboxItem
                  key={col.id}
                  className={"font-mono text-xs"}
                  checked={col.getIsVisible()}
                  onCheckedChange={(value) => col.toggleVisibility(value)}
                >
                  {col.id}
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
                  <TableHead key={header.id} className={"whitespace-nowrap"}>
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
                  <TableCell key={cell.id} className={"max-w-48 truncate"}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {rowsData.total_pages > 1 && (
        <div className={"flex items-center justify-between"}>
          <p className={"text-xs text-muted-foreground"}>
            Page {rowsData.page} of {rowsData.total_pages} &middot; {rowsData.total.toLocaleString()} rows
          </p>
          <div className={"flex items-center gap-1"}>
            <Button
              variant={"outline"}
              size={"sm"}
              disabled={rowsData.page <= 1 || isLoading}
              onClick={() => onPageChange(rowsData.page - 1)}
            >
              Previous
            </Button>
            <Button
              variant={"outline"}
              size={"sm"}
              disabled={rowsData.page >= rowsData.total_pages || isLoading}
              onClick={() => onPageChange(rowsData.page + 1)}
            >
              Next
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

interface TableDetailsDialogProps {
  table: LogGroupTable;
  files: LogGroupFile[];
  onClose: () => void;
}

function TableDetailsDialog({ table, files, onClose }: TableDetailsDialogProps) {
  const presentation = getTablePresentation(table, files);

  return (
    <Dialog
      open={true}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className={"flex max-h-[90vh] flex-col overflow-hidden sm:max-w-2xl"}>
        <DialogHeader className={"shrink-0"}>
          <DialogTitle>{presentation.displayName}</DialogTitle>
          <DialogDescription>
            {table.name}
            {" · "}
            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"} &middot;{" "}
            {table.row_count.toLocaleString()} {table.row_count === 1 ? "row" : "rows"}
            {table.is_normalized && " · Normalized"}
          </DialogDescription>
        </DialogHeader>

        <div className={"min-h-0 flex-1 overflow-y-auto"}>
          <div className={"flex flex-col divide-y rounded-md border"}>
            {table.columns.map((column) => (
              <ColumnDetailRow key={column.name} column={column} />
            ))}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

interface ColumnDetailRowProps {
  column: LogTableColumn;
}

function ColumnDetailRow({ column }: ColumnDetailRowProps) {
  return (
    <div className={"flex flex-col gap-1 px-3 py-2.5"}>
      <div className={"flex items-center gap-2"}>
        <span className={"font-mono text-sm font-medium"}>{column.name}</span>
        <div className={"ml-auto flex shrink-0 items-center gap-1"}>
          <Badge variant={"outline"} className={"font-mono text-xs"}>
            {column.type}
          </Badge>
          {column.not_null && (
            <Badge variant={"secondary"} className={"text-xs"}>
              NOT NULL
            </Badge>
          )}
          {column.primary_key && <Badge className={"text-xs"}>PK</Badge>}
        </div>
      </div>
      {column.description !== "" && <p className={"text-xs text-muted-foreground"}>{column.description}</p>}
      {column.default_value !== null && (
        <p className={"text-xs text-muted-foreground"}>
          Default: <span className={"font-mono"}>{column.default_value}</span>
        </p>
      )}
    </div>
  );
}

interface TablePresentation {
  displayName: string;
  fileBadge: string | null;
}

function getTablePresentation(table: LogGroupTable, files: LogGroupFile[]): TablePresentation {
  if (table.name === "logs") {
    return {
      displayName: "Normalized Logs",
      fileBadge: null,
    };
  }

  if (table.name.startsWith("logs_")) {
    const fileId = table.name.slice("logs_".length);
    const file = files.find((item) => item.id === fileId);
    if (file !== undefined) {
      return {
        displayName: `Logs for ${file.name}`,
        fileBadge: file.name,
      };
    }
  }

  return {
    displayName: table.name,
    fileBadge: null,
  };
}
