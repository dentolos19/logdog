"use client";

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
                logGroupId={logGroupId}
                onInfoClick={() => setSelectedTable(table)}
                presentation={presentation}
                table={table}
              />
            );
          })}
        </div>
      )}

      {selectedTable !== null && (
        <TableDetailsDialog files={files} onClose={() => setSelectedTable(null)} table={selectedTable} />
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
    <Collapsible onOpenChange={handleToggle} open={isOpen}>
      <div className={"flex items-center gap-3 rounded-md border p-4"}>
        <Button
          aria-label={isOpen ? "Collapse rows" : "Expand rows"}
          className={"size-7 shrink-0"}
          onClick={handleToggle}
          size={"icon"}
          variant={"ghost"}
        >
          {isOpen ? <ChevronDownIcon className={"size-3.5"} /> : <ChevronRightIcon className={"size-3.5"} />}
        </Button>
        <span className={"font-medium text-sm"}>{presentation.displayName}</span>
        <div className={"ml-auto flex items-center gap-1.5"}>
          {table.is_normalized && <Badge variant={"outline"}>Normalized</Badge>}
          <Badge variant={"secondary"}>
            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
          </Badge>
          <Badge variant={"secondary"}>
            {table.row_count.toLocaleString()} {table.row_count === 1 ? "row" : "rows"}
          </Badge>
          <Button
            aria-label={`View details for ${presentation.displayName}`}
            className={"size-7"}
            onClick={(e) => {
              e.stopPropagation();
              onInfoClick();
            }}
            size={"icon"}
            variant={"ghost"}
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
              <p className={"text-destructive text-sm"}>{error}</p>
              <Button onClick={() => void loadRows(page)} size={"sm"} variant={"outline"}>
                Try again
              </Button>
            </div>
          )}

          {rowsData !== null && error === null && (
            <>
              {rowsData.rows.length === 0 ? (
                <p className={"py-4 text-center text-muted-foreground text-sm"}>No rows in this table.</p>
              ) : (
                <RowsDataTable isLoading={isLoading} onPageChange={(p) => void loadRows(p)} rowsData={rowsData} />
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
              className={"-ml-3 h-7 gap-1 font-medium font-mono text-xs"}
              onClick={() => column.toggleSorting(sorted === "asc")}
              size={"sm"}
              variant={"ghost"}
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
            <Button className={"gap-1.5"} size={"sm"} variant={"outline"}>
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
                  checked={col.getIsVisible()}
                  className={"font-mono text-xs"}
                  key={col.id}
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

      {rowsData.total_pages > 1 && (
        <div className={"flex items-center justify-between"}>
          <p className={"text-muted-foreground text-xs"}>
            Page {rowsData.page} of {rowsData.total_pages} &middot; {rowsData.total.toLocaleString()} rows
          </p>
          <div className={"flex items-center gap-1"}>
            <Button
              disabled={rowsData.page <= 1 || isLoading}
              onClick={() => onPageChange(rowsData.page - 1)}
              size={"sm"}
              variant={"outline"}
            >
              Previous
            </Button>
            <Button
              disabled={rowsData.page >= rowsData.total_pages || isLoading}
              onClick={() => onPageChange(rowsData.page + 1)}
              size={"sm"}
              variant={"outline"}
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
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      open={true}
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
              <ColumnDetailRow column={column} key={column.name} />
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
        <span className={"font-medium font-mono text-sm"}>{column.name}</span>
        <div className={"ml-auto flex shrink-0 items-center gap-1"}>
          <Badge className={"font-mono text-xs"} variant={"outline"}>
            {column.type}
          </Badge>
          {column.not_null && (
            <Badge className={"text-xs"} variant={"secondary"}>
              NOT NULL
            </Badge>
          )}
          {column.primary_key && <Badge className={"text-xs"}>PK</Badge>}
        </div>
      </div>
      {column.description !== "" && <p className={"text-muted-foreground text-xs"}>{column.description}</p>}
      {column.default_value !== null && (
        <p className={"text-muted-foreground text-xs"}>
          Default: <span className={"font-mono"}>{column.default_value}</span>
        </p>
      )}
    </div>
  );
}

interface TablePresentation {
  displayName: string;
}

function getTablePresentation(table: LogGroupTable, files: LogGroupFile[]): TablePresentation {
  if (table.name === "logs") {
    const sourceFiles = files.filter((file) => files.some((f) => f.id === file.id));
    if (sourceFiles.length > 0) {
      return {
        displayName: `Normalized Logs (${sourceFiles.length} source file${sourceFiles.length === 1 ? "" : "s"})`,
      };
    }
    return {
      displayName: "Normalized Logs",
    };
  }

  if (table.name.startsWith("logs_")) {
    const fileId = table.name.slice("logs_".length);
    const file = files.find((item) => item.id === fileId);
    if (file !== undefined) {
      return {
        displayName: `Logs for ${file.name}`,
      };
    }
  }

  return {
    displayName: table.name,
  };
}
