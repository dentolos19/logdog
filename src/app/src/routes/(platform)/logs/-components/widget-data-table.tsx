import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { ArrowDownIcon, ArrowUpIcon, ChevronLeftIcon, ChevronRightIcon, DownloadIcon } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { Button } from "#/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "#/components/ui/table";

type WidgetDataTableProps = {
  columns: string[];
  rows: unknown[][];
  title?: string;
};

function escapeCsvValue(value: unknown): string {
  const str = value === null || value === undefined ? "" : String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

export function WidgetDataTable({ columns, rows, title }: WidgetDataTableProps) {
  const [sorting, setSorting] = useState<SortingState>([]);

  const columnDefs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      columns.map((col) => ({
        accessorKey: col,
        header: col,
        cell: ({ getValue }) => {
          const value = getValue();
          if (value === null || value === undefined) {
            return <span className={"text-muted-foreground"}>—</span>;
          }
          return String(value);
        },
      })),
    [columns],
  );

  const data = useMemo(
    () =>
      rows.map((row) => {
        const record: Record<string, unknown> = {};
        columns.forEach((col, index) => {
          record[col] = row[index] ?? null;
        });
        return record;
      }),
    [columns, rows],
  );

  const table = useReactTable({
    data,
    columns: columnDefs,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    onSortingChange: setSorting,
    state: { sorting },
    initialState: { pagination: { pageSize: 10 } },
  });

  const handleDownloadCsv = useCallback(() => {
    const headerRow = columns.map(escapeCsvValue).join(",");
    const dataRows = rows.map((row) => row.map(escapeCsvValue).join(","));
    const csv = [headerRow, ...dataRows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${title ?? "query_results"}.csv`;
    anchor.click();
    URL.revokeObjectURL(url);
  }, [columns, rows, title]);

  if (columns.length === 0) {
    return <p className={"text-muted-foreground text-sm"}>No results.</p>;
  }

  return (
    <div className={"flex flex-col gap-2"}>
      <div className={"flex items-center justify-between"}>
        {title && <span className={"font-medium text-sm"}>{title}</span>}
        <Button className={"ml-auto"} onClick={handleDownloadCsv} size={"sm"} variant={"ghost"}>
          <DownloadIcon />
          CSV
        </Button>
      </div>

      <div className={"rounded-md border"}>
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead
                    className={"cursor-pointer select-none"}
                    key={header.id}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <div className={"flex items-center gap-1"}>
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {header.column.getIsSorted() === "asc" && <ArrowUpIcon className={"size-3"} />}
                      {header.column.getIsSorted() === "desc" && <ArrowDownIcon className={"size-3"} />}
                    </div>
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length === 0 ? (
              <TableRow>
                <TableCell className={"text-center text-muted-foreground"} colSpan={columns.length}>
                  No data.
                </TableCell>
              </TableRow>
            ) : (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</TableCell>
                  ))}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className={"flex items-center justify-between"}>
        <span className={"text-muted-foreground text-xs"}>
          {rows.length} row{rows.length === 1 ? "" : "s"}
        </span>
        <div className={"flex items-center gap-1"}>
          <Button
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
            size={"icon-sm"}
            variant={"outline"}
          >
            <ChevronLeftIcon />
          </Button>
          <span className={"px-2 text-muted-foreground text-xs"}>
            {table.getState().pagination.pageIndex + 1} / {table.getPageCount()}
          </span>
          <Button
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
            size={"icon-sm"}
            variant={"outline"}
          >
            <ChevronRightIcon />
          </Button>
        </div>
      </div>
    </div>
  );
}
