import {
  type Column,
  type ColumnDef,
  type ColumnFiltersState,
  flexRender,
  type PaginationState,
  type Row,
  type RowData,
  type RowSelectionState,
  type SortingState,
  type Table as TanstackTable,
  stockFeatures,
  tableFeatures,
  createFacetedRowModel,
  createFacetedUniqueValues,
  createFilteredRowModel,
  createPaginatedRowModel,
  createSortedRowModel,
  filterFns,
  sortFns,
  useTable,
  type ColumnVisibilityState,
} from "@tanstack/react-table";
import {
  ArrowDownIcon,
  ArrowUpIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  ChevronsLeftIcon,
  ChevronsRightIcon,
  ChevronsUpDownIcon,
  Settings2Icon,
} from "lucide-react";
import type * as React from "react";
import { Button } from "#/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "#/components/ui/dropdown-menu";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "#/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "#/components/ui/table";
import { cn } from "#/lib/utils";

export const dataTableFeatures = tableFeatures({
  ...stockFeatures,
  facetedRowModel: createFacetedRowModel(),
  facetedUniqueValues: createFacetedUniqueValues(),
  filteredRowModel: createFilteredRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  sortedRowModel: createSortedRowModel(),
  filterFns,
  sortFns,
});
export type DataTableFeatures = typeof dataTableFeatures;

declare module "@tanstack/react-table" {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  interface ColumnMeta<in out TFeatures extends import("@tanstack/react-table").TableFeatures, in out TData extends RowData, TValue extends import("@tanstack/react-table").CellData = import("@tanstack/react-table").CellData> {
    label?: string;
  }
}

interface DataTableProps<TData extends RowData> {
  columns: ColumnDef<DataTableFeatures, TData>[];
  data: TData[];
  pageCount?: number;
  toolbar?: (table: TanstackTable<DataTableFeatures, TData>) => React.ReactNode;
  state?: {
    sorting?: SortingState;
    columnFilters?: ColumnFiltersState;
    columnVisibility?: ColumnVisibilityState;
    rowSelection?: RowSelectionState;
    pagination?: PaginationState;
  };
  onPaginationChange?: (updater: PaginationState | ((prev: PaginationState) => PaginationState)) => void;
  onSortingChange?: (updater: SortingState | ((prev: SortingState) => SortingState)) => void;
  onColumnFiltersChange?: (updater: ColumnFiltersState | ((prev: ColumnFiltersState) => ColumnFiltersState)) => void;
  onColumnVisibilityChange?: (updater: ColumnVisibilityState | ((prev: ColumnVisibilityState) => ColumnVisibilityState)) => void;
  onRowSelectionChange?: (updater: RowSelectionState | ((prev: RowSelectionState) => RowSelectionState)) => void;
  manualPagination?: boolean;
  manualSorting?: boolean;
  manualFiltering?: boolean;
  enableRowSelection?: boolean | ((row: Row<DataTableFeatures, TData>) => boolean);
  enableSorting?: boolean;
  enableColumnFilters?: boolean;
  enableHiding?: boolean;
  getRowId?: (row: TData, index: number, parent?: Row<DataTableFeatures, TData>) => string;
  className?: string;
}

function DataTable<TData extends RowData>({
  columns,
  data,
  pageCount,
  toolbar,
  state,
  onPaginationChange,
  onSortingChange,
  onColumnFiltersChange,
  onColumnVisibilityChange,
  onRowSelectionChange,
  manualPagination = false,
  manualSorting = false,
  manualFiltering = false,
  enableRowSelection = false,
  enableSorting = true,
  enableColumnFilters = true,
  enableHiding = true,
  getRowId,
  className,
}: DataTableProps<TData>) {
  const table = useTable({
    features: dataTableFeatures,
    data,
    columns,
    pageCount,
    state,
    onPaginationChange,
    onSortingChange,
    onColumnFiltersChange,
    onColumnVisibilityChange,
    onRowSelectionChange,
    manualPagination,
    manualSorting,
    manualFiltering,
    enableRowSelection,
    enableSorting,
    enableColumnFilters,
    enableHiding,
    getRowId,

  });

  return (
    <div className={cn("flex flex-col gap-4", className)}>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id}>
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length > 0 ? (
              table.getRowModel().rows.map((row) => (
                <TableRow data-state={row.getIsSelected() && "selected"} key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell className="h-24 text-center" colSpan={columns.length}>
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <DataTablePagination table={table}>{toolbar?.(table)}</DataTablePagination>
    </div>
  );
}

function DataTableColumnHeader<TData extends RowData>({
  column,
  title,
  className,
}: {
  column: Column<DataTableFeatures, TData>;
  title: string;
  className?: string;
}) {
  if (!column.getCanSort()) {
    return <div className={cn(className)}>{title}</div>;
  }

  return (
    <div className={cn("flex items-center space-x-2", className)}>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button className="-ml-3 h-8 data-[state=open]:bg-accent" size="sm" variant="ghost">
            <span>{title}</span>
            {column.getIsSorted() === "desc" ? (
              <ArrowDownIcon className="ml-2 size-4" />
            ) : column.getIsSorted() === "asc" ? (
              <ArrowUpIcon className="ml-2 size-4" />
            ) : (
              <ChevronsUpDownIcon className="ml-2 size-4" />
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuCheckboxItem
            checked={column.getIsSorted() === "asc"}
            onClick={() => column.toggleSorting(false)}
          >
            <ArrowUpIcon className="mr-2 size-3.5 text-muted-foreground/70" />
            Ascending
          </DropdownMenuCheckboxItem>
          <DropdownMenuCheckboxItem
            checked={column.getIsSorted() === "desc"}
            onClick={() => column.toggleSorting(true)}
          >
            <ArrowDownIcon className="mr-2 size-3.5 text-muted-foreground/70" />
            Descending
          </DropdownMenuCheckboxItem>
          {column.getIsSorted() && (
            <DropdownMenuCheckboxItem checked={false} onClick={() => column.clearSorting()}>
              <ChevronsUpDownIcon className="mr-2 size-3.5 text-muted-foreground/70" />
              Reset
            </DropdownMenuCheckboxItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

function DataTablePagination<TData extends RowData>({ table, children }: { table: TanstackTable<DataTableFeatures, TData>; children?: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between px-2">
      {children}
      <div className="flex items-center space-x-6 lg:space-x-8">
        <div className="flex items-center space-x-2">
          <p className="font-medium text-sm">Rows per page</p>
          <Select
            onValueChange={(value) => {
              table.setPageSize(Number(value));
            }}
            value={`${table.store.state.pagination.pageSize}`}
          >
            <SelectTrigger className="h-8 w-[70px]">
              <SelectValue placeholder={table.store.state.pagination.pageSize} />
            </SelectTrigger>
            <SelectContent side="top">
              {[10, 20, 30, 40, 50].map((pageSize) => (
                <SelectItem key={pageSize} value={`${pageSize}`}>
                  {pageSize}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex w-[100px] items-center justify-center font-medium text-sm">
          Page {table.store.state.pagination.pageIndex + 1} of {table.getPageCount()}
        </div>
        <div className="flex items-center space-x-2">
          <Button
            className="hidden size-8 lg:flex"
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.setPageIndex(0)}
            size="icon"
            variant="outline"
          >
            <span className="sr-only">Go to first page</span>
            <ChevronsLeftIcon />
          </Button>
          <Button
            className="size-8"
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
            size="icon"
            variant="outline"
          >
            <span className="sr-only">Go to previous page</span>
            <ChevronLeftIcon />
          </Button>
          <Button
            className="size-8"
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
            size="icon"
            variant="outline"
          >
            <span className="sr-only">Go to next page</span>
            <ChevronRightIcon />
          </Button>
          <Button
            className="hidden size-8 lg:flex"
            disabled={!table.getCanNextPage()}
            onClick={() => table.setPageIndex(table.getPageCount() - 1)}
            size="icon"
            variant="outline"
          >
            <span className="sr-only">Go to last page</span>
            <ChevronsRightIcon />
          </Button>
        </div>
      </div>
    </div>
  );
}

function DataTableViewOptions<TData extends RowData>({
  table,
  label = "View",
  className,
}: {
  table: TanstackTable<DataTableFeatures, TData>;
  label?: string;
  className?: string;
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button className={cn("ml-auto", className)} size="sm" variant="outline">
          <Settings2Icon />
          {label}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {table
          .getAllColumns()
          .filter((column) => typeof column.accessorFn !== "undefined" && column.getCanHide())
          .map((column) => {
            return (
              <DropdownMenuCheckboxItem
                checked={column.getIsVisible()}
                key={column.id}
                onCheckedChange={(value) => column.toggleVisibility(!!value)}
              >
                {column.columnDef.meta?.label ?? column.id}
              </DropdownMenuCheckboxItem>
            );
          })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

export { DataTable, DataTableColumnHeader, DataTablePagination, DataTableViewOptions };
