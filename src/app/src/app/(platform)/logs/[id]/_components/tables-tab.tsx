"use client";

import { DatabaseZapIcon, InfoIcon } from "lucide-react";
import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { LogGroupTable, LogTableColumn } from "@/lib/api/types";

interface TablesTabProps {
  tables: LogGroupTable[];
}

export function TablesTab({ tables }: TablesTabProps) {
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
          {tables.map((table) => (
            <div key={table.id} className={"flex items-center gap-3 rounded-md border p-4"}>
              <span className={"font-mono text-sm font-medium"}>{table.name}</span>
              <div className={"ml-auto flex items-center gap-1.5"}>
                {table.is_normalized && <Badge variant={"outline"}>Normalized</Badge>}
                <Badge variant={"secondary"}>
                  {table.columns.length} {table.columns.length === 1 ? "column" : "columns"}
                </Badge>
                <Badge variant={"secondary"}>
                  {table.row_count.toLocaleString()} {table.row_count === 1 ? "row" : "rows"}
                </Badge>
                <Button
                  variant={"ghost"}
                  size={"icon"}
                  onClick={() => setSelectedTable(table)}
                  aria-label={`View details for ${table.name}`}
                  className={"size-7"}
                >
                  <InfoIcon className={"size-3.5"} />
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}

      {selectedTable !== null && <TableDetailsDialog table={selectedTable} onClose={() => setSelectedTable(null)} />}
    </>
  );
}

interface TableDetailsDialogProps {
  table: LogGroupTable;
  onClose: () => void;
}

function TableDetailsDialog({ table, onClose }: TableDetailsDialogProps) {
  return (
    <Dialog
      open={true}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className={"flex max-h-[80vh] flex-col overflow-hidden sm:max-w-xl"}>
        <DialogHeader>
          <DialogTitle className={"font-mono"}>{table.name}</DialogTitle>
          <DialogDescription>
            {table.columns.length} {table.columns.length === 1 ? "column" : "columns"} &middot;{" "}
            {table.row_count.toLocaleString()} {table.row_count === 1 ? "row" : "rows"}
            {table.is_normalized && " · Normalized"}
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className={"flex-1"}>
          <div className={"flex flex-col divide-y rounded-md border"}>
            {table.columns.map((column) => (
              <ColumnDetailRow key={column.name} column={column} />
            ))}
          </div>
        </ScrollArea>
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
