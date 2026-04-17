import { Link } from "@tanstack/react-router";
import { format } from "date-fns";
import { DatabaseZapIcon, DownloadIcon, FileSpreadsheetIcon, FileTextIcon, InfoIcon } from "lucide-react";
import { useMemo, useState } from "react";
import { toast } from "sonner";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "#/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { downloadLogFile, type LogFile, type LogProcess } from "#/lib/server";
import {
  formatFileSize,
  getTableDisplayName,
  inferTablesFromProcesses,
  type TableColumn,
  type TableSummary,
} from "#/routes/(platform)/logs/-components/table-summaries";

type TablesTabProps = {
  entryId: string;
  files: LogFile[];
  processes: LogProcess[];
};

export function TablesTab({ entryId, files, processes }: TablesTabProps) {
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
            <TableItem entryId={entryId} key={table.id} onInfoClick={() => setSelectedTable(table)} table={table} />
          ))}
        </div>
      )}

      {selectedTable !== null && <TableDetailsDialog onClose={() => setSelectedTable(null)} table={selectedTable} />}
    </>
  );
}

function TableItem({ table, entryId, onInfoClick }: { table: TableSummary; entryId: string; onInfoClick: () => void }) {
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  const formattedDate =
    table.sourceFile !== null ? format(new Date(table.sourceFile.created_at), "MMM d, yyyy 'at' h:mm a") : "Unknown";
  const formattedSize = table.sourceFile !== null ? formatFileSize(table.sourceFile.size) : "Unknown";

  const onDownload = async () => {
    if (table.sourceFile === null) {
      return;
    }

    setIsDownloading(true);
    setDownloadError(null);
    try {
      const blob = await downloadLogFile(entryId, table.sourceFile.id);
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
  };

  return (
    <div className={"flex flex-col gap-2 rounded-md border p-4"}>
      <div className={"flex items-center gap-3"}>
        <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
        <div className={"flex flex-1 flex-col gap-0.5 overflow-hidden"}>
          <span className={"truncate font-medium font-mono text-sm"}>{getTableDisplayName(table)}</span>
          <span className={"text-muted-foreground text-xs"}>Table: {table.name}</span>
        </div>

        <div className={"ml-auto flex shrink-0 items-center gap-1.5"}>
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
          Uploaded: <span className={"text-foreground"}>{formattedDate}</span>
        </span>
        <span>
          Size: <span className={"text-foreground"}>{formattedSize}</span>
        </span>
      </div>

      <div className={"flex items-center gap-2"}>
        <Button asChild size={"sm"} variant={"ghost"}>
          <Link params={{ id: entryId, tableId: table.id }} to={"/logs/$id/$tableId"}>
            <FileSpreadsheetIcon />
            View
          </Link>
        </Button>
        <Button onClick={() => onInfoClick()} size={"sm"} variant={"ghost"}>
          <InfoIcon />
          Details
        </Button>
        <Button
          disabled={isDownloading || table.sourceFile === null}
          onClick={() => void onDownload()}
          size={"sm"}
          variant={"ghost"}
        >
          <DownloadIcon />
          Download
        </Button>
      </div>

      {downloadError !== null && <p className={"text-destructive text-xs"}>{downloadError}</p>}
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
