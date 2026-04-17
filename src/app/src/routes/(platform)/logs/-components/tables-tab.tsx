import { Link } from "@tanstack/react-router";
import { format } from "date-fns";
import { DatabaseZapIcon, DownloadIcon, FileSpreadsheetIcon, FileTextIcon, InfoIcon } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "#/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { downloadLogFile, type LogFile, type LogProcess } from "#/lib/server";
import {
  formatFileSize,
  inferTablesFromProcesses,
  type TableColumn,
  type TableSummary,
} from "#/routes/(platform)/logs/-components/table-summaries";

type TablesTabProps = {
  entryId: string;
  files: LogFile[];
  highlightRequest: { key: number; fileId: string | null; tableIds: string[] } | null;
  onHighlightHandled: () => void;
  processes: LogProcess[];
};

type TableGroup = {
  id: string;
  label: string;
  sourceFile: LogFile | null;
  tables: TableSummary[];
  totalRows: number;
};

export function TablesTab({ entryId, files, highlightRequest, onHighlightHandled, processes }: TablesTabProps) {
  const tables = useMemo(() => inferTablesFromProcesses(files, processes), [files, processes]);
  const tableGroups = useMemo(() => groupTablesBySourceFile(tables), [tables]);
  const tableGroupIdByTableId = useMemo(() => {
    const mapping = new Map<string, string>();

    for (const group of tableGroups) {
      for (const table of group.tables) {
        mapping.set(table.id, group.id);
      }
    }

    return mapping;
  }, [tableGroups]);
  const [selectedTable, setSelectedTable] = useState<TableSummary | null>(null);
  const [highlightedTableIds, setHighlightedTableIds] = useState<Set<string>>(new Set());
  const [highlightedGroupId, setHighlightedGroupId] = useState<string | null>(null);
  const groupElementsRef = useRef<Map<string, HTMLElement>>(new Map());
  const tableElementsRef = useRef<Map<string, HTMLDivElement>>(new Map());

  const setGroupElement = useCallback((groupId: string, element: HTMLElement | null) => {
    if (element === null) {
      groupElementsRef.current.delete(groupId);
      return;
    }

    groupElementsRef.current.set(groupId, element);
  }, []);

  const setTableElement = useCallback((tableId: string, element: HTMLDivElement | null) => {
    if (element === null) {
      tableElementsRef.current.delete(tableId);
      return;
    }

    tableElementsRef.current.set(tableId, element);
  }, []);

  useEffect(() => {
    if (highlightRequest === null) {
      return;
    }

    const matchedTableIds = highlightRequest.tableIds.filter((tableId) => tableElementsRef.current.has(tableId));
    const firstMatchedTableId = matchedTableIds[0] ?? null;
    const matchedGroupId =
      highlightRequest.fileId ??
      (firstMatchedTableId !== null ? (tableGroupIdByTableId.get(firstMatchedTableId) ?? null) : null);
    const targetElement =
      (matchedGroupId !== null ? groupElementsRef.current.get(matchedGroupId) : undefined) ??
      (firstMatchedTableId !== null ? tableElementsRef.current.get(firstMatchedTableId) : undefined);

    onHighlightHandled();

    if (targetElement !== undefined) {
      targetElement.scrollIntoView({ behavior: "smooth", block: "center" });
      setHighlightedGroupId(matchedGroupId);
      setHighlightedTableIds(new Set(matchedTableIds));

      const timeoutId = window.setTimeout(() => {
        setHighlightedGroupId((previous) => (previous === matchedGroupId ? null : previous));
        setHighlightedTableIds(new Set());
      }, 1700);

      return () => {
        window.clearTimeout(timeoutId);
      };
    }
  }, [highlightRequest, onHighlightHandled, tableGroupIdByTableId]);

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
          {tableGroups.map((group) => (
            <TableGroupSection
              entryId={entryId}
              group={group}
              highlightedTableIds={highlightedTableIds}
              isHighlighted={highlightedGroupId === group.id}
              key={group.id}
              onInfoClick={(table) => setSelectedTable(table)}
              setGroupElement={setGroupElement}
              setTableElement={setTableElement}
            />
          ))}
        </div>
      )}

      {selectedTable !== null && <TableDetailsDialog onClose={() => setSelectedTable(null)} table={selectedTable} />}
    </>
  );
}

function TableGroupSection({
  entryId,
  group,
  highlightedTableIds,
  isHighlighted,
  onInfoClick,
  setGroupElement,
  setTableElement,
}: {
  entryId: string;
  group: TableGroup;
  highlightedTableIds: Set<string>;
  isHighlighted: boolean;
  onInfoClick: (table: TableSummary) => void;
  setGroupElement: (groupId: string, element: HTMLElement | null) => void;
  setTableElement: (tableId: string, element: HTMLDivElement | null) => void;
}) {
  const formattedDate =
    group.sourceFile !== null ? format(new Date(group.sourceFile.created_at), "MMM d, yyyy 'at' h:mm a") : null;
  const formattedSize = group.sourceFile !== null ? formatFileSize(group.sourceFile.size) : null;

  return (
    <section
      className={`flex flex-col gap-3 rounded-md border p-4 transition-all duration-500 ${
        isHighlighted ? "bg-primary/10 ring-1 ring-primary/40" : ""
      }`}
      ref={(element) => setGroupElement(group.id, element)}
    >
      <div className={"flex items-center gap-3"}>
        <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
        <div className={"flex flex-1 flex-col gap-0.5 overflow-hidden"}>
          <span className={"truncate font-medium font-mono text-sm"}>{group.label}</span>
          <span className={"text-muted-foreground text-xs"}>
            {group.tables.length} {group.tables.length === 1 ? "table" : "tables"} · {group.totalRows.toLocaleString()}{" "}
            {group.totalRows === 1 ? "row" : "rows"}
          </span>
        </div>
        <div className={"ml-auto flex shrink-0 items-center gap-1.5"}>
          <Badge variant={"secondary"}>
            {group.tables.length} {group.tables.length === 1 ? "table" : "tables"}
          </Badge>
          <Badge variant={"secondary"}>
            {group.totalRows.toLocaleString()} {group.totalRows === 1 ? "row" : "rows"}
          </Badge>
        </div>
      </div>

      {formattedDate !== null && formattedSize !== null && (
        <div className={"flex flex-wrap items-center gap-x-3 gap-y-1 text-muted-foreground text-xs"}>
          <span>
            Uploaded: <span className={"text-foreground"}>{formattedDate}</span>
          </span>
          <span>
            Size: <span className={"text-foreground"}>{formattedSize}</span>
          </span>
        </div>
      )}

      <div className={"flex flex-col divide-y rounded-md border"}>
        {group.tables.map((table) => (
          <TableItem
            entryId={entryId}
            isHighlighted={highlightedTableIds.has(table.id)}
            key={table.id}
            onInfoClick={onInfoClick}
            setTableElement={setTableElement}
            table={table}
          />
        ))}
      </div>
    </section>
  );
}

function TableItem({
  table,
  entryId,
  isHighlighted,
  onInfoClick,
  setTableElement,
}: {
  table: TableSummary;
  entryId: string;
  isHighlighted: boolean;
  onInfoClick: (table: TableSummary) => void;
  setTableElement: (tableId: string, element: HTMLDivElement | null) => void;
}) {
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

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
    <div
      className={`flex flex-col gap-2 rounded-sm px-3 py-3 transition-all duration-500 ${
        isHighlighted ? "bg-primary/10 ring-1 ring-primary/40 motion-safe:animate-pulse" : ""
      }`}
      ref={(element) => setTableElement(table.id, element)}
    >
      <div className={"flex items-center gap-3"}>
        <FileSpreadsheetIcon className={"size-4 shrink-0 text-muted-foreground"} />
        <div className={"flex flex-1 flex-col gap-0.5 overflow-hidden"}>
          <span className={"truncate font-medium font-mono text-sm"}>{table.name}</span>
          {table.sourceFile === null && <span className={"text-muted-foreground text-xs"}>Source file unknown</span>}
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

      <div className={"flex items-center gap-2"}>
        <Button asChild size={"sm"} variant={"ghost"}>
          <Link params={{ id: entryId, tableId: table.id }} to={"/logs/$id/$tableId"}>
            <FileSpreadsheetIcon />
            View
          </Link>
        </Button>
        <Button onClick={() => onInfoClick(table)} size={"sm"} variant={"ghost"}>
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
          <DialogTitle>{table.name}</DialogTitle>
          <DialogDescription>
            {table.sourceFile?.name ?? "Unknown source file"} · {table.columns.length}{" "}
            {table.columns.length === 1 ? "column" : "columns"} · {table.rowCount.toLocaleString()}{" "}
            {table.rowCount === 1 ? "row" : "rows"}
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

function groupTablesBySourceFile(tables: TableSummary[]) {
  const groupsById = new Map<string, TableGroup>();

  for (const table of tables) {
    const sourceFile = table.sourceFile;
    const groupId = sourceFile?.id ?? "unknown-source";
    const existingGroup = groupsById.get(groupId);

    if (existingGroup !== undefined) {
      existingGroup.tables.push(table);
      existingGroup.totalRows += table.rowCount;
      continue;
    }

    groupsById.set(groupId, {
      id: groupId,
      label: sourceFile?.name ?? "Unknown Source File",
      sourceFile,
      tables: [table],
      totalRows: table.rowCount,
    });
  }

  return [...groupsById.values()]
    .map((group) => ({
      ...group,
      tables: [...group.tables].sort((leftTable, rightTable) => leftTable.name.localeCompare(rightTable.name)),
    }))
    .sort((leftGroup, rightGroup) => leftGroup.label.localeCompare(rightGroup.label));
}
