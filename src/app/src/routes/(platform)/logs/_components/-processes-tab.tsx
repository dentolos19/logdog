import { format } from "date-fns";
import { AlertCircleIcon, CheckCircle2Icon, ClockIcon, InfoIcon } from "lucide-react";
import { useState } from "react";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "#/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "#/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { Progress } from "#/components/ui/progress";
import { ScrollArea } from "#/components/ui/scroll-area";
import { Spinner } from "#/components/ui/spinner";
import type { LogProcess } from "#/lib/server";

type ProcessesTabProps = {
  processes: LogProcess[];
  isLoading: boolean;
  error: string | null;
};

export function ProcessesTab({ processes, isLoading, error }: ProcessesTabProps) {
  const [selectedProcess, setSelectedProcess] = useState<LogProcess | null>(null);

  if (isLoading) {
    return (
      <div className={"flex items-center justify-center py-12"}>
        <Spinner />
      </div>
    );
  }

  if (error !== null) {
    return (
      <Alert variant={"destructive"}>
        <AlertCircleIcon className={"size-4"} />
        <AlertTitle>Failed to load processes</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (processes.length === 0) {
    return (
      <Empty className={"border"}>
        <EmptyHeader>
          <EmptyMedia variant={"icon"}>
            <ClockIcon />
          </EmptyMedia>
          <EmptyTitle>No processes yet</EmptyTitle>
          <EmptyDescription>Upload log files to start a parsing run.</EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }

  return (
    <>
      <div className={"flex flex-col gap-2"}>
        {processes.map((process) => (
          <ProcessRow
            key={process.id}
            onViewDetails={process.result !== null ? () => setSelectedProcess(process) : undefined}
            process={process}
          />
        ))}
      </div>

      {selectedProcess !== null && selectedProcess.result !== null && (
        <ProcessDetailsDialog onClose={() => setSelectedProcess(null)} process={selectedProcess} />
      )}
    </>
  );
}

function ProcessRow({ process, onViewDetails }: { process: LogProcess; onViewDetails?: () => void }) {
  const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");
  const parsedResult = asRecord(process.result);
  const generatedTables = parsedResult !== null ? asArray(parsedResult.generated_tables) : [];
  const generatedTableCount = generatedTables.length;
  const isInProgress = process.status === "queued" || process.status === "processing";

  const getStatusLabel = () => {
    if (process.status === "completed") {
      return generatedTableCount > 0
        ? `Created ${generatedTableCount} ${generatedTableCount === 1 ? "table" : "tables"}`
        : "Ingestion complete";
    }
    if (process.status === "failed") {
      return "Ingestion failed";
    }
    if (process.status === "processing") {
      return "Parsing and loading...";
    }
    return "Queued for processing...";
  };

  return (
    <div className={"flex flex-col gap-1.5 rounded-md border p-4"}>
      <div className={"flex items-center gap-3"}>
        <ProcessStatusIcon status={process.status} />
        <div className={"flex flex-1 flex-col gap-0.5"}>
          <span className={"font-medium text-sm"}>{getStatusLabel()}</span>
          <span className={"text-muted-foreground text-xs"}>{formattedDate}</span>
        </div>
        <ProcessStatusBadge status={process.status} />
        {!isInProgress && onViewDetails !== undefined && (
          <Button className={"shrink-0"} onClick={onViewDetails} size={"sm"} variant={"ghost"}>
            <InfoIcon />
            Details
          </Button>
        )}
      </div>

      {process.status === "completed" && parsedResult !== null && (
        <div className={"flex items-center gap-2 text-muted-foreground text-xs"}>
          <span>{generatedTableCount} table(s) generated</span>
          <span>·</span>
          <span>{Math.round(asNumber(parsedResult.confidence, 0) * 100)}% confidence</span>
          <span>·</span>
          <span>{asArray(parsedResult.file_observations).length} file(s) analyzed</span>
        </div>
      )}

      {process.status === "failed" && process.error !== null && (
        <p className={"rounded bg-destructive/10 px-2 py-1 font-mono text-destructive text-xs"}>{process.error}</p>
      )}

      {parsedResult !== null && asArray(parsedResult.file_observations).length > 0 && (
        <ProcessFileList
          fileObservations={asArray(parsedResult.file_observations)}
          generatedTables={generatedTables}
          processStatus={process.status}
        />
      )}
    </div>
  );
}

function ProcessStatusIcon({ status }: { status: string }) {
  if (status === "completed") {
    return <CheckCircle2Icon className={"size-4 shrink-0 text-green-500"} />;
  }
  if (status === "failed") {
    return <AlertCircleIcon className={"size-4 shrink-0 text-destructive"} />;
  }
  return <Spinner className={"size-4 shrink-0"} />;
}

function ProcessStatusBadge({ status }: { status: string }) {
  if (status === "completed") {
    return <Badge variant={"secondary"}>Completed</Badge>;
  }
  if (status === "failed") {
    return <Badge variant={"destructive"}>Failed</Badge>;
  }
  if (status === "processing") {
    return <Badge variant={"outline"}>Processing</Badge>;
  }
  return <Badge variant={"outline"}>Queued</Badge>;
}

function ProcessDetailsDialog({ process, onClose }: { process: LogProcess; onClose: () => void }) {
  const details = asRecord(process.result);
  if (details === null) {
    return null;
  }

  const confidencePercent = Math.round(asNumber(details.confidence, 0) * 100);
  const generatedTables = asArray(details.generated_tables);
  const fileObservations = asArray(details.file_observations);
  const columns = asArray(details.columns);
  const warnings = asArray(details.warnings);
  const assumptions = asArray(details.assumptions);
  const segmentation = asRecord(details.segmentation);
  const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");

  return (
    <Dialog
      onOpenChange={(open) => {
        if (!open) {
          onClose();
        }
      }}
      open
    >
      <DialogContent className={"flex max-h-[85vh] flex-col overflow-hidden sm:max-w-2xl"}>
        <DialogHeader>
          <DialogTitle>Process Details</DialogTitle>
          <DialogDescription>{formattedDate}</DialogDescription>
        </DialogHeader>
        <ScrollArea className={"flex-1"}>
          <div className={"flex flex-col gap-4 py-1 pr-4"}>
            <p className={"text-muted-foreground text-sm"}>
              {asString(details.schema_summary, "No summary available.")}
            </p>

            <div className={"flex flex-col gap-1.5"}>
              <div className={"flex items-center justify-between"}>
                <span className={"font-medium text-muted-foreground text-xs"}>Confidence</span>
                <span className={"font-medium text-xs"}>{confidencePercent}%</span>
              </div>
              <Progress className={"h-1.5"} value={confidencePercent} />
            </div>

            {segmentation !== null && (
              <div className={"flex flex-col gap-1"}>
                <span className={"font-medium text-muted-foreground text-xs"}>Segmentation</span>
                <div className={"flex flex-wrap items-center gap-2"}>
                  <Badge variant={"outline"}>{asString(segmentation.strategy, "unknown")}</Badge>
                  <span className={"text-muted-foreground text-xs"}>
                    {asString(segmentation.rationale, "No rationale.")}
                  </span>
                </div>
              </div>
            )}

            {fileObservations.length > 0 && (
              <div className={"flex flex-col gap-1.5"}>
                <span className={"font-medium text-muted-foreground text-xs"}>Files ({fileObservations.length})</span>
                <div className={"flex flex-col divide-y rounded-md border"}>
                  {fileObservations.map((observation, index) => {
                    const row = asRecord(observation) ?? {};
                    return (
                      <div
                        className={"flex items-center gap-3 px-3 py-2"}
                        key={`${asString(row.filename, "file")}-${index}`}
                      >
                        <span className={"flex-1 truncate font-mono text-xs"}>
                          {asString(row.filename, "Unknown file")}
                        </span>
                        <Badge className={"shrink-0 text-xs"} variant={"secondary"}>
                          {asString(row.detected_format, "unknown")}
                        </Badge>
                        <span className={"shrink-0 text-muted-foreground text-xs"}>
                          {asNumber(row.line_count, 0)} lines
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            <div className={"flex flex-col gap-1.5"}>
              <span className={"font-medium text-muted-foreground text-xs"}>
                Generated Tables ({generatedTables.length})
              </span>
              <div className={"flex flex-col divide-y rounded-md border"}>
                {generatedTables.map((table, index) => {
                  const row = asRecord(table) ?? {};
                  const fileName = asNullableString(row.file_name);
                  const isNormalized = Boolean(row.is_normalized);
                  return (
                    <div
                      className={"flex items-center gap-3 px-3 py-2"}
                      key={`${asString(row.table_name, "table")}-${index}`}
                    >
                      <span className={"flex-1 font-medium text-xs"}>
                        {isNormalized
                          ? "Normalized Logs"
                          : fileName !== null
                            ? `Logs for ${fileName}`
                            : asString(row.table_name, "Unnamed table")}
                      </span>
                      {isNormalized ? (
                        <Badge className={"shrink-0 text-xs"} variant={"outline"}>
                          Normalized
                        </Badge>
                      ) : fileName !== null ? (
                        <Badge className={"max-w-40 shrink-0 truncate text-xs"} variant={"outline"}>
                          {fileName}
                        </Badge>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            </div>

            <div className={"flex flex-col gap-1.5"}>
              <span className={"font-medium text-muted-foreground text-xs"}>Columns ({columns.length})</span>
              <Accordion collapsible type={"single"}>
                <AccordionItem className={"rounded-md border"} value={"columns"}>
                  <AccordionTrigger className={"px-3 py-2 text-xs hover:no-underline"}>
                    View {columns.length} inferred columns
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className={"flex flex-col divide-y"}>
                      {columns.map((column, index) => {
                        const row = asRecord(column) ?? {};
                        return (
                          <div
                            className={"flex items-start gap-3 px-3 py-2"}
                            key={`${asString(row.name, "column")}-${index}`}
                          >
                            <div className={"flex flex-1 flex-col gap-0.5"}>
                              <span className={"font-medium font-mono text-xs"}>{asString(row.name, "unknown")}</span>
                              {asString(row.description, "") !== "" && (
                                <p className={"text-muted-foreground text-xs"}>{asString(row.description, "")}</p>
                              )}
                            </div>
                            <div className={"flex shrink-0 items-center gap-1"}>
                              <Badge className={"font-mono text-xs"} variant={"outline"}>
                                {asString(row.sql_type, "TEXT")}
                              </Badge>
                              <Badge className={"text-xs"} variant={"secondary"}>
                                {asString(row.kind, "detected")}
                              </Badge>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            </div>

            {warnings.length > 0 && (
              <Alert variant={"destructive"}>
                <AlertCircleIcon className={"size-4"} />
                <AlertTitle>Warnings</AlertTitle>
                <AlertDescription className={"flex flex-col gap-1"}>
                  {warnings.map((warning, index) => (
                    <span key={index}>{String(warning)}</span>
                  ))}
                </AlertDescription>
              </Alert>
            )}

            {assumptions.length > 0 && (
              <Alert>
                <InfoIcon className={"size-4"} />
                <AlertTitle>Assumptions</AlertTitle>
                <AlertDescription className={"flex flex-col gap-1"}>
                  {assumptions.map((assumption, index) => (
                    <span key={index}>{String(assumption)}</span>
                  ))}
                </AlertDescription>
              </Alert>
            )}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}

function ProcessFileList({
  fileObservations,
  generatedTables,
  processStatus,
}: {
  fileObservations: unknown[];
  generatedTables: unknown[];
  processStatus: string;
}) {
  const status =
    processStatus === "completed"
      ? "completed"
      : processStatus === "failed"
        ? "failed"
        : processStatus === "processing"
          ? "processing"
          : "queued";

  const getVariant = (value: string) => {
    if (value === "completed") {
      return "secondary" as const;
    }
    if (value === "failed") {
      return "destructive" as const;
    }
    return "outline" as const;
  };

  return (
    <Accordion className={"w-full"} collapsible type={"single"}>
      <AccordionItem className={"border-none"} value={"files"}>
        <AccordionTrigger className={"h-auto rounded-md border px-3 py-2 text-xs hover:bg-muted/50 hover:no-underline"}>
          <div className={"flex items-center gap-2"}>
            <span className={"font-medium"}>Files ({fileObservations.length})</span>
            <span className={"text-muted-foreground"}>View breakdown</span>
          </div>
        </AccordionTrigger>
        <AccordionContent>
          <div className={"flex flex-col divide-y rounded-md border"}>
            {fileObservations.map((observation, index) => {
              const row = asRecord(observation) ?? {};
              const filename = asString(row.filename, "unknown");
              const matched = generatedTables.find((table) => {
                const tableRecord = asRecord(table);
                if (tableRecord === null) {
                  return false;
                }

                return asString(tableRecord.file_name, "") === filename;
              });
              const matchedRecord = matched ? asRecord(matched) : null;

              return (
                <div className={"flex items-center gap-3 px-3 py-2"} key={`${filename}-${index}`}>
                  <span className={"flex-1 truncate font-mono text-xs"}>{filename}</span>
                  <Badge className={"shrink-0 text-xs"} variant={getVariant(status)}>
                    {status.charAt(0).toUpperCase() + status.slice(1)}
                  </Badge>
                  {matchedRecord !== null ? (
                    <Badge className={"max-w-40 shrink-0 truncate text-xs"} variant={"outline"}>
                      {asString(matchedRecord.table_name, "table")}
                    </Badge>
                  ) : (
                    <span className={"shrink-0 text-muted-foreground text-xs"}>No table generated</span>
                  )}
                </div>
              );
            })}
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
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

function asString(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}
