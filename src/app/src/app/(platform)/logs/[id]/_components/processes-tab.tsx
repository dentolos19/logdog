"use client";

import { format } from "date-fns";
import { AlertCircleIcon, CheckCircle2Icon, ClockIcon, InfoIcon } from "lucide-react";
import { useState } from "react";

import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Spinner } from "@/components/ui/spinner";
import type { LogProcess, ProcessResultDetails } from "@/lib/api/types";

interface ProcessesTabProps {
  processes: LogProcess[];
  isLoading: boolean;
  error: string | null;
}

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
          <EmptyDescription>Upload log files to start a preprocessing run.</EmptyDescription>
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
            process={process}
            onViewDetails={process.result !== null ? () => setSelectedProcess(process) : undefined}
          />
        ))}
      </div>

      {selectedProcess !== null && selectedProcess.result !== null && (
        <ProcessDetailsDialog process={selectedProcess} onClose={() => setSelectedProcess(null)} />
      )}
    </>
  );
}

interface ProcessRowProps {
  process: LogProcess;
  onViewDetails?: () => void;
}

function ProcessRow({ process, onViewDetails }: ProcessRowProps) {
  const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");
  const generatedTableCount = process.result?.generated_tables.length ?? 0;

  return (
    <div className={"flex flex-col gap-1.5 rounded-md border p-4"}>
      <div className={"flex items-center gap-3"}>
        <ProcessStatusIcon status={process.status} />
        <div className={"flex flex-1 flex-col gap-0.5"}>
          <span className={"text-sm font-medium"}>
            {process.status === "completed"
              ? `Created ${generatedTableCount} ${generatedTableCount === 1 ? "table" : "tables"}`
              : process.status === "failed"
                ? "Preprocessing failed"
                : "Processing…"}
          </span>
          <span className={"text-xs text-muted-foreground"}>{formattedDate}</span>
        </div>
        <ProcessStatusBadge status={process.status} />
        {onViewDetails !== undefined && (
          <Button variant={"ghost"} size={"sm"} onClick={onViewDetails} className={"shrink-0"}>
            <InfoIcon />
            Details
          </Button>
        )}
      </div>

      {process.status === "completed" && process.result !== null && (
        <div className={"flex items-center gap-2 text-xs text-muted-foreground"}>
          <span>{process.result.generated_tables.length} table(s) generated</span>
          <span>&middot;</span>
          <span>{Math.round(process.result.confidence * 100)}% confidence</span>
          <span>&middot;</span>
          <span>{process.result.file_observations.length} file(s) analyzed</span>
        </div>
      )}

      {process.status === "failed" && process.error !== null && (
        <p className={"rounded bg-destructive/10 px-2 py-1 font-mono text-xs text-destructive"}>{process.error}</p>
      )}
    </div>
  );
}

interface ProcessStatusIconProps {
  status: string;
}

function ProcessStatusIcon({ status }: ProcessStatusIconProps) {
  if (status === "completed") {
    return <CheckCircle2Icon className={"size-4 shrink-0 text-green-500"} />;
  }
  if (status === "failed") {
    return <AlertCircleIcon className={"size-4 shrink-0 text-destructive"} />;
  }
  return <Spinner className={"size-4 shrink-0"} />;
}

interface ProcessStatusBadgeProps {
  status: string;
}

function ProcessStatusBadge({ status }: ProcessStatusBadgeProps) {
  if (status === "completed") {
    return <Badge variant={"secondary"}>Completed</Badge>;
  }
  if (status === "failed") {
    return <Badge variant={"destructive"}>Failed</Badge>;
  }
  return <Badge variant={"outline"}>Processing</Badge>;
}

interface ProcessDetailsDialogProps {
  process: LogProcess;
  onClose: () => void;
}

function ProcessDetailsDialog({ process, onClose }: ProcessDetailsDialogProps) {
  // result is guaranteed non-null by the caller
  const details = process.result as ProcessResultDetails;
  const confidencePercent = Math.round(details.confidence * 100);
  const formattedDate = format(new Date(process.created_at), "MMM d, yyyy 'at' h:mm a");

  return (
    <Dialog
      open={true}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent className={"flex max-h-[85vh] flex-col overflow-hidden sm:max-w-2xl"}>
        <DialogHeader>
          <DialogTitle>Process Details</DialogTitle>
          <DialogDescription>{formattedDate}</DialogDescription>
        </DialogHeader>
        <ScrollArea className={"flex-1"}>
          <div className={"flex flex-col gap-4 py-1 pr-4"}>
            <p className={"text-sm text-muted-foreground"}>{details.schema_summary}</p>

            <div className={"flex flex-col gap-1.5"}>
              <div className={"flex items-center justify-between"}>
                <span className={"text-xs font-medium text-muted-foreground"}>Confidence</span>
                <span className={"text-xs font-medium"}>{confidencePercent}%</span>
              </div>
              <Progress value={confidencePercent} className={"h-1.5"} />
            </div>

            <div className={"flex flex-col gap-1"}>
              <span className={"text-xs font-medium text-muted-foreground"}>Segmentation</span>
              <div className={"flex flex-wrap items-center gap-2"}>
                <Badge variant={"outline"}>{details.segmentation.strategy}</Badge>
                <span className={"text-xs text-muted-foreground"}>{details.segmentation.rationale}</span>
              </div>
            </div>

            {details.file_observations.length > 0 && (
              <div className={"flex flex-col gap-1.5"}>
                <span className={"text-xs font-medium text-muted-foreground"}>
                  Files ({details.file_observations.length})
                </span>
                <div className={"flex flex-col divide-y rounded-md border"}>
                  {details.file_observations.map((observation) => (
                    <div key={observation.filename} className={"flex items-center gap-3 px-3 py-2"}>
                      <span className={"flex-1 truncate font-mono text-xs"}>{observation.filename}</span>
                      <Badge variant={"secondary"} className={"shrink-0 text-xs"}>
                        {observation.detected_format}
                      </Badge>
                      <span className={"shrink-0 text-xs text-muted-foreground"}>{observation.line_count} lines</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className={"flex flex-col gap-1.5"}>
              <span className={"text-xs font-medium text-muted-foreground"}>
                Generated Tables ({details.generated_tables.length})
              </span>
              <div className={"flex flex-col divide-y rounded-md border"}>
                {details.generated_tables.map((table) => (
                  <div key={table.table_name} className={"flex items-center gap-3 px-3 py-2"}>
                    <span className={"flex-1 text-xs font-medium"}>{getGeneratedTableLabel(table)}</span>
                    {table.is_normalized ? (
                      <Badge variant={"outline"} className={"shrink-0 text-xs"}>
                        Normalized
                      </Badge>
                    ) : (
                      table.file_name !== null && (
                        <Badge variant={"outline"} className={"max-w-40 shrink-0 truncate text-xs"}>
                          {table.file_name}
                        </Badge>
                      )
                    )}
                  </div>
                ))}
              </div>
            </div>

            <div className={"flex flex-col gap-1.5"}>
              <span className={"text-xs font-medium text-muted-foreground"}>Columns ({details.columns.length})</span>
              <Accordion type={"single"} collapsible={true}>
                <AccordionItem value={"columns"} className={"rounded-md border"}>
                  <AccordionTrigger className={"px-3 py-2 text-xs hover:no-underline"}>
                    View {details.columns.length} inferred columns
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className={"flex flex-col divide-y"}>
                      {details.columns.map((column) => (
                        <div key={column.name} className={"flex items-start gap-3 px-3 py-2"}>
                          <div className={"flex flex-1 flex-col gap-0.5"}>
                            <span className={"font-mono text-xs font-medium"}>{column.name}</span>
                            {column.description !== "" && (
                              <p className={"text-xs text-muted-foreground"}>{column.description}</p>
                            )}
                          </div>
                          <div className={"flex shrink-0 items-center gap-1"}>
                            <Badge variant={"outline"} className={"font-mono text-xs"}>
                              {column.sql_type}
                            </Badge>
                            <Badge variant={"secondary"} className={"text-xs"}>
                              {column.kind}
                            </Badge>
                          </div>
                        </div>
                      ))}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            </div>

            {details.warnings.length > 0 && (
              <Alert variant={"destructive"}>
                <AlertCircleIcon className={"size-4"} />
                <AlertTitle>Warnings</AlertTitle>
                <AlertDescription className={"flex flex-col gap-1"}>
                  {details.warnings.map((warning, index) => (
                    <span key={index}>{warning}</span>
                  ))}
                </AlertDescription>
              </Alert>
            )}

            {details.assumptions.length > 0 && (
              <Alert>
                <InfoIcon className={"size-4"} />
                <AlertTitle>Assumptions</AlertTitle>
                <AlertDescription className={"flex flex-col gap-1"}>
                  {details.assumptions.map((assumption, index) => (
                    <span key={index}>{assumption}</span>
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

function getGeneratedTableLabel(table: ProcessResultDetails["generated_tables"][number]): string {
  if (table.is_normalized) {
    return "Normalized Logs";
  }

  if (table.file_name !== null) {
    return `Logs for ${table.file_name}`;
  }

  return table.table_name;
}
