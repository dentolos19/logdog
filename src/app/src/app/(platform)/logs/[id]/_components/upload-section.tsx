"use client";

import { AlertCircleIcon, DatabaseZapIcon, FileTextIcon, InfoIcon, UploadIcon, XIcon } from "lucide-react";
import { useCallback, useRef, useState } from "react";

import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Progress } from "@/components/ui/progress";
import { Spinner } from "@/components/ui/spinner";
import { uploadLogFiles } from "@/lib/api";
import type { InferredColumn, PreprocessResult } from "@/lib/api/types";
import { cn } from "@/lib/utils";

interface UploadSectionProps {
  logGroupId: string;
  onUploadSuccess: () => void;
}

// Maps the column kind identifier to a human-readable label.
const KIND_LABELS: Record<string, string> = {
  baseline: "Baseline",
  detected: "Detected",
  llm_inferred: "LLM Inferred",
};

type BadgeVariant = "default" | "secondary" | "outline";

// Maps the column kind identifier to a badge variant for visual differentiation.
const KIND_BADGE_VARIANTS: Record<string, BadgeVariant> = {
  baseline: "secondary",
  detected: "default",
  llm_inferred: "outline",
};

export function UploadSection({ logGroupId, onUploadSuccess }: UploadSectionProps) {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [result, setResult] = useState<PreprocessResult | null>(null);
  const [isSchemaDialogOpen, setIsSchemaDialogOpen] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback((newFiles: FileList | File[]) => {
    const fileArray = Array.from(newFiles);
    setSelectedFiles((previous) => {
      const existingNames = new Set(previous.map((file) => file.name));
      const deduplicated = fileArray.filter((file) => !existingNames.has(file.name));
      return [...previous, ...deduplicated];
    });
    setUploadError(null);
  }, []);

  const removeFile = (filename: string) => {
    setSelectedFiles((previous) => previous.filter((file) => file.name !== filename));
  };

  const onDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragOver(true);
  };

  const onDragLeave = () => {
    setIsDragOver(false);
  };

  const onDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragOver(false);
    if (event.dataTransfer.files.length > 0) {
      addFiles(event.dataTransfer.files);
    }
  };

  const onFileInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files.length > 0) {
      addFiles(event.target.files);
      // Reset the value so the same file can be re-selected after removal.
      event.target.value = "";
    }
  };

  const onUpload = async () => {
    if (selectedFiles.length === 0) return;
    setIsUploading(true);
    setUploadError(null);
    setResult(null);
    try {
      const response = await uploadLogFiles(logGroupId, selectedFiles);
      setResult(response.process_result);
      setSelectedFiles([]);
      onUploadSuccess();
      setIsSchemaDialogOpen(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Upload failed. Please try again.";
      setUploadError(message);
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <section className={"flex flex-col gap-3"}>
      <h2 className={"text-sm font-semibold"}>Upload Logs</h2>

      {/* Drop zone — also acts as a click target to open the file picker. */}
      <div
        role={"button"}
        tabIndex={0}
        onClick={() => fileInputRef.current?.click()}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") fileInputRef.current?.click();
        }}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
        className={[
          "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center transition-colors",
          isDragOver ? "border-primary bg-primary/5" : "hover:bg-muted/40",
        ].join(" ")}
      >
        <div className={"flex size-10 shrink-0 items-center justify-center rounded-lg bg-muted"}>
          <UploadIcon className={"size-4 text-muted-foreground"} />
        </div>
        <div className={"flex flex-col gap-0.5"}>
          <p className={"text-sm font-medium"}>Drop files here or click to browse</p>
          <p className={"text-xs text-muted-foreground"}>Supports plaintext, JSON Lines, CSV, syslog, and more.</p>
        </div>
      </div>
      <input ref={fileInputRef} type={"file"} multiple={true} className={"hidden"} onChange={onFileInputChange} />

      {/* Selected files list shown before upload. */}
      {selectedFiles.length > 0 && (
        <ul className={"flex flex-col gap-1.5"}>
          {selectedFiles.map((file) => (
            <li key={file.name} className={"flex items-center gap-2 rounded-md border px-3 py-2"}>
              <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
              <span className={"flex-1 truncate font-mono text-sm"}>{file.name}</span>
              <span className={"shrink-0 text-xs text-muted-foreground"}>{(file.size / 1024).toFixed(1)} KB</span>
              <button
                type={"button"}
                aria-label={`Remove ${file.name}`}
                onClick={(event) => {
                  event.stopPropagation();
                  removeFile(file.name);
                }}
                className={
                  "ml-1 shrink-0 rounded-sm opacity-60 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring"
                }
              >
                <XIcon className={"size-3.5"} />
              </button>
            </li>
          ))}
        </ul>
      )}

      {/* Upload action row shown only when files are staged. */}
      {selectedFiles.length > 0 && (
        <div className={"flex items-center justify-between gap-3"}>
          <p className={"text-xs text-muted-foreground"}>
            {selectedFiles.length} {selectedFiles.length === 1 ? "file" : "files"} selected
          </p>
          <Button onClick={onUpload} disabled={isUploading} size={"sm"}>
            {isUploading ? <Spinner /> : <UploadIcon />}
            {isUploading ? "Analyzing…" : "Upload & Analyze"}
          </Button>
        </div>
      )}

      {/* Error state from a failed upload attempt. */}
      {uploadError !== null && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Upload Failed</AlertTitle>
          <AlertDescription>{uploadError}</AlertDescription>
        </Alert>
      )}

      {/* Button to review the schema analysis from the last upload. */}
      {result !== null && (
        <div className={"flex justify-end"}>
          <Button variant={"outline"} size={"sm"} onClick={() => setIsSchemaDialogOpen(true)}>
            <DatabaseZapIcon />
            View Schema Analysis
          </Button>
        </div>
      )}

      {/* Schema analysis dialog */}
      {result !== null && (
        <Dialog open={isSchemaDialogOpen} onOpenChange={setIsSchemaDialogOpen}>
          <DialogContent className={"flex max-h-[90vh] flex-col overflow-hidden sm:max-w-3xl"}>
            <DialogHeader className={"shrink-0"}>
              <DialogTitle>Schema Analysis</DialogTitle>
            </DialogHeader>
            <div className={"min-h-0 flex-1 overflow-y-auto pr-1"}>
              <PreprocessorResultPanel result={result} className={"border-0 p-0"} />
            </div>
          </DialogContent>
        </Dialog>
      )}
    </section>
  );
}

interface PreprocessorResultPanelProps {
  result: PreprocessResult;
  className?: string;
}

function PreprocessorResultPanel({ result, className }: PreprocessorResultPanelProps) {
  const confidencePercent = Math.round(result.confidence * 100);

  return (
    <div className={cn("flex flex-col gap-4 rounded-lg border p-4", className)}>
      {/* Summary header row */}
      <div className={"flex flex-col gap-1.5"}>
        <div className={"flex items-center justify-between gap-2"}>
          <h3 className={"text-sm font-semibold"}>Schema Analysis</h3>
          <Badge variant={"secondary"}>{result.schema_version}</Badge>
        </div>
        <p className={"text-sm text-muted-foreground"}>{result.schema_summary}</p>
      </div>

      {/* Confidence score bar */}
      <div className={"flex flex-col gap-1.5"}>
        <div className={"flex items-center justify-between"}>
          <span className={"text-xs font-medium text-muted-foreground"}>Confidence</span>
          <span className={"text-xs font-medium"}>{confidencePercent}%</span>
        </div>
        <Progress value={confidencePercent} className={"h-1.5"} />
      </div>

      {/* Segmentation strategy detected across files */}
      <div className={"flex flex-col gap-1"}>
        <span className={"text-xs font-medium text-muted-foreground"}>Segmentation</span>
        <div className={"flex flex-wrap items-center gap-2"}>
          <Badge variant={"outline"}>{result.segmentation.strategy}</Badge>
          <span className={"text-xs text-muted-foreground"}>{result.segmentation.rationale}</span>
        </div>
      </div>

      {/* Per-file format detection results */}
      {result.file_observations.length > 0 && (
        <div className={"flex flex-col gap-1.5"}>
          <span className={"text-xs font-medium text-muted-foreground"}>Files ({result.file_observations.length})</span>
          <div className={"flex flex-col divide-y rounded-md border"}>
            {result.file_observations.map((observation) => (
              <div key={observation.filename} className={"flex items-center gap-3 px-3 py-2"}>
                <FileTextIcon className={"size-3.5 shrink-0 text-muted-foreground"} />
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

      {/* Inferred schema columns */}
      <div className={"flex flex-col gap-1.5"}>
        <span className={"text-xs font-medium text-muted-foreground"}>Columns ({result.columns.length})</span>
        <Accordion type={"single"} collapsible={true}>
          <AccordionItem value={"columns"} className={"rounded-md border"}>
            <AccordionTrigger className={"px-3 py-2 text-xs hover:no-underline"}>
              View {result.columns.length} inferred columns
            </AccordionTrigger>
            <AccordionContent>
              <div className={"flex flex-col divide-y"}>
                {result.columns.map((column) => (
                  <ColumnRow key={column.name} column={column} />
                ))}
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </div>

      {/* Sample records from the uploaded files */}
      {result.sample_records.length > 0 && (
        <div className={"flex flex-col gap-1.5"}>
          <span className={"text-xs font-medium text-muted-foreground"}>
            Sample Records ({result.sample_records.length})
          </span>
          <Accordion type={"single"} collapsible={true}>
            <AccordionItem value={"samples"} className={"rounded-md border"}>
              <AccordionTrigger className={"px-3 py-2 text-xs hover:no-underline"}>
                Preview {result.sample_records.length} records
              </AccordionTrigger>
              <AccordionContent>
                <div className={"flex flex-col divide-y"}>
                  {result.sample_records.map((record, index) => (
                    <div key={index} className={"flex flex-col gap-1 p-3"}>
                      <span className={"font-mono text-[10px] text-muted-foreground"}>
                        {record.source_file}:{record.line_start}
                      </span>
                      <pre
                        className={
                          "overflow-x-auto whitespace-pre-wrap break-all font-mono text-[10px] text-foreground"
                        }
                      >
                        {JSON.stringify(record.fields, null, 2)}
                      </pre>
                    </div>
                  ))}
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        </div>
      )}

      {/* Warnings surfaced by the preprocessor */}
      {result.warnings.length > 0 && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Warnings</AlertTitle>
          <AlertDescription className={"flex flex-col gap-1"}>
            {result.warnings.map((warning, index) => (
              <span key={index}>{warning}</span>
            ))}
          </AlertDescription>
        </Alert>
      )}

      {/* Assumptions the preprocessor made about ambiguous fields */}
      {result.assumptions.length > 0 && (
        <Alert>
          <InfoIcon className={"size-4"} />
          <AlertTitle>Assumptions</AlertTitle>
          <AlertDescription className={"flex flex-col gap-1"}>
            {result.assumptions.map((assumption, index) => (
              <span key={index}>{assumption}</span>
            ))}
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
}

interface ColumnRowProps {
  column: InferredColumn;
}

// Extracted into its own component to keep the column list render loop clean.
function ColumnRow({ column }: ColumnRowProps) {
  const badgeVariant = KIND_BADGE_VARIANTS[column.kind] ?? "secondary";
  const kindLabel = KIND_LABELS[column.kind] ?? column.kind;

  return (
    <div className={"flex flex-col gap-1 px-3 py-2"}>
      <div className={"flex items-center gap-2"}>
        <span className={"font-mono text-xs font-medium"}>{column.name}</span>
        <Badge variant={"outline"} className={"shrink-0 font-mono text-[10px]"}>
          {column.sql_type}
        </Badge>
        <Badge variant={badgeVariant} className={"ml-auto shrink-0 text-[10px]"}>
          {kindLabel}
        </Badge>
      </div>
      {column.description && <p className={"text-xs text-muted-foreground"}>{column.description}</p>}
      {column.example_values.length > 0 && (
        <div className={"flex flex-wrap gap-1"}>
          {column.example_values.slice(0, 4).map((exampleValue, index) => (
            <span
              key={index}
              className={"rounded-sm bg-muted px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground"}
            >
              {exampleValue}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
