"use client";

import {
  AlertCircleIcon,
  CheckCircle2Icon,
  ChevronDownIcon,
  ClockIcon,
  CpuIcon,
  FileTextIcon,
  FlaskConicalIcon,
  UploadIcon,
  XIcon,
} from "lucide-react";
import { useCallback, useRef, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Spinner } from "@/components/ui/spinner";
import { testParser } from "@/lib/api";
import type { FileParseResult } from "@/lib/api/types";
import { cn } from "@/lib/utils";

export function ParserTesterButton() {
  return (
    <Sheet>
      <SheetTrigger asChild={true}>
        <Button variant={"outline"}>
          <FlaskConicalIcon />
          Test Parser
        </Button>
      </SheetTrigger>
      <SheetContent side={"right"} className={"flex w-full flex-col gap-0 p-0 sm:max-w-xl"}>
        <SheetHeader className={"border-b px-6 py-4"}>
          <SheetTitle>Semi-Structured Parser Tester</SheetTitle>
          <SheetDescription>
            Upload log files to run them through the parsing pipeline. Results are not stored.
          </SheetDescription>
        </SheetHeader>
        <ParserTesterBody />
      </SheetContent>
    </Sheet>
  );
}

function ParserTesterBody() {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [isParsing, setIsParsing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [results, setResults] = useState<FileParseResult[] | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback((incoming: FileList | File[]) => {
    const arr = Array.from(incoming);
    setSelectedFiles((prev) => {
      const existing = new Set(prev.map((f) => f.name));
      return [...prev, ...arr.filter((f) => !existing.has(f.name))];
    });
    setError(null);
    setResults(null);
  }, []);

  const removeFile = (name: string) => {
    setSelectedFiles((prev) => prev.filter((f) => f.name !== name));
    setResults(null);
  };

  const onDragOver = (e: React.DragEvent) => { e.preventDefault(); setIsDragOver(true); };
  const onDragLeave = () => setIsDragOver(false);
  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    if (e.dataTransfer.files.length > 0) addFiles(e.dataTransfer.files);
  };
  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.length) { addFiles(e.target.files); e.target.value = ""; }
  };

  const onRun = async () => {
    if (selectedFiles.length === 0) return;
    setIsParsing(true);
    setError(null);
    setResults(null);
    try {
      const data = await testParser(selectedFiles);
      setResults(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Parse failed.");
    } finally {
      setIsParsing(false);
    }
  };

  return (
    <ScrollArea className={"flex-1"}>
      <div className={"flex flex-col gap-4 p-6"}>
        {/* Drop zone */}
        <div
          role={"button"}
          tabIndex={0}
          onClick={() => fileInputRef.current?.click()}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") fileInputRef.current?.click(); }}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
          className={cn(
            "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center transition-colors",
            isDragOver ? "border-primary bg-primary/5" : "hover:bg-muted/40",
          )}
        >
          <div className={"flex size-10 items-center justify-center rounded-lg bg-muted"}>
            <UploadIcon className={"size-4 text-muted-foreground"} />
          </div>
          <div className={"flex flex-col gap-0.5"}>
            <p className={"text-sm font-medium"}>Drop files here or click to browse</p>
            <p className={"text-xs text-muted-foreground"}>Plaintext, JSON, CSV, syslog, key=value, and more</p>
          </div>
        </div>
        <input ref={fileInputRef} type={"file"} multiple={true} className={"hidden"} onChange={onFileChange} />

        {/* Staged files */}
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
                  onClick={(e) => { e.stopPropagation(); removeFile(file.name); }}
                  className={"ml-1 shrink-0 rounded-sm opacity-60 hover:opacity-100"}
                >
                  <XIcon className={"size-3.5"} />
                </button>
              </li>
            ))}
          </ul>
        )}

        {/* Run button */}
        {selectedFiles.length > 0 && (
          <div className={"flex items-center justify-between gap-3"}>
            <p className={"text-xs text-muted-foreground"}>
              {selectedFiles.length} {selectedFiles.length === 1 ? "file" : "files"} selected
            </p>
            <Button onClick={onRun} disabled={isParsing} size={"sm"}>
              {isParsing ? <Spinner /> : <CpuIcon />}
              {isParsing ? "Parsing…" : "Run Parser"}
            </Button>
          </div>
        )}

        {/* Error */}
        {error !== null && (
          <Alert variant={"destructive"}>
            <AlertCircleIcon className={"size-4"} />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Results */}
        {results !== null && (
          <div className={"flex flex-col gap-3"}>
            <Separator />
            <p className={"text-sm font-semibold"}>
              Results — {results.length} {results.length === 1 ? "file" : "files"}
            </p>
            {results.map((r) => (
              <ParseResultCard key={r.filename} result={r} />
            ))}
          </div>
        )}
      </div>
    </ScrollArea>
  );
}

function ParseResultCard({ result }: { result: FileParseResult }) {
  const pct = Math.round(result.confidence * 100);
  const row = result.log_row;

  const semFields = [
    { label: "Equipment", value: row.equipment_id },
    { label: "Lot", value: row.lot_id },
    { label: "Wafer", value: row.wafer_id },
    { label: "Recipe", value: row.recipe_id },
    { label: "Step", value: row.step_id },
    { label: "Module", value: row.module_id },
  ].filter((f) => f.value);

  return (
    <div className={"flex flex-col gap-3 rounded-lg border p-4"}>
      {/* Header row */}
      <div className={"flex items-start justify-between gap-2"}>
        <div className={"flex items-center gap-2"}>
          <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
          <span className={"font-mono text-sm font-medium"}>{result.filename}</span>
        </div>
        <div className={"flex shrink-0 flex-wrap items-center justify-end gap-1.5"}>
          {result.ai_fallback_used && (
            <Badge variant={"outline"} className={"text-[10px]"}>AI fallback</Badge>
          )}
          {result.format_detected && (
            <Badge variant={"secondary"} className={"text-[10px]"}>{result.format_detected}</Badge>
          )}
          <Badge variant={"outline"} className={"text-[10px]"}>{row.event_type}</Badge>
        </div>
      </div>

      {/* Confidence + parse_confidence */}
      <div className={"flex flex-col gap-1"}>
        <div className={"flex items-center justify-between"}>
          <span className={"text-xs text-muted-foreground"}>Parse confidence</span>
          <span className={"text-xs font-medium"}>{pct}%</span>
        </div>
        <Progress value={pct} className={"h-1.5"} />
      </div>

      {/* Quick stats */}
      <div className={"flex flex-wrap gap-3"}>
        <div className={"flex items-center gap-1 text-xs text-muted-foreground"}>
          <ClockIcon className={"size-3"} />
          {result.total_latency_ms.toFixed(1)} ms
        </div>
        <div className={"flex items-center gap-1 text-xs text-muted-foreground"}>
          <CheckCircle2Icon className={"size-3"} />
          {result.stages_executed.join(" → ")}
        </div>
      </div>

      {/* Baseline fields grid */}
      <div className={"grid grid-cols-2 gap-x-4 gap-y-2 rounded-md bg-muted/50 p-3"}>
        {[
          { label: "log_level", value: row.log_level },
          { label: "source_type", value: row.source_type },
          { label: "timestamp", value: row.timestamp },
          { label: "timestamp_raw", value: row.timestamp_raw },
          { label: "source", value: row.source || null },
          { label: "schema_version", value: row.schema_version },
        ].map(({ label, value }) =>
          value ? (
            <div key={label} className={"flex flex-col gap-0.5"}>
              <span className={"font-mono text-[10px] text-muted-foreground"}>{label}</span>
              <span className={"truncate font-mono text-xs"}>{value}</span>
            </div>
          ) : null,
        )}
      </div>

      {/* Message */}
      {row.message && (
        <p className={"rounded-md border px-3 py-2 font-mono text-xs text-muted-foreground"}>{row.message}</p>
      )}

      {/* Semiconductor fields */}
      {semFields.length > 0 && (
        <div className={"flex flex-wrap gap-2"}>
          {semFields.map(({ label, value }) => (
            <div key={label} className={"flex items-center gap-1 rounded-sm bg-muted px-2 py-1"}>
              <span className={"text-[10px] text-muted-foreground"}>{label}:</span>
              <span className={"font-mono text-[10px] font-medium"}>{value}</span>
            </div>
          ))}
        </div>
      )}

      {/* additional_data (collapsible) */}
      {Object.keys(row.additional_data).length > 0 && (
        <Collapsible>
          <CollapsibleTrigger className={"flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"}>
            <ChevronDownIcon className={"size-3 transition-transform [[data-state=open]_&]:rotate-180"} />
            additional_data ({Object.keys(row.additional_data).length} fields)
          </CollapsibleTrigger>
          <CollapsibleContent>
            <pre className={"mt-2 overflow-x-auto rounded-md bg-muted p-3 font-mono text-[10px] text-foreground"}>
              {JSON.stringify(row.additional_data, null, 2)}
            </pre>
          </CollapsibleContent>
        </Collapsible>
      )}
    </div>
  );
}
