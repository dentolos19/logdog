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
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "#/components/ui/collapsible";
import { Progress } from "#/components/ui/progress";
import { ScrollArea } from "#/components/ui/scroll-area";
import { Separator } from "#/components/ui/separator";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "#/components/ui/sheet";
import { Spinner } from "#/components/ui/spinner";
import { cn } from "#/lib/utils";

type LocalParseResult = {
  filename: string;
  stages_executed: string[];
  confidence: number;
  format_detected: string | null;
  total_latency_ms: number;
  ai_fallback_used: boolean;
  log_row: {
    timestamp: string | null;
    source: string;
    source_type: string;
    log_level: string;
    event_type: string;
    message: string;
    schema_version: string;
    additional_data: Record<string, unknown>;
    equipment_id: string | null;
    lot_id: string | null;
    wafer_id: string | null;
    recipe_id: string | null;
    step_id: string | null;
    module_id: string | null;
  };
};

export function ParserTesterButton() {
  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button variant={"outline"}>
          <FlaskConicalIcon />
          Test Parser
        </Button>
      </SheetTrigger>
      <SheetContent className={"flex w-full flex-col gap-0 p-0 sm:max-w-xl"} side={"right"}>
        <SheetHeader className={"border-b px-6 py-4"}>
          <SheetTitle>Semi-Structured Parser Tester</SheetTitle>
          <SheetDescription>
            Upload log files to preview client-side parsing estimates. Backend parser test endpoint is not enabled yet.
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
  const [results, setResults] = useState<LocalParseResult[] | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback((incoming: FileList | File[]) => {
    const nextFiles = Array.from(incoming);
    setSelectedFiles((previous) => {
      const existingNames = new Set(previous.map((file) => file.name));
      return [...previous, ...nextFiles.filter((file) => !existingNames.has(file.name))];
    });
    setError(null);
    setResults(null);
  }, []);

  const removeFile = (name: string) => {
    setSelectedFiles((previous) => previous.filter((file) => file.name !== name));
    setResults(null);
  };

  const onRun = async () => {
    if (selectedFiles.length === 0) {
      return;
    }

    setIsParsing(true);
    setError(null);
    setResults(null);

    try {
      const generatedResults = await Promise.all(
        selectedFiles.map(async (file) => {
          const startedAt = performance.now();
          const text = await file.text();
          const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
          const firstLine = lines[0] ?? "";
          const detectedFormat = detectFormat(firstLine);
          const inferredLevel = detectLevel(firstLine);
          const latency = performance.now() - startedAt;

          return {
            filename: file.name,
            stages_executed: ["classification", "field-extraction", "normalization"],
            confidence: Math.min(0.98, Math.max(0.4, 0.6 + Math.min(lines.length, 100) / 250)),
            format_detected: detectedFormat,
            total_latency_ms: latency,
            ai_fallback_used: detectedFormat === "plain_text",
            log_row: {
              timestamp: null,
              source: file.name,
              source_type: "file",
              log_level: inferredLevel,
              event_type: inferredLevel === "ERROR" ? "error_event" : "log_event",
              message: firstLine || "No message detected.",
              schema_version: "local-preview-1.0",
              additional_data: {
                line_count: lines.length,
                char_count: text.length,
              },
              equipment_id: null,
              lot_id: null,
              wafer_id: null,
              recipe_id: null,
              step_id: null,
              module_id: null,
            },
          };
        }),
      );

      setResults(generatedResults);
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Parser preview failed.");
    } finally {
      setIsParsing(false);
    }
  };

  return (
    <ScrollArea className={"flex-1"}>
      <div className={"flex flex-col gap-4 p-6"}>
        <div
          className={cn(
            "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center transition-colors",
            isDragOver ? "border-primary bg-primary/5" : "hover:bg-muted/40",
          )}
          onClick={() => fileInputRef.current?.click()}
          onDragLeave={() => setIsDragOver(false)}
          onDragOver={(event) => {
            event.preventDefault();
            setIsDragOver(true);
          }}
          onDrop={(event) => {
            event.preventDefault();
            setIsDragOver(false);
            if (event.dataTransfer.files.length > 0) {
              addFiles(event.dataTransfer.files);
            }
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              fileInputRef.current?.click();
            }
          }}
          role={"button"}
          tabIndex={0}
        >
          <div className={"flex size-10 items-center justify-center rounded-lg bg-muted"}>
            <UploadIcon className={"size-4 text-muted-foreground"} />
          </div>
          <div className={"flex flex-col gap-0.5"}>
            <p className={"font-medium text-sm"}>Drop files here or click to browse</p>
            <p className={"text-muted-foreground text-xs"}>Plaintext, JSON, CSV, syslog, key=value, and more</p>
          </div>
        </div>

        <input
          className={"hidden"}
          multiple
          onChange={(event) => {
            if (event.target.files?.length) {
              addFiles(event.target.files);
              event.target.value = "";
            }
          }}
          ref={fileInputRef}
          type={"file"}
        />

        {selectedFiles.length > 0 && (
          <ul className={"flex flex-col gap-1.5"}>
            {selectedFiles.map((file) => (
              <li className={"flex items-center gap-2 rounded-md border px-3 py-2"} key={file.name}>
                <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
                <span className={"flex-1 truncate font-mono text-sm"}>{file.name}</span>
                <span className={"shrink-0 text-muted-foreground text-xs"}>{(file.size / 1024).toFixed(1)} KB</span>
                <button
                  aria-label={`Remove ${file.name}`}
                  className={"ml-1 shrink-0 rounded-sm opacity-60 hover:opacity-100"}
                  onClick={(event) => {
                    event.stopPropagation();
                    removeFile(file.name);
                  }}
                  type={"button"}
                >
                  <XIcon className={"size-3.5"} />
                </button>
              </li>
            ))}
          </ul>
        )}

        {selectedFiles.length > 0 && (
          <div className={"flex items-center justify-between gap-3"}>
            <p className={"text-muted-foreground text-xs"}>
              {selectedFiles.length} {selectedFiles.length === 1 ? "file" : "files"} selected
            </p>
            <Button disabled={isParsing} onClick={() => void onRun()} size={"sm"}>
              {isParsing ? <Spinner /> : <CpuIcon />}
              {isParsing ? "Parsing..." : "Run Parser"}
            </Button>
          </div>
        )}

        {error !== null && (
          <Alert variant={"destructive"}>
            <AlertCircleIcon className={"size-4"} />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {results !== null && (
          <div className={"flex flex-col gap-3"}>
            <Separator />
            <p className={"font-semibold text-sm"}>
              Results - {results.length} {results.length === 1 ? "file" : "files"}
            </p>
            {results.map((result) => (
              <ParseResultCard key={result.filename} result={result} />
            ))}
          </div>
        )}
      </div>
    </ScrollArea>
  );
}

function ParseResultCard({ result }: { result: LocalParseResult }) {
  const percent = Math.round(result.confidence * 100);
  const row = result.log_row;

  const semiconductorFields = [
    { label: "Equipment", value: row.equipment_id },
    { label: "Lot", value: row.lot_id },
    { label: "Wafer", value: row.wafer_id },
    { label: "Recipe", value: row.recipe_id },
    { label: "Step", value: row.step_id },
    { label: "Module", value: row.module_id },
  ].filter((field) => field.value);

  return (
    <div className={"flex flex-col gap-3 rounded-lg border p-4"}>
      <div className={"flex items-start justify-between gap-2"}>
        <div className={"flex items-center gap-2"}>
          <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
          <span className={"font-medium font-mono text-sm"}>{result.filename}</span>
        </div>
        <div className={"flex shrink-0 flex-wrap items-center justify-end gap-1.5"}>
          {result.ai_fallback_used && (
            <Badge className={"text-[10px]"} variant={"outline"}>
              AI fallback
            </Badge>
          )}
          {result.format_detected !== null && (
            <Badge className={"text-[10px]"} variant={"secondary"}>
              {result.format_detected}
            </Badge>
          )}
          <Badge className={"text-[10px]"} variant={"outline"}>
            {row.event_type}
          </Badge>
        </div>
      </div>

      <div className={"flex flex-col gap-1"}>
        <div className={"flex items-center justify-between"}>
          <span className={"text-muted-foreground text-xs"}>Parse confidence</span>
          <span className={"font-medium text-xs"}>{percent}%</span>
        </div>
        <Progress className={"h-1.5"} value={percent} />
      </div>

      <div className={"flex flex-wrap gap-3"}>
        <div className={"flex items-center gap-1 text-muted-foreground text-xs"}>
          <ClockIcon className={"size-3"} />
          {result.total_latency_ms.toFixed(1)} ms
        </div>
        <div className={"flex items-center gap-1 text-muted-foreground text-xs"}>
          <CheckCircle2Icon className={"size-3"} />
          {result.stages_executed.join(" -> ")}
        </div>
      </div>

      <div className={"grid grid-cols-2 gap-x-4 gap-y-2 rounded-md bg-muted/50 p-3"}>
        {[
          { label: "log_level", value: row.log_level },
          { label: "source_type", value: row.source_type },
          { label: "timestamp", value: row.timestamp },
          { label: "source", value: row.source },
          { label: "schema_version", value: row.schema_version },
        ].map(({ label, value }) =>
          value ? (
            <div className={"flex flex-col gap-0.5"} key={label}>
              <span className={"font-mono text-[10px] text-muted-foreground"}>{label}</span>
              <span className={"truncate font-mono text-xs"}>{value}</span>
            </div>
          ) : null,
        )}
      </div>

      <p className={"rounded-md border px-3 py-2 font-mono text-muted-foreground text-xs"}>{row.message}</p>

      {semiconductorFields.length > 0 && (
        <div className={"flex flex-wrap gap-2"}>
          {semiconductorFields.map((field) => (
            <div className={"flex items-center gap-1 rounded-sm bg-muted px-2 py-1"} key={field.label}>
              <span className={"text-[10px] text-muted-foreground"}>{field.label}:</span>
              <span className={"font-medium font-mono text-[10px]"}>{field.value}</span>
            </div>
          ))}
        </div>
      )}

      {Object.keys(row.additional_data).length > 0 && (
        <Collapsible>
          <CollapsibleTrigger className={"flex items-center gap-1 text-muted-foreground text-xs hover:text-foreground"}>
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

function detectFormat(line: string) {
  const trimmed = line.trim();
  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    return "json_lines";
  }
  if (trimmed.includes(",") && trimmed.split(",").length > 2) {
    return "csv";
  }
  if (/\w+=/.test(trimmed)) {
    return "logfmt";
  }
  if (/\b(?:GET|POST|PUT|PATCH|DELETE)\b/.test(trimmed)) {
    return "access_log";
  }
  return "plain_text";
}

function detectLevel(line: string) {
  const upper = line.toUpperCase();
  if (upper.includes("ERROR") || upper.includes("CRIT") || upper.includes("FATAL")) {
    return "ERROR";
  }
  if (upper.includes("WARN")) {
    return "WARN";
  }
  if (upper.includes("DEBUG")) {
    return "DEBUG";
  }
  return "INFO";
}
