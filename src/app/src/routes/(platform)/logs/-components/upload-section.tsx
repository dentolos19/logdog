import { AlertCircleIcon, CheckCircle2Icon, FileTextIcon, UploadIcon, XIcon } from "lucide-react";
import { useCallback, useRef, useState } from "react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Progress } from "#/components/ui/progress";
import { Spinner } from "#/components/ui/spinner";
import { type UploadFileOutcome, uploadLogFiles } from "#/lib/server";

type UploadSectionProps = {
  logEntryId: string;
  onUploadSuccess: () => void;
};

export function UploadSection({ logEntryId, onUploadSuccess }: UploadSectionProps) {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [queueOutcomes, setQueueOutcomes] = useState<UploadFileOutcome[]>([]);
  const [queuedProcessIds, setQueuedProcessIds] = useState<string[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback((incoming: FileList | File[]) => {
    const incomingArray = Array.from(incoming);
    setSelectedFiles((previous) => {
      const existing = new Set(previous.map((file) => file.name));
      return [...previous, ...incomingArray.filter((file) => !existing.has(file.name))];
    });
    setUploadError(null);
  }, []);

  const removeFile = (filename: string) => {
    setSelectedFiles((previous) => previous.filter((file) => file.name !== filename));
  };

  const onUpload = async () => {
    if (selectedFiles.length === 0) {
      return;
    }

    setIsUploading(true);
    setUploadError(null);
    setQueueOutcomes([]);
    setQueuedProcessIds([]);

    try {
      const response = await uploadLogFiles(logEntryId, selectedFiles);
      const queuedOutcomes = response.outcomes.filter((outcome) => outcome.status === "queued");
      const failedOutcomes = response.outcomes.filter((outcome) => outcome.status !== "queued");

      setQueueOutcomes(response.outcomes);
      setQueuedProcessIds(response.process_ids);
      setSelectedFiles([]);

      if (failedOutcomes.length === 0) {
        toast.success(`Queued ${queuedOutcomes.length} ${queuedOutcomes.length === 1 ? "process" : "processes"}.`);
      } else {
        toast.warning(
          `Queued ${queuedOutcomes.length} ${queuedOutcomes.length === 1 ? "process" : "processes"}; ${failedOutcomes.length} failed.`,
        );
      }

      await onUploadSuccess();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Upload failed. Please try again.";
      setUploadError(message);
      toast.error(message);
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <section className={"flex flex-col gap-3"}>
      <h2 className={"font-semibold text-sm"}>Upload Logs</h2>

      <div
        className={[
          "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center transition-colors",
          isDragOver ? "border-primary bg-primary/5" : "hover:bg-muted/40",
        ].join(" ")}
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
        <div className={"flex size-10 shrink-0 items-center justify-center rounded-lg bg-muted"}>
          <UploadIcon className={"size-4 text-muted-foreground"} />
        </div>
        <div className={"flex flex-col gap-0.5"}>
          <p className={"font-medium text-sm"}>Drop files here or click to browse</p>
          <p className={"text-muted-foreground text-xs"}>
            Supports plaintext, JSON Lines, CSV, syslog, and mixed files.
          </p>
        </div>
      </div>

      <input
        className={"hidden"}
        multiple
        onChange={(event) => {
          if (event.target.files && event.target.files.length > 0) {
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
                className={
                  "ml-1 shrink-0 rounded-sm opacity-60 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring"
                }
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
          <Button disabled={isUploading} onClick={() => void onUpload()} size={"sm"}>
            {isUploading ? <Spinner /> : <UploadIcon />}
            {isUploading ? "Uploading..." : "Upload and Process"}
          </Button>
        </div>
      )}

      {isUploading && <Progress className={"h-1.5"} value={45} />}

      {uploadError !== null && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Upload Failed</AlertTitle>
          <AlertDescription>{uploadError}</AlertDescription>
        </Alert>
      )}

      {queueOutcomes.length > 0 && (
        <Alert>
          <CheckCircle2Icon className={"size-4 text-green-500"} />
          <AlertTitle>Processes Queued</AlertTitle>
          <AlertDescription className={"flex flex-col gap-2"}>
            <span>
              Queued {queueOutcomes.filter((outcome) => outcome.status === "queued").length} of {queueOutcomes.length}{" "}
              processes.
            </span>
            {queueOutcomes.some((outcome) => outcome.status !== "queued") && (
              <span className={"text-destructive text-xs"}>
                {queueOutcomes.filter((outcome) => outcome.status !== "queued").length} file(s) could not be queued.
              </span>
            )}
            {queuedProcessIds.length > 0 && (
              <div className={"flex flex-wrap gap-1.5"}>
                {queuedProcessIds.map((processId) => (
                  <Badge key={processId} variant={"outline"}>
                    process_id: {processId}
                  </Badge>
                ))}
              </div>
            )}
            <div className={"flex flex-col gap-1 rounded-md border p-2"}>
              {queueOutcomes.map((outcome) => (
                <div className={"flex items-center justify-between gap-2 text-xs"} key={outcome.file_id}>
                  <span className={"truncate font-mono"}>{outcome.filename}</span>
                  <Badge variant={outcome.status === "queued" ? "secondary" : "destructive"}>{outcome.status}</Badge>
                </div>
              ))}
            </div>
          </AlertDescription>
        </Alert>
      )}
    </section>
  );
}
