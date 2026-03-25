"use client";

import { AlertCircleIcon, CheckCircle2Icon, CpuIcon, FileTextIcon, UploadIcon, XIcon } from "lucide-react";
import { useCallback, useRef, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Spinner } from "@/components/ui/spinner";
import { uploadLogFiles } from "@/lib/api";
import type { ClassificationResult } from "@/lib/api/types";
import { cn } from "@/lib/utils";

interface UploadSectionProps {
  logGroupId: string;
  onUploadSuccess: () => void;
}

export function UploadSection({ logGroupId, onUploadSuccess }: UploadSectionProps) {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragOver, setIsDragOver] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [classification, setClassification] = useState<ClassificationResult | null>(null);
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
      event.target.value = "";
    }
  };

  const onUpload = async () => {
    if (selectedFiles.length === 0) return;
    setIsUploading(true);
    setUploadError(null);
    setClassification(null);
    try {
      const response = await uploadLogFiles(logGroupId, selectedFiles);
      setClassification(response.classification);
      setSelectedFiles([]);
      onUploadSuccess();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Upload failed. Please try again.";
      setUploadError(message);
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <section className={"flex flex-col gap-3"}>
      <h2 className={"font-semibold text-sm"}>Upload Logs</h2>

      {/* Drop zone */}
      <div
        className={[
          "flex cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-8 text-center transition-colors",
          isDragOver ? "border-primary bg-primary/5" : "hover:bg-muted/40",
        ].join(" ")}
        onClick={() => fileInputRef.current?.click()}
        onDragLeave={onDragLeave}
        onDragOver={onDragOver}
        onDrop={onDrop}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") fileInputRef.current?.click();
        }}
        role={"button"}
        tabIndex={0}
      >
        <div className={"flex size-10 shrink-0 items-center justify-center rounded-lg bg-muted"}>
          <UploadIcon className={"size-4 text-muted-foreground"} />
        </div>
        <div className={"flex flex-col gap-0.5"}>
          <p className={"font-medium text-sm"}>Drop files here or click to browse</p>
          <p className={"text-muted-foreground text-xs"}>Supports plaintext, JSON Lines, CSV, syslog, and more.</p>
        </div>
      </div>
      <input className={"hidden"} multiple={true} onChange={onFileInputChange} ref={fileInputRef} type={"file"} />

      {/* Staged file list */}
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

      {/* Upload action row */}
      {selectedFiles.length > 0 && (
        <div className={"flex items-center justify-between gap-3"}>
          <p className={"text-muted-foreground text-xs"}>
            {selectedFiles.length} {selectedFiles.length === 1 ? "file" : "files"} selected
          </p>
          <Button disabled={isUploading} onClick={onUpload} size={"sm"}>
            {isUploading ? <Spinner /> : <UploadIcon />}
            {isUploading ? "Uploading…" : "Upload & Process"}
          </Button>
        </div>
      )}

      {/* Error state */}
      {uploadError !== null && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Upload Failed</AlertTitle>
          <AlertDescription>{uploadError}</AlertDescription>
        </Alert>
      )}

      {/* Classification summary shown after a successful upload */}
      {classification !== null && <ClassificationCard classification={classification} />}
    </section>
  );
}

interface ClassificationCardProps {
  classification: ClassificationResult;
  className?: string;
}

function ClassificationCard({ classification, className }: ClassificationCardProps) {
  const confidencePercent = Math.round(classification.confidence * 100);

  return (
    <div className={cn("flex flex-col gap-3 rounded-lg border p-4", className)}>
      {/* Header */}
      <div className={"flex items-center gap-2"}>
        <CheckCircle2Icon className={"size-4 shrink-0 text-green-500"} />
        <span className={"flex-1 font-medium text-sm"}>Files queued for processing</span>
        <Badge className={"shrink-0 font-mono text-xs"} variant={"outline"}>
          {classification.schema_version}
        </Badge>
      </div>

      {/* Pipeline + format row */}
      <div className={"flex flex-wrap gap-2"}>
        <div className={"flex items-center gap-1.5 rounded-md bg-muted px-2 py-1"}>
          <CpuIcon className={"size-3 text-muted-foreground"} />
          <span className={"font-medium text-xs"}>{classification.selected_parser_key}</span>
        </div>
        <Badge className={"text-xs"} variant={"secondary"}>
          {classification.dominant_format}
        </Badge>
        <Badge className={"text-xs"} variant={"outline"}>
          {classification.structural_class}
        </Badge>
      </div>

      {/* Confidence bar */}
      <div className={"flex flex-col gap-1"}>
        <div className={"flex items-center justify-between"}>
          <span className={"text-muted-foreground text-xs"}>Confidence</span>
          <span className={"font-medium text-xs"}>{confidencePercent}%</span>
        </div>
        <Progress className={"h-1.5"} value={confidencePercent} />
      </div>

      {/* Per-file classification */}
      {classification.file_classifications.length > 0 && (
        <div className={"flex flex-col divide-y rounded-md border"}>
          {classification.file_classifications.map((fc) => (
            <div className={"flex items-center gap-3 px-3 py-2"} key={fc.filename}>
              <FileTextIcon className={"size-3.5 shrink-0 text-muted-foreground"} />
              <span className={"flex-1 truncate font-mono text-xs"}>{fc.filename}</span>
              <Badge className={"shrink-0 text-xs"} variant={"secondary"}>
                {fc.detected_format}
              </Badge>
              <span className={"shrink-0 text-muted-foreground text-xs"}>{fc.line_count} lines</span>
            </div>
          ))}
        </div>
      )}

      {/* Warnings */}
      {classification.warnings.length > 0 && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Warnings</AlertTitle>
          <AlertDescription className={"flex flex-col gap-1"}>
            {classification.warnings.map((w, i) => (
              <span key={i}>{w}</span>
            ))}
          </AlertDescription>
        </Alert>
      )}

      <p className={"text-muted-foreground text-xs"}>
        Check the <strong>Processes</strong> tab to track ingestion progress.
      </p>
    </div>
  );
}
