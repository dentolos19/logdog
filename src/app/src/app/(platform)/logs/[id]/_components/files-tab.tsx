"use client";

import { format } from "date-fns";
import { AlertCircleIcon, DownloadIcon, FileTextIcon, FolderOpenIcon } from "lucide-react";
import { useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Spinner } from "@/components/ui/spinner";
import { downloadLogFile } from "@/lib/api";
import type { LogGroupFile } from "@/lib/api/types";

interface FilesTabProps {
  logGroupId: string;
  files: LogGroupFile[];
  isLoading: boolean;
  error: string | null;
}

export function FilesTab({ logGroupId, files, isLoading, error }: FilesTabProps) {
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
        <AlertTitle>Failed to load files</AlertTitle>
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (files.length === 0) {
    return (
      <Empty className={"border"}>
        <EmptyHeader>
          <EmptyMedia variant={"icon"}>
            <FolderOpenIcon />
          </EmptyMedia>
          <EmptyTitle>No files yet</EmptyTitle>
          <EmptyDescription>Upload log files and they will appear here for download.</EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }

  return (
    <div className={"flex flex-col gap-2"}>
      {files.map((file) => (
        <FileRow file={file} key={file.id} logGroupId={logGroupId} />
      ))}
    </div>
  );
}

interface FileRowProps {
  logGroupId: string;
  file: LogGroupFile;
}

function FileRow({ logGroupId, file }: FileRowProps) {
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  const formattedDate = format(new Date(file.created_at), "MMM d, yyyy 'at' h:mm a");
  const formattedSize = formatFileSize(file.size);

  const onDownload = async () => {
    setIsDownloading(true);
    setDownloadError(null);
    try {
      const blob = await downloadLogFile(logGroupId, file.id);
      // Trigger a native browser download without opening a new tab.
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = file.name;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setDownloadError(err instanceof Error ? err.message : "Download failed. Please try again.");
    } finally {
      setIsDownloading(false);
    }
  };

  return (
    <div className={"flex flex-col gap-1.5 rounded-md border p-4"}>
      <div className={"flex items-center gap-3"}>
        <FileTextIcon className={"size-4 shrink-0 text-muted-foreground"} />
        <div className={"flex flex-1 flex-col gap-0.5 overflow-hidden"}>
          <span className={"truncate font-medium font-mono text-sm"}>{file.name}</span>
          <span className={"text-muted-foreground text-xs"}>{formattedDate}</span>
        </div>
        <div className={"ml-auto flex shrink-0 items-center gap-2"}>
          <Badge className={"text-xs"} variant={"secondary"}>
            {formattedSize}
          </Badge>
          <Badge className={"max-w-32 truncate text-xs"} variant={"outline"}>
            {file.mime_type}
          </Badge>
          <Button
            aria-label={`Download ${file.name}`}
            disabled={isDownloading}
            onClick={onDownload}
            size={"sm"}
            variant={"ghost"}
          >
            {isDownloading ? <Spinner /> : <DownloadIcon />}
            Download
          </Button>
        </div>
      </div>
      {downloadError !== null && <p className={"text-destructive text-xs"}>{downloadError}</p>}
    </div>
  );
}

const BYTES_PER_KB = 1024;
const BYTES_PER_MB = 1024 * 1024;

function formatFileSize(bytes: number): string {
  if (bytes >= BYTES_PER_MB) {
    return `${(bytes / BYTES_PER_MB).toFixed(1)} MB`;
  }
  if (bytes >= BYTES_PER_KB) {
    return `${(bytes / BYTES_PER_KB).toFixed(1)} KB`;
  }
  return `${bytes} B`;
}
