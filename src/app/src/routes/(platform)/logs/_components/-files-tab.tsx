import { format } from "date-fns";
import { AlertCircleIcon, DownloadIcon, FileTextIcon, FolderOpenIcon, Trash2Icon } from "lucide-react";
import { useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { Spinner } from "#/components/ui/spinner";
import { deleteLogFile, downloadLogFile, type LogFile } from "#/lib/server";

type FilesTabProps = {
  entryId: string;
  files: LogFile[];
  isLoading: boolean;
  error: string | null;
  onFilesChanged: () => void;
};

export function FilesTab({ entryId, files, isLoading, error, onFilesChanged }: FilesTabProps) {
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
        <FileRow entryId={entryId} file={file} key={file.id} onFilesChanged={onFilesChanged} />
      ))}
    </div>
  );
}

function FileRow({ entryId, file, onFilesChanged }: { entryId: string; file: LogFile; onFilesChanged: () => void }) {
  const [isDownloading, setIsDownloading] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const formattedDate = format(new Date(file.created_at), "MMM d, yyyy 'at' h:mm a");
  const formattedSize = formatFileSize(file.size);

  const onDownload = async () => {
    setIsDownloading(true);
    setActionError(null);
    try {
      const blob = await downloadLogFile(entryId, file.id);
      const blobUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = blobUrl;
      anchor.download = file.name;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(blobUrl);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Download failed. Please try again.");
    } finally {
      setIsDownloading(false);
    }
  };

  const onDelete = async () => {
    setIsDeleting(true);
    setActionError(null);
    try {
      await deleteLogFile(entryId, file.id);
      onFilesChanged();
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Delete failed. Please try again.");
    } finally {
      setIsDeleting(false);
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
            {file.content_type}
          </Badge>

          <Button
            aria-label={`Download ${file.name}`}
            disabled={isDownloading || isDeleting}
            onClick={() => void onDownload()}
            size={"sm"}
            variant={"ghost"}
          >
            {isDownloading ? <Spinner /> : <DownloadIcon />}
            Download
          </Button>

          <Button
            aria-label={`Delete ${file.name}`}
            disabled={isDownloading || isDeleting}
            onClick={() => void onDelete()}
            size={"sm"}
            variant={"ghost"}
          >
            {isDeleting ? <Spinner /> : <Trash2Icon />}
            Delete
          </Button>
        </div>
      </div>

      {actionError !== null && <p className={"text-destructive text-xs"}>{actionError}</p>}
    </div>
  );
}

const bytesPerKilobyte = 1024;
const bytesPerMegabyte = 1024 * 1024;

function formatFileSize(bytes: number) {
  if (bytes >= bytesPerMegabyte) {
    return `${(bytes / bytesPerMegabyte).toFixed(1)} MB`;
  }
  if (bytes >= bytesPerKilobyte) {
    return `${(bytes / bytesPerKilobyte).toFixed(1)} KB`;
  }
  return `${bytes} B`;
}
