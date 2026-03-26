"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { MoreHorizontalIcon, PencilIcon, Trash2Icon } from "lucide-react";
import { useRouter } from "next/navigation";
import { use, useCallback, useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { PageHeader } from "@/app/(platform)/_components/page-header";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { deleteLog, getLog, getLogFiles, getLogProcesses, updateLog } from "@/lib/api";
import type { LogGroup, LogGroupFile, LogProcess } from "@/lib/api/types";
import { ChatbotTab } from "./_components/chatbot-tab";
import { FilesTab } from "./_components/files-tab";
import { ProcessesTab } from "./_components/processes-tab";
import { TablesTab } from "./_components/tables-tab";
import { UploadSection } from "./_components/upload-section";

const renameLogGroupSchema = z.object({
  name: z.string().min(1, "Name is required.").max(255),
});

type RenameLogGroupValues = z.infer<typeof renameLogGroupSchema>;

export default function LogGroupPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const router = useRouter();

  const [logGroup, setLogGroup] = useState<LogGroup | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  const [processes, setProcesses] = useState<LogProcess[]>([]);
  const [processesLoading, setProcessesLoading] = useState(false);
  const [processesError, setProcessesError] = useState<string | null>(null);

  const [files, setFiles] = useState<LogGroupFile[]>([]);
  const [filesLoading, setFilesLoading] = useState(false);
  const [filesError, setFilesError] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState("data");

  const [isRenameDialogOpen, setIsRenameDialogOpen] = useState(false);
  const [isDeleteAlertOpen, setIsDeleteAlertOpen] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);

  const renameForm = useForm<RenameLogGroupValues>({
    resolver: zodResolver(renameLogGroupSchema),
    defaultValues: { name: "" },
  });

  const fetchLogGroup = useCallback(async () => {
    setFetchError(null);
    try {
      const data = await getLog(id);
      setLogGroup(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to load log group.";
      const isNotFound = message.includes("404") || message.toLowerCase().includes("not found");
      if (isNotFound) {
        router.push("/logs");
        return;
      }
      setFetchError(message);
    }
  }, [id, router]);

  const fetchProcesses = useCallback(async () => {
    setProcessesLoading(true);
    setProcessesError(null);
    try {
      const data = await getLogProcesses(id);
      setProcesses(data);
    } catch (err) {
      setProcessesError(err instanceof Error ? err.message : "Failed to load processes.");
    } finally {
      setProcessesLoading(false);
    }
  }, [id]);

  const fetchFiles = useCallback(async () => {
    setFilesLoading(true);
    setFilesError(null);
    try {
      const data = await getLogFiles(id);
      setFiles(data);
    } catch (err) {
      setFilesError(err instanceof Error ? err.message : "Failed to load files.");
    } finally {
      setFilesLoading(false);
    }
  }, [id]);

  // Refresh all tab data after a successful upload.
  const onUploadSuccess = useCallback(async () => {
    await Promise.all([fetchLogGroup(), fetchProcesses(), fetchFiles()]);
  }, [fetchLogGroup, fetchProcesses, fetchFiles]);

  useEffect(() => {
    fetchLogGroup();
    fetchProcesses();
    fetchFiles();
  }, [fetchLogGroup, fetchProcesses, fetchFiles]);

  useEffect(() => {
    if (activeTab !== "processes") return;

    const isTerminal = (p: LogProcess) => p.status === "completed" || p.status === "failed";

    const tick = () => {
      if (processes.length > 0 && processes.every(isTerminal)) return;
      fetchProcesses();
    };

    const interval = setInterval(tick, 3000);
    return () => clearInterval(interval);
  }, [activeTab, processes, fetchProcesses]);

  const openRenameDialog = () => {
    renameForm.reset({ name: logGroup?.name ?? "" });
    setIsRenameDialogOpen(true);
  };

  const onRename = async (values: RenameLogGroupValues) => {
    try {
      const updated = await updateLog(id, values.name.trim());
      setLogGroup(updated);
      setIsRenameDialogOpen(false);
    } catch (err) {
      renameForm.setError("name", {
        message: err instanceof Error ? err.message : "Failed to rename log group.",
      });
    }
  };

  const onDelete = async () => {
    setIsDeleting(true);
    try {
      await deleteLog(id);
      router.push("/logs");
    } catch (err) {
      setIsDeleting(false);
      setFetchError(err instanceof Error ? err.message : "Failed to delete log group.");
      setIsDeleteAlertOpen(false);
    }
  };

  return (
    <div className={"flex h-full flex-col"}>
      <PageHeader
        breadcrumbs={
          logGroup
            ? [{ label: "Logs", href: "/logs" }, { label: logGroup.name }]
            : fetchError
              ? [{ label: "Logs", href: "/logs" }]
              : undefined
        }
        loading={logGroup === null && fetchError === null}
      >
        {logGroup !== null && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild={true}>
              <Button aria-label={"Options"} size={"icon"} variant={"ghost"}>
                <MoreHorizontalIcon />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align={"end"}>
              <DropdownMenuItem onSelect={openRenameDialog}>
                <PencilIcon />
                Rename
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={() => setIsDeleteAlertOpen(true)} variant={"destructive"}>
                <Trash2Icon />
                Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </PageHeader>

      <main className={"flex flex-1 flex-col gap-6 overflow-auto p-6"}>
        {logGroup === null && fetchError === null && (
          <div className={"flex flex-col gap-6"}>
            <Skeleton className={"h-28 w-full rounded-lg"} />
            <Skeleton className={"h-48 w-full rounded-lg"} />
          </div>
        )}

        {fetchError !== null && (
          <div className={"flex flex-col items-center gap-4 py-12 text-center"}>
            <p className={"text-destructive text-sm"}>{fetchError}</p>
            <Button onClick={fetchLogGroup} size={"sm"} variant={"outline"}>
              Try again
            </Button>
          </div>
        )}

        {logGroup !== null && (
          <Tabs className={"flex flex-col gap-4"} onValueChange={setActiveTab} value={activeTab}>
            <TabsList className={"w-fit"}>
              <TabsTrigger value={"data"}>Data</TabsTrigger>
              <TabsTrigger value={"chatbot"}>Chatbot</TabsTrigger>
              <TabsTrigger value={"processes"}>Processes</TabsTrigger>
              <TabsTrigger value={"files"}>Files</TabsTrigger>
            </TabsList>

            {/* Tab 1: Upload + Tables */}
            <TabsContent className={"flex flex-col gap-6"} value={"data"}>
              <UploadSection logGroupId={id} onUploadSuccess={onUploadSuccess} />

              <section className={"flex flex-col gap-3"}>
                <div className={"flex items-center gap-2"}>
                  <h2 className={"font-semibold text-sm"}>Tables</h2>
                </div>
                <TablesTab files={files} logGroupId={id} tables={logGroup.tables} />
              </section>
            </TabsContent>

            {/* Tab 2: Processes */}
            <TabsContent className={"flex flex-col gap-3"} value={"processes"}>
              <div className={"flex items-center gap-2"}>
                <h2 className={"font-semibold text-sm"}>Processes</h2>
              </div>
              <ProcessesTab error={processesError} isLoading={processesLoading} processes={processes} />
            </TabsContent>

            {/* Tab 3: Chatbot */}
            <TabsContent className={"flex flex-col gap-3"} value={"chatbot"}>
              <div className={"flex items-center gap-2"}>
                <h2 className={"font-semibold text-sm"}>Chatbot</h2>
              </div>
              {activeTab === "chatbot" ? (
                <ChatbotTab logGroupId={id} tableNames={logGroup.tables.map((table) => table.name)} />
              ) : null}
            </TabsContent>

            {/* Tab 4: Files */}
            <TabsContent className={"flex flex-col gap-3"} value={"files"}>
              <div className={"flex items-center gap-2"}>
                <h2 className={"font-semibold text-sm"}>Files</h2>
              </div>
              <FilesTab error={filesError} files={files} isLoading={filesLoading} logGroupId={id} />
            </TabsContent>
          </Tabs>
        )}
      </main>

      {/* Rename Dialog */}
      <Dialog onOpenChange={setIsRenameDialogOpen} open={isRenameDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rename Log Group</DialogTitle>
            <DialogDescription>Enter a new name for this log group.</DialogDescription>
          </DialogHeader>
          <Form {...renameForm}>
            <form className={"flex flex-col gap-4"} onSubmit={renameForm.handleSubmit(onRename)}>
              <FormField
                control={renameForm.control}
                name={"name"}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Name</FormLabel>
                    <FormControl>
                      <Input autoComplete={"off"} placeholder={"e.g. Production API Logs"} {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <div className={"flex justify-end gap-2"}>
                <Button onClick={() => setIsRenameDialogOpen(false)} type={"button"} variant={"outline"}>
                  Cancel
                </Button>
                <Button disabled={renameForm.formState.isSubmitting} type={"submit"}>
                  {renameForm.formState.isSubmitting ? <Spinner /> : "Rename"}
                </Button>
              </div>
            </form>
          </Form>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Alert */}
      <AlertDialog onOpenChange={setIsDeleteAlertOpen} open={isDeleteAlertOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete log group?</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete <strong className={"text-foreground"}>{logGroup?.name}</strong> and all of
              its associated data. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isDeleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction disabled={isDeleting} onClick={onDelete} variant={"destructive"}>
              {isDeleting ? <Spinner /> : "Delete"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
