"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { ChevronRightIcon, PlusIcon, ScrollTextIcon } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { PageHeader } from "@/app/(platform)/_components/page-header";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { createLog, getLogs } from "@/lib/api";
import type { LogGroupListItem } from "@/lib/api/types";

const createLogGroupSchema = z.object({
  name: z.string().min(1, "Name is required.").max(255),
});

type CreateLogGroupValues = z.infer<typeof createLogGroupSchema>;

export default function LogsPage() {
  const router = useRouter();
  const [logGroups, setLogGroups] = useState<LogGroupListItem[] | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const form = useForm<CreateLogGroupValues>({
    resolver: zodResolver(createLogGroupSchema),
    defaultValues: { name: "" },
  });

  const fetchLogGroups = useCallback(async () => {
    setFetchError(null);
    try {
      const response = await getLogs();
      const groups = Array.isArray(response) ? response : (response as { data: LogGroupListItem[] }).data || [];
      setLogGroups(groups);
    } catch (err) {
      setFetchError(err instanceof Error ? err.message : "Failed to load log groups.");
    }
  }, []);

  useEffect(() => {
    fetchLogGroups();
  }, [fetchLogGroups]);

  const onSubmit = async (values: CreateLogGroupValues) => {
    try {
      const newLogGroup = await createLog(values.name.trim());
      form.reset();
      setIsDialogOpen(false);
      router.push(`/logs/${newLogGroup.id}`);
    } catch (err) {
      form.setError("name", {
        message: err instanceof Error ? err.message : "Failed to create log group.",
      });
    }
  };

  const handleDialogOpenChange = (open: boolean) => {
    if (!open) form.reset();
    setIsDialogOpen(open);
  };

  return (
    <div className={"flex h-full flex-col"}>
      <Dialog open={isDialogOpen} onOpenChange={handleDialogOpenChange}>
        <PageHeader breadcrumbs={[{ label: "Logs" }]}>
          <DialogTrigger asChild={true}>
            <Button size={"sm"}>
              <PlusIcon />
              New Log Group
            </Button>
          </DialogTrigger>
        </PageHeader>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>New Log Group</DialogTitle>
            <DialogDescription>Create a new log group to organize and analyze your log files.</DialogDescription>
          </DialogHeader>
          <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className={"flex flex-col gap-4"}>
              <FormField
                control={form.control}
                name={"name"}
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Name</FormLabel>
                    <FormControl>
                      <Input placeholder={"e.g. Production API Logs"} autoComplete={"off"} {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <div className={"flex justify-end gap-2"}>
                <Button type={"button"} variant={"outline"} onClick={() => handleDialogOpenChange(false)}>
                  Cancel
                </Button>
                <Button type={"submit"} disabled={form.formState.isSubmitting}>
                  {form.formState.isSubmitting ? <Spinner /> : "Create"}
                </Button>
              </div>
            </form>
          </Form>
        </DialogContent>
      </Dialog>

      <main className={"flex flex-1 flex-col overflow-auto p-6"}>
        {logGroups === null && fetchError === null && (
          <div className={"flex flex-col gap-2"}>
            {[1, 2, 3].map((index) => (
              <div key={index} className={"flex items-center gap-4 rounded-md border p-4"}>
                <div className={"flex flex-col gap-1.5"}>
                  <Skeleton className={"h-4 w-40"} />
                  <Skeleton className={"h-3 w-24"} />
                </div>
                <Skeleton className={"ml-auto size-4 rounded-full"} />
              </div>
            ))}
          </div>
        )}

        {fetchError !== null && (
          <div className={"flex flex-col items-center gap-4 py-12 text-center"}>
            <p className={"text-sm text-destructive"}>{fetchError}</p>
            <Button variant={"outline"} size={"sm"} onClick={fetchLogGroups}>
              Try again
            </Button>
          </div>
        )}

        {logGroups !== null && logGroups.length === 0 && (
          <Empty className={"border"}>
            <EmptyHeader>
              <EmptyMedia variant={"icon"}>
                <ScrollTextIcon />
              </EmptyMedia>
              <EmptyTitle>No log groups yet</EmptyTitle>
              <EmptyDescription>Create a log group to start organizing and analyzing your log files.</EmptyDescription>
            </EmptyHeader>
            <Button onClick={() => setIsDialogOpen(true)}>
              <PlusIcon />
              New Log Group
            </Button>
          </Empty>
        )}

        {logGroups !== null && logGroups.length > 0 && (
          <div className={"flex flex-col gap-2"}>
            {logGroups.map((logGroup) => (
              <Link
                key={logGroup.id}
                href={`/logs/${logGroup.id}`}
                className={"group flex items-center gap-4 rounded-md border p-4 transition-colors hover:bg-accent/50"}
              >
                <div className={"flex flex-col gap-0.5"}>
                  <span className={"text-sm font-medium"}>{logGroup.name}</span>
                  <span className={"text-xs text-muted-foreground"}>
                    {new Date(logGroup.created_at).toLocaleDateString("en-US", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    })}
                  </span>
                </div>
                <ChevronRightIcon
                  className={"ml-auto size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5"}
                />
              </Link>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
