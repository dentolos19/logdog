"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Fragment, type ReactNode, useEffect } from "react";

import { AppSidebar } from "@/app/(platform)/_components/app-sidebar";
import { PageHeaderProvider, usePageHeaderContext } from "@/app/(platform)/_components/page-header-context";
import { useAuth } from "@/components/auth-provider";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { ScrollArea, ScrollBar } from "@/components/ui/scroll-area";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";

function PlatformInner({ children }: { children: ReactNode }) {
  const { breadcrumbs, isLoading, setActionsContainer } = usePageHeaderContext();

  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className={"h-svh overflow-hidden"}>
        <header className={"sticky top-0 z-20 flex h-14 shrink-0 items-center gap-2 border-b bg-sidebar px-4"}>
          <SidebarTrigger className={"-ml-1"} />
          {isLoading ? (
            <Skeleton className={"h-4 w-48"} />
          ) : breadcrumbs.length > 0 ? (
            <Breadcrumb>
              <BreadcrumbList>
                {breadcrumbs.map((item, index) => {
                  const isLast = index === breadcrumbs.length - 1;
                  return (
                    <Fragment key={`${item.label}-${index}`}>
                      {index > 0 && <BreadcrumbSeparator />}
                      <BreadcrumbItem>
                        {isLast || !item.href ? (
                          <BreadcrumbPage>{item.label}</BreadcrumbPage>
                        ) : (
                          <BreadcrumbLink asChild={true}>
                            <Link href={item.href}>{item.label}</Link>
                          </BreadcrumbLink>
                        )}
                      </BreadcrumbItem>
                    </Fragment>
                  );
                })}
              </BreadcrumbList>
            </Breadcrumb>
          ) : null}
          <div className={"ml-auto flex items-center gap-2"} ref={setActionsContainer} />
        </header>
        <ScrollArea className={"min-h-0 flex-1"}>
          <div className={"min-h-full"}>{children}</div>
          <ScrollBar orientation="horizontal" />
        </ScrollArea>
      </SidebarInset>
    </SidebarProvider>
  );
}

export default function PlatformLayout({ children }: { children: ReactNode }) {
  const { user, isLoading } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!isLoading && !user) {
      router.push("/auth");
    }
  }, [isLoading, user, router]);

  if (isLoading) {
    return (
      <div className={"flex min-h-screen items-center justify-center"}>
        <Spinner className={"size-6"} />
      </div>
    );
  }

  if (!user) return null;

  return (
    <PageHeaderProvider>
      <PlatformInner>{children}</PlatformInner>
    </PageHeaderProvider>
  );
}
