import { createFileRoute, Link, Outlet, useNavigate } from "@tanstack/react-router";
import { Fragment, useEffect, useMemo } from "react";
import { useAuth } from "#/components/auth-provider";
import Loading from "#/components/loading";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "#/components/ui/breadcrumb";
import { ScrollArea } from "#/components/ui/scroll-area";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "#/components/ui/sidebar";
import { Skeleton } from "#/components/ui/skeleton";
import { AppSidebar } from "#/routes/(platform)/-components/app-sidebar";
import { PageHeaderProvider, usePageHeaderContext } from "#/routes/(platform)/-components/page-header-context";

export const Route = createFileRoute("/(platform)")({
  component: PlatformRoute,
});

function PlatformRoute() {
  const navigate = useNavigate();
  const { user, isLoading } = useAuth();

  useEffect(() => {
    if (!isLoading && user === null) {
      void navigate({ to: "/auth" });
    }
  }, [isLoading, navigate, user]);

  if (isLoading) {
    return (
      <div className={"flex min-h-screen items-center justify-center"}>
        <Loading />
      </div>
    );
  }

  if (user === null) {
    return (
      <div className={"flex min-h-screen items-center justify-center"}>
        <Loading />
      </div>
    );
  }

  return (
    <PageHeaderProvider>
      <PlatformInner />
    </PageHeaderProvider>
  );
}

function PlatformInner() {
  const { breadcrumbs, isLoading, setActionsContainer } = usePageHeaderContext();
  const breadcrumbNodes = useMemo(() => {
    return breadcrumbs.map((item, index) => {
      const isLast = index === breadcrumbs.length - 1;
      return (
        <Fragment key={`${item.label}-${index}`}>
          {index > 0 && <BreadcrumbSeparator />}
          <BreadcrumbItem>
            {isLast || !item.href ? (
              <BreadcrumbPage>{item.label}</BreadcrumbPage>
            ) : (
              <BreadcrumbLink asChild>
                <Link to={item.href}>{item.label}</Link>
              </BreadcrumbLink>
            )}
          </BreadcrumbItem>
        </Fragment>
      );
    });
  }, [breadcrumbs]);

  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className={"h-dvh overflow-hidden"}>
        <header className={"sticky top-0 z-20 flex h-12 shrink-0 items-center gap-2 border-b bg-sidebar px-4"}>
          <SidebarTrigger className={"-ml-1"} />
          {isLoading ? (
            <Skeleton className={"h-4 w-48"} />
          ) : breadcrumbs.length > 0 ? (
            <Breadcrumb>
              <BreadcrumbList>{breadcrumbNodes}</BreadcrumbList>
            </Breadcrumb>
          ) : null}
          <div className={"ml-auto flex items-center gap-2"} ref={setActionsContainer} />
        </header>

        <ScrollArea className={"h-[calc(100dvh-3rem)] w-full min-w-0 overflow-x-hidden"}>
          <Outlet />
        </ScrollArea>
      </SidebarInset>
    </SidebarProvider>
  );
}
