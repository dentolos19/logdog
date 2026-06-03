import { type ReactNode, useEffect, useMemo } from "react";
import { createPortal } from "react-dom";

import { type BreadcrumbItem, usePageHeaderContext } from "#/routes/(platform)/-components/page-header-context";

type PageHeaderProps = {
  breadcrumbs?: BreadcrumbItem[];
  loading?: boolean;
  children?: ReactNode;
};

export function PageHeader({ breadcrumbs, loading = false, children }: PageHeaderProps) {
  const { setBreadcrumbs, setIsLoading, actionsContainer } = usePageHeaderContext();
  const breadcrumbItems = useMemo(() => breadcrumbs ?? [], [breadcrumbs]);

  useEffect(() => {
    setBreadcrumbs(breadcrumbItems);
    return () => setBreadcrumbs([]);
  }, [breadcrumbItems, setBreadcrumbs]);

  useEffect(() => {
    setIsLoading(loading);
    return () => setIsLoading(false);
  }, [loading, setIsLoading]);

  if (children !== undefined && actionsContainer !== null) {
    return createPortal(children, actionsContainer);
  }

  return null;
}
