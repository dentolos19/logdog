import { type ReactNode, useEffect } from "react";
import { createPortal } from "react-dom";
import { type BreadcrumbItem, usePageHeaderContext } from "#/routes/(platform)/_components/-page-header-context";

type PageHeaderProps = {
  breadcrumbs?: BreadcrumbItem[];
  loading?: boolean;
  children?: ReactNode;
};

export function PageHeader({ breadcrumbs, loading = false, children }: PageHeaderProps) {
  const { setBreadcrumbs, setIsLoading, actionsContainer } = usePageHeaderContext();
  const breadcrumbsKey = JSON.stringify(breadcrumbs ?? []);

  useEffect(() => {
    setBreadcrumbs(breadcrumbs ?? []);
    return () => setBreadcrumbs([]);
  }, [breadcrumbsKey, breadcrumbs, setBreadcrumbs]);

  useEffect(() => {
    setIsLoading(loading);
    return () => setIsLoading(false);
  }, [loading, setIsLoading]);

  if (children !== undefined && actionsContainer !== null) {
    return createPortal(children, actionsContainer);
  }

  return null;
}
