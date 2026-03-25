"use client";

import { type ReactNode, useEffect } from "react";
import { createPortal } from "react-dom";

import { type BreadcrumbItem, usePageHeaderContext } from "./page-header-context";

interface PageHeaderProps {
	/** Breadcrumb trail. Last item is the current page (no link needed). */
	breadcrumbs?: BreadcrumbItem[];
	/** Show a loading skeleton instead of breadcrumbs. */
	loading?: boolean;
	/** Action buttons/components to render in the header's trailing slot. */
	children?: ReactNode;
}

/**
 * Renders into the platform layout's shared header.
 * Place inside a page or workbench component to configure the header for that route.
 *
 * @example
 * <PageHeader breadcrumbs={[{ label: "Logs", href: "/logs" }, { label: group.name }]}>
 *   <Button>Action</Button>
 * </PageHeader>
 */
export function PageHeader({ breadcrumbs, loading = false, children }: PageHeaderProps) {
	const { setBreadcrumbs, setIsLoading, actionsContainer } = usePageHeaderContext();

	// eslint-disable-next-line react-hooks/exhaustive-deps
	const breadcrumbsKey = JSON.stringify(breadcrumbs ?? []);

	useEffect(() => {
		setBreadcrumbs(breadcrumbs ?? []);
		return () => setBreadcrumbs([]);
		// breadcrumbsKey is the JSON-serialized breadcrumbs array, stable for value comparisons
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [breadcrumbsKey, setBreadcrumbs]);

	useEffect(() => {
		setIsLoading(loading);
		return () => setIsLoading(false);
	}, [loading, setIsLoading]);

	if (children !== undefined && actionsContainer !== null) {
		return createPortal(children, actionsContainer);
	}

	return null;
}
