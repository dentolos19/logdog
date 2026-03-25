"use client";

import { createContext, type ReactNode, useCallback, useContext, useState } from "react";

export type BreadcrumbItem = { label: string; href?: string };

interface PageHeaderContextValue {
	breadcrumbs: BreadcrumbItem[];
	isLoading: boolean;
	actionsContainer: HTMLDivElement | null;
	setBreadcrumbs: (items: BreadcrumbItem[]) => void;
	setIsLoading: (loading: boolean) => void;
	setActionsContainer: (el: HTMLDivElement | null) => void;
}

const PageHeaderContext = createContext<PageHeaderContextValue>({
	breadcrumbs: [],
	isLoading: false,
	actionsContainer: null,
	setBreadcrumbs: () => {},
	setIsLoading: () => {},
	setActionsContainer: () => {},
});

export function PageHeaderProvider({ children }: { children: ReactNode }) {
	const [breadcrumbs, setBreadcrumbsState] = useState<BreadcrumbItem[]>([]);
	const [isLoading, setIsLoadingState] = useState(false);
	const [actionsContainer, setActionsContainer] = useState<HTMLDivElement | null>(null);

	const setBreadcrumbs = useCallback((items: BreadcrumbItem[]) => {
		setBreadcrumbsState((prev) => {
			return JSON.stringify(prev) === JSON.stringify(items) ? prev : items;
		});
	}, []);

	const setIsLoading = useCallback((loading: boolean) => {
		setIsLoadingState(loading);
	}, []);

	return (
		<PageHeaderContext.Provider
			value={{ breadcrumbs, isLoading, actionsContainer, setBreadcrumbs, setIsLoading, setActionsContainer }}
		>
			{children}
		</PageHeaderContext.Provider>
	);
}

export function usePageHeaderContext() {
	return useContext(PageHeaderContext);
}
