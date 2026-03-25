"use client";

import { useRouter } from "next/navigation";
import { createContext, type ReactNode, useCallback, useContext, useEffect, useState } from "react";
import { $fetch } from "@/lib/api";
import type { User } from "@/lib/api/types";

interface AuthContextValue {
	user: User | null;
	isLoading: boolean;
	isAuthenticated: boolean;
	signIn: (email: string, password: string) => Promise<void>;
	signUp: (email: string, password: string) => Promise<void>;
	signOut: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
	const [user, setUser] = useState<User | null>(null);
	const [isLoading, setIsLoading] = useState(true);

	const fetchMe = useCallback(async (): Promise<User | null> => {
		const { data } = await $fetch("/auth/me");
		return data ?? null;
	}, []);

	// On mount, attempt to restore the session. If the access token is expired,
	// try the refresh endpoint before giving up.
	useEffect(() => {
		const restore = async () => {
			let me = await fetchMe();

			if (me === null) {
				const { error } = await $fetch("/auth/refresh");
				if (!error) {
					me = await fetchMe();
				}
			}

			setUser(me);
			setIsLoading(false);
		};

		restore();
	}, [fetchMe]);

	const signIn = useCallback(async (email: string, password: string): Promise<void> => {
		const { data, error } = await $fetch("/auth/login", {
			body: { email, password },
		});

		if (error) throw new Error(error.message ?? "Sign-in failed.");
		setUser(data!);
	}, []);

	const signUp = useCallback(async (email: string, password: string): Promise<void> => {
		const { data, error } = await $fetch("/auth/register", {
			body: { email, password },
		});

		if (error) throw new Error(error.message ?? "Sign-up failed.");
		setUser(data!);
	}, []);

	const signOut = useCallback(async (): Promise<void> => {
		await $fetch("/auth/logout");
		setUser(null);
	}, []);

	return (
		<AuthContext.Provider
			value={{
				user,
				isLoading,
				isAuthenticated: user !== null,
				signIn,
				signUp,
				signOut,
			}}
		>
			{children}
		</AuthContext.Provider>
	);
}

export const useAuth = () => {
	const ctx = useContext(AuthContext);
	if (ctx === null) {
		throw new Error("useAuth must be used inside <AuthProvider>.");
	}
	return ctx;
};

export const useRequireAuth = (redirectTo = "/login") => {
	const { user, isLoading } = useAuth();
	const router = useRouter();

	useEffect(() => {
		if (!isLoading && user === null) {
			router.push(redirectTo);
		}
	}, [isLoading, user, redirectTo, router]);

	return { user, isLoading };
};
