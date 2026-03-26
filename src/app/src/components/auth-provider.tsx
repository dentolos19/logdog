"use client";

import { useRouter } from "next/navigation";
import { createContext, type ReactNode, useCallback, useContext, useEffect, useState } from "react";
import { $fetch } from "@/lib/api";
import type { User } from "@/lib/api/types";
import { clearAuthSession, getRefreshToken, setAuthSession } from "@/lib/auth";

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
    const { data, error } = await $fetch("/auth/me");
    if (error || data === undefined) {
      return null;
    }

    return data;
  }, []);

  const refreshSession = useCallback(async (): Promise<User | null> => {
    const refreshToken = getRefreshToken();
    if (refreshToken === null) {
      return null;
    }

    const { data, error } = await $fetch("/auth/refresh", {
      body: { refresh_token: refreshToken },
    });

    if (error || data === undefined) {
      clearAuthSession();
      return null;
    }

    setAuthSession({
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
    });

    return data.user;
  }, []);

  useEffect(() => {
    const restore = async () => {
      const me = (await fetchMe()) ?? (await refreshSession());

      if (me === null) {
        clearAuthSession();
      }

      setUser(me);
      setIsLoading(false);
    };

    restore();
  }, [fetchMe, refreshSession]);

  const signIn = useCallback(async (email: string, password: string): Promise<void> => {
    const { data, error } = await $fetch("/auth/login", {
      body: { email, password },
    });

    if (error || data === undefined) {
      throw new Error(error?.message ?? "Sign-in failed.");
    }

    setAuthSession({
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
    });
    setUser(data.user);
  }, []);

  const signUp = useCallback(async (email: string, password: string): Promise<void> => {
    const { data, error } = await $fetch("/auth/register", {
      body: { email, password },
    });

    if (error || data === undefined) {
      throw new Error(error?.message ?? "Sign-up failed.");
    }

    setAuthSession({
      accessToken: data.access_token,
      refreshToken: data.refresh_token,
    });
    setUser(data.user);
  }, []);

  const signOut = useCallback(async (): Promise<void> => {
    await $fetch("/auth/logout").catch(() => undefined);
    clearAuthSession();
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
