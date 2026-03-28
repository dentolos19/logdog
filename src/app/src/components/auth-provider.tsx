import { createContext, type ReactNode, useCallback, useContext, useEffect, useState } from "react";
import { clearAuthTokens, getMe, login, logout, refreshTokens, register } from "#/lib/server";

type AuthUser = {
  id: string;
  email: string;
  created_at: string;
};

type AuthContextValue = {
  user: AuthUser | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  signIn: (email: string, password: string) => Promise<void>;
  signUp: (email: string, password: string) => Promise<void>;
  signOut: () => Promise<void>;
  refreshSession: () => Promise<AuthUser | null>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const fetchCurrentUser = useCallback(async () => {
    try {
      const me = await getMe();
      setUser(me);
      return me;
    } catch {
      return null;
    }
  }, []);

  const refreshSession = useCallback(async () => {
    try {
      await refreshTokens();
    } catch {
      clearAuthTokens();
      setUser(null);
      return null;
    }

    return fetchCurrentUser();
  }, [fetchCurrentUser]);

  useEffect(() => {
    const restore = async () => {
      const me = (await fetchCurrentUser()) ?? (await refreshSession());
      if (me === null) {
        clearAuthTokens();
        setUser(null);
      }
      setIsLoading(false);
    };

    restore();
  }, [fetchCurrentUser, refreshSession]);

  const signIn = useCallback(async (email: string, password: string) => {
    await login({ email, password });
    const me = await getMe();
    setUser(me);
  }, []);

  const signUp = useCallback(async (email: string, password: string) => {
    await register({ email, password });
    await login({ email, password });
    const me = await getMe();
    setUser(me);
  }, []);

  const signOut = useCallback(async () => {
    try {
      await logout();
    } catch {
      clearAuthTokens();
    }
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
        refreshSession,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === null) {
    throw new Error("useAuth must be used inside <AuthProvider>.");
  }
  return context;
}
