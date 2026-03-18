"use client";

import { AuthProvider } from "@/components/auth-provider";
import ThemeProvider from "@/components/theme-provider";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { ReactNode } from "react";

export default function AppProvider({ children }: { children: ReactNode }) {
  return (
    <ThemeProvider attribute={"class"} defaultTheme={"system"} disableTransitionOnChange enableSystem>
      <AuthProvider>
        <TooltipProvider>
          {children}
          <Toaster />
        </TooltipProvider>
      </AuthProvider>
    </ThemeProvider>
  );
}
