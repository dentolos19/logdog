"use client";

import ThemeProvider from "@/components/theme-provider";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import type { ReactNode } from "react";

export default function AppProvider({ children }: { children: ReactNode }) {
  return (
    <ThemeProvider attribute={"class"} defaultTheme={"system"} disableTransitionOnChange enableSystem>
      <TooltipProvider>
        {children}
        <Toaster />
      </TooltipProvider>
    </ThemeProvider>
  );
}
