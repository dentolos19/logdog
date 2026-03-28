import { ClerkProvider } from "@clerk/tanstack-react-start";
import type { ReactNode } from "react";
import ThemeProvider from "#/components/theme-provider";
import { Toaster } from "#/components/ui/sonner";
import { TooltipProvider } from "#/components/ui/tooltip";

export default function AppProvider({ children }: { children: ReactNode }) {
  return (
    <ClerkProvider>
      <ThemeProvider>
        <TooltipProvider>
          {children}
          <Toaster />
        </TooltipProvider>
      </ThemeProvider>
    </ClerkProvider>
  );
}
