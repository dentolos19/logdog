import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import AppProvider from "@/components/app-provider";
import { cn } from "@/lib/utils";
import "./globals.css";

const fontSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const fontMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Logdog",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang={"en"} suppressHydrationWarning>
      <body className={cn(fontSans.variable, fontMono.variable, "antialiased")}>
        <AppProvider>{children}</AppProvider>
      </body>
    </html>
  );
}
