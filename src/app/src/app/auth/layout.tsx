"use client";

import { useAuth } from "@/components/auth-provider";
import { Spinner } from "@/components/ui/spinner";
import { useRouter } from "next/navigation";
import { useEffect, type ReactNode } from "react";

export default function PlatformLayout({ children }: { children: ReactNode }) {
  const { user, isLoading } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!isLoading && !!user) {
      router.push("/dashboard");
    }
  }, [isLoading, user, router]);

  if (isLoading) {
    return (
      <div className={"flex min-h-screen items-center justify-center"}>
        <Spinner className={"size-6"} />
      </div>
    );
  }

  if (user) return null;

  return <>{children}</>;
}
