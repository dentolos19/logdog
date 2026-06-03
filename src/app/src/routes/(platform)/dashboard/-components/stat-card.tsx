import type { LucideIcon } from "lucide-react";

import { Card, CardContent } from "#/components/ui/card";
import { Skeleton } from "#/components/ui/skeleton";
import { cn } from "#/lib/utils";

type StatCardProps = {
  title: string;
  value: string | number;
  description?: string;
  icon: LucideIcon;
  loading?: boolean;
  className?: string;
};

export function StatCard({ title, value, description, icon: Icon, loading = false, className }: StatCardProps) {
  return (
    <Card className={cn("min-h-[120px]", className)}>
      <CardContent className={"flex h-full flex-col justify-between gap-3"}>
        <div className={"flex items-start justify-between"}>
          <div className={"flex flex-col gap-1"}>
            <span className={"text-muted-foreground text-sm"}>{title}</span>
            {loading ? <Skeleton className={"h-8 w-20"} /> : <span className={"text-3xl font-semibold"}>{value}</span>}
          </div>
          <div className={"bg-muted rounded-lg p-2"}>
            <Icon className={"text-muted-foreground size-5"} />
          </div>
        </div>
        {description !== undefined && <span className={"text-muted-foreground text-xs"}>{description}</span>}
      </CardContent>
    </Card>
  );
}
