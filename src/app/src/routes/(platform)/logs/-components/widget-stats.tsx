import { Card, CardContent, CardHeader, CardTitle } from "#/components/ui/card";

type StatItem = {
  label: string;
  value: string | number;
  description?: string;
};

type WidgetStatsProps = {
  stats: StatItem[];
};

export function WidgetStats({ stats }: WidgetStatsProps) {
  return (
    <div className={"grid grid-cols-2 gap-3 sm:grid-cols-3"}>
      {stats.map((stat) => (
        <Card key={stat.label}>
          <CardHeader className={"pb-2"}>
            <CardTitle className={"text-muted-foreground text-xs font-medium"}>{stat.label}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className={"text-2xl font-bold"}>{stat.value}</div>
            {stat.description && <p className={"text-muted-foreground mt-1 text-xs"}>{stat.description}</p>}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
