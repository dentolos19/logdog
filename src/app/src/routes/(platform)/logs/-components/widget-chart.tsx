import { useMemo } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, XAxis, YAxis } from "recharts";
import { type ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent } from "#/components/ui/chart";

type WidgetChartProps = {
  chart_type: "bar" | "line" | "pie";
  data: Record<string, unknown>[];
  x_key: string;
  y_key: string;
  title?: string;
};

const CHART_COLORS = [
  "hsl(var(--chart-1))",
  "hsl(var(--chart-2))",
  "hsl(var(--chart-3))",
  "hsl(var(--chart-4))",
  "hsl(var(--chart-5))",
];

export function WidgetChart({ chart_type, data, x_key, y_key, title }: WidgetChartProps) {
  const chartConfig = useMemo<ChartConfig>(() => {
    const config: ChartConfig = {};
    config[y_key] = { label: y_key, color: CHART_COLORS[0] };
    return config;
  }, [y_key]);

  const sanitizedData = useMemo(
    () =>
      data.map((item) => ({
        ...item,
        [x_key]: item[x_key] ?? "",
        [y_key]: typeof item[y_key] === "number" ? item[y_key] : Number.parseFloat(String(item[y_key])) || 0,
      })),
    [data, x_key, y_key],
  );

  return (
    <div className={"flex flex-col gap-2"}>
      {title && <span className={"font-medium text-sm"}>{title}</span>}
      <ChartContainer className={"max-h-[300px] w-full"} config={chartConfig}>
        {chart_type === "bar" ? (
          <BarChart accessibilityLayer data={sanitizedData}>
            <CartesianGrid vertical={false} />
            <XAxis axisLine={false} dataKey={x_key} tick={{ fontSize: 11 }} tickLine={false} tickMargin={8} />
            <YAxis axisLine={false} tick={{ fontSize: 11 }} tickLine={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Bar dataKey={y_key} fill={`var(--color-${y_key})`} radius={[4, 4, 0, 0]} />
          </BarChart>
        ) : chart_type === "line" ? (
          <LineChart accessibilityLayer data={sanitizedData}>
            <CartesianGrid vertical={false} />
            <XAxis axisLine={false} dataKey={x_key} tick={{ fontSize: 11 }} tickLine={false} tickMargin={8} />
            <YAxis axisLine={false} tick={{ fontSize: 11 }} tickLine={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Line dataKey={y_key} dot={false} stroke={`var(--color-${y_key})`} strokeWidth={2} />
          </LineChart>
        ) : (
          <PieChart>
            <ChartTooltip content={<ChartTooltipContent />} />
            <Pie
              cx={"50%"}
              cy={"50%"}
              data={sanitizedData}
              dataKey={y_key}
              label={({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`}
              labelLine={true}
              nameKey={x_key}
              outerRadius={100}
            >
              {sanitizedData.map((_, index) => (
                <Cell fill={CHART_COLORS[index % CHART_COLORS.length]} key={`cell-${index}`} />
              ))}
            </Pie>
            <Legend />
          </PieChart>
        )}
      </ChartContainer>
    </div>
  );
}
