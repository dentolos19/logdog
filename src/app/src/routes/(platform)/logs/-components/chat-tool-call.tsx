import type { UIMessage } from "@tanstack/ai-react";
import { ChevronDownIcon, ChevronRightIcon, WrenchIcon } from "lucide-react";
import { useState } from "react";
import { Button } from "#/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "#/components/ui/collapsible";
import { WidgetChart } from "#/routes/(platform)/logs/-components/widget-chart";
import { WidgetDataTable } from "#/routes/(platform)/logs/-components/widget-data-table";
import { WidgetReportDownload } from "#/routes/(platform)/logs/-components/widget-report-download";
import { WidgetStats } from "#/routes/(platform)/logs/-components/widget-stats";

type ToolCallPart = UIMessage["parts"][number];

type ToolCallRendererProps = {
  part: ToolCallPart;
  isUser: boolean;
};

function parseJsonSafe(value: unknown): Record<string, unknown> | null {
  if (typeof value === "object" && value !== null) {
    return value as Record<string, unknown>;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value) as unknown;
      if (typeof parsed === "object" && parsed !== null) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      return null;
    }
  }
  return null;
}

function ToolWidgetOutput({ part }: { part: ToolCallPart }) {
  const output = "output" in part ? part.output : undefined;
  const parsed = parseJsonSafe(output);

  if (part.name === "execute_sql_query" && parsed) {
    const columns = Array.isArray(parsed.columns) ? (parsed.columns as string[]) : [];
    const rows = Array.isArray(parsed.rows) ? (parsed.rows as unknown[][]) : [];
    const message = typeof parsed.message === "string" ? parsed.message : "";
    const status = typeof parsed.status === "string" ? parsed.status : "error";

    return (
      <div className={"flex flex-col gap-2"}>
        {status === "error" && message && <p className={"text-destructive text-xs"}>{message}</p>}
        <WidgetDataTable columns={columns} rows={rows} />
      </div>
    );
  }

  if (part.name === "render_widget" && parsed) {
    const type = typeof parsed.type === "string" ? parsed.type : "";

    if (type === "data_table") {
      const columns = Array.isArray(parsed.columns) ? (parsed.columns as string[]) : [];
      const rows = Array.isArray(parsed.rows) ? (parsed.rows as unknown[][]) : [];
      const title = typeof parsed.title === "string" ? parsed.title : undefined;
      return <WidgetDataTable columns={columns} rows={rows} title={title} />;
    }

    if (type === "chart") {
      const chartType = typeof parsed.chart_type === "string" ? parsed.chart_type : "bar";
      const data = Array.isArray(parsed.data) ? (parsed.data as Record<string, unknown>[]) : [];
      const xKey = typeof parsed.x_key === "string" ? parsed.x_key : "";
      const yKey = typeof parsed.y_key === "string" ? parsed.y_key : "";
      const title = typeof parsed.title === "string" ? parsed.title : undefined;
      return (
        <WidgetChart
          chart_type={chartType as "bar" | "line" | "pie"}
          data={data}
          title={title}
          x_key={xKey}
          y_key={yKey}
        />
      );
    }

    if (type === "stats") {
      const stats = Array.isArray(parsed.stats)
        ? (parsed.stats as Array<{ label: string; value: string | number; description?: string }>)
        : [];
      return <WidgetStats stats={stats} />;
    }
  }

  if (part.name === "generate_report" && parsed) {
    const title = typeof parsed.title === "string" ? parsed.title : "Report";
    const downloadUrl = typeof parsed.download_url === "string" ? parsed.download_url : undefined;
    const message = typeof parsed.message === "string" ? parsed.message : "Report generated.";
    const status = typeof parsed.status === "string" ? parsed.status : "error";

    if (status === "ok") {
      return <WidgetReportDownload download_url={downloadUrl} message={message} title={title} />;
    }

    return <p className={"text-destructive text-xs"}>{message}</p>;
  }

  return null;
}

function formatToolLabel(name: string): string {
  return name
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

export function ToolCallRenderer({ part, isUser }: ToolCallRendererProps) {
  const [isOpen, setIsOpen] = useState(false);

  if (part.type !== "tool-call") {
    return null;
  }

  const toolName = typeof part.name === "string" ? part.name : "tool";
  const hasOutput = "output" in part && part.output !== undefined;
  const isWidgetTool =
    toolName === "execute_sql_query" || toolName === "render_widget" || toolName === "generate_report";

  if (isWidgetTool && hasOutput) {
    return (
      <div className={"my-2"}>
        <Collapsible onOpenChange={setIsOpen} open={isOpen}>
          <CollapsibleTrigger asChild>
            <Button className={"h-auto gap-2 px-3 py-1.5 text-muted-foreground text-xs"} variant={"ghost"}>
              {isOpen ? <ChevronDownIcon className={"size-3"} /> : <ChevronRightIcon className={"size-3"} />}
              <WrenchIcon className={"size-3"} />
              {formatToolLabel(toolName)}
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className={"mt-1 rounded-lg border bg-muted/30 p-3"}>
              <ToolWidgetOutput part={part} />
            </div>
          </CollapsibleContent>
        </Collapsible>
        {!isOpen && (
          <div className={"mt-1"}>
            <ToolWidgetOutput part={part} />
          </div>
        )}
      </div>
    );
  }

  const outputText = hasOutput
    ? (() => {
        try {
          return JSON.stringify(part.output, null, 2);
        } catch {
          return String(part.output);
        }
      })()
    : null;

  return (
    <div className={"my-2"}>
      <Collapsible onOpenChange={setIsOpen} open={isOpen}>
        <CollapsibleTrigger asChild>
          <Button className={"h-auto gap-2 px-3 py-1.5 text-muted-foreground text-xs"} variant={"ghost"}>
            {isOpen ? <ChevronDownIcon className={"size-3"} /> : <ChevronRightIcon className={"size-3"} />}
            <WrenchIcon className={"size-3"} />
            {formatToolLabel(toolName)}
            {!hasOutput && <span className={"text-muted-foreground/60"}>…</span>}
          </Button>
        </CollapsibleTrigger>
        <CollapsibleContent>
          {outputText && (
            <pre className={"mt-1 max-h-[300px] overflow-auto rounded-md bg-muted p-3 font-mono text-xs"}>
              {outputText}
            </pre>
          )}
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
}
