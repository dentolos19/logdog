import { PlayIcon, SparklesIcon } from "lucide-react";
import { type FormEvent, useCallback, useState } from "react";
import { toast } from "sonner";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Button } from "#/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "#/components/ui/card";
import { DataTable } from "#/components/ui/data-table";
import { Input } from "#/components/ui/input";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import { executeNlQuery, type NlQueryResult } from "#/lib/server";

type QueryTabProps = {
  entryId: string;
};

export function QueryTab({ entryId }: QueryTabProps) {
  const [question, setQuestion] = useState("");
  const [result, setResult] = useState<NlQueryResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = useCallback(
    async (event: FormEvent) => {
      event.preventDefault();
      const trimmed = question.trim();
      if (trimmed.length === 0) {
        return;
      }

      setIsLoading(true);
      setError(null);
      try {
        const data = await executeNlQuery(entryId, trimmed);
        setResult(data);
      } catch (err) {
        const message = err instanceof Error ? err.message : "Query failed.";
        setError(message);
        toast.error(message);
      } finally {
        setIsLoading(false);
      }
    },
    [entryId, question],
  );

  return (
    <div className={"flex flex-col gap-6"}>
      <form className={"flex gap-2"} onSubmit={handleSubmit}>
        <Input
          className={"flex-1"}
          disabled={isLoading}
          onChange={(event) => setQuestion(event.target.value)}
          placeholder={"e.g. Show me the top 10 errors by frequency"}
          value={question}
        />
        <Button disabled={isLoading || question.trim().length === 0} type={"submit"}>
          {isLoading ? <Spinner className={"size-3"} /> : <PlayIcon />}
          Run Query
        </Button>
      </form>

      {isLoading && (
        <div className={"flex flex-col gap-4"}>
          <Skeleton className={"h-24 w-full rounded-lg"} />
          <Skeleton className={"h-48 w-full rounded-lg"} />
        </div>
      )}

      {error !== null && (
        <Alert variant={"destructive"}>
          <AlertTitle>Query failed</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {result !== null && !isLoading && (
        <div className={"flex flex-col gap-4"}>
          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"flex items-center gap-2 text-sm"}>
                <SparklesIcon className={"size-4 text-primary"} />
                Generated SQL
              </CardTitle>
            </CardHeader>
            <CardContent>
              <pre className={"overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs"}>{result.sql}</pre>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className={"pb-2"}>
              <CardTitle className={"text-sm"}>Results ({result.results.length} rows)</CardTitle>
            </CardHeader>
            <CardContent>
              {result.results.length === 0 ? (
                <p className={"text-muted-foreground text-sm"}>No results returned.</p>
              ) : (
                <QueryResultsTable columns={result.columns} data={result.results} />
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

function QueryResultsTable({ columns, data }: { columns: string[]; data: Array<Record<string, unknown>> }) {
  const tableColumns = columns.map((col) => ({
    id: col,
    accessorKey: col,
    header: col,
    cell: ({ getValue }: { getValue: () => unknown }) => {
      const value = getValue();
      return <span className={"font-mono text-xs"}>{value === null ? "null" : String(value)}</span>;
    },
  }));

  return <DataTable columns={tableColumns} data={data} />;
}
