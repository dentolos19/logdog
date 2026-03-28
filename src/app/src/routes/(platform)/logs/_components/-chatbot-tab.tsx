import { AlertCircleIcon, BotIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Card, CardContent } from "#/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupTextarea } from "#/components/ui/input-group";

type ChatbotTabProps = {
  entryId: string;
  tableNames: string[];
};

export function ChatbotTab({ entryId, tableNames }: ChatbotTabProps) {
  const promptSuggestions =
    tableNames.length > 0
      ? [
          `Summarize the data available in ${tableNames.slice(0, 2).join(", ")}.`,
          "Find unusual spikes or anomalies.",
          "What are the top failure patterns in these logs?",
        ]
      : ["Summarize what data is currently available.", "What should I upload next for better analysis?"];

  return (
    <Card className={"relative border-0 bg-transparent shadow-none"}>
      <CardContent className={"flex flex-col gap-4 px-0 pb-0"}>
        <Alert>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Chat Endpoint Not Enabled</AlertTitle>
          <AlertDescription>
            The backend chat endpoints are not available yet for this migration. This tab preserves the old app design
            while we complete API parity.
          </AlertDescription>
        </Alert>

        <div className={"pb-4"}>
          <Empty className={"border"}>
            <EmptyHeader>
              <EmptyMedia variant={"icon"}>
                <BotIcon />
              </EmptyMedia>
              <EmptyTitle>Assistant Workspace</EmptyTitle>
              <EmptyDescription>
                Once chat routes are implemented, this panel will support contextual Q&A over log tables.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        </div>

        <div
          className={
            "sticky bottom-0 z-10 -mx-4 bg-background/95 px-4 pt-2 pb-4 backdrop-blur supports-backdrop-filter:bg-background/60 sm:-mx-6 sm:px-6"
          }
        >
          <div className={"mb-4 flex flex-wrap gap-2"}>
            {promptSuggestions.map((suggestion) => (
              <Button
                className={"rounded-full"}
                disabled
                key={suggestion}
                size={"sm"}
                type={"button"}
                variant={"secondary"}
              >
                {suggestion}
              </Button>
            ))}
          </div>

          <div className={"mb-3 flex items-center gap-2"}>
            <Badge variant={"outline"}>log_entry_id: {entryId}</Badge>
            <Badge variant={"secondary"}>{tableNames.length} table hints</Badge>
          </div>

          <InputGroup className={"bg-background shadow-sm"}>
            <InputGroupTextarea
              className={"min-h-[3rem] resize-none py-3"}
              disabled
              placeholder={"Ask about anomalies, trends, or table insights..."}
              rows={1}
            />
            <InputGroupAddon align={"inline-end"}>
              <InputGroupButton
                className={"mr-1 size-8 rounded-full"}
                disabled
                size={"icon-sm"}
                type={"button"}
                variant={"default"}
              >
                Send
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>
        </div>
      </CardContent>
    </Card>
  );
}
