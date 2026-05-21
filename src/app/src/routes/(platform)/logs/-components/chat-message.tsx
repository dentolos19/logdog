import { mermaid } from "@streamdown/mermaid";
import type { UIMessage } from "@tanstack/ai-react";
import { BotIcon, CheckIcon, CopyIcon, UserIcon, WrenchIcon } from "lucide-react";
import { useCallback, useState } from "react";
import { Streamdown } from "streamdown";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";

type ChatMessageItemProps = {
  message: UIMessage;
  entryId?: string;
  groupName?: string;
  tableNameMap?: Record<string, string>;
};

function redactIds(text: string, entryId?: string, groupName?: string, tableNameMap?: Record<string, string>): string {
  let result = text;
  if (entryId && groupName && entryId !== groupName && entryId.length > 0) {
    result = result.replaceAll(entryId, groupName);
  }
  if (tableNameMap) {
    for (const [rawName, displayName] of Object.entries(tableNameMap)) {
      if (rawName !== displayName) {
        result = result.replaceAll(rawName, displayName);
      }
    }
  }
  return result;
}

function parseTextFromMessage(message: UIMessage) {
  return message.parts
    .map((part) => {
      if (part.type === "text" && typeof part.content === "string") {
        return part.content;
      }
      return "";
    })
    .filter((value) => value.length > 0)
    .join("\n");
}

function MarkdownMessage({ content, isUser }: { content: string; isUser: boolean }) {
  return (
    <Streamdown className={`text-sm ${isUser ? "streamdown-user" : "streamdown-assistant"}`} plugins={{ mermaid }}>
      {content}
    </Streamdown>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(() => {
    void navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [text]);

  return (
    <Button
      className={"size-7 opacity-0 group-hover:opacity-100"}
      onClick={handleCopy}
      size={"icon-sm"}
      variant={"ghost"}
    >
      {copied ? <CheckIcon className={"size-3 text-green-500"} /> : <CopyIcon className={"size-3"} />}
    </Button>
  );
}

export function ChatMessageItem({ message, entryId, groupName, tableNameMap }: ChatMessageItemProps) {
  const isUser = message.role === "user";
  const rawText = parseTextFromMessage(message);
  // Redact internal identifiers (entryId UUID and raw table_name values) from
  // assistant messages so users never see raw identifiers in rendered text or
  // when copying.
  const text = isUser ? rawText : redactIds(rawText, entryId, groupName, tableNameMap);
  const toolCallCount = message.parts.filter((part) => part.type === "tool-call").length;

  if (text.length === 0) {
    return null;
  }

  return (
    <div className={`group flex gap-3 ${isUser ? "flex-row-reverse" : "flex-row"}`}>
      <div
        className={
          "flex size-8 shrink-0 items-center justify-center rounded-full " +
          (isUser ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground")
        }
      >
        {isUser ? <UserIcon className={"size-4"} /> : <BotIcon className={"size-4"} />}
      </div>

      <div className={`flex max-w-[85%] flex-col ${isUser ? "items-end" : "items-start"}`}>
        <div
          className={
            "relative rounded-2xl px-4 py-3 " +
            (isUser
              ? "bg-primary text-primary-foreground rounded-br-md"
              : "border bg-card text-card-foreground rounded-bl-md")
          }
        >
          <div className={"text-sm"}>
            <MarkdownMessage content={text} isUser={isUser} />
          </div>

          {!isUser && (
            <div className={"absolute top-2 right-2"}>
              <CopyButton text={text} />
            </div>
          )}
        </div>

        {!isUser && toolCallCount > 0 && (
          <Badge className={"mt-1.5"} variant={"outline"}>
            <WrenchIcon className={"size-3"} />
            {toolCallCount}
          </Badge>
        )}
      </div>
    </div>
  );
}
