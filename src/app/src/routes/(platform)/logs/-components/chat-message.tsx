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
    <Streamdown
      className={`text-sm leading-relaxed ${isUser ? "streamdown-user" : "streamdown-assistant"}`}
      plugins={{ mermaid }}
    >
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
      aria-label={copied ? "Copied message" : "Copy message"}
      className={
        "absolute top-2 right-2 size-7 rounded-full opacity-0 shadow-xs transition-all duration-200 " +
        "group-hover:opacity-100 group-hover:shadow-sm hover:scale-105 active:scale-95 " +
        (copied
          ? "bg-green-500/10 text-green-600 opacity-100"
          : "bg-background/80 text-muted-foreground backdrop-blur-sm")
      }
      onClick={handleCopy}
      size={"icon-sm"}
      variant={"ghost"}
    >
      {copied ? <CheckIcon className={"size-3 text-green-500"} /> : <CopyIcon className={"size-3"} />}
    </Button>
  );
}

function BotAvatar() {
  return (
    <div
      className={
        "flex size-8 shrink-0 items-center justify-center rounded-full " +
        "bg-gradient-to-br from-primary/10 to-primary/5 ring-1 ring-primary/10 text-primary"
      }
    >
      <BotIcon className={"size-4"} />
    </div>
  );
}

function UserAvatar() {
  return (
    <div
      className={
        "flex size-8 shrink-0 items-center justify-center rounded-full " +
        "bg-gradient-to-br from-primary to-primary/80 text-primary-foreground shadow-sm"
      }
    >
      <UserIcon className={"size-4"} />
    </div>
  );
}

export function ChatMessageItem({ message, entryId, groupName, tableNameMap }: ChatMessageItemProps) {
  const isUser = message.role === "user";
  const rawText = parseTextFromMessage(message);
  const text = isUser ? rawText : redactIds(rawText, entryId, groupName, tableNameMap);
  const toolCallCount = message.parts.filter((part) => part.type === "tool-call").length;

  if (text.length === 0) {
    return null;
  }

  return (
    <div
      className={`group flex items-start gap-3 ${isUser ? "flex-row-reverse" : "flex-row"}`}
      style={{ animation: "none" }}
    >
      {isUser ? <UserAvatar /> : <BotAvatar />}

      <div className={`flex max-w-[80%] flex-col ${isUser ? "items-end" : "items-start"}`}>
        <div
          className={
            "relative rounded-2xl px-4 py-3 shadow-xs " +
            (isUser
              ? "bg-primary text-primary-foreground rounded-br-sm shadow-primary/10"
              : "border bg-card text-card-foreground rounded-bl-sm shadow-sm")
          }
        >
          <div className={"pr-6 text-sm"}>
            <MarkdownMessage content={text} isUser={isUser} />
          </div>

          {!isUser && <CopyButton text={text} />}
        </div>

        {!isUser && toolCallCount > 0 && (
          <Badge
            className={
              "mt-1.5 gap-1 rounded-full border-muted-foreground/10 bg-muted/50 px-2 py-0.5 text-[10px] " +
              "font-normal text-muted-foreground/70"
            }
            variant={"outline"}
          >
            <WrenchIcon className={"size-2.5"} />
            {toolCallCount} {toolCallCount === 1 ? "tool call" : "tool calls"}
          </Badge>
        )}
      </div>
    </div>
  );
}
