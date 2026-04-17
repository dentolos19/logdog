import { mermaid } from "@streamdown/mermaid";
import type { UIMessage } from "@tanstack/ai-react";
import { BotIcon, CheckIcon, CopyIcon, UserIcon } from "lucide-react";
import { useCallback, useState } from "react";
import { Streamdown } from "streamdown";
import { Button } from "#/components/ui/button";
import { isWidgetTool, ToolCallBadge, WidgetToolOutput } from "#/routes/(platform)/logs/-components/chat-tool-call";

type ChatMessageItemProps = {
  message: UIMessage;
};

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

export function ChatMessageItem({ message }: ChatMessageItemProps) {
  const isUser = message.role === "user";
  const text = parseTextFromMessage(message);

  const toolParts = message.parts.filter((part) => part.type === "tool-call");
  const widgetParts = toolParts.filter(isWidgetTool);
  const nonWidgetParts = toolParts.filter((part) => !isWidgetTool(part));

  if (text.length === 0 && toolParts.length === 0) {
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
          {text.length > 0 && (
            <div className={"text-sm"}>
              <MarkdownMessage content={text} isUser={isUser} />
            </div>
          )}

          {!isUser && text.length > 0 && (
            <div className={"absolute top-2 right-2"}>
              <CopyButton text={text} />
            </div>
          )}
        </div>

        {!isUser && nonWidgetParts.length > 0 && (
          <div className={"mt-1.5 flex flex-wrap gap-1.5"}>
            {nonWidgetParts.map((part, index) => (
              <ToolCallBadge key={`badge-${message.id}-${index}`} part={part} />
            ))}
          </div>
        )}

        {!isUser &&
          widgetParts.map((part, index) => (
            <div className={"mt-2 w-full"} key={`widget-${message.id}-${index}`}>
              <WidgetToolOutput part={part} />
            </div>
          ))}
      </div>
    </div>
  );
}
