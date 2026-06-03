import { mermaid } from "@streamdown/mermaid";
import type { UIMessage } from "@tanstack/ai-react";
import { BotIcon, CheckIcon, CopyIcon, UserIcon, WrenchIcon } from "lucide-react";
import { useCallback, useState } from "react";
import { Streamdown } from "streamdown";

import { Avatar, AvatarFallback } from "#/components/ui/avatar";
import { Badge } from "#/components/ui/badge";
import { Bubble, BubbleContent } from "#/components/ui/bubble";
import { Button } from "#/components/ui/button";
import { Message, MessageAvatar, MessageContent } from "#/components/ui/message";
import { cn } from "#/lib/utils";

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
      className={cn("text-sm leading-relaxed", isUser ? "streamdown-user" : "streamdown-assistant")}
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
      className={cn(
        "absolute top-1 right-1 opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
        copied && "text-primary opacity-100",
      )}
      onClick={handleCopy}
      size={"icon-sm"}
      variant={"ghost"}
    >
      {copied ? <CheckIcon /> : <CopyIcon />}
    </Button>
  );
}

function BotAvatar() {
  return (
    <Avatar>
      <AvatarFallback>
        <BotIcon />
      </AvatarFallback>
    </Avatar>
  );
}

function UserAvatar() {
  return (
    <Avatar>
      <AvatarFallback className="bg-primary text-primary-foreground">
        <UserIcon />
      </AvatarFallback>
    </Avatar>
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
    <Message align={isUser ? "end" : "start"} className="group">
      <MessageAvatar>{isUser ? <UserAvatar /> : <BotAvatar />}</MessageAvatar>
      <MessageContent>
        <Bubble align={isUser ? "end" : "start"} variant={isUser ? "default" : "outline"}>
          <BubbleContent className="relative px-4 py-3">
            <div className={cn("text-sm", !isUser && "pr-6")}>
              <MarkdownMessage content={text} isUser={isUser} />
            </div>
            {!isUser && <CopyButton text={text} />}
          </BubbleContent>
        </Bubble>

        {!isUser && toolCallCount > 0 && (
          <Badge variant="outline">
            <WrenchIcon data-icon="inline-start" />
            {toolCallCount} {toolCallCount === 1 ? "tool call" : "tool calls"}
          </Badge>
        )}
      </MessageContent>
    </Message>
  );
}
