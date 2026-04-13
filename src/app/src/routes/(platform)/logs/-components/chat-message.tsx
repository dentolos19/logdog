import type { UIMessage } from "@tanstack/ai-react";
import { BotIcon, CheckIcon, CopyIcon, UserIcon } from "lucide-react";
import { useCallback, useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Button } from "#/components/ui/button";
import { ToolCallRenderer } from "#/routes/(platform)/logs/-components/chat-tool-call";

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

function hasToolCallParts(message: UIMessage) {
  return message.parts.some((part) => part.type === "tool-call");
}

function MarkdownMessage({ content, isUser }: { content: string; isUser: boolean }) {
  return (
    <Markdown
      components={{
        a: ({ children, href, ...props }) => (
          <a
            {...props}
            className={"font-medium underline underline-offset-2"}
            href={href}
            rel={"noreferrer noopener"}
            target={"_blank"}
          >
            {children}
          </a>
        ),
        blockquote: ({ children, ...props }) => (
          <blockquote
            {...props}
            className={
              isUser
                ? "my-3 border-l-2 border-primary-foreground/60 pl-3 opacity-90"
                : "my-3 border-l-2 border-border pl-3 text-muted-foreground"
            }
          >
            {children}
          </blockquote>
        ),
        code: ({ children, className, ...props }) => {
          const hasLanguageClass = className?.includes("language-") ?? false;
          if (hasLanguageClass) {
            return (
              <code
                {...props}
                className={
                  isUser
                    ? "block overflow-x-auto rounded-md bg-primary-foreground/15 p-3 font-mono text-xs"
                    : "block overflow-x-auto rounded-md bg-muted p-3 font-mono text-xs"
                }
              >
                {children}
              </code>
            );
          }

          return (
            <code
              {...props}
              className={
                isUser
                  ? "rounded bg-primary-foreground/20 px-1.5 py-0.5 font-mono text-[0.8em]"
                  : "rounded bg-muted px-1.5 py-0.5 font-mono text-[0.8em]"
              }
            >
              {children}
            </code>
          );
        },
        h1: ({ children, ...props }) => (
          <h1 {...props} className={"mt-4 mb-2 font-semibold text-base"}>
            {children}
          </h1>
        ),
        h2: ({ children, ...props }) => (
          <h2 {...props} className={"mt-4 mb-2 font-semibold text-sm"}>
            {children}
          </h2>
        ),
        h3: ({ children, ...props }) => (
          <h3 {...props} className={"mt-3 mb-2 font-semibold text-sm"}>
            {children}
          </h3>
        ),
        li: ({ children, ...props }) => (
          <li {...props} className={"my-1"}>
            {children}
          </li>
        ),
        ol: ({ children, ...props }) => (
          <ol {...props} className={"my-2 list-decimal space-y-1 pl-5"}>
            {children}
          </ol>
        ),
        p: ({ children, ...props }) => (
          <p {...props} className={"my-2 leading-relaxed"}>
            {children}
          </p>
        ),
        pre: ({ children, ...props }) => (
          <pre {...props} className={"my-3 overflow-x-auto whitespace-pre-wrap wrap-break-word"}>
            {children}
          </pre>
        ),
        table: ({ children, ...props }) => (
          <div className={"my-3 overflow-x-auto"}>
            <table {...props} className={"min-w-2xs w-full border-collapse text-left text-sm"}>
              {children}
            </table>
          </div>
        ),
        td: ({ children, ...props }) => (
          <td {...props} className={isUser ? "border border-primary-foreground/25 px-2 py-1" : "border px-2 py-1"}>
            {children}
          </td>
        ),
        th: ({ children, ...props }) => (
          <th
            {...props}
            className={
              isUser
                ? "border border-primary-foreground/25 px-2 py-1.5 font-semibold"
                : "border bg-muted/40 px-2 py-1.5 font-semibold"
            }
          >
            {children}
          </th>
        ),
        ul: ({ children, ...props }) => (
          <ul {...props} className={"my-2 list-disc space-y-1 pl-5"}>
            {children}
          </ul>
        ),
      }}
      remarkPlugins={[remarkGfm]}
    >
      {content}
    </Markdown>
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
  const hasToolCalls = hasToolCallParts(message);

  if (text.length === 0 && !hasToolCalls) {
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

        {!isUser &&
          message.parts
            .filter((part) => part.type === "tool-call")
            .map((part, index) => <ToolCallRenderer isUser={isUser} key={`tool-${message.id}-${index}`} part={part} />)}
      </div>
    </div>
  );
}
