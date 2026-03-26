"use client";

import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport, type UIMessage } from "ai";
import { AlertCircleIcon, BotIcon, SendHorizontalIcon, SquareIcon } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupTextarea } from "@/components/ui/input-group";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Spinner } from "@/components/ui/spinner";
import { getLogChatMessages } from "@/lib/api";
import type { ChatMessage } from "@/lib/api/types";
import { getBearerAuthHeaders } from "@/lib/auth-session";

interface ChatbotTabProps {
  logGroupId: string;
  tableNames: string[];
}

function normalizeMessage(message: ChatMessage): UIMessage {
  const role =
    message.role === "user" || message.role === "assistant" || message.role === "system" ? message.role : "assistant";

  if (Array.isArray(message.parts)) {
    return {
      ...message,
      id: typeof message.id === "string" ? message.id : crypto.randomUUID(),
      role,
      parts: message.parts,
    } as UIMessage;
  }

  const fallbackText = typeof message.content === "string" ? message.content : "";
  return {
    ...message,
    id: typeof message.id === "string" ? message.id : crypto.randomUUID(),
    role,
    parts: [{ type: "text", text: fallbackText }],
  } as UIMessage;
}

function renderMessageText(message: UIMessage) {
  const textParts = message.parts
    .filter((part): part is { type: "text"; text: string } => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");

  return textParts.length > 0 ? textParts : "No text content.";
}

function renderMarkdown(messageText: string) {
  return (
    <ReactMarkdown
      className={"prose prose-sm dark:prose-invert wrap-break-word max-w-none"}
      remarkPlugins={[remarkGfm]}
    >
      {messageText}
    </ReactMarkdown>
  );
}

function buildPromptSuggestions(tableNames: string[]) {
  if (tableNames.length === 0) {
    return [
      "Summarize what data is available in this log group.",
      "What should I upload next to improve analysis quality?",
    ];
  }

  const selectedTableNames = tableNames.slice(0, 3);
  return [
    `Give me a high-level summary of insights from ${selectedTableNames.join(", ")}.`,
    `Identify unusual values or trends in ${selectedTableNames[0]}.`,
    "What are the most useful next questions I should ask about these logs?",
  ];
}

export function ChatbotTab({ logGroupId, tableNames }: ChatbotTabProps) {
  const [draftMessage, setDraftMessage] = useState("");
  const [isHydrating, setIsHydrating] = useState(true);
  const [hydrateError, setHydrateError] = useState<string | null>(null);

  const { messages, sendMessage, setMessages, status, error, stop } = useChat({
    id: `log-group-${logGroupId}`,
    transport: new DefaultChatTransport({
      api: `/api/logs/${encodeURIComponent(logGroupId)}/chat`,
      headers: getBearerAuthHeaders(),
    }),
  });

  const suggestions = useMemo(() => buildPromptSuggestions(tableNames), [tableNames]);
  const isStreaming = status === "submitted" || status === "streaming";

  useEffect(() => {
    let isMounted = true;

    const hydrateMessages = async () => {
      setIsHydrating(true);
      setHydrateError(null);

      try {
        const persistedMessages = await getLogChatMessages(logGroupId);
        if (!isMounted) return;

        const normalizedMessages = persistedMessages.map(normalizeMessage);
        setMessages(normalizedMessages);
      } catch (hydrateMessagesError) {
        if (!isMounted) return;
        const errorMessage =
          hydrateMessagesError instanceof Error
            ? hydrateMessagesError.message
            : "Failed to load chat history for this log group.";
        setHydrateError(errorMessage);
      } finally {
        if (isMounted) {
          setIsHydrating(false);
        }
      }
    };

    hydrateMessages();

    return () => {
      isMounted = false;
    };
  }, [logGroupId, setMessages]);

  const submitMessage = () => {
    const trimmed = draftMessage.trim();
    if (!trimmed || isStreaming) {
      return;
    }

    sendMessage({ text: trimmed });
    setDraftMessage("");
  };

  return (
    <Card className={"h-[70vh] min-h-[30rem]"}>
      <CardHeader className={"gap-3"}>
        <CardTitle>Chatbot</CardTitle>
        <CardDescription>
          Ask questions about this log group. Responses are grounded in your log tables and stored history.
        </CardDescription>
      </CardHeader>

      <CardContent className={"flex min-h-0 flex-1 flex-col gap-4"}>
        {hydrateError !== null && (
          <Alert variant={"destructive"}>
            <AlertCircleIcon />
            <AlertTitle>Failed to load chat history</AlertTitle>
            <AlertDescription>{hydrateError}</AlertDescription>
          </Alert>
        )}

        <ScrollArea className={"flex-1 rounded-lg border"}>
          {isHydrating ? (
            <div className={"flex items-center justify-center py-12"}>
              <Spinner />
            </div>
          ) : messages.length === 0 ? (
            <div className={"p-4"}>
              <Empty className={"border"}>
                <EmptyHeader>
                  <EmptyMedia variant={"icon"}>
                    <BotIcon />
                  </EmptyMedia>
                  <EmptyTitle>No messages yet</EmptyTitle>
                  <EmptyDescription>Start a conversation to analyze this log group.</EmptyDescription>
                </EmptyHeader>
              </Empty>
            </div>
          ) : (
            <div className={"flex flex-col gap-3 p-4"}>
              {messages.map((message) => {
                const isUser = message.role === "user";
                const text = renderMessageText(message);

                return (
                  <div className={isUser ? "flex justify-end" : "flex justify-start"} key={message.id}>
                    <div className={"max-w-[85%] rounded-lg border bg-card px-3 py-2"}>
                      <div className={"mb-2 flex items-center gap-2"}>
                        <Badge variant={isUser ? "default" : "secondary"}>{isUser ? "You" : "Assistant"}</Badge>
                      </div>
                      <div className={"text-sm leading-relaxed"}>{renderMarkdown(text)}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </ScrollArea>

        {error != null && (
          <Alert variant={"destructive"}>
            <AlertCircleIcon />
            <AlertTitle>Chat request failed</AlertTitle>
            <AlertDescription>
              {error.message.length > 0 ? error.message : "Something went wrong while generating a response."}
            </AlertDescription>
          </Alert>
        )}

        <div className={"flex flex-wrap gap-2"}>
          {suggestions.map((suggestion) => (
            <Button
              disabled={isStreaming}
              key={suggestion}
              onClick={() => {
                if (isStreaming) return;
                sendMessage({ text: suggestion });
              }}
              size={"sm"}
              type={"button"}
              variant={"outline"}
            >
              {suggestion}
            </Button>
          ))}
        </div>

        <form
          className={"flex flex-col gap-3"}
          onSubmit={(event) => {
            event.preventDefault();
            submitMessage();
          }}
        >
          <InputGroup>
            <InputGroupTextarea
              disabled={isStreaming}
              onChange={(event) => setDraftMessage(event.currentTarget.value)}
              placeholder={"Ask about anomalies, trends, or table insights..."}
              rows={3}
              value={draftMessage}
            />
            <InputGroupAddon align={"inline-end"}>
              <InputGroupButton
                disabled={isStreaming}
                onClick={submitMessage}
                size={"sm"}
                type={"button"}
                variant={"secondary"}
              >
                <SendHorizontalIcon data-icon={"inline-end"} />
                Send
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>

          {isStreaming && (
            <div className={"flex items-center justify-between gap-2"}>
              <div className={"flex items-center gap-2 text-muted-foreground text-sm"}>
                <Spinner />
                Generating response...
              </div>
              <Button onClick={() => stop()} size={"sm"} type={"button"} variant={"outline"}>
                <SquareIcon data-icon={"inline-start"} />
                Stop
              </Button>
            </div>
          )}
        </form>
      </CardContent>
    </Card>
  );
}
