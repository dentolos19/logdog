import { type ModelMessage, modelMessagesToUIMessages } from "@tanstack/ai";
import { fetchServerSentEvents, type UIMessage, useChat } from "@tanstack/ai-react";
import {
  AlertCircleIcon,
  BotIcon,
  FileTextIcon,
  LightbulbIcon,
  SendHorizontalIcon,
  SparklesIcon,
  SquareIcon,
} from "lucide-react";
import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Button } from "#/components/ui/button";
import { Spinner } from "#/components/ui/spinner";
import { type ChatMessage, getAccessToken, getLogChatMessages, replaceLogChatMessages } from "#/lib/server";
import { streamLogChat } from "#/lib/server/chat";
import { ChatMessageItem } from "#/routes/(platform)/logs/-components/chat-message";

type ChatbotTabProps = {
  entryId: string;
  tableNames: string[];
};

function createSuggestions(tableNames: string[]) {
  if (tableNames.length === 0) {
    return [
      "List available tables and schemas for this log group.",
      "What should I upload next for better analysis?",
      "What missing tables or fields are blocking deeper analysis?",
    ];
  }

  const selected = tableNames.slice(0, 3);
  return [
    "Summarize key insights from all available tables.",
    `Analyze ${selected.join(", ")} for anomalies.`,
    "Show error rate trends over time.",
  ];
}

const REPORT_PROMPT =
  "Generate a comprehensive analysis report for this log group. " +
  "Query all available tables, identify key insights, anomalies, and trends. " +
  "Present findings with data tables and charts, then compile everything into a structured report using generate_report.";

function safeSerialize(value: unknown) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function parsePersistableTextFromMessage(message: UIMessage) {
  const textContent = message.parts
    .map((part) => {
      if (part.type === "text" && typeof part.content === "string") {
        return part.content;
      }
      return "";
    })
    .filter((value) => value.length > 0)
    .join("\n");

  if (textContent.length > 0) {
    return textContent;
  }

  const toolEntries = message.parts
    .map((part) => {
      if (part.type !== "tool-call") {
        return "";
      }

      const toolName = typeof part.name === "string" ? part.name : "tool";
      const output = "output" in part ? part.output : undefined;
      if (output !== undefined) {
        return `[Tool output: ${toolName}] ${safeSerialize(output)}`;
      }

      return `[Tool call: ${toolName}]`;
    })
    .filter((value) => value.length > 0);

  return toolEntries.join("\n");
}

function normalizePersistedMessages(messages: ChatMessage[]) {
  const modelMessages: ModelMessage[] = [];
  for (const message of messages) {
    const role = message.role === "user" || message.role === "assistant" ? message.role : null;
    if (role === null) {
      continue;
    }

    const content = typeof message.content === "string" ? message.content : "";
    if (content.length === 0) {
      continue;
    }

    modelMessages.push({ role, content });
  }

  return modelMessagesToUIMessages(modelMessages);
}

function toPersistedMessages(messages: UIMessage[]) {
  const persisted: ChatMessage[] = [];

  for (const message of messages) {
    if (message.role !== "user" && message.role !== "assistant") {
      continue;
    }

    const content = parsePersistableTextFromMessage(message);
    if (content.length === 0) {
      continue;
    }

    persisted.push({
      id: message.id,
      role: message.role,
      content,
      parts: [{ type: "text", content }],
    });
  }

  return persisted;
}

function hasVisibleContent(message: UIMessage) {
  return message.parts.some((part) => {
    if (part.type === "text" && typeof part.content === "string" && part.content.length > 0) {
      return true;
    }
    if (part.type === "tool-call") {
      return true;
    }
    return false;
  });
}

export function ChatbotTab({ entryId, tableNames }: ChatbotTabProps) {
  const [draftMessage, setDraftMessage] = useState("");
  const [hydrateError, setHydrateError] = useState<string | null>(null);
  const [persistError, setPersistError] = useState<string | null>(null);
  const [isHydrating, setIsHydrating] = useState(true);
  const [isPersisting, setIsPersisting] = useState(false);
  const [isAtBottom, setIsAtBottom] = useState(true);

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const suggestions = useMemo(() => createSuggestions(tableNames), [tableNames]);
  const token = getAccessToken();
  const authorizationHeader = token ? `Bearer ${token}` : "";
  const origin = typeof window === "undefined" ? "http://localhost" : window.location.origin;

  const { messages, sendMessage, setMessages, stop, isLoading, status, error } = useChat({
    id: `log-entry-${entryId}`,
    connection: fetchServerSentEvents("/", {
      fetchClient: async (_url, init) => {
        const bodyText = typeof init?.body === "string" ? init.body : "";
        let parsedBody: { messages?: unknown[] } = {};
        if (bodyText.length > 0) {
          try {
            parsedBody = JSON.parse(bodyText) as { messages?: unknown[] };
          } catch {
            parsedBody = {};
          }
        }

        return streamLogChat({
          data: {
            entryId,
            authorizationHeader,
            origin,
            messages: Array.isArray(parsedBody.messages) ? parsedBody.messages : [],
          },
          signal: init?.signal ?? undefined,
        });
      },
    }),
  });

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, []);

  const handleScroll = useCallback(() => {
    const container = scrollContainerRef.current;
    if (!container) {
      return;
    }

    const { scrollTop, scrollHeight, clientHeight } = container;
    const atBottom = scrollHeight - scrollTop - clientHeight < 50;
    setIsAtBottom(atBottom);
  }, []);

  useEffect(() => {
    if (isAtBottom || isLoading) {
      scrollToBottom();
    }
  }, [messages, isLoading, isAtBottom, scrollToBottom]);

  useEffect(() => {
    let cancelled = false;

    const hydrate = async () => {
      setHydrateError(null);
      setPersistError(null);
      setIsHydrating(true);

      try {
        const savedMessages = await getLogChatMessages(entryId);
        if (cancelled) {
          return;
        }

        const normalizedMessages = normalizePersistedMessages(savedMessages);
        setMessages(normalizedMessages);
      } catch (hydrateError) {
        if (!cancelled) {
          setHydrateError(hydrateError instanceof Error ? hydrateError.message : "Failed to load chat history.");
          setMessages([]);
        }
      } finally {
        if (!cancelled) {
          setIsHydrating(false);
        }
      }
    };

    void hydrate();

    return () => {
      cancelled = true;
    };
  }, [entryId, setMessages]);

  useEffect(() => {
    if (isHydrating || status !== "ready") {
      return;
    }

    let cancelled = false;

    const persist = async () => {
      setPersistError(null);
      setIsPersisting(true);

      try {
        await replaceLogChatMessages(entryId, {
          messages: toPersistedMessages(messages),
        });
      } catch (persistError) {
        if (!cancelled) {
          setPersistError(persistError instanceof Error ? persistError.message : "Failed to save chat history.");
        }
      } finally {
        if (!cancelled) {
          setIsPersisting(false);
        }
      }
    };

    void persist();

    return () => {
      cancelled = true;
    };
  }, [entryId, isHydrating, messages, status]);

  const submitMessage = async () => {
    const trimmed = draftMessage.trim();
    if (trimmed.length === 0 || isLoading) {
      return;
    }

    setDraftMessage("");
    await sendMessage(trimmed);
  };

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    void submitMessage();
  };

  const visibleMessages = useMemo(() => messages.filter(hasVisibleContent), [messages]);

  return (
    <div className={"flex h-full flex-col"}>
      {hydrateError !== null && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Failed to load chat history</AlertTitle>
          <AlertDescription>{hydrateError}</AlertDescription>
        </Alert>
      )}

      {persistError !== null && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Failed to save chat history</AlertTitle>
          <AlertDescription>{persistError}</AlertDescription>
        </Alert>
      )}

      {error !== undefined && (
        <Alert variant={"destructive"}>
          <AlertCircleIcon className={"size-4"} />
          <AlertTitle>Chat request failed</AlertTitle>
          <AlertDescription>{error.message || "Failed to generate a response."}</AlertDescription>
        </Alert>
      )}

      <div className={"flex-1 overflow-y-auto"} onScroll={handleScroll} ref={scrollContainerRef}>
        {isHydrating ? (
          <div className={"flex items-center justify-center py-12"}>
            <Spinner />
          </div>
        ) : visibleMessages.length === 0 ? (
          <div className={"flex h-full flex-col items-center justify-center px-4"}>
            <div className={"flex max-w-md flex-col items-center gap-6 text-center"}>
              <div className={"flex size-14 items-center justify-center rounded-2xl bg-muted"}>
                <BotIcon className={"size-7 text-muted-foreground"} />
              </div>
              <div className={"flex flex-col gap-2"}>
                <h2 className={"font-semibold text-lg"}>Log Analysis Assistant</h2>
                <p className={"text-muted-foreground text-sm"}>
                  Ask questions about your log data. I can query tables, find anomalies, generate charts, and compile
                  reports.
                </p>
              </div>
              <div className={"flex w-full flex-col gap-2"}>
                {suggestions.map((suggestion) => (
                  <Button
                    className={"h-auto w-full justify-start gap-2 px-4 py-3 text-left text-sm"}
                    disabled={isLoading}
                    key={suggestion}
                    onClick={() => void sendMessage(suggestion)}
                    variant={"outline"}
                  >
                    <SparklesIcon className={"size-4 shrink-0 text-muted-foreground"} />
                    {suggestion}
                  </Button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className={"mx-auto max-w-3xl space-y-6 py-6"}>
            {visibleMessages.map((message) => (
              <ChatMessageItem key={message.id} message={message} />
            ))}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>

      {!isAtBottom && visibleMessages.length > 0 && (
        <div className={"flex justify-center py-1"}>
          <Button className={"rounded-full shadow-md"} onClick={scrollToBottom} size={"sm"} variant={"secondary"}>
            Scroll to bottom
          </Button>
        </div>
      )}

      <div className={"shrink-0 border-t bg-background/95 backdrop-blur supports-backdrop-filter:bg-background/60"}>
        <div className={"mx-auto max-w-3xl"}>
          {visibleMessages.length > 0 && (
            <div className={"flex gap-2 overflow-x-auto px-4 pt-3 pb-2"}>
              {suggestions.map((suggestion) => (
                <Button
                  className={"shrink-0 rounded-full"}
                  disabled={isLoading}
                  key={suggestion}
                  onClick={() => void sendMessage(suggestion)}
                  size={"sm"}
                  type={"button"}
                  variant={"secondary"}
                >
                  <LightbulbIcon className={"size-3"} />
                  {suggestion}
                </Button>
              ))}
              <Button
                className={"shrink-0 rounded-full"}
                disabled={isLoading}
                onClick={() => void sendMessage(REPORT_PROMPT)}
                size={"sm"}
                type={"button"}
                variant={"outline"}
              >
                <FileTextIcon className={"size-3"} />
                Generate Report
              </Button>
            </div>
          )}

          <form className={"flex items-end gap-2 px-4 pb-4"} onSubmit={onSubmit}>
            <div className={"relative flex-1"}>
              <textarea
                className={
                  "max-h-[200px] min-h-[44px] w-full resize-none rounded-xl border bg-background px-4 py-3 text-sm shadow-sm" +
                  "placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" +
                  "disabled:cursor-not-allowed disabled:opacity-50"
                }
                disabled={isLoading}
                onChange={(event) => setDraftMessage(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void submitMessage();
                  }
                }}
                placeholder={"Ask about anomalies, trends, or table insights..."}
                rows={1}
                value={draftMessage}
              />
            </div>

            {isLoading ? (
              <Button
                className={"size-11 shrink-0 rounded-xl"}
                onClick={() => stop()}
                size={"icon"}
                type={"button"}
                variant={"outline"}
              >
                <SquareIcon className={"size-4"} />
                <span className={"sr-only"}>Stop</span>
              </Button>
            ) : (
              <Button
                className={"size-11 shrink-0 rounded-xl"}
                disabled={!draftMessage.trim()}
                size={"icon"}
                type={"submit"}
                variant={"default"}
              >
                <SendHorizontalIcon className={"size-4"} />
                <span className={"sr-only"}>Send</span>
              </Button>
            )}
          </form>
        </div>
      </div>
    </div>
  );
}
