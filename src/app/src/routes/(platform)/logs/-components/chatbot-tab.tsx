import { type ModelMessage, modelMessagesToUIMessages } from "@tanstack/ai";
import { fetchServerSentEvents, type UIMessage, useChat } from "@tanstack/ai-react";
import { AlertCircleIcon, BotIcon, SendHorizontalIcon, SquareIcon } from "lucide-react";
import { type FormEvent, useEffect, useMemo, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "#/components/ui/alert";
import { Badge } from "#/components/ui/badge";
import { Button } from "#/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupTextarea } from "#/components/ui/input-group";
import { Spinner } from "#/components/ui/spinner";
import { type ChatMessage, getAccessToken, getLogChatMessages, replaceLogChatMessages } from "#/lib/server";
import { streamLogChat } from "#/lib/server/chat";

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
    "Confirm currently available tables and key columns before analysis.",
    `Summarize key insights from ${selected.join(", ")}.`,
    "Find unusual spikes or anomalies.",
  ];
}

function safeSerialize(value: unknown) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
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

function parsePersistableTextFromMessage(message: UIMessage) {
  const textContent = parseTextFromMessage(message);
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

export function ChatbotTab({ entryId, tableNames }: ChatbotTabProps) {
  const [draftMessage, setDraftMessage] = useState("");
  const [hydrateError, setHydrateError] = useState<string | null>(null);
  const [persistError, setPersistError] = useState<string | null>(null);
  const [isHydrating, setIsHydrating] = useState(true);
  const [isPersisting, setIsPersisting] = useState(false);

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

  return (
    <div className={"flex flex-col gap-4"}>
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

      <div className={"flex-1 pb-4"}>
        {isHydrating ? (
          <div className={"flex items-center justify-center py-12"}>
            <Spinner />
          </div>
        ) : messages.length === 0 ? (
          <Empty className={"border"}>
            <EmptyHeader>
              <EmptyMedia variant={"icon"}>
                <BotIcon />
              </EmptyMedia>
              <EmptyTitle>Assistant Workspace</EmptyTitle>
              <EmptyDescription>Ask questions to analyze this log group and its parsed tables.</EmptyDescription>
            </EmptyHeader>
          </Empty>
        ) : (
          <div className={"flex flex-col gap-4"}>
            {messages.map((message) => {
              const isUser = message.role === "user";
              const text = parseTextFromMessage(message);
              if (text.length === 0) {
                return null;
              }

              return (
                <div className={isUser ? "flex justify-end" : "flex justify-start"} key={message.id}>
                  <div
                    className={
                      isUser
                        ? "max-w-[90%] rounded-lg bg-primary px-4 py-3 text-primary-foreground"
                        : "max-w-[90%] rounded-lg border bg-card px-4 py-3 text-card-foreground"
                    }
                  >
                    <div className={"mb-2 font-medium text-xs opacity-80"}>{isUser ? "You" : "Assistant"}</div>
                    <p className={"text-sm leading-relaxed whitespace-pre-wrap"}>{text}</p>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div
        className={
          "sticky bottom-0 z-10 -mx-4 bg-background/95 px-4 pt-2 pb-4 backdrop-blur supports-backdrop-filter:bg-background/60 sm:-mx-6 sm:px-6"
        }
      >
        <div className={"mb-4 flex flex-wrap gap-2"}>
          {suggestions.map((suggestion) => (
            <Button
              className={"rounded-full"}
              disabled={isLoading}
              key={suggestion}
              onClick={() => {
                if (isLoading) {
                  return;
                }
                void sendMessage(suggestion);
              }}
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
          {isPersisting && <Badge variant={"outline"}>Saving…</Badge>}
        </div>

        <form className={"flex flex-col gap-3"} onSubmit={onSubmit}>
          <InputGroup className={"bg-background shadow-sm"}>
            <InputGroupTextarea
              className={"min-h-[3rem] resize-none py-3"}
              disabled={isLoading}
              onChange={(event) => setDraftMessage(event.currentTarget.value)}
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
            <InputGroupAddon align={"inline-end"}>
              <InputGroupButton
                className={"mr-1 size-8 rounded-full"}
                disabled={isLoading || !draftMessage.trim()}
                size={"icon-sm"}
                type={"submit"}
                variant={"default"}
              >
                <SendHorizontalIcon />
                <span className={"sr-only"}>Send</span>
              </InputGroupButton>
            </InputGroupAddon>
          </InputGroup>

          {isLoading && (
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
      </div>
    </div>
  );
}
