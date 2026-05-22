import type { UIMessage } from "@tanstack/ai-react";

import { fetchServerSentEvents, useChat } from "@tanstack/ai-react";
import {
  AlertCircleIcon,
  ArrowDownIcon,
  BotIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  EraserIcon,
  SendHorizontalIcon,
  SparklesIcon,
} from "lucide-react";
import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button } from "#/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "#/components/ui/collapsible";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupTextarea } from "#/components/ui/input-group";
import { Spinner } from "#/components/ui/spinner";
import { getLogChatMessages, replaceLogChatMessages } from "#/lib/server";
import { generateChatSuggestions, streamLogChat } from "#/lib/server/chat";
import { ChatMessageItem } from "#/routes/(platform)/logs/-components/chat-message";

type ChatbotTabProps = {
  entryId: string;
  groupName: string;
  tables: Array<{ id: string; name: string }>;
};

type StarterMessage = {
  display: string;
  prompt: string;
};

const STARTER_MESSAGES: StarterMessage[] = [
  {
    display: "Summarize all logs in this group",
    prompt:
      "Summarize all logs in this group. Use sensible defaults: count rows, preview data, check fields, and describe each table. Do not ask what to include.",
  },
  {
    display: "Show me a summary of all uploaded data.",
    prompt:
      "Show me a summary of all uploaded data across every table. Use sensible defaults and do not ask follow-up questions.",
  },
  {
    display: "Are there any anomalies or errors across all tables?",
    prompt:
      "Are there any anomalies or errors across all tables? Look for error counts, status failures, spikes, null-heavy columns, and unusual values. Do not ask clarifying questions.",
  },
];

function toPersistedMessages(messages: UIMessage[]) {
  return messages
    .filter((message) => message.role === "user" || message.role === "assistant")
    .map((message) => ({
      id: message.id,
      role: message.role,
      content: message.parts
        .filter((part) => part.type === "text" && typeof part.content === "string")
        .map((part) => (part as { content: string }).content)
        .join("\n"),
      parts: message.parts as Array<Record<string, unknown>>,
    }));
}

function restoreUIMessages(messages: Array<{ role: string; parts?: Array<Record<string, unknown>>; id?: string }>) {
  return messages
    .filter((msg) => msg.role === "user" || msg.role === "assistant")
    .map((msg) => ({
      id: msg.id ?? crypto.randomUUID(),
      role: msg.role as "user" | "assistant",
      parts: (msg.parts ?? []) as UIMessage["parts"],
    })) as UIMessage[];
}

function hasVisibleContent(message: UIMessage) {
  return message.parts.some(
    (part) => part.type === "text" && typeof part.content === "string" && part.content.length > 0,
  );
}

function ErrorBadge({ label, message }: { label: string; message: string }) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <Collapsible onOpenChange={setIsOpen} open={isOpen}>
      <CollapsibleTrigger asChild>
        <button
          className={
            "inline-flex cursor-pointer items-center gap-1 rounded-full bg-destructive/10 px-2 py-0.5" +
            "text-destructive text-xs transition-colors hover:bg-destructive/20"
          }
          type={"button"}
        >
          <AlertCircleIcon className={"size-2.5"} />
          {label}
          {isOpen ? <ChevronUpIcon className={"size-3"} /> : <ChevronDownIcon className={"size-3"} />}
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <p className={"mt-2 max-w-md rounded-md bg-destructive/5 px-3 py-2 text-destructive text-xs"}>{message}</p>
      </CollapsibleContent>
    </Collapsible>
  );
}

type Suggestion = {
  display: string;
  prompt: string;
};

function messagesEqual(a: UIMessage[], b: UIMessage[]) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i].id !== b[i].id) return false;
    if (a[i].role !== b[i].role) return false;
    if (a[i].parts.length !== b[i].parts.length) return false;
  }
  return true;
}

function ThinkingIndicator() {
  return (
    <div className={"flex items-start gap-3"}>
      <div
        className={
          "flex size-8 shrink-0 items-center justify-center rounded-full" +
          "bg-gradient-to-br from-primary/10 to-primary/5 text-primary ring-1 ring-primary/10"
        }
      >
        <BotIcon className={"size-4"} />
      </div>
      <div className={"rounded-2xl rounded-bl-sm border bg-card px-4 py-3 shadow-sm"}>
        <div className={"flex items-center gap-2.5"}>
          <div className={"flex items-center gap-1"}>
            <span
              className={"size-1.5 animate-bounce rounded-full bg-muted-foreground/40"}
              style={{ animationDelay: "0ms" }}
            />
            <span
              className={"size-1.5 animate-bounce rounded-full bg-muted-foreground/40"}
              style={{ animationDelay: "150ms" }}
            />
            <span
              className={"size-1.5 animate-bounce rounded-full bg-muted-foreground/40"}
              style={{ animationDelay: "300ms" }}
            />
          </div>
          <span className={"text-muted-foreground text-xs"}>Thinking...</span>
        </div>
      </div>
    </div>
  );
}

export function ChatbotTab({ entryId, groupName, tables }: ChatbotTabProps) {
  const tableNameMap = useMemo(() => {
    const map: Record<string, string> = {};
    for (const table of tables) {
      if (table.id !== table.name) {
        map[table.id] = table.name;
      }
    }
    return map;
  }, [tables]);
  const [draftMessage, setDraftMessage] = useState("");
  const [hydrateError, setHydrateError] = useState<string | null>(null);
  const [persistError, setPersistError] = useState<string | null>(null);
  const [isHydrating, setIsHydrating] = useState(true);
  const [isAtBottom, setIsAtBottom] = useState(true);
  const [aiSuggestions, setAiSuggestions] = useState<Suggestion[]>([]);
  const [isGeneratingSuggestions, setIsGeneratingSuggestions] = useState(false);
  const lastAssistantMessageIdRef = useRef<string | null>(null);

  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const hydratedMessagesRef = useRef<UIMessage[]>([]);
  const hasHydratedRef = useRef(false);
  const inputTextareaRef = useRef<HTMLTextAreaElement>(null);

  const { messages, sendMessage, setMessages, stop, isLoading, status, error } = useChat({
    id: `log-group-${entryId}`,
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
    const atBottom = scrollHeight - scrollTop - clientHeight < 60;
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

        const restoredMessages = restoreUIMessages(savedMessages);
        hydratedMessagesRef.current = restoredMessages;
        hasHydratedRef.current = true;
        setMessages(restoredMessages);
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

    if (hasHydratedRef.current && messagesEqual(messages, hydratedMessagesRef.current)) {
      return;
    }

    let cancelled = false;

    const persist = async () => {
      setPersistError(null);

      try {
        await replaceLogChatMessages(entryId, {
          messages: toPersistedMessages(messages),
        });
      } catch (persistError) {
        if (!cancelled) {
          setPersistError(persistError instanceof Error ? persistError.message : "Failed to save chat history.");
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
    setAiSuggestions([]);
    await sendMessage(trimmed);
  };

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    void submitMessage();
  };

  const handleClearChat = useCallback(async () => {
    stop();
    setDraftMessage("");
    setMessages([]);
    setAiSuggestions([]);
    hydratedMessagesRef.current = [];
    try {
      await replaceLogChatMessages(entryId, { messages: [] });
    } catch {
      setPersistError("Failed to clear chat history.");
    }
  }, [entryId, setMessages, stop]);

  const visibleMessages = useMemo(() => messages.filter(hasVisibleContent), [messages]);
  const hasMessages = visibleMessages.length > 0;
  const lastMessageIsAssistant = useMemo(
    () => hasMessages && visibleMessages[visibleMessages.length - 1].role === "assistant",
    [hasMessages, visibleMessages],
  );

  // Generate AI suggestions after each complete assistant response
  useEffect(() => {
    if (!lastMessageIsAssistant || isLoading || isHydrating) {
      return;
    }

    const lastAssistantMessage = visibleMessages[visibleMessages.length - 1];
    if (!lastAssistantMessage || lastAssistantMessage.id === lastAssistantMessageIdRef.current) {
      return;
    }

    lastAssistantMessageIdRef.current = lastAssistantMessage.id;

    const recentForSuggestions = visibleMessages.slice(-4).map((msg) => {
      const text = msg.parts
        .filter((p) => p.type === "text" && typeof p.content === "string")
        .map((p) => (p as { content: string }).content)
        .join("\n");
      return { role: msg.role as "user" | "assistant", content: text.slice(0, 2000) };
    });

    if (recentForSuggestions.length === 0) {
      return;
    }

    setIsGeneratingSuggestions(true);

    void generateChatSuggestions({
      data: {
        entryId,
        recentMessages: recentForSuggestions,
        hasTables: tables.length > 0,
      },
    })
      .then((suggestions) => {
        setAiSuggestions(
          suggestions.map((display) => ({
            display,
            prompt: display,
          })),
        );
      })
      .catch(() => {
        const fallback: Suggestion[] =
          tables.length > 0
            ? [
                { display: "Explore key trends", prompt: "Explore key trends across all tables" },
                { display: "Find top anomalies", prompt: "Find the most important anomalies across all tables" },
                { display: "Show a quick summary", prompt: "Show a quick summary of all tables" },
              ]
            : [
                { display: "How do I upload logs?", prompt: "How do I upload logs?" },
                { display: "What data works?", prompt: "What kind of log data does Logdog support?" },
                { display: "Show me the basics", prompt: "Show me the basics of getting started" },
              ];
        setAiSuggestions(fallback);
      })
      .finally(() => {
        setIsGeneratingSuggestions(false);
      });
  }, [lastMessageIsAssistant, isLoading, isHydrating, entryId, visibleMessages, tables.length]);

  // Focus input after streaming completes
  useEffect(() => {
    if (!isLoading && hasMessages && inputTextareaRef.current) {
      inputTextareaRef.current.focus();
    }
  }, [isLoading, hasMessages]);

  return (
    <div className={"relative flex min-h-0 flex-1 flex-col"}>
      {(hydrateError !== null || persistError !== null || error !== undefined) && (
        <div className={"flex flex-wrap gap-2 px-4 pt-2 pb-1"}>
          {hydrateError !== null && <ErrorBadge label={"Load failed"} message={hydrateError} />}
          {persistError !== null && <ErrorBadge label={"Save failed"} message={persistError} />}
          {error !== undefined && (
            <ErrorBadge label={"Chat failed"} message={error.message || "Failed to generate a response."} />
          )}
        </div>
      )}

      <div
        className={
          "flex min-h-0 flex-1 flex-col overflow-y-auto" +
          "scrollbar-thin scrollbar-thumb-muted-foreground/10 scrollbar-track-transparent hover:scrollbar-thumb-muted-foreground/20" +
          (hasMessages ? "" : "bg-gradient-to-b from-muted/20 to-background")
        }
        onScroll={handleScroll}
        ref={scrollContainerRef}
      >
        {isHydrating ? (
          <div className={"flex flex-1 items-center justify-center"}>
            <Spinner />
          </div>
        ) : !hasMessages ? (
          <div className={"mx-auto mt-16 flex w-full max-w-lg flex-1 flex-col items-center justify-center px-6"}>
            <div className={"flex flex-col items-center gap-6 text-center"}>
              <div
                className={
                  "flex size-16 items-center justify-center rounded-2xl" +
                  "bg-gradient-to-br from-primary/10 via-primary/5 to-primary/0 ring-1 ring-primary/10" +
                  "shadow-primary/5 shadow-sm"
                }
              >
                <BotIcon className={"size-8 text-primary"} />
              </div>
              <div className={"flex flex-col gap-2"}>
                <h2 className={"font-semibold text-xl tracking-tight"}>Log Analysis Chatbot</h2>
                <p className={"mx-auto max-w-sm text-balance text-muted-foreground text-sm leading-relaxed"}>
                  Ask questions about <span className={"font-medium text-foreground"}>{groupName}</span>. I can query
                  tables, summarize logs, find anomalies, and generate charts.
                </p>
              </div>
              <div className={"mb-16 flex w-full flex-col gap-2.5"}>
                <div className={"flex items-center gap-2 px-1"}>
                  <span className={"h-px flex-1 bg-border/50"} />
                  <span className={"font-medium text-[10px] text-muted-foreground/50 uppercase tracking-widest"}>
                    Get started
                  </span>
                  <span className={"h-px flex-1 bg-border/50"} />
                </div>
                {STARTER_MESSAGES.map((message, i) => (
                  <Button
                    className={
                      "group/start h-auto w-full justify-start gap-3 border-border/50 px-4 py-3 text-left text-sm" +
                      "shadow-xs transition-all duration-200 hover:border-border hover:shadow-sm" +
                      "hover:-translate-y-0.5 active:translate-y-0"
                    }
                    disabled={isLoading}
                    key={message.display}
                    onClick={() => {
                      void sendMessage(message.prompt);
                    }}
                    style={{ animationDelay: `${i * 80}ms` } as React.CSSProperties}
                    variant={"outline"}
                  >
                    <span
                      className={
                        "flex size-7 shrink-0 items-center justify-center rounded-lg" +
                        "bg-muted/50 text-muted-foreground/60 transition-colors duration-200" +
                        "group-hover/start:bg-primary/10 group-hover/start:text-primary"
                      }
                    >
                      <SparklesIcon className={"size-3.5"} />
                    </span>
                    <span className={"line-clamp-2 font-normal"}>{message.display}</span>
                  </Button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className={"mx-auto w-full max-w-4xl space-y-4 px-4 py-6"}>
            {visibleMessages.map((message) => (
              <div
                className={"fade-in slide-in-from-bottom-1 animate-in duration-300"}
                key={message.id}
                style={{ animationDelay: "0ms", animationFillMode: "both" } as React.CSSProperties}
              >
                <ChatMessageItem
                  entryId={entryId}
                  groupName={groupName}
                  message={message}
                  tableNameMap={tableNameMap}
                />
              </div>
            ))}
            {isLoading && !lastMessageIsAssistant && (
              <div className={"fade-in slide-in-from-bottom-1 animate-in duration-300"}>
                <ThinkingIndicator />
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>

      {/* Scroll to bottom button */}
      {!isAtBottom && hasMessages && (
        <div className={"fade-in slide-in-from-bottom-2 absolute right-6 bottom-28 z-10 animate-in duration-200"}>
          <Button
            className={
              "h-9 w-9 rounded-full shadow-lg ring-1 ring-border/50" +
              "transition-all duration-200 hover:scale-105 hover:shadow-xl active:scale-95"
            }
            onClick={scrollToBottom}
            size={"icon-sm"}
            variant={"secondary"}
          >
            <ArrowDownIcon className={"size-4"} />
            <span className={"sr-only"}>Scroll to bottom</span>
          </Button>
        </div>
      )}

      {/* Input bar */}
      {!isLoading && (
        <div
          className={
            "shrink-0 border-t bg-background/80 backdrop-blur-xl supports-[backdrop-filter]:bg-background/60" +
            "z-10 shadow-[0_-1px_0_0] shadow-border/50" +
            (hasMessages ? "" : "border-t-transparent")
          }
        >
          <div className={"mx-auto max-w-4xl"}>
            {/* AI Suggestions */}
            {hasMessages && !isLoading && (
              <div className={"flex flex-wrap items-center gap-2 px-4 pt-3 pb-2"}>
                {isGeneratingSuggestions && aiSuggestions.length === 0 && (
                  <span className={"flex items-center gap-1.5 text-muted-foreground/60 text-xs"}>
                    <Spinner className={"size-3"} />
                    Generating suggestions&hellip;
                  </span>
                )}
                {aiSuggestions.map((suggestion) => (
                  <Button
                    className={
                      "group/pill h-auto gap-1.5 rounded-full border-border/50 px-3 py-1.5 text-xs" +
                      "shadow-xs transition-all duration-200 hover:border-border hover:shadow-sm" +
                      "hover:bg-accent active:scale-95"
                    }
                    key={suggestion.display}
                    onClick={() => void sendMessage(suggestion.prompt)}
                    size={"sm"}
                    variant={"outline"}
                  >
                    <SparklesIcon
                      className={
                        "size-3 shrink-0 text-muted-foreground/50 transition-colors duration-200" +
                        "group-hover/pill:text-primary"
                      }
                    />
                    {suggestion.display}
                  </Button>
                ))}
              </div>
            )}

            {/* Input form */}
            <form className={"flex items-center justify-center gap-2 px-4 pt-3 pb-2"} onSubmit={onSubmit}>
              <InputGroup
                className={
                  "bg-background shadow-sm transition-all duration-200" +
                  "focus-within:border-primary/30 focus-within:shadow-md focus-within:ring-0"
                }
              >
                <InputGroupTextarea
                  className={"max-h-[200px] min-h-[44px] py-3 text-sm"}
                  disabled={isLoading}
                  onChange={(event) => setDraftMessage(event.currentTarget.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && !event.shiftKey) {
                      event.preventDefault();
                      void submitMessage();
                    }
                  }}
                  placeholder={"Ask about anomalies, trends, or table insights..."}
                  ref={inputTextareaRef}
                  rows={1}
                  value={draftMessage}
                />
                <InputGroupAddon align={"inline-end"}>
                  <InputGroupButton
                    className={
                      "mr-1 size-8 rounded-full transition-all duration-200" +
                      (draftMessage.trim() ? "shadow-sm hover:shadow-md active:scale-95" : "opacity-50")
                    }
                    disabled={isLoading || !draftMessage.trim()}
                    size={"icon-sm"}
                    type={"submit"}
                    variant={"default"}
                  >
                    <SendHorizontalIcon className={"size-4"} />
                    <span className={"sr-only"}>Send</span>
                  </InputGroupButton>
                </InputGroupAddon>
              </InputGroup>
            </form>

            {/* Clear chat */}
            {hasMessages && (
              <div className={"flex items-center justify-end gap-2 px-4 pt-1 pb-4"}>
                <Button
                  className={
                    "h-auto gap-1.5 rounded-full px-3 py-1 text-muted-foreground/60 text-xs" +
                    "transition-all duration-200 hover:bg-muted/50 hover:text-muted-foreground active:scale-95"
                  }
                  disabled={isLoading}
                  onClick={() => void handleClearChat()}
                  size={"sm"}
                  type={"button"}
                  variant={"ghost"}
                >
                  <EraserIcon className={"size-3 shrink-0"} />
                  <span className={"truncate"}>Clear chat</span>
                </Button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
