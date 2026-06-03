import type { UIMessage } from "@tanstack/ai-react";
import { fetchServerSentEvents, useChat } from "@tanstack/ai-react";
import {
  AlertCircleIcon,
  BotIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  EraserIcon,
  SendHorizontalIcon,
  SparklesIcon,
} from "lucide-react";
import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Alert, AlertDescription } from "#/components/ui/alert";
import { Avatar, AvatarFallback } from "#/components/ui/avatar";
import { Bubble, BubbleContent } from "#/components/ui/bubble";
import { Button } from "#/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "#/components/ui/collapsible";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "#/components/ui/empty";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupTextarea } from "#/components/ui/input-group";
import { Message, MessageAvatar, MessageContent } from "#/components/ui/message";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from "#/components/ui/message-scroller";
import { Spinner } from "#/components/ui/spinner";
import { generateChatSuggestions, streamLogChat } from "#/lib/chat";
import { getLogChatMessages, replaceLogChatMessages } from "#/lib/logs";
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

const MAX_PERSISTED_MESSAGES = 500;
const MAX_MESSAGE_CONTENT_LENGTH = 100_000;

function trimChatMessages<T>(messages: T[]) {
  return messages.slice(-MAX_PERSISTED_MESSAGES);
}

function toPersistedMessages(messages: UIMessage[]) {
  return trimChatMessages(messages)
    .filter((message) => message.role === "user" || message.role === "assistant")
    .map((message) => ({
      id: message.id,
      role: message.role,
      content: message.parts
        .filter((part) => part.type === "text" && typeof part.content === "string")
        .map((part) => (part as { content: string }).content)
        .join("\n")
        .slice(0, MAX_MESSAGE_CONTENT_LENGTH),
      parts: message.parts as Array<Record<string, unknown>>,
    }));
}

function restoreUIMessages(
  messages: Array<{ role: string; content?: string; parts?: Array<Record<string, unknown>>; id?: string }>,
) {
  return trimChatMessages(messages)
    .filter((msg) => msg.role === "user" || msg.role === "assistant")
    .map((msg) => {
      const restoredParts =
        Array.isArray(msg.parts) && msg.parts.length > 0
          ? (msg.parts as UIMessage["parts"])
          : typeof msg.content === "string" && msg.content.length > 0
            ? ([{ type: "text", content: msg.content }] as UIMessage["parts"])
            : ([] as UIMessage["parts"]);

      return {
        id: msg.id ?? crypto.randomUUID(),
        role: msg.role as "user" | "assistant",
        parts: restoredParts,
      };
    }) as UIMessage[];
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
        <Button className="rounded-full" size="xs" type="button" variant="outline">
          <AlertCircleIcon data-icon="inline-start" />
          {label}
          {isOpen ? <ChevronUpIcon data-icon="inline-end" /> : <ChevronDownIcon data-icon="inline-end" />}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <Alert className="mt-2 max-w-md" variant="destructive">
          <AlertDescription>{message}</AlertDescription>
        </Alert>
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
    <Message>
      <MessageAvatar>
        <Avatar>
          <AvatarFallback>
            <BotIcon />
          </AvatarFallback>
        </Avatar>
      </MessageAvatar>
      <MessageContent>
        <Bubble variant="outline">
          <BubbleContent className="text-muted-foreground flex items-center gap-2">
            <Spinner /> Thinking&hellip;
          </BubbleContent>
        </Bubble>
      </MessageContent>
    </Message>
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
  const [aiSuggestions, setAiSuggestions] = useState<Suggestion[]>([]);
  const [isGeneratingSuggestions, setIsGeneratingSuggestions] = useState(false);
  const lastAssistantMessageIdRef = useRef<string | null>(null);

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
            messages: Array.isArray(parsedBody.messages) ? trimChatMessages(parsedBody.messages) : [],
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
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden">
      {(hydrateError !== null || persistError !== null || error !== undefined) && (
        <div className="mx-auto flex w-full max-w-4xl flex-wrap gap-2 px-4 pt-3">
          {hydrateError !== null && <ErrorBadge label={"Load failed"} message={hydrateError} />}
          {persistError !== null && <ErrorBadge label={"Save failed"} message={persistError} />}
          {error !== undefined && (
            <ErrorBadge label={"Chat failed"} message={error.message || "Failed to generate a response."} />
          )}
        </div>
      )}

      <MessageScrollerProvider autoScroll>
        <MessageScroller className="flex-1">
          <MessageScrollerViewport>
            <MessageScrollerContent className="mx-auto w-full max-w-4xl px-4 py-6">
              {isHydrating ? (
                <div className="flex flex-1 items-center justify-center">
                  <Spinner />
                </div>
              ) : !hasMessages ? (
                <Empty className="mx-auto max-w-xl border-0 py-10">
                  <EmptyHeader>
                    <EmptyMedia variant="icon">
                      <BotIcon />
                    </EmptyMedia>
                    <EmptyTitle>Ask about {groupName}</EmptyTitle>
                    <EmptyDescription>
                      Query tables, summarize patterns, and investigate unusual activity in this log group.
                    </EmptyDescription>
                  </EmptyHeader>
                  <EmptyContent>
                    {STARTER_MESSAGES.map((message) => (
                      <Button
                        className="h-auto w-full justify-start py-3 text-left whitespace-normal"
                        disabled={isLoading}
                        key={message.display}
                        onClick={() => void sendMessage(message.prompt)}
                        variant="outline"
                      >
                        <SparklesIcon data-icon="inline-start" />
                        {message.display}
                      </Button>
                    ))}
                  </EmptyContent>
                </Empty>
              ) : (
                <>
                  {visibleMessages.map((message) => (
                    <MessageScrollerItem key={message.id} scrollAnchor={message.id === visibleMessages.at(-1)?.id}>
                      <ChatMessageItem
                        entryId={entryId}
                        groupName={groupName}
                        message={message}
                        tableNameMap={tableNameMap}
                      />
                    </MessageScrollerItem>
                  ))}
                  {isLoading && !lastMessageIsAssistant && (
                    <MessageScrollerItem scrollAnchor>
                      <ThinkingIndicator />
                    </MessageScrollerItem>
                  )}
                </>
              )}
            </MessageScrollerContent>
          </MessageScrollerViewport>
          <MessageScrollerButton />
        </MessageScroller>
      </MessageScrollerProvider>

      <div className="bg-background shrink-0 px-4 pb-5">
        <div className="mx-auto flex max-w-4xl flex-col gap-3">
          {hasMessages && !isLoading && (isGeneratingSuggestions || aiSuggestions.length > 0) && (
            <div className="flex flex-wrap items-center gap-2">
              {isGeneratingSuggestions && aiSuggestions.length === 0 ? (
                <span className="text-muted-foreground flex items-center gap-2 text-xs">
                  <Spinner /> Generating suggestions&hellip;
                </span>
              ) : (
                aiSuggestions.map((suggestion) => (
                  <Button
                    className="rounded-full"
                    key={suggestion.display}
                    onClick={() => void sendMessage(suggestion.prompt)}
                    size="sm"
                    variant="outline"
                  >
                    <SparklesIcon data-icon="inline-start" />
                    {suggestion.display}
                  </Button>
                ))
              )}
            </div>
          )}

          <form onSubmit={onSubmit}>
            <InputGroup className="bg-background shadow-sm">
              <InputGroupTextarea
                className="max-h-48 min-h-12"
                disabled={isLoading}
                onChange={(event) => setDraftMessage(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void submitMessage();
                  }
                }}
                placeholder="Ask about anomalies, trends, or table insights…"
                ref={inputTextareaRef}
                rows={1}
                value={draftMessage}
              />
              <InputGroupAddon align="block-end">
                <span className="text-xs">Enter to send · Shift+Enter for a new line</span>
                <div className="ml-auto flex items-center gap-1">
                  {hasMessages && (
                    <InputGroupButton
                      disabled={isLoading}
                      onClick={() => void handleClearChat()}
                      size="icon-xs"
                      type="button"
                      variant="ghost"
                    >
                      <EraserIcon />
                      <span className="sr-only">Clear chat</span>
                    </InputGroupButton>
                  )}
                  {isLoading ? (
                    <InputGroupButton onClick={stop} size="icon-sm" type="button" variant="secondary">
                      <Spinner />
                      <span className="sr-only">Stop response</span>
                    </InputGroupButton>
                  ) : (
                    <InputGroupButton disabled={!draftMessage.trim()} size="icon-sm" type="submit" variant="default">
                      <SendHorizontalIcon />
                      <span className="sr-only">Send message</span>
                    </InputGroupButton>
                  )}
                </div>
              </InputGroupAddon>
            </InputGroup>
          </form>
        </div>
      </div>
    </div>
  );
}
