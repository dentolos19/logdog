import { BotIcon, MessageCircleIcon, SendHorizontalIcon, UserIcon, XIcon } from "lucide-react";
import { type FormEvent, useCallback, useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import { Button } from "#/components/ui/button";
import { ScrollArea } from "#/components/ui/scroll-area";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "#/components/ui/sheet";
import { Skeleton } from "#/components/ui/skeleton";
import { Spinner } from "#/components/ui/spinner";
import { Textarea } from "#/components/ui/textarea";
import { type ChatHistoryMessage, streamChatWithLogs } from "#/lib/server";

type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  content: string;
  isStreaming?: boolean;
};

type LogChatPanelProps = {
  entryId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
};

export function LogChatPanel({ entryId, open, onOpenChange }: LogChatPanelProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);
  const [abortController, setAbortController] = useState<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const scrollToBottom = useCallback(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  const handleSend = useCallback(async () => {
    const trimmed = draft.trim();
    if (trimmed.length === 0 || isStreaming) {
      return;
    }

    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content: trimmed,
    };

    const assistantMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: "assistant",
      content: "",
      isStreaming: true,
    };

    setMessages((prev) => [...prev, userMessage, assistantMessage]);
    setDraft("");
    setIsStreaming(true);

    const controller = new AbortController();
    setAbortController(controller);

    const history: ChatHistoryMessage[] = messages.map((msg) => ({
      role: msg.role,
      content: msg.content,
    }));

    try {
      let accumulated = "";
      for await (const token of streamChatWithLogs(entryId, trimmed, history, controller.signal)) {
        accumulated += token;
        setMessages((prev) => {
          const last = prev[prev.length - 1];
          if (last && last.role === "assistant") {
            return [...prev.slice(0, -1), { ...last, content: accumulated }];
          }
          return prev;
        });
      }

      setMessages((prev) => {
        const last = prev[prev.length - 1];
        if (last && last.role === "assistant") {
          return [...prev.slice(0, -1), { ...last, isStreaming: false }];
        }
        return prev;
      });
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        return;
      }

      const message = error instanceof Error ? error.message : "Chat failed.";
      toast.error(message);
      setMessages((prev) => {
        const last = prev[prev.length - 1];
        if (last && last.role === "assistant") {
          return [...prev.slice(0, -1), { ...last, content: `Error: ${message}`, isStreaming: false }];
        }
        return prev;
      });
    } finally {
      setIsStreaming(false);
      setAbortController(null);
    }
  }, [draft, entryId, isStreaming, messages]);

  const handleSubmit = (event: FormEvent) => {
    event.preventDefault();
    void handleSend();
  };

  const handleStop = useCallback(() => {
    if (abortController) {
      abortController.abort();
    }
  }, [abortController]);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      void handleSend();
    }
  };

  return (
    <Sheet onOpenChange={onOpenChange} open={open}>
      <SheetContent className={"flex h-dvh w-full flex-col sm:max-w-lg"} showCloseButton={false} side={"right"}>
        <SheetHeader className={"shrink-0 border-b px-4 pt-4 pb-4"}>
          <div className={"flex items-center justify-between"}>
            <SheetTitle className={"flex items-center gap-2 text-base"}>
              <BotIcon className={"size-5 text-primary"} />
              Chat with Logs
            </SheetTitle>
            <Button onClick={() => onOpenChange(false)} size={"icon-sm"} variant={"ghost"}>
              <XIcon />
            </Button>
          </div>
        </SheetHeader>

        <div className={"flex min-h-0 flex-1 flex-col"}>
          {messages.length === 0 ? (
            <div className={"flex flex-1 flex-col items-center justify-center gap-4 px-4 text-center"}>
              <div className={"flex size-12 items-center justify-center rounded-2xl bg-primary/10"}>
                <MessageCircleIcon className={"size-6 text-primary"} />
              </div>
              <div className={"flex flex-col gap-1"}>
                <p className={"font-medium text-sm"}>Ask about your logs</p>
                <p className={"text-muted-foreground text-xs"}>
                  Get insights, find anomalies, or summarize patterns in your log data.
                </p>
              </div>
            </div>
          ) : (
            <ScrollArea className={"flex-1 px-4"} ref={scrollRef}>
              <div className={"flex flex-col gap-4 py-4"}>
                {messages.map((message) => (
                  <ChatBubble key={message.id} message={message} />
                ))}
                {isStreaming && messages[messages.length - 1]?.role === "assistant" && (
                  <div className={"flex items-center gap-2 self-start rounded-lg bg-muted px-3 py-2"}>
                    <Skeleton className={"h-3 w-16"} />
                  </div>
                )}
              </div>
            </ScrollArea>
          )}

          <form className={"shrink-0 border-t px-4 pt-4 pb-4"} onSubmit={handleSubmit}>
            <div className={"relative"}>
              <Textarea
                className={"min-h-[80px] resize-none pr-12"}
                disabled={isStreaming}
                onChange={(event) => setDraft(event.target.value)}
                onKeyDown={handleKeyDown}
                placeholder={"Ask a question about your logs..."}
                ref={textareaRef}
                rows={3}
                value={draft}
              />
              <div className={"absolute right-2 bottom-2 flex items-center gap-1"}>
                {isStreaming ? (
                  <Button onClick={handleStop} size={"icon-sm"} type={"button"} variant={"secondary"}>
                    <Spinner className={"size-3"} />
                  </Button>
                ) : (
                  <Button disabled={draft.trim().length === 0} size={"icon-sm"} type={"submit"} variant={"default"}>
                    <SendHorizontalIcon className={"size-4"} />
                  </Button>
                )}
              </div>
            </div>
            <p className={"text-muted-foreground text-xs"}>Press Enter to send, Shift+Enter for new line.</p>
          </form>
        </div>
      </SheetContent>
    </Sheet>
  );
}

function ChatBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";

  return (
    <div className={`flex gap-3 ${isUser ? "flex-row-reverse" : "flex-row"}`}>
      <div
        className={`flex size-8 shrink-0 items-center justify-center rounded-full ${
          isUser ? "bg-primary text-primary-foreground" : "bg-muted"
        }`}
      >
        {isUser ? <UserIcon className={"size-4"} /> : <BotIcon className={"size-4"} />}
      </div>
      <div
        className={`max-w-[80%] rounded-lg px-3 py-2 text-sm ${
          isUser ? "bg-primary text-primary-foreground" : "bg-muted"
        }`}
      >
        <div className={"whitespace-pre-wrap leading-relaxed"}>{message.content || " "}</div>
      </div>
    </div>
  );
}
