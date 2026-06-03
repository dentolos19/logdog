import {
  MessageScroller as MessageScrollerPrimitive,
  useMessageScroller,
  useMessageScrollerScrollable,
  useMessageScrollerVisibility,
} from "@shadcn/react/message-scroller";
import { ArrowDownIcon } from "lucide-react";
import * as React from "react";

import { Button } from "#/components/ui/button";
import { cn } from "#/lib/utils";

function MessageScrollerProvider(props: React.ComponentProps<typeof MessageScrollerPrimitive.Provider>) {
  return <MessageScrollerPrimitive.Provider {...props} />;
}

function MessageScroller({ className, ...props }: React.ComponentProps<typeof MessageScrollerPrimitive.Root>) {
  return (
    <MessageScrollerPrimitive.Root
      className={cn("group/message-scroller relative flex size-full min-h-0 flex-col overflow-hidden", className)}
      data-slot="message-scroller"
      {...props}
    />
  );
}

function MessageScrollerViewport({
  className,
  ...props
}: React.ComponentProps<typeof MessageScrollerPrimitive.Viewport>) {
  return (
    <MessageScrollerPrimitive.Viewport
      className={cn("size-full min-h-0 min-w-0 overflow-y-auto overscroll-contain", className)}
      data-slot="message-scroller-viewport"
      {...props}
    />
  );
}

function MessageScrollerContent({
  className,
  ...props
}: React.ComponentProps<typeof MessageScrollerPrimitive.Content>) {
  return (
    <MessageScrollerPrimitive.Content
      className={cn("flex h-max min-h-full flex-col gap-6", className)}
      data-slot="message-scroller-content"
      {...props}
    />
  );
}

function MessageScrollerItem({
  className,
  scrollAnchor = false,
  ...props
}: React.ComponentProps<typeof MessageScrollerPrimitive.Item>) {
  return (
    <MessageScrollerPrimitive.Item
      className={cn("min-w-0 shrink-0", className)}
      data-slot="message-scroller-item"
      scrollAnchor={scrollAnchor}
      {...props}
    />
  );
}

function MessageScrollerButton({
  children,
  className,
  direction = "end",
  render,
  size = "icon-sm",
  variant = "secondary",
  ...props
}: React.ComponentProps<typeof MessageScrollerPrimitive.Button> &
  Pick<React.ComponentProps<typeof Button>, "size" | "variant">) {
  return (
    <MessageScrollerPrimitive.Button
      className={cn(
        "absolute inset-s-1/2 -translate-x-1/2 rounded-full shadow-md transition-[translate,scale,opacity] data-[active=false]:pointer-events-none data-[active=false]:translate-y-full data-[active=false]:scale-95 data-[active=false]:opacity-0 data-[direction=end]:bottom-4 data-[direction=start]:top-4 data-[direction=start]:rotate-180",
        className,
      )}
      data-direction={direction}
      data-slot="message-scroller-button"
      direction={direction}
      render={render ?? <Button size={size} variant={variant} />}
      {...props}
    >
      {children ?? (
        <>
          <ArrowDownIcon />
          <span className="sr-only">{direction === "end" ? "Scroll to latest" : "Scroll to start"}</span>
        </>
      )}
    </MessageScrollerPrimitive.Button>
  );
}

export {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
  useMessageScroller,
  useMessageScrollerScrollable,
  useMessageScrollerVisibility,
};
