import { streamText, type UIMessage } from "ai";
import {
  buildLogDataContext,
  buildSystemPrompt,
  getOpenRouterClient,
  getOpenRouterModel,
  persistMessages,
  toModelMessages,
} from "@/lib/ai";

const MAX_DURATION_SECONDS = 45;
interface RouteContext {
  params: Promise<{ id: string }> | { id: string };
}

export const maxDuration = MAX_DURATION_SECONDS;

export async function POST(request: Request, context: RouteContext) {
  const { id: logGroupId } = await context.params;
  const cookieHeader = request.headers.get("cookie") ?? "";

  const body = (await request.json()) as {
    messages?: UIMessage[];
  };

  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    return Response.json({ message: "A non-empty messages array is required." }, { status: 400 });
  }

  const modelMessages = toModelMessages(body.messages);
  if (modelMessages.length === 0) {
    return Response.json({ message: "No text messages were provided." }, { status: 400 });
  }

  let contextBlock = "No table context is currently available for this log group.";
  try {
    contextBlock = await buildLogDataContext(logGroupId, cookieHeader);
  } catch (error) {
    console.error("Failed to build log data context for chat route.", error);
  }

  try {
    const openrouter = getOpenRouterClient();
    const result = streamText({
      model: openrouter.chat(getOpenRouterModel()),
      system: buildSystemPrompt(contextBlock),
      messages: modelMessages,
    });

    result.consumeStream();

    return result.toUIMessageStreamResponse({
      originalMessages: body.messages,
      onFinish: async ({ messages }) => {
        try {
          await persistMessages(logGroupId, messages, cookieHeader);
        } catch (error) {
          console.error("Failed to persist chat messages.", error);
        }
      },
      onError: () => "Something went wrong while generating the chat response.",
    });
  } catch (error) {
    console.error("Failed to run chat generation.", error);
    return Response.json({ message: "Failed to generate chat response." }, { status: 500 });
  }
}
