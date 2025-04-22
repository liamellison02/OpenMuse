import { convertToCoreMessages, streamText } from 'ai';
import type { CoreMessage } from 'ai';
import { registry } from '@/lib/ai-config';
import { retrieveContext } from '@/lib/rag';

export const maxDuration = 60;

export async function POST(req: Request) {
  const { messages }: { messages: CoreMessage[] } = await req.json();

  const lastUserMessage = messages[messages.length - 1];
  const query = lastUserMessage?.content;

  if (!query || typeof query !== 'string') {
    return new Response('Invalid query', { status: 400 });
  }

  const context = await retrieveContext(query);

  const systemPrompt: CoreMessage = {
    role: 'system',
    content: `You are a helpful assistant for all NBA related questions. Answer the user's question based on the following context.
    Reason step-by-step before providing the final answer.
    Context:
    ---
    ${context}
    ---
    User Question: ${query}
Answer:`,
  };

  const messagesWithContext: CoreMessage[] = [
    systemPrompt,
  ];

  const result = await streamText({
    model: registry.languageModel('openrouter:openai/gpt-4o-mini'), 
    messages: messagesWithContext, 
    temperature: 0.7,
    maxTokens: 1024,
    async onFinish({ text }) {
      console.log('Stream finished. Final text:', text);
    },
  });

  return result.toDataStreamResponse();
}
