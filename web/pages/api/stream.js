import { OpenAI } from "openai";
import { getRelevantDocs } from "../../utils/vectorSearch";

export const config = {
  api: {
    bodyParser: false,
  },
};

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).end();
      return;
    }
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");

    let body = "";
    req.on("data", chunk => { body += chunk; });
    req.on("end", async () => {
      try {
        let query;
        try {
          query = JSON.parse(body).query;
        } catch {
          res.write(`event: error\ndata: Invalid body\n\n`);
          res.end();
          return;
        }
        if (!query) {
          res.write(`event: error\ndata: Missing query\n\n`);
          res.end();
          return;
        }
        try {
          const docs = await getRelevantDocs(query, 5);
          const context = docs.map((d, i) => `Doc ${i + 1}: ${d.text}`).join("\n");
          const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
          const messages = [
            { role: "system", content: `You are an assistant providing step-by-step reasoning and answers.\n
                Use the provided context to answer. If the question is complex, break it down into steps and explain each step in detail.\n
                If you need to search the web, use the search function. If you don't know the answer, say so.\n
                Do not add any additional detail that does not directly answer the question unless otherwise told to do so.\n
                If you are asked to provide sources, only provide the sources that are directly relevant to the question.\n
                ` },
            { role: "user", content: `Context:\n${context}\n\nUser: ${query}` }
          ];
          const stream = await openai.chat.completions.create({
            model: "gpt-4.1-mini",
            messages,
            stream: true,
            temperature: 0.3
          });
          let buffer = "";
          for await (const chunk of stream) {
            const token = chunk.choices?.[0]?.delta?.content || "";
            if (!token) continue;
            buffer += token;
            res.write(`data: ${JSON.stringify({ token })}\n\n`);
          }
          res.write(`event: end\ndata: {"done":true}\n\n`);
          res.end();
        } catch (err) {
          res.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`);
          res.end();
        }
      } catch (err) {
        res.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
      }
    });
  } catch (err) {
    res.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`);
    res.end();
  }
}
