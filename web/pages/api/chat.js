import { OpenAI } from "openai";
import clientPromise from "../../utils/mongodb";
import { getRelevantDocs } from "../../utils/vectorSearch";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }
  const { query } = req.body;
  if (!query) {
    return res.status(400).json({ error: "Missing query" });
  }
  try {
    // 1. Vector search for relevant docs
    const docs = await getRelevantDocs(query, 3);
    // 2. Compose system/context prompt
    const context = docs.map((d, i) => `Doc ${i + 1}: ${d.text}`).join("\n");
    // 3. Call OpenAI
    const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    const completion = await openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [
        { role: "system", content: `You are a helpful NBA Insight Assistant providing answers using step-by-step reasoning.\n
          Use the provided context to answer. If the question is complex, break it down into steps and explain each step in detail.\n
          If you need to search the web, use the search function. If you don't know the answer, say so.\n
          Do not add any additional detail that does not directly answer the question unless otherwise told to do so.\n
          If you are asked to provide sources, only provide the sources that are directly relevant to the question.\n
          ` },
        { role: "user", content: `Context:\n${context}\n\nUser: ${query}` }
      ]
    });
    const answer = completion.choices[0].message.content;
    res.status(200).json({ answer, sources: docs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
