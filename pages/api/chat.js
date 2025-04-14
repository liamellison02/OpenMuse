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
      model: "gpt-3.5-turbo",
      messages: [
        { role: "system", content: "You are a helpful assistant. Use the provided context to answer." },
        { role: "user", content: `Context:\n${context}\n\nUser: ${query}` }
      ]
    });
    const answer = completion.choices[0].message.content;
    res.status(200).json({ answer, sources: docs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
