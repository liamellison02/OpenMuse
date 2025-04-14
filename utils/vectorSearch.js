import clientPromise from "./mongodb";
import { OpenAI } from "openai";

// Helper: embed text with OpenAI
export async function embedText(text) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const resp = await openai.embeddings.create({
    model: "text-embedding-ada-002",
    input: text
  });
  return resp.data[0].embedding;
}

// Helper: search MongoDB Atlas for relevant docs
export async function getRelevantDocs(query, k = 3) {
  const db = (await clientPromise).db();
  const collection = db.collection("docs");
  const queryEmbedding = await embedText(query);
  // MongoDB Atlas vector search aggregation
  const pipeline = [
    {
      $vectorSearch: {
        index: "vector_index",
        path: "embedding",
        queryVector: queryEmbedding,
        numCandidates: 50,
        limit: k
      }
    },
    { $project: { text: 1, score: { $meta: "vectorSearchScore" } } }
  ];
  const results = await collection.aggregate(pipeline).toArray();
  return results;
}
