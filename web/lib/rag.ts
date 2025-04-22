import { MongoClient } from 'mongodb';
import { MongoDBAtlasVectorSearch } from '@langchain/mongodb';
import { OpenAIEmbeddings } from '@langchain/openai';

const client = new MongoClient(process.env.MONGODB_URI || '');
const namespace = 'openmuse.nba';
const [dbName, collectionName] = namespace.split('.');
const collection = client.db(dbName).collection(collectionName);

if (!process.env.OPENAI_API_KEY || !process.env.MONGODB_URI) {
  throw new Error('Missing required environment variables: OPENAI_API_KEY and MONGODB_URI');
}

const vectorStore = new MongoDBAtlasVectorSearch(
  new OpenAIEmbeddings({
    modelName: 'text-embedding-ada-002',
  }),
  {
    collection: collection,
    indexName: 'vector_index',
    textKey: 'text',
    embeddingKey: 'embedding',
  }
);

/**
 * Retrieves relevant context from MongoDB Atlas Vector Search based on the user query.
 *
 * @param query The user's query string.
 * @param k The number of documents to retrieve (default: 4).
 * @returns A promise that resolves to the formatted context string.
 */
export async function retrieveContext(query: string, k = 4): Promise<string> {
  console.log(`Retrieving context for query: "${query}" from MongoDB`);
  try {
    const results = await vectorStore.similaritySearch(query, k);

    const formattedContext = results
      .map((doc) => doc.pageContent)
      .join('\n\n---\n\n');

    console.log(`Retrieved context: ${results.length} documents found.`);
    return formattedContext;
  } catch (error) {
    console.error('Error retrieving context from MongoDB:', error);
    return 'Error retrieving context. Please check the logs.';
  } finally {
    await client.close();
  }
}
