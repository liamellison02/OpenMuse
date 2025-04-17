# OpenMuse AI

A simple Next.js Retrieval-Augmented Generation chatbot.

## Getting Started

1. **Install dependencies**
   ```bash
   cd web
   npm install
   ```

2. **Set up environment variables**
   - Copy `.env.local` and add your keys:
     ```
     OPENAI_API_KEY=your-openai-api-key
     MONGODB_URI=your-mongodb-uri
     ```

3. **Run locally**
   ```bash
   cd web
   npm run dev
   ```
   App runs at [http://localhost:3000](http://localhost:3000)

## File Structure
- `pages/index.js` – Chat UI
- `pages/api/stream.js` – RAG API route
- `utils/mongodb.js` – MongoDB connection helper
- `utils/vectorSearch.js` – Embedding & vector search

---

MIT License
