import { useState } from "react";

export default function Home() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const sendMessage = async (e) => {
    e.preventDefault();
    if (!input.trim()) return;
    const userMsg = { role: "user", content: input };
    setMessages((msgs) => [...msgs, userMsg]);
    setLoading(true);
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: input })
    });
    const data = await res.json();
    setMessages((msgs) => [
      ...msgs,
      { role: "assistant", content: data.answer, sources: data.sources }
    ]);
    setInput("");
    setLoading(false);
  };

  return (
    <main style={{ maxWidth: 600, margin: "40px auto", padding: 24 }}>
      <h1>OpenMuse RAG Chatbot</h1>
      <div style={{ minHeight: 200, marginBottom: 24 }}>
        {messages.map((msg, i) => (
          <div key={i} style={{ margin: "12px 0" }}>
            <b>{msg.role === "user" ? "You" : "Bot"}:</b> {msg.content}
            {msg.sources && (
              <div style={{ fontSize: 12, color: "#666", marginTop: 4 }}>
                <b>Sources:</b>
                <ul>
                  {msg.sources.map((src, j) => (
                    <li key={j}>{src.text.slice(0, 80)}...</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ))}
        {loading && <div>Bot is thinking...</div>}
      </div>
      <form onSubmit={sendMessage} style={{ display: "flex", gap: 8 }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          placeholder="Ask a question..."
          style={{ flex: 1, padding: 8 }}
          disabled={loading}
        />
        <button type="submit" disabled={loading}>Send</button>
      </form>
    </main>
  );
}
