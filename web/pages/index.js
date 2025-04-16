import { useState, useRef, useEffect } from "react";

export default function Home() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const chatEndRef = useRef(null);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

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
      <h1 style={{ textAlign: "center", marginBottom: 24 }}>OpenMuse RAG Chatbot</h1>
      <div
        style={{
          background: "#f7f7fa",
          borderRadius: 16,
          minHeight: 320,
          maxHeight: 480,
          overflowY: "auto",
          padding: 16,
          marginBottom: 24,
          boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
          display: "flex",
          flexDirection: "column"
        }}
      >
        {messages.map((msg, i) => (
          <div
            key={i}
            style={{
              alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
              maxWidth: "80%",
              margin: "8px 0",
              display: "flex",
              flexDirection: "column",
              gap: 4
            }}
          >
            <div
              style={{
                background: msg.role === "user" ? "#0078fe" : "#fff",
                color: msg.role === "user" ? "#fff" : "#222",
                borderRadius: msg.role === "user" ? "18px 18px 4px 18px" : "18px 18px 18px 4px",
                padding: "12px 16px",
                fontSize: 16,
                boxShadow: msg.role === "user"
                  ? "0 2px 6px rgba(0,120,254,0.08)"
                  : "0 2px 6px rgba(0,0,0,0.04)",
                wordBreak: "break-word"
              }}
            >
              {msg.content}
            </div>
            {msg.sources && (
              <div style={{ fontSize: 12, color: "#666", marginLeft: 8 }}>
                <b>Sources:</b>
                <ul style={{ margin: 0, paddingLeft: 16 }}>
                  {msg.sources.map((src, j) => (
                    <li key={j}>{src.text.slice(0, 80)}...</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ))}
        {loading && (
          <div
            style={{
              alignSelf: "flex-start",
              color: "#888",
              fontStyle: "italic",
              margin: "8px 0 0 0"
            }}
          >
            Bot is thinking...
          </div>
        )}
        <div ref={chatEndRef} />
      </div>
      <form onSubmit={sendMessage} style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          placeholder="Ask a question..."
          style={{
            flex: 1,
            padding: "12px 14px",
            borderRadius: 24,
            border: "1px solid #ddd",
            fontSize: 16,
            outline: "none",
            boxShadow: "0 1px 2px rgba(0,0,0,0.03)",
            transition: "border 0.2s"
          }}
          disabled={loading}
          autoFocus
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          style={{
            padding: "10px 24px",
            borderRadius: 24,
            background: loading || !input.trim() ? "#ccc" : "#0078fe",
            color: "#fff",
            border: "none",
            fontWeight: 600,
            fontSize: 16,
            cursor: loading || !input.trim() ? "not-allowed" : "pointer",
            transition: "background 0.2s"
          }}
        >
          Send
        </button>
      </form>
    </main>
  );
}
