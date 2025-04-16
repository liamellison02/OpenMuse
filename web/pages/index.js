import { useState, useRef, useEffect } from "react";

export default function Home() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const chatEndRef = useRef(null);

  // NBA Team colors - Lakers purple and gold
  const nbaColors = {
    primary: "#552583", // Lakers purple
    secondary: "#FDB927", // Lakers gold
    dark: "#000000",
    light: "#ffffff",
    lightGray: "#f0f0f0",
    courtColor: "#f9f3e8" // Basketball court color
  };

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
    <main style={{ 
      maxWidth: 800, 
      margin: "20px auto", 
      padding: 24,
      fontFamily: "'Roboto', 'Helvetica Neue', sans-serif",
      backgroundColor: nbaColors.light, 
      borderRadius: 12,
      boxShadow: "0 4px 20px rgba(0,0,0,0.08)"
    }}>
      <h1 style={{ 
        textAlign: "center", 
        marginBottom: 24, 
        color: nbaColors.primary,
        fontSize: 32,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "1px",
        textShadow: `1px 1px 1px ${nbaColors.secondary}`
      }}>
        OpenMuse RAG Chatbot
      </h1>
      
      <div style={{
        position: "relative",
        background: nbaColors.courtColor,
        backgroundImage: "repeating-linear-gradient(#e5dfd5 0px, #e5dfd5 1px, transparent 1px, transparent 10px)",
        borderRadius: 16,
        minHeight: 400,
        maxHeight: 500,
        overflowY: "auto",
        padding: 20,
        marginBottom: 24,
        boxShadow: "inset 0 2px 12px rgba(0,0,0,0.1)",
        border: `3px solid ${nbaColors.primary}`,
        display: "flex",
        flexDirection: "column"
      }}>
        {/* Center court line */}
        <div style={{
          position: "absolute",
          left: 0,
          right: 0,
          top: "50%",
          height: 2,
          backgroundColor: nbaColors.primary,
          opacity: 0.3,
          zIndex: 0
        }} />
        
        {/* Center court circle */}
        <div style={{
          position: "absolute",
          left: "50%",
          top: "50%",
          width: 100,
          height: 100,
          borderRadius: "50%",
          border: `2px solid ${nbaColors.primary}`,
          transform: "translate(-50%, -50%)",
          opacity: 0.3,
          zIndex: 0
        }} />
        
        {messages.map((msg, i) => (
          <div
            key={i}
            style={{
              alignSelf: msg.role === "user" ? "flex-end" : "flex-start",
              maxWidth: "80%",
              margin: "10px 0",
              display: "flex",
              flexDirection: "column",
              gap: 4,
              zIndex: 1
            }}
          >
            <div
              style={{
                background: msg.role === "user" ? nbaColors.primary : nbaColors.secondary,
                color: msg.role === "user" ? nbaColors.light : nbaColors.dark,
                borderRadius: msg.role === "user" ? "18px 18px 4px 18px" : "18px 18px 18px 4px",
                padding: "14px 18px",
                fontSize: 16,
                fontWeight: 500,
                boxShadow: msg.role === "user"
                  ? `0 2px 10px rgba(85, 37, 131, 0.3)`
                  : `0 2px 10px rgba(253, 185, 39, 0.3)`,
                wordBreak: "break-word"
              }}
            >
              {msg.content}
            </div>
            {msg.sources && (
              <div style={{ 
                fontSize: 13, 
                color: "#555", 
                marginLeft: 8,
                backgroundColor: "rgba(255,255,255,0.7)",
                padding: "6px 10px",
                borderRadius: 8,
                marginTop: 4
              }}>
                <b>Game Highlights:</b>
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
              color: "#555",
              fontStyle: "italic",
              margin: "8px 0 0 0",
              padding: "8px 12px",
              backgroundColor: "rgba(255,255,255,0.7)",
              borderRadius: 12,
              zIndex: 1
            }}
          >
            Coach is drawing up a play...
          </div>
        )}
        <div ref={chatEndRef} />
      </div>
      <form onSubmit={sendMessage} style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <input
          value={input}
          onChange={e => setInput(e.target.value)}
          placeholder="Ask about your favorite team or player..."
          style={{
            flex: 1,
            padding: "15px 18px",
            borderRadius: 24,
            border: `2px solid ${nbaColors.primary}`,
            fontSize: 16,
            outline: "none",
            boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
            transition: "all 0.2s",
            backgroundColor: nbaColors.lightGray
          }}
          disabled={loading}
          autoFocus
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          style={{
            padding: "15px 30px",
            borderRadius: 24,
            background: loading || !input.trim() ? "#999" : nbaColors.primary,
            color: nbaColors.light,
            border: "none",
            fontWeight: 600,
            fontSize: 16,
            cursor: loading || !input.trim() ? "not-allowed" : "pointer",
            transition: "background 0.2s",
            boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
            textTransform: "uppercase",
            letterSpacing: "0.5px"
          }}
        >
          Shoot
        </button>
      </form>
    </main>
  );
}
