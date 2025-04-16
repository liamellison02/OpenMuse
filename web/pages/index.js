import { useState, useRef, useEffect } from "react";

function parseSteps(answer) {
  if (!answer) return [];
  const lines = answer.split(/\n+/).map(l => l.trim()).filter(Boolean);
  const steps = [];
  let current = "";
  for (let line of lines) {
    if (/^(\d+\.|[-•]) /.test(line)) {
      if (current) steps.push(current);
      current = line.replace(/^(\d+\.|[-•]) /, "");
    } else {
      current += (current ? " " : "") + line;
    }
  }
  if (current) steps.push(current);
  return steps.length > 1 ? steps : [answer];
}

export default function Home() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [streamedSteps, setStreamedSteps] = useState([]);
  const [showSources, setShowSources] = useState(null);
  const chatEndRef = useRef(null);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading, streamedSteps]);

  const sendMessage = async (e) => {
    e.preventDefault();
    if (!input.trim()) return;
    const userMsg = { role: "user", content: input };
    setMessages((msgs) => [...msgs, userMsg]);
    setLoading(true);
    setStreamedSteps([]);
    setShowSources(null);

    let answer = "";
    // Add a placeholder assistant message for streaming
    setMessages((msgs) => [...msgs, { role: "assistant", content: "" }]);
    await fetchEventSourceSSE("/api/stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: input })
    }, (token) => {
      answer += token;
      setMessages((msgs) => {
        const updated = [...msgs];
        // Find last assistant message and update its content
        for (let i = updated.length - 1; i >= 0; --i) {
          if (updated[i].role === "assistant") {
            updated[i] = { ...updated[i], content: answer };
            break;
          }
        }
        return updated;
      });
    }, () => {
      setMessages((msgs) => {
        // After stream, parse steps for the final assistant message
        const updated = [...msgs];
        for (let i = updated.length - 1; i >= 0; --i) {
          if (updated[i].role === "assistant") {
            updated[i] = {
              ...updated[i],
              steps: parseSteps(answer)
            };
            break;
          }
        }
        return updated;
      });
      setLoading(false);
    });
    setInput("");
  };

  // Production-ready SSE polyfill using fetch-event-source
  async function fetchEventSourceSSE(url, options, onToken, onEnd) {
    const { fetchEventSource } = await import("@microsoft/fetch-event-source");
    await fetchEventSource(url, {
      ...options,
      onmessage(ev) {
        if (!ev.data) return;
        try {
          const { token } = JSON.parse(ev.data);
          if (token) onToken(token);
        } catch {}
      },
      onclose() { onEnd(); },
      onerror(err) { onEnd(); },
      openWhenHidden: true
    });
  }

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
        {messages.map((msg, i) =>
          msg.role === "assistant" && msg.steps && msg.steps.length > 1 ? (
            <div key={i} className="bubble bubble-assistant" style={{ alignSelf: "flex-start" }}>
              <div style={{ flex: 1 }}>
                {msg.steps.map((step, j) => (
                  <div key={j} className="step-card">
                    <span className="step-label">Step {j + 1}:</span> {step}
                  </div>
                ))}
                {showSources && i === messages.length - 1 && (
                  <div style={{ fontSize: 12, color: "#666", marginLeft: 8 }}>
                    <b>Sources:</b>
                    <ul style={{ margin: 0, paddingLeft: 16 }}>
                      {showSources.map((src, j) => (
                        <li key={j}>{src.text.slice(0, 90)}{src.text.length > 90 ? "..." : ""}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div key={i} className={`bubble bubble-${msg.role}`} style={{ alignSelf: msg.role === "user" ? "flex-end" : "flex-start" }}>
              <div className="bubble-content">{msg.content}</div>
              {msg.sources && (
                <div style={{ fontSize: 12, color: "#666", marginLeft: 8 }}>
                  <b>Sources:</b>
                  <ul style={{ margin: 0, paddingLeft: 16 }}>
                    {msg.sources.map((src, j) => (
                      <li key={j}>{src.text.slice(0, 90)}{src.text.length > 90 ? "..." : ""}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )
        )}
        {loading && (
          <div className="typing">
            <span>Thinking{Array(3).fill(0).map((_, i) => <span key={i}>.</span>)}</span>
          </div>
        )}
        <div ref={chatEndRef} />
      </div>
      <form onSubmit={sendMessage} style={{ display: "flex", gap: 8 }}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask a question..."
          style={{ flex: 1, padding: 12, borderRadius: 8, border: "1px solid #ccc", fontSize: 16 }}
          disabled={loading}
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          style={{ background: "#0078fe", color: "#fff", border: "none", borderRadius: 8, padding: "0 20px", fontSize: 16, fontWeight: 500, cursor: loading ? "not-allowed" : "pointer" }}
        >
          Send
        </button>
      </form>
      <style>{`
        .typing span {
          animation: blink 1.2s infinite;
        }
        .typing span:nth-child(2) { animation-delay: 0.2s; }
        .typing span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes blink {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.2; }
        }
      `}</style>
    </main>
  );
}
