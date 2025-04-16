import { useState, useRef, useEffect } from "react";
import { FaBasketballBall, FaSearch } from "react-icons/fa";
import Header from "../components/Header";
import Footer from "../components/Footer";
import ChatMessage from "../components/ChatMessage";
import ChatInput from "../components/ChatInput";
import LoadingDots from "../components/LoadingDots";

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
  const [showWelcome, setShowWelcome] = useState(true);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading, streamedSteps]);

  // Hide welcome message when user sends first message
  useEffect(() => {
    if (messages.length > 0) {
      setShowWelcome(false);
    }
  }, [messages]);

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
    <div className="min-h-screen flex flex-col bg-gradient-to-b from-gray-50 to-gray-100">
      <Header />
      
      <main className="flex-grow container mx-auto px-4 py-6 max-w-4xl">
        {/* Chat container with court background */}
        <div className="relative bg-white rounded-xl shadow-nba overflow-hidden border-2 border-nba-blue">
          {/* Top scoreboard-like bar */}
          <div className="bg-gradient-to-r from-nba-blue to-nba-red p-3 flex justify-between items-center">
            <div className="flex items-center">
              <FaBasketballBall className="text-white mr-2" />
              <span className="text-white font-scoreboard">NBA CHAT</span>
            </div>
            <div className="text-white font-scoreboard">
              {messages.length} MESSAGES
            </div>
          </div>
          
          {/* Messages area with court texture background */}
          <div 
            className="h-[500px] overflow-y-auto p-4 bg-court bg-opacity-10"
            style={{
              backgroundImage: "url('/court-bg.svg')",
              backgroundSize: "cover",
              backgroundPosition: "center",
              backgroundBlendMode: "overlay"
            }}
          >
            {showWelcome && (
              <div className="flex justify-center items-center h-full">
                <div className="bg-white bg-opacity-90 p-6 rounded-xl shadow-lg max-w-md text-center border-2 border-lakers-gold">
                  <FaBasketballBall className="text-ball-orange text-4xl mx-auto mb-4" />
                  <h2 className="text-2xl font-bold text-nba-blue mb-3">Welcome to NBA Insight Assist!</h2>
                  <p className="text-gray-700 mb-4">
                    Your AI assistant for all things NBA. Ask me about players, teams, stats, history, or anything basketball related!
                  </p>
                  <div className="bg-gray-100 p-3 rounded-lg text-left">
                    <p className="text-sm font-medium text-gray-700 mb-2">Try asking:</p>
                    <ul className="text-sm text-gray-600 space-y-1">
                      <li className="flex items-center"><FaSearch className="text-nba-red mr-2" /> Who is the all-time scoring leader?</li>
                      <li className="flex items-center"><FaSearch className="text-nba-red mr-2" /> Compare LeBron James and Michael Jordan</li>
                      <li className="flex items-center"><FaSearch className="text-nba-red mr-2" /> Which team has the most championships?</li>
                    </ul>
                  </div>
                </div>
              </div>
            )}
            
            {messages.map((msg, i) => (
              <ChatMessage key={i} message={msg} />
            ))}
            
            {loading && <LoadingDots />}
            
            <div ref={chatEndRef} />
          </div>
          
          {/* Input area with court-side styling */}
          <div className="p-4 bg-gray-100 border-t-2 border-nba-blue">
            <ChatInput 
              input={input} 
              setInput={setInput} 
              handleSubmit={sendMessage} 
              loading={loading} 
            />
          </div>
        </div>
      </main>
      
      <Footer />
    </div>
  );
}
