import { useState, useRef, useEffect } from "react";
import { FaBasketballBall, FaSearch } from "react-icons/fa";
import Header from "../components/Header";
import Footer from "../components/Footer";
import ChatMessage from "../components/ChatMessage";
import ChatInput from "../components/ChatInput";
import LoadingDots from "../components/LoadingDots";

export default function Home() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const chatEndRef = useRef(null);
  const [showWelcome, setShowWelcome] = useState(true);

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

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
