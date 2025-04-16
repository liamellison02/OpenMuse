import React from 'react';
import { FaPaperPlane, FaBasketballBall } from 'react-icons/fa';

const ChatInput = ({ input, setInput, handleSubmit, loading }) => {
  return (
    <form 
      onSubmit={handleSubmit}
      className="relative mt-4 bg-white rounded-full shadow-nba border border-gray-200 flex items-center p-1"
    >
      <div className="flex-shrink-0 pl-3">
        <FaBasketballBall className="text-ball-orange h-5 w-5" />
      </div>
      <input
        type="text"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        placeholder="Ask about NBA stats, players, teams..."
        className="flex-grow py-3 px-4 bg-transparent focus:outline-none text-gray-700"
        disabled={loading}
        autoFocus
      />
      <button
        type="submit"
        disabled={loading || !input.trim()}
        className={`flex-shrink-0 rounded-full p-3 flex items-center justify-center transition-colors ${
          loading || !input.trim() 
            ? 'bg-gray-300 cursor-not-allowed' 
            : 'bg-gradient-to-r from-nba-blue to-nba-red hover:from-nba-red hover:to-nba-blue text-white cursor-pointer'
        }`}
      >
        <FaPaperPlane className={loading ? 'opacity-50' : ''} />
      </button>
    </form>
  );
};

export default ChatInput;
