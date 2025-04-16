import React from 'react';
import { FaUser, FaRobot, FaBasketballBall } from 'react-icons/fa';

const ChatMessage = ({ message }) => {
  const isUser = message.role === 'user';
  
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4`}>
      <div className={`max-w-[80%] ${isUser ? 'message-user' : 'message-assistant'}`}>
        {/* Avatar and message container */}
        <div className="flex items-start">
          {/* Avatar for assistant */}
          {!isUser && (
            <div className="flex-shrink-0 h-10 w-10 rounded-full bg-nba-blue flex items-center justify-center mr-2">
              <FaRobot className="text-white text-lg" />
            </div>
          )}
          
          {/* Message content */}
          <div>
            {/* Message bubble */}
            <div className={`px-4 py-3 rounded-2xl shadow-md ${
              isUser 
                ? 'bg-gradient-to-r from-nba-blue to-lakers-purple text-white' 
                : 'bg-white border border-gray-200'
            }`}>
              {/* Basketball icon for user messages */}
              {isUser && (
                <div className="flex items-center mb-1">
                  <FaBasketballBall className="text-ball-orange mr-2" />
                  <span className="font-bold">You</span>
                </div>
              )}
              
              <p className="text-sm md:text-base whitespace-pre-wrap">{message.content}</p>
            </div>
            
            {/* Sources section for assistant messages */}
            {!isUser && message.sources && message.sources.length > 0 && (
              <div className="mt-2 bg-gray-100 p-2 rounded-lg border border-gray-200 text-xs">
                <p className="font-bold text-nba-blue mb-1">Sources:</p>
                <ul className="list-disc pl-4 space-y-1">
                  {message.sources.map((source, idx) => (
                    <li key={idx} className="text-gray-700">
                      {source.text.slice(0, 80)}...
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
          
          {/* Avatar for user */}
          {isUser && (
            <div className="flex-shrink-0 h-10 w-10 rounded-full bg-lakers-purple flex items-center justify-center ml-2">
              <FaUser className="text-white text-lg" />
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ChatMessage;
