import React from 'react';
import { FaBasketballBall } from 'react-icons/fa';

const LoadingDots = () => {
  return (
    <div className="flex items-center space-x-2 mt-2 ml-12">
      <div className="flex space-x-1">
        <FaBasketballBall className="text-ball-orange animate-bounce h-3 w-3" style={{ animationDelay: '0ms' }} />
        <FaBasketballBall className="text-ball-orange animate-bounce h-3 w-3" style={{ animationDelay: '300ms' }} />
        <FaBasketballBall className="text-ball-orange animate-bounce h-3 w-3" style={{ animationDelay: '600ms' }} />
      </div>
    </div>
  );
};

export default LoadingDots;
