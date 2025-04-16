import React from 'react';
import { FaBasketballBall } from 'react-icons/fa';

const Header = () => {
  return (
    <header className="bg-gradient-to-r from-nba-blue to-nba-red py-6 px-4 shadow-lg rounded-b-lg">
      <div className="container mx-auto flex items-center justify-center">
        <FaBasketballBall className="text-white text-4xl mr-3 animate-spin-slow" />
        <h1 className="text-3xl md:text-4xl font-bold text-white font-scoreboard tracking-wider">
          NBA INSIGHT ASSIST
        </h1>
        <FaBasketballBall className="text-white text-4xl ml-3 animate-spin-slow" />
      </div>
      <p className="text-center text-white mt-2 font-medium">Your AI Assistant for NBA Knowledge</p>
    </header>
  );
};

export default Header;
