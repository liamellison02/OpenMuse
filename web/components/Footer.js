import React from 'react';
import { FaGithub, FaBasketballBall } from 'react-icons/fa';

const Footer = () => {
  return (
    <footer className="mt-8 py-4 text-center text-sm text-gray-500">
      <div className="flex items-center justify-center space-x-2">
        <FaBasketballBall className="text-ball-orange" />
        <span>NBA Insight Assist - Powered by OpenMuse RAG</span>
        <FaBasketballBall className="text-ball-orange" />
      </div>
      <div className="mt-2 flex items-center justify-center">
        <a 
          href="https://github.com/liamellison02/OpenMuse" 
          target="_blank" 
          rel="noopener noreferrer"
          className="text-nba-blue hover:text-nba-red transition-colors flex items-center"
        >
          <FaGithub className="mr-1" /> View on GitHub
        </a>
      </div>
    </footer>
  );
};

export default Footer;
