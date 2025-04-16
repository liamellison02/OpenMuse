/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // NBA theme colors
        'nba-blue': '#17408B',     // NBA official blue
        'nba-red': '#C9082A',      // NBA official red
        'lakers-purple': '#552583', // Lakers purple
        'lakers-gold': '#FDB927',   // Lakers gold
        'court': '#E8B276',        // Basketball court wood color
        'ball-orange': '#E35205',  // Basketball orange
      },
      backgroundImage: {
        'court-pattern': "url('/court-bg.svg')",
      },
      fontFamily: {
        'scoreboard': ['Orbitron', 'sans-serif'],
      },
      boxShadow: {
        'nba': '0 4px 14px 0 rgba(0, 0, 0, 0.2)',
      },
      animation: {
        'spin-slow': 'spin 3s linear infinite',
      },
    },
  },
  plugins: [],
}
