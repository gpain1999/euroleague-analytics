import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './lib/**/*.{js,ts,jsx,tsx,mdx}'
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // Palette dark (inspirée Linear/Vercel)
        bg: {
          DEFAULT: '#0A0A0B',
          card: '#141416',
          elevated: '#1A1A1D',
          hover: '#222226'
        },
        fg: {
          DEFAULT: '#F5F5F5',
          muted: '#A0A0A0',
          subtle: '#6B6B70'
        },
        border: {
          DEFAULT: '#2A2A2E',
          strong: '#3A3A40'
        },
        // Accent EuroLeague
        accent: {
          DEFAULT: '#2E75B6',
          hover: '#3A8AD0',
          muted: '#1C4B75'
        },
        // Sémantique
        success: '#22C55E',
        danger: '#EF4444',
        warning: '#F59E0B'
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Menlo', 'monospace']
      }
    }
  },
  plugins: []
};

export default config;
