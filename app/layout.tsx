import type { Metadata } from 'next';
import Link from 'next/link';
import './globals.css';

export const metadata: Metadata = {
  title: {
    default: 'EuroLeague Analytics',
    template: '%s — EuroLeague Analytics'
  },
  description:
    'Plateforme web d\'analyse statistique avancée dédiée à l\'EuroLeague de basket-ball.',
  robots: {
    index: true,
    follow: true
  }
};

export default function RootLayout({
  children
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="fr" className="dark">
      <body>
        <div className="min-h-screen">
          <header className="border-b border-border bg-bg-card/60 backdrop-blur">
            <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-4">
              <Link
                href="/"
                className="flex items-center gap-2 font-semibold tracking-tight"
              >
                <span className="text-accent">EuroLeague</span>
                <span className="text-fg-muted">Analytics</span>
              </Link>
              <nav className="flex items-center gap-6 text-sm text-fg-muted">
                <Link href="/" className="hover:text-fg">
                  Accueil
                </Link>
                <span className="text-fg-subtle">Phase 0</span>
              </nav>
            </div>
          </header>

          <main className="mx-auto max-w-6xl px-4 py-10">{children}</main>

          <footer className="mt-20 border-t border-border py-8">
            <div className="mx-auto max-w-6xl px-4 text-xs text-fg-subtle">
              Données :{' '}
              <a
                href="https://pypi.org/project/euroleague-api/"
                target="_blank"
                rel="noreferrer"
                className="underline hover:text-fg-muted"
              >
                euroleague_api
              </a>{' '}
              (API officielle EuroLeague). Projet non commercial à but
              analytique.
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
