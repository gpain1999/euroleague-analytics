import { duckdbInspect, duckdbQuery, DuckDBNotFoundError } from '@/lib/duckdb';

type Game = {
  season: number;
  gamecode: number;
  date: string;
  home_code: string;
  away_code: string;
  home_score: number;
  away_score: number;
};

async function fetchHealth() {
  try {
    const info = await duckdbInspect();
const sample = await duckdbQuery<{
      season: number;
      gamecode: number;
      date: string;
      home_code: string;
      away_code: string;
      home_score: number;
      away_score: number;
    }>(
      `SELECT season, gamecode,
              CAST(date AS VARCHAR) AS date,
              home_code, away_code, home_score, away_score
       FROM dim_games
       ORDER BY date DESC
       LIMIT 5`
    );
    return { ok: true as const, info, sample };
  } catch (err) {
    if (err instanceof DuckDBNotFoundError) {
      return { ok: false as const, reason: 'not-found' as const, message: err.message };
    }
    return {
      ok: false as const,
      reason: 'error' as const,
      message: err instanceof Error ? err.message : String(err)
    };
  }
}

export default async function HomePage() {
  const health = await fetchHealth();

  return (
    <div className="space-y-8">
      {/* Hero */}
      <section className="card">
        <h1 className="text-3xl font-bold tracking-tight">
          EuroLeague Analytics
        </h1>
        <p className="mt-2 max-w-2xl text-fg-muted">
          Plateforme d&apos;analyse statistique avancée pour l&apos;EuroLeague.
          Ce site est en phase de bootstrap (Phase 0) : pipeline, moteur de
          splits et pages fonctionnelles arrivent progressivement.
        </p>

        <div className="mt-6 flex gap-3">
          {health.ok ? (
            <span className="rounded-md border border-success/40 bg-success/10 px-3 py-1.5 text-xs font-medium text-success">
              ● DuckDB OK
            </span>
          ) : (
            <span className="rounded-md border border-danger/40 bg-danger/10 px-3 py-1.5 text-xs font-medium text-danger">
              ● DuckDB : {health.reason === 'not-found' ? 'base absente' : 'erreur'}
            </span>
          )}
        </div>
      </section>

      {/* Status DB */}
      {health.ok ? (
        <>
          <section className="card">
            <h2 className="text-lg font-semibold">Base de données</h2>
            <p className="mt-1 text-sm text-fg-muted">
              Contenu actuel de <code className="font-mono">{health.info.path}</code>
            </p>
            <div className="mt-4 overflow-hidden rounded-md border border-border">
              <table className="w-full text-sm">
                <thead className="bg-bg-elevated text-left text-fg-muted">
                  <tr>
                    <th className="px-3 py-2 font-medium">Table</th>
                    <th className="px-3 py-2 font-medium">Lignes</th>
                  </tr>
                </thead>
                <tbody>
                  {health.info.tables.map((t) => (
                    <tr key={t.name} className="border-t border-border">
                      <td className="px-3 py-2 font-mono">{t.name}</td>
                      <td className="px-3 py-2 tabular-nums">
                        {t.rows.toLocaleString('fr-FR')}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="card">
            <h2 className="text-lg font-semibold">Derniers matchs (démo)</h2>
            <p className="mt-1 text-sm text-fg-muted">
              Extrait de <code className="font-mono">dim_games</code> pour valider la
              chaîne Next.js → DuckDB.
            </p>
            <div className="mt-4 overflow-hidden rounded-md border border-border">
              <table className="w-full text-sm">
                <thead className="bg-bg-elevated text-left text-fg-muted">
                  <tr>
                    <th className="px-3 py-2 font-medium">Date</th>
                    <th className="px-3 py-2 font-medium">Saison</th>
                    <th className="px-3 py-2 font-medium">Match</th>
                    <th className="px-3 py-2 font-medium">Score</th>
                  </tr>
                </thead>
                <tbody>
                  {health.sample.map((g) => (
                    <tr
                      key={`${g.season}-${g.gamecode}`}
                      className="border-t border-border"
                    >
                      <td className="px-3 py-2 tabular-nums text-fg-muted">{g.date}</td>
                      <td className="px-3 py-2 tabular-nums">{g.season}-{(g.season + 1) % 100}</td>
                      <td className="px-3 py-2 font-mono">
                        {g.home_code} <span className="text-fg-subtle">vs</span> {g.away_code}
                      </td>
                      <td className="px-3 py-2 tabular-nums">
                        {g.home_score} - {g.away_score}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : (
<section className="card border-danger/40">
          <h2 className="text-lg font-semibold">
            {health.reason === 'not-found'
              ? 'Base de données introuvable'
              : 'Erreur DuckDB'}
          </h2>
          <p className="mt-2 text-sm text-fg-muted">{health.message}</p>
          {health.reason === 'not-found' && (
            <pre className="mt-4 overflow-x-auto rounded-md border border-border bg-bg-elevated p-4 font-mono text-xs">
{`# Génère une base DuckDB de démonstration :
python pipeline/scripts/bootstrap_demo_db.py`}
            </pre>
          )}
        </section>
      )}

      {/* Roadmap */}
      <section className="card">
        <h2 className="text-lg font-semibold">Feuille de route</h2>
        <ol className="mt-3 space-y-1 text-sm text-fg-muted">
          <li>
            <span className="mr-2 text-success">✓</span>Phase 0 — Setup
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 1 — Pipeline d&apos;ingestion
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 2 — Moteur de splits
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 3 — Pages de base
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 4 — Pages avancées
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 5 — Polish
          </li>
          <li>
            <span className="mr-2 text-fg-subtle">○</span>Phase 6 — Déploiement final
          </li>
        </ol>
      </section>
    </div>
  );
}
