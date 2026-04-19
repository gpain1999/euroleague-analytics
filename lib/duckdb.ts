import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import path from 'node:path';
import fs from 'node:fs';

/**
 * Accès centralisé à DuckDB.
 *
 * En Phase 0, la base pointe vers un fichier de démo généré par
 * pipeline/scripts/bootstrap_demo_db.py. À partir de la Phase 1, ce fichier
 * sera produit par le pipeline réel et versionné sur la branche `data`.
 *
 * La connexion est mise en cache au niveau du process pour éviter de
 * réouvrir le fichier à chaque requête API. DuckDB supporte un seul writer
 * mais de multiples readers concurrents, et ici l'API est read-only.
 */

const DUCKDB_PATH = process.env.DUCKDB_PATH ?? 'public/data/euroleague.duckdb';

let instancePromise: Promise<DuckDBInstance> | null = null;

async function getInstance(): Promise<DuckDBInstance> {
  if (!instancePromise) {
    const absPath = path.resolve(process.cwd(), DUCKDB_PATH);

    if (!fs.existsSync(absPath)) {
      throw new DuckDBNotFoundError(
        `Base DuckDB introuvable à ${absPath}. ` +
          `Lance d'abord : python pipeline/scripts/bootstrap_demo_db.py`
      );
    }

    instancePromise = DuckDBInstance.create(absPath, {
      access_mode: 'READ_ONLY'
    });
  }
  return instancePromise;
}

/**
 * Exécute une requête SQL et retourne les lignes comme des objets typés.
 *
 * Usage :
 *   const rows = await duckdbQuery<{ season: number; games: number }>(`
 *     SELECT season, COUNT(*) AS games FROM dim_games GROUP BY season
 *   `);
 */
export async function duckdbQuery<T = Record<string, unknown>>(
  sql: string,
  params: unknown[] = []
): Promise<T[]> {
  const instance = await getInstance();
  const conn: DuckDBConnection = await instance.connect();
  try {
    const reader =
      params.length > 0
        ? await conn.runAndReadAll(sql, params)
        : await conn.runAndReadAll(sql);
    return reader.getRowObjects() as T[];
  } finally {
    conn.closeSync();
  }
}

/**
 * Retourne les infos basiques sur la base : tables présentes et nb de lignes
 * de chaque. Utile pour la page admin et le healthcheck.
 */
export async function duckdbInspect(): Promise<{
  path: string;
  tables: Array<{ name: string; rows: number }>;
}> {
  const tables = await duckdbQuery<{ name: string }>(
    `SELECT table_name AS name FROM information_schema.tables
     WHERE table_schema = 'main' ORDER BY name`
  );

  const withCounts = await Promise.all(
    tables.map(async (t) => {
      const rows = await duckdbQuery<{ n: bigint }>(
        `SELECT COUNT(*) AS n FROM "${t.name}"`
      );
      return { name: t.name, rows: Number(rows[0]?.n ?? 0) };
    })
  );

  return {
    path: DUCKDB_PATH,
    tables: withCounts
  };
}

export class DuckDBNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DuckDBNotFoundError';
  }
}
