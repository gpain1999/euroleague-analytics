import { DuckDBInstance, DuckDBConnection } from '@duckdb/node-api';
import path from 'node:path';
import fs from 'node:fs';

const DUCKDB_PATH = process.env.DUCKDB_PATH ?? 'public/data/euroleague.duckdb';

let instancePromise: Promise<DuckDBInstance> | null = null;
let connectionPromise: Promise<DuckDBConnection> | null = null;

async function getConnection(): Promise<DuckDBConnection> {
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
  if (!connectionPromise) {
    const instance = await instancePromise;
    connectionPromise = instance.connect();
  }
  return connectionPromise;
}

/**
 * Exécute une requête SQL et retourne les lignes typées.
 * La connexion est mise en cache au niveau du process.
 *
 * Phase 0 : pas de paramètres liés (pas nécessaire sur la base de démo).
 * Phase 2+ : on ajoutera une version paramétrée typée proprement avec
 * prepared statements pour le moteur de splits.
 */
export async function duckdbQuery<T = Record<string, unknown>>(
  sql: string
): Promise<T[]> {
  const conn = await getConnection();
  const reader = await conn.runAndReadAll(sql);
  return reader.getRowObjects() as T[];
}

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

  return { path: DUCKDB_PATH, tables: withCounts };
}

export class DuckDBNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DuckDBNotFoundError';
  }
}