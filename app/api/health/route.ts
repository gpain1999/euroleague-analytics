import { NextResponse } from 'next/server';
import { duckdbInspect, DuckDBNotFoundError } from '@/lib/duckdb';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET() {
  const started = Date.now();
  try {
    const info = await duckdbInspect();
    return NextResponse.json({
      status: 'ok',
      duckdb: info,
      elapsed_ms: Date.now() - started
    });
  } catch (err) {
    if (err instanceof DuckDBNotFoundError) {
      return NextResponse.json(
        {
          status: 'database_missing',
          message: err.message
        },
        { status: 503 }
      );
    }
    return NextResponse.json(
      {
        status: 'error',
        message: err instanceof Error ? err.message : String(err)
      },
      { status: 500 }
    );
  }
}
