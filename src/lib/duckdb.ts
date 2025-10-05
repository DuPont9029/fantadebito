import { AsyncDuckDB, selectBundle, ConsoleLogger } from '@duckdb/duckdb-wasm';

// Minimal DuckDB WASM init: no complex libs, pure HTTP fetch.
let dbInstance: AsyncDuckDB | null = null;

async function initDuckDB(): Promise<AsyncDuckDB> {
  if (dbInstance) return dbInstance;
  const bundle = await selectBundle({
    mvp: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm',
      mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js',
    },
    eh: {
      mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm',
      mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js',
    },
  });

  // Costruisci il Worker da un blob per evitare errori same-origin
  let workerScript = await fetch(bundle.mainWorker!).then((r) => {
    if (!r.ok) throw new Error(`Impossibile caricare worker: ${r.status} ${r.statusText}`);
    return r.text();
  });
  // Rimuovi eventuale riferimento a sourceMappingURL per evitare 404 del dev server
  workerScript = workerScript.replace(/\/\/# sourceMappingURL=.*/g, '');
  const workerBlob = new Blob([workerScript], { type: 'text/javascript' });
  const worker = new Worker(URL.createObjectURL(workerBlob));
  const logger = new ConsoleLogger();
  const db = new AsyncDuckDB(logger, worker);
  // Instantiate WASM module before opening DB
  await db.instantiate(bundle.mainModule);
  await db.open({
    // enable httpfs fallbacks for remote parquet
    filesystem: { allowFullHTTPReads: true },
    allowUnsignedExtensions: true,
  });
  // Enable S3-style HTTP with signed/anonymous URLs; keep it simple via httpfs.
  const conn = await db.connect();
  try {
    await conn.query('INSTALL httpfs');
    await conn.query('LOAD httpfs');
  } finally {
    await conn.close();
  }
  dbInstance = db;
  return dbInstance;
}

type RowObject = Record<string, unknown>;

export async function query(sql: string): Promise<RowObject[]> {
  const db = await initDuckDB();
  const conn = await db.connect();
  try {
    const result = await conn.query(sql);
    // Restituisce direttamente array di oggetti JavaScript
    return result.toArray() as unknown as RowObject[];
  } finally {
    await conn.close();
  }
}

export async function queryFirst(sql: string): Promise<RowObject | null> {
  const rows = await query(sql);
  return rows[0] ?? null;
}

export type User = {
  id: string;
  username: string;
  password: string; // For demo simplicity; in production use hashes.
  wins?: number;
  losses?: number;
  is_admin?: boolean;
};

// Simple auth: reads users from remote Parquet via HTTP.
// URL is provided by env: NEXT_PUBLIC_USERS_PARQUET_URL
export async function authenticate(username: string, password: string): Promise<User | null> {
  let usersParquetUrl: string | undefined = process.env.NEXT_PUBLIC_USERS_PARQUET_URL;
  // Se l'URL non è presente in env, prova a inizializzare lato server e ottenere l'URL pubblico
  if (!usersParquetUrl) {
    try {
      const res = await fetch('/api/init', { method: 'POST' });
      const data = await res.json().catch(() => null);
      const url = data?.readUrl || data?.publicUrl;
      if (res.ok && url) {
        usersParquetUrl = url;
      } else {
        const msg = (data && (data.message || data.error)) || res.statusText || 'init sconosciuta';
        throw new Error(`Init fallita: ${msg}`);
      }
    } catch (e) {
      const err = e as Error;
      throw new Error(`Init fallita: ${err?.message || String(e)}`);
    }
  }
  if (!usersParquetUrl) {
    throw new Error('Config mancante: NEXT_PUBLIC_USERS_PARQUET_URL non impostata e init non ha fornito un URL');
  }
  // Create a temporary view over the remote Parquet; DuckDB supports http.
  const safeUrl = usersParquetUrl.replace(/'/g, "''");
  const sql = `SELECT * FROM read_parquet('${safeUrl}') WHERE username = '${username.replace(/'/g, "''")}' AND password = '${password.replace(/'/g, "''")}';`;
  const row = await queryFirst(sql);
  if (!row) return null;
  return {
    id: String(row.id ?? ''),
    username: String(row.username ?? ''),
    password: String(row.password ?? ''),
    wins: Number(row.wins ?? 0),
    losses: Number(row.losses ?? 0),
    is_admin: Boolean(row.is_admin ?? false),
  };
}

// Helper to read debts bets schema if needed later.
// Rimosso: modello Bet locale. Le scommesse ora sono solo condivise via API.