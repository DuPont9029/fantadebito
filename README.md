Fantadebito — scommesse scolastiche con DuckDB WASM e Parquet remoto

Obiettivo
- UI minimale per login e scommesse su M.N. (stile fantamorto).
- Autenticazione letta da `users.parquet` ospitato su Cubbit S3 (link HTTP/HTTPS).
- Nessun backend: tutto gira in browser usando DuckDB WASM + httpfs.

Prerequisiti
- Node 18+ e pnpm installati.
- Un URL pubblico al file `users.parquet` (schema minimo: `id`, `username`, `password`) configurato in env.
  - Crea `.env.local` e imposta: `NEXT_PUBLIC_USERS_PARQUET_URL=https://<cubbit-bucket>/<path>/users.parquet`

Avvio
1. Installa dipendenze: `pnpm install`
2. Configura `.env.local` come sopra
3. Avvia dev server: `pnpm dev`
4. Apri `http://localhost:3000` e inserisci le tue credenziali

Tecnico
- DuckDB WASM inizializzato via `selectBundle` con worker e modulo serviti da jsDelivr.
- `httpfs` viene installato e caricato per leggere Parquet su HTTP/S, con fallback ai full reads.
- Scommesse salvate in `localStorage` per semplicità.

Note e assunzioni
- Le password sono in chiaro nel parquet solo per demo. In produzione usare hash.
- Il bucket deve supportare CORS per richieste dal browser al file Parquet.
- Se il server non supporta range requests, è abilitato il fallback ai full HTTP reads.

Licenza
- Prototipo didattico. Usa a tuo rischio.
