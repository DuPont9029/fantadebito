import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetReader, ParquetSchema, ParquetWriter } from "parquetjs-lite";

export const runtime = "nodejs";

function getEnv(name: string, optional = false): string | undefined {
  const v = process.env[name];
  if (!v && !optional) throw new Error(`Env mancante: ${name}`);
  return v;
}

function getS3Client() {
  const endpoint = getEnv("S3_ENDPOINT", true) || "https://s3.cubbit.eu";
  const region = getEnv("S3_REGION", true) || "us-east-1";
  const accessKeyId = getEnv("S3_ACCESS_KEY_ID")!;
  const secretAccessKey = getEnv("S3_SECRET_ACCESS_KEY")!;
  const forcePathStyle = (getEnv("S3_FORCE_PATH_STYLE", true) || "false").toLowerCase() === "true";
  return new S3Client({ region, endpoint, forcePathStyle, credentials: { accessKeyId, secretAccessKey } });
}

type Row = {
  id: string;
  owner_id: string;
  subject: string;
  esito: string;
  sospensione_json: string;
  invite_code: string;
  participants_json: string;
  created_at: string;
  terminated_at: string;
  realized: string;
};

const schema = new ParquetSchema({
  id: { type: "UTF8" },
  owner_id: { type: "UTF8" },
  subject: { type: "UTF8" },
  esito: { type: "UTF8" },
  sospensione_json: { type: "UTF8" },
  invite_code: { type: "UTF8" },
  participants_json: { type: "UTF8" },
  created_at: { type: "UTF8" },
  terminated_at: { type: "UTF8" },
  realized: { type: "UTF8" },
});

type UserRow = { id: string; username: string; password: string; wins: number; losses: number; is_admin: boolean };
const userSchema = new ParquetSchema({
  id: { type: "UTF8" },
  username: { type: "UTF8" },
  password: { type: "UTF8" },
  wins: { type: "INT32" },
  losses: { type: "INT32" },
  is_admin: { type: "BOOLEAN" },
});

async function readAll(stream: any): Promise<Buffer> {
  const chunks: Buffer[] = [];
  return await new Promise((resolve, reject) => {
    stream.on("data", (chunk: any) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

export async function POST(req: Request) {
  try {
    const { userId, betId } = await req.json().catch(() => ({ userId: "", betId: "" }));
    if (!userId || !betId) {
      return NextResponse.json({ status: "error", message: "userId e betId richiesti" }, { status: 400 });
    }

    // Verifica permesso admin sull'utente
    const bucketUsers = getEnv("S3_BUCKET")!;
    const prefixUsers = getEnv("S3_PREFIX", true) || "";
    const keyUsers = `${prefixUsers}users.parquet`;
    const s3Users = getS3Client();
    try {
      await s3Users.send(new HeadObjectCommand({ Bucket: bucketUsers, Key: keyUsers }));
      const objUsers = await s3Users.send(new GetObjectCommand({ Bucket: bucketUsers, Key: keyUsers }));
      const bodyUsers = await readAll(objUsers.Body as any);
      const readerUsers = await ParquetReader.openBuffer(bodyUsers);
      const cursorUsers = readerUsers.getCursor();
      let isAdmin = false;
      for (;;) {
        const rec = await cursorUsers.next();
        if (!rec) break;
        if (String(rec.id) === String(userId)) {
          isAdmin = Boolean(rec.is_admin ?? false);
          break;
        }
      }
      await readerUsers.close();
      if (!isAdmin) {
        return NextResponse.json({ status: "error", message: "Solo admin può eliminare" }, { status: 403 });
      }
    } catch (e) {
      return NextResponse.json({ status: "error", message: "Verifica admin fallita" }, { status: 500 });
    }

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}bets.parquet`;
    const s3 = getS3Client();

    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await readAll(obj.Body as any);
    const reader = await ParquetReader.openBuffer(body);
    const cursor = reader.getCursor();
    const rows: Row[] = [];
    for (;;) {
      const rec = await cursor.next();
      if (!rec) break;
      rows.push({
        id: String(rec.id),
        owner_id: String(rec.owner_id),
        subject: String(rec.subject),
        esito: String(rec.esito ?? "ammissione"),
        sospensione_json: String(rec.sospensione_json ?? "[]"),
        invite_code: String(rec.invite_code),
        participants_json: String(rec.participants_json),
        created_at: String(rec.created_at),
        terminated_at: String(rec.terminated_at ?? ""),
        realized: String(rec.realized ?? ""),
      });
    }
    await reader.close();

    const target = rows.find((r) => r.id === String(betId));
    if (!target) return NextResponse.json({ status: "error", message: "Bet non trovata" }, { status: 404 });
    const kept = rows.filter((r) => r.id !== String(betId));

    // Se la scommessa era già terminata, storniamo i contatori W/L degli utenti coinvolti
    if (String(target.terminated_at)) {
      const realizedStr = String(target.realized);
      if (realizedStr === "true" || realizedStr === "false") {
        // Calcola winners/losers come nella terminazione
        let participants: { userId: string; stance?: string }[] = [];
        try {
          const raw = JSON.parse(target.participants_json || "[]");
          participants = Array.isArray(raw)
            ? raw.map((p: any) => (typeof p === "string" ? { userId: String(p) } : { userId: String(p.userId || ""), stance: p.stance }))
            : [];
        } catch (_) {
          participants = [];
        }
        const winners = participants
          .filter((p) => (realizedStr === "true" ? p.stance === "favorevole" : p.stance === "contrario"))
          .map((p) => p.userId);
        const losers = participants
          .filter((p) => (realizedStr === "true" ? p.stance === "contrario" : p.stance === "favorevole"))
          .map((p) => p.userId);

        // Leggi utenti
        const usersKey = `${prefix}users.parquet`;
        await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: usersKey }));
        const usersObj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: usersKey }));
        const usersBody = await readAll(usersObj.Body as any);
        const userReader = await ParquetReader.openBuffer(usersBody);
        const userCursor = userReader.getCursor();
        const users: UserRow[] = [];
        for (;;) {
          const rec = await userCursor.next();
          if (!rec) break;
          users.push({
            id: String(rec.id),
            username: String(rec.username),
            password: String(rec.password),
            wins: Number(rec.wins ?? 0),
            losses: Number(rec.losses ?? 0),
            is_admin: Boolean(rec.is_admin ?? false),
          });
        }
        await userReader.close();

        // Storna counters: -1 a wins dei winners, -1 a losses dei losers (non sotto zero)
        const idxById = new Map<string, number>();
        users.forEach((u, i) => idxById.set(String(u.id), i));
        for (const uid of winners) {
          const i = idxById.get(String(uid));
          if (typeof i === "number") users[i].wins = Math.max(0, Number(users[i].wins || 0) - 1);
        }
        for (const uid of losers) {
          const i = idxById.get(String(uid));
          if (typeof i === "number") users[i].losses = Math.max(0, Number(users[i].losses || 0) - 1);
        }

        // Scrivi utenti aggiornati preservando schema esteso
        const outUserChunks: Buffer[] = [];
        const { Writable } = await import("node:stream");
        const userSink = new Writable({
          write(chunk, _enc, cb) {
            outUserChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            cb();
          },
        });
        const userWriter = await ParquetWriter.openStream(userSchema, userSink as any);
        for (const r of users) await userWriter.appendRow(r);
        await userWriter.close();
        const newUsersBody = Buffer.concat(outUserChunks);
        await s3.send(new PutObjectCommand({ Bucket: bucket, Key: usersKey, Body: newUsersBody, ContentType: "application/octet-stream" }));
      }
    }

    const outChunks: Buffer[] = [];
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const writer = await ParquetWriter.openStream(schema, sink as any);
    for (const r of kept) await writer.appendRow(r);
    await writer.close();
    const newBody = Buffer.concat(outChunks);

    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    return NextResponse.json({ status: "deleted", betId }, { status: 200 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}