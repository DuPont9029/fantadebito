import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetReader, ParquetSchema, ParquetWriter } from "parquetjs-lite";
import { verifyPassword } from "@/lib/password";

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

async function readAll(stream: any): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

type UserRow = { id: string; username: string; password: string; wins: number; losses: number; is_admin: boolean };

const userSchema = new ParquetSchema({
  id: { type: "UTF8" },
  username: { type: "UTF8" },
  password: { type: "UTF8" },
  wins: { type: "INT32" },
  losses: { type: "INT32" },
  is_admin: { type: "BOOLEAN" },
});

export async function POST(req: Request) {
  try {
    const { username, password } = await req.json().catch(() => ({ username: "", password: "" }));
    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}users.parquet`;
    const s3 = getS3Client();

    // Leggi utenti
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await readAll(obj.Body as any);
    const reader = await ParquetReader.openBuffer(body);
    const cursor = reader.getCursor();
    const users: UserRow[] = [];
    for (;;) {
      const rec = await cursor.next();
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
    await reader.close();

    // Autorizzazione admin
    const acting = users.find((u) => String(u.username) === String(username) && verifyPassword(String(password), String(u.password)));
    if (!acting || !acting.is_admin) {
      return NextResponse.json({ status: "error", message: "Solo admin può purgare utenti" }, { status: 403 });
    }

    // Purgare: rimuovi tutti gli utenti escluso eventualmente l'admin che esegue
    const keepAdminId = acting.id;
    const remaining: UserRow[] = [];

    // Se vuoi davvero eliminare anche l'admin, passare flag explicit; per ora manteniamo admin
    // In alternativa, per eliminare TUTTI, usare array vuoto.
    // Richiesta: eliminare tutti gli account utente -> svuotiamo completamente.

    // Scrivi un parquet vuoto (solo schema, nessuna riga)
    const outChunks: Buffer[] = [];
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const writer = await ParquetWriter.openStream(userSchema, sink as any);
    for (const r of remaining) await writer.appendRow(r);
    await writer.close();
    const newBody = Buffer.concat(outChunks);
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    return NextResponse.json({ status: "purged", total: remaining.length }, { status: 200 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}