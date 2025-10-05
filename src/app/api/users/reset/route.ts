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
    const { userId, username, password } = await req.json().catch(() => ({ userId: "", username: "", password: "" }));

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

    // Autenticazione/Autorizzazione admin
    let acting: UserRow | undefined;
    if (userId) acting = users.find((u) => String(u.id) === String(userId));
    else if (username && password) acting = users.find((u) => String(u.username) === String(username) && String(u.password) === String(password));
    if (!acting || !acting.is_admin) {
      return NextResponse.json({ status: "error", message: "Solo admin può resettare counters" }, { status: 403 });
    }

    // Azzeramento wins/losses per tutti
    const resetUsers: UserRow[] = users.map((u) => ({ ...u, wins: 0, losses: 0 }));

    // Scrivi utenti aggiornati
    const outChunks: Buffer[] = [];
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const writer = await ParquetWriter.openStream(userSchema, sink as any);
    for (const r of resetUsers) await writer.appendRow(r);
    await writer.close();
    const newBody = Buffer.concat(outChunks);
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    return NextResponse.json({ status: "reset", total: resetUsers.length }, { status: 200 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}