import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "parquetjs-lite";
import { ParquetReader } from "parquetjs-lite";
import { hashPassword } from "@/lib/password";

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

export async function POST(req: Request) {
  try {
    const { username, password } = await req.json().catch(() => ({ username: "", password: "" }));
    if (!username || !password) {
      return NextResponse.json({ status: "error", message: "Username e password sono richiesti" }, { status: 400 });
    }
    if (String(username).length < 3) {
      return NextResponse.json({ status: "error", message: "Username troppo corto" }, { status: 400 });
    }

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}users.parquet`;
    const s3 = getS3Client();

    let rows: Array<{ id: string; username: string; password: string; wins: number; losses: number; is_admin?: boolean }> = [];

    // Se esiste, leggi i record esistenti
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
      const body = await readAll(obj.Body as any);
      const reader = await ParquetReader.openBuffer(body);
      const cursor = reader.getCursor();
      for (;;) {
        const rec = await cursor.next();
        if (!rec) break;
        rows.push({
          id: String(rec.id),
          username: String(rec.username),
          password: String(rec.password),
          wins: Number(rec.wins ?? 0),
          losses: Number(rec.losses ?? 0),
          is_admin: Boolean(rec.is_admin ?? false),
        });
      }
      await reader.close();
    } catch (e) {
      // Non esiste: si parte da lista vuota
    }

    // Verifica duplicato
    if (rows.some((r) => r.username === username)) {
      return NextResponse.json({ status: "error", message: "Username già esistente" }, { status: 409 });
    }

    // Aggiungi nuovo utente (hashing password)
    const id = Math.random().toString(36).slice(2);
    const passwordHash = hashPassword(String(password));
    rows.push({ id, username, password: passwordHash, wins: 0, losses: 0, is_admin: false });

    // Scrivi nuovo parquet con tutti gli utenti
    const outChunks: Buffer[] = [];
    // ParquetWriter.openStream richiede uno stream Writable; usiamo un bridge semplice
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const schema = new ParquetSchema({
      id: { type: "UTF8" },
      username: { type: "UTF8" },
      password: { type: "UTF8" },
      wins: { type: "INT32" },
      losses: { type: "INT32" },
      is_admin: { type: "BOOLEAN" },
    });
    const writer = await ParquetWriter.openStream(schema, sink as any);
    for (const r of rows) {
      await writer.appendRow(r);
    }
    await writer.close();
    const newBody = Buffer.concat(outChunks);

    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    return NextResponse.json({ status: "created", user: { id, username } }, { status: 201 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}