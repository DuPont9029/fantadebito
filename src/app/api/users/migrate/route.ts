import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "parquetjs-lite";
import { ParquetReader } from "parquetjs-lite";

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

type UserRowIn = { id?: any; username?: any; password?: any; wins?: any; losses?: any; is_admin?: any };
type UserRowOut = { id: string; username: string; password: string; wins: number; losses: number; is_admin: boolean };

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
    const { make_admin_username, make_admin_userId } = await req.json().catch(() => ({ make_admin_username: "", make_admin_userId: "" }));

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}users.parquet`;
    const s3 = getS3Client();

    // Deve esistere già; altrimenti suggerire /api/init
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    } catch (e) {
      return NextResponse.json({ status: "error", message: "users.parquet non trovato. Esegui /api/init" }, { status: 404 });
    }

    const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await readAll(obj.Body as any);
    const reader = await ParquetReader.openBuffer(body);
    const cursor = reader.getCursor();

    const rowsIn: UserRowIn[] = [];
    for (;;) {
      const rec = await cursor.next();
      if (!rec) break;
      rowsIn.push(rec as UserRowIn);
    }
    await reader.close();

    const promoteUsername = String(make_admin_username || "").trim();
    const promoteUserId = String(make_admin_userId || "").trim();
    const outRows: UserRowOut[] = rowsIn.map((r) => {
      const id = String(r.id ?? "");
      const username = String(r.username ?? "");
      const password = String(r.password ?? "");
      const wins = Number(r.wins ?? 0);
      const losses = Number(r.losses ?? 0);
      let is_admin = Boolean(r.is_admin ?? false);
      if (!is_admin) {
        if (promoteUsername && username === promoteUsername) is_admin = true;
        if (promoteUserId && id === promoteUserId) is_admin = true;
      }
      return { id, username, password, wins, losses, is_admin };
    });

    const outChunks: Buffer[] = [];
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const writer = await ParquetWriter.openStream(userSchema, sink as any);
    for (const r of outRows) await writer.appendRow(r);
    await writer.close();
    const newBody = Buffer.concat(outChunks);
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    const promoted = outRows.find((r) => r.is_admin && (r.username === promoteUsername || r.id === promoteUserId));
    return NextResponse.json(
      {
        status: "migrated",
        total: outRows.length,
        promoted: promoted ? { id: promoted.id, username: promoted.username } : null,
      },
      { status: 200 }
    );
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}