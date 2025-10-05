import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
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

export async function POST(req: Request) {
  try {
    const { userId } = await req.json().catch(() => ({ userId: "" }));
    if (!userId) {
      return NextResponse.json({ status: "error", message: "userId richiesto" }, { status: 400 });
    }

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}users.parquet`;
    const s3 = getS3Client();

    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await readAll(obj.Body as any);
    const reader = await ParquetReader.openBuffer(body);
    const cursor = reader.getCursor();

    let found: { id: string; username: string; wins: number; losses: number } | null = null;
    for (;;) {
      const rec = await cursor.next();
      if (!rec) break;
      if (String(rec.id) === String(userId)) {
        found = {
          id: String(rec.id),
          username: String(rec.username),
          wins: Number(rec.wins ?? 0),
          losses: Number(rec.losses ?? 0),
        };
        break;
      }
    }
    await reader.close();

    if (!found) {
      return NextResponse.json({ status: "error", message: "Utente non trovato" }, { status: 404 });
    }
    return NextResponse.json({ status: "ok", user: found }, { status: 200 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}