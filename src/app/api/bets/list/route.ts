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
    // Lista pubblica: non richiede userId
    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}bets.parquet`;
    const s3 = getS3Client();

    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const body = await readAll(obj.Body as any);
    const reader = await ParquetReader.openBuffer(body);
    const cursor = reader.getCursor();
    const results: any[] = [];
    for (;;) {
      const rec = await cursor.next();
      if (!rec) break;
      const rawParticipants = JSON.parse(String(rec.participants_json || "[]"));
      const participants: string[] = Array.isArray(rawParticipants)
        ? rawParticipants.map((p: any) => (typeof p === "string" ? String(p) : String(p.userId || ""))).filter(Boolean)
        : [];
      const stances: { userId: string; stance?: string }[] = Array.isArray(rawParticipants)
        ? rawParticipants.map((p: any) => (typeof p === "string" ? { userId: String(p) } : { userId: String(p.userId || ""), stance: p.stance }))
        : [];
      const esito = String(rec.esito ?? "ammissione");
      const sospensione = JSON.parse(String(rec.sospensione_json ?? "[]"));
      results.push({
        id: String(rec.id),
        owner_id: String(rec.owner_id),
        subject: String(rec.subject),
        esito,
        sospensione,
        invite_code: String(rec.invite_code),
        participants,
        stances,
        created_at: String(rec.created_at),
        terminated_at: String(rec.terminated_at ?? ""),
        realized: String(rec.realized ?? ""),
      });
    }
    await reader.close();

    return NextResponse.json({ status: "ok", bets: results }, { status: 200 });
  } catch (error: any) {
    if (String(error?.message || "").includes("Not Found")) {
      return NextResponse.json({ status: "ok", bets: [] }, { status: 200 });
    }
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}