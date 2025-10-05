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

type Row = {
  id: string;
  owner_id: string;
  subject: string;
  esito: string; // 'ammissione' | 'sospensione' | 'non_ammissione'
  sospensione_json: string; // JSON array di { subject, grade }
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

export async function POST(req: Request) {
  try {
    const { userId, subject, esito, sospensione, stance } = await req.json().catch(() => ({ userId: "", subject: "", esito: "", sospensione: [], stance: "favorevole" }));
    if (!userId || !subject) {
      return NextResponse.json({ status: "error", message: "userId e subject sono richiesti" }, { status: 400 });
    }
    const validEsiti = ["ammissione", "sospensione", "non_ammissione"];
    const chosenEsito = String(esito || "ammissione");
    if (!validEsiti.includes(chosenEsito)) {
      return NextResponse.json({ status: "error", message: "esito non valido" }, { status: 400 });
    }
    const validStance = ["favorevole", "contrario"];
    const chosenStance = validStance.includes(String(stance)) ? String(stance) : "favorevole";

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const key = `${prefix}bets.parquet`;
    const s3 = getS3Client();

    let rows: Row[] = [];
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
    } catch (e) {
      // nessun file: lista vuota
    }

    const id = Math.random().toString(36).slice(2);
    const invite = Math.random().toString(36).slice(2, 8).toUpperCase();
    const created_at = new Date().toISOString();
    const participantsObjs = [{ userId: String(userId), stance: chosenStance }];
    const participants = participantsObjs.map((p) => p.userId);
    const sosp = Array.isArray(sospensione) ? sospensione.map((i: any) => ({ subject: String(i.subject || ""), grade: Number(i.grade || 0) })) : [];
    const newRow: Row = {
      id,
      owner_id: String(userId),
      subject: String(subject),
      esito: chosenEsito,
      sospensione_json: JSON.stringify(chosenEsito === "sospensione" ? sosp : []),
      invite_code: invite,
      participants_json: JSON.stringify(participantsObjs),
      created_at,
      terminated_at: "",
      realized: "",
    };
    rows.push(newRow);

    // Scrivi parquet
    const outChunks: Buffer[] = [];
    const { Writable } = await import("node:stream");
    const sink = new Writable({
      write(chunk, _enc, cb) {
        outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });
    const writer = await ParquetWriter.openStream(schema, sink as any);
    for (const r of rows) await writer.appendRow(r);
    await writer.close();
    const newBody = Buffer.concat(outChunks);

    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: newBody, ContentType: "application/octet-stream" }));

    return NextResponse.json({ status: "created", bet: { ...newRow, participants } }, { status: 201 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}