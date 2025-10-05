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

type BetRow = {
  id: string;
  owner_id: string;
  subject: string;
  esito: string;
  sospensione_json: string;
  invite_code: string;
  participants_json: string;
  created_at: string;
  terminated_at: string;
  realized: string; // "true" | "false" | ""
};

const betSchema = new ParquetSchema({
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
    const { userId, betId, realized } = await req.json().catch(() => ({ userId: "", betId: "", realized: undefined }));
    if (!userId || !betId || typeof realized === "undefined") {
      return NextResponse.json({ status: "error", message: "userId, betId e realized sono richiesti" }, { status: 400 });
    }
    const realizedStr = String(Boolean(realized)); // "true" or "false"

    const bucket = getEnv("S3_BUCKET")!;
    const prefix = getEnv("S3_PREFIX", true) || "";
    const betsKey = `${prefix}bets.parquet`;
    const usersKey = `${prefix}users.parquet`;
    const s3 = getS3Client();

    // Read all bets
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: betsKey }));
    const betsObj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: betsKey }));
    const betsBody = await readAll(betsObj.Body as any);
    const betReader = await ParquetReader.openBuffer(betsBody);
    const betCursor = betReader.getCursor();
    const betRows: BetRow[] = [];
    for (;;) {
      const rec = await betCursor.next();
      if (!rec) break;
      betRows.push({
        id: String(rec.id),
        owner_id: String(rec.owner_id),
        subject: String(rec.subject),
        esito: String(rec.esito ?? "ammissione"),
        sospensione_json: String(rec.sospensione_json ?? "[]"),
        invite_code: String(rec.invite_code),
        participants_json: String(rec.participants_json ?? "[]"),
        created_at: String(rec.created_at),
        terminated_at: String(rec.terminated_at ?? ""),
        realized: String(rec.realized ?? ""),
      });
    }
    await betReader.close();

    const idx = betRows.findIndex((r) => r.id === String(betId));
    if (idx === -1) return NextResponse.json({ status: "error", message: "Bet non trovata" }, { status: 404 });
    const target = betRows[idx];
    if (String(target.terminated_at)) {
      return NextResponse.json({ status: "error", message: "Bet già terminata" }, { status: 409 });
    }

    // Parse participants with stances
    const raw = JSON.parse(target.participants_json || "[]");
    const participants: { userId: string; stance?: string }[] = Array.isArray(raw)
      ? raw.map((p: any) => (typeof p === "string" ? { userId: String(p) } : { userId: String(p.userId || ""), stance: p.stance }))
      : [];

    // Determine winners/losers
    const winners = participants.filter((p) => (realizedStr === "true" ? p.stance === "favorevole" : p.stance === "contrario")).map((p) => p.userId);
    const losers = participants.filter((p) => (realizedStr === "true" ? p.stance === "contrario" : p.stance === "favorevole")).map((p) => p.userId);

    // Read users
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

    // Autorizzazione: il creatore o un admin possono terminare
    const actingUser = users.find((u) => String(u.id) === String(userId));
    const isAdmin = Boolean(actingUser?.is_admin);
    if (String(target.owner_id) !== String(userId) && !isAdmin) {
      return NextResponse.json({ status: "error", message: "Solo il creatore o un admin può terminare" }, { status: 403 });
    }

    // Update counters
    const userIndexById = new Map<string, number>();
    users.forEach((u, i) => userIndexById.set(String(u.id), i));
    for (const uid of winners) {
      const i = userIndexById.get(String(uid));
      if (typeof i === "number") users[i].wins = Number(users[i].wins || 0) + 1;
    }
    for (const uid of losers) {
      const i = userIndexById.get(String(uid));
      if (typeof i === "number") users[i].losses = Number(users[i].losses || 0) + 1;
    }

    // Write users back with extended schema
    {
      const outChunks: Buffer[] = [];
      const { Writable } = await import("node:stream");
      const sink = new Writable({
        write(chunk, _enc, cb) {
          outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          cb();
        },
      });
      const writer = await ParquetWriter.openStream(userSchema, sink as any);
      for (const r of users) await writer.appendRow(r);
      await writer.close();
      const newBody = Buffer.concat(outChunks);
      await s3.send(
        new PutObjectCommand({ Bucket: bucket, Key: usersKey, Body: newBody, ContentType: "application/octet-stream" })
      );
    }

    // Update bet termination fields
    const terminatedAt = new Date().toISOString();
    betRows[idx] = { ...target, terminated_at: terminatedAt, realized: realizedStr };

    // Write bets back with extended schema
    {
      const outChunks: Buffer[] = [];
      const { Writable } = await import("node:stream");
      const sink = new Writable({
        write(chunk, _enc, cb) {
          outChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
          cb();
        },
      });
      const writer = await ParquetWriter.openStream(betSchema, sink as any);
      for (const r of betRows) await writer.appendRow(r);
      await writer.close();
      const newBody = Buffer.concat(outChunks);
      await s3.send(
        new PutObjectCommand({ Bucket: bucket, Key: betsKey, Body: newBody, ContentType: "application/octet-stream" })
      );
    }

    return NextResponse.json(
      { status: "terminated", betId, winners, losers, realized: realizedStr },
      { status: 200 }
    );
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
  }
}