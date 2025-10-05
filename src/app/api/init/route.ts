import { NextResponse } from "next/server";
import { S3Client, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "parquetjs-lite";
import { Writable } from "node:stream";

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
  return new S3Client({
    region,
    endpoint,
    forcePathStyle,
    credentials: { accessKeyId, secretAccessKey },
  });
}

export async function POST() {
  try {
    const bucket = getEnv("S3_BUCKET")!; // es: "fantadebito"
    const prefix = getEnv("S3_PREFIX", true) || ""; // es: "" o "cartella/"
    const key = `${prefix}users.parquet`;

    const s3 = getS3Client();
    // Verifica esistenza
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      const readUrl = await getSignedUrl(
        s3,
        new GetObjectCommand({ Bucket: bucket, Key: key }),
        { expiresIn: 60 * 60 }
      );
      return NextResponse.json({ status: "exists", bucket, key, readUrl }, { status: 200 });
    } catch (err: any) {
      // Se non esiste, prosegui alla creazione
    }

    // Schema utenti con counters wins/losses e flag admin
    const schema = new ParquetSchema({
      id: { type: "UTF8" },
      username: { type: "UTF8" },
      password: { type: "UTF8" },
      wins: { type: "INT32" },
      losses: { type: "INT32" },
      is_admin: { type: "BOOLEAN" },
    });

    // Scrive Parquet in memoria
    const chunks: Buffer[] = [];
    const out = new Writable({
      write(chunk, _enc, cb) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        cb();
      },
    });

    const writer = await ParquetWriter.openStream(schema, out);
    // Utenti di esempio — demo admin, mn utente normale
    await writer.appendRow({ id: "1", username: "demo", password: "demo", wins: 0, losses: 0, is_admin: true });
    await writer.appendRow({ id: "2", username: "mn", password: "1234", wins: 0, losses: 0, is_admin: false });
    await writer.close();
    const body = Buffer.concat(chunks);

    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: "application/octet-stream",
        // Niente ACL pubbliche; affidiamoci a URL presigned
      })
    );

    // Genera URL presigned per lettura
    const readUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: bucket, Key: key }),
      { expiresIn: 60 * 60 }
    );

    return NextResponse.json({ status: "created", bucket, key, readUrl }, { status: 201 });
  } catch (error: any) {
    return NextResponse.json({ status: "error", message: error?.message || String(error) }, { status: 500 });
}
}