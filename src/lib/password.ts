import { randomBytes, pbkdf2Sync } from "node:crypto";

const DEFAULT_ITERATIONS = 310000; // NIST guidance-level iterations
const KEYLEN = 32; // 256-bit
const DIGEST = "sha256";

export function hashPassword(plain: string, iterations = DEFAULT_ITERATIONS): string {
  const salt = randomBytes(16).toString("hex");
  const hash = pbkdf2Sync(plain, salt, iterations, KEYLEN, DIGEST).toString("hex");
  return `pbkdf2$${iterations}$${salt}$${hash}`;
}

export function verifyPassword(plain: string, stored: string): boolean {
  if (!stored) return false;
  // Support legacy plaintext passwords
  if (!stored.startsWith("pbkdf2$")) {
    return stored === plain;
  }
  try {
    const parts = stored.split("$");
    // pbkdf2$<iterations>$<salt>$<hash>
    const iterations = Number(parts[1] || DEFAULT_ITERATIONS);
    const salt = parts[2];
    const expected = parts[3];
    if (!salt || !expected || !iterations) return false;
    const hash = pbkdf2Sync(plain, salt, iterations, KEYLEN, DIGEST).toString("hex");
    return hash === expected;
  } catch (_) {
    return false;
  }
}