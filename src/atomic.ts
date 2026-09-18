/**
 * manager-machine/atomic.ts — ATOMIC FILE WRITES.
 *
 * Extracted against the chaos-audit finding class (ledgers
 * .trident/chaos-audit-plugin.md F26, bug_hunter_report.md F14):
 * "non-atomic writeFileSync on a flag read by every process... crash mid-write
 * leaves torn JSON that every reader parses as clear (fail-open)" and
 * "crash mid-write permanently bricks META/STATUS/flags" — the tmp+fsync+
 * rename law. Readers get either the OLD bytes or the NEW bytes, never torn.
 */
import { appendFileSync, closeSync, existsSync, fsyncSync, openSync, renameSync, unlinkSync, writeFileSync } from "node:fs";

/** Write file atomically: tmp in the SAME dir (same filesystem) → fsync → rename. */
export function atomicWrite(path: string, data: string | Uint8Array): void {
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  const fd = openSync(tmp, "w");
  try {
    writeFileSync(fd, data);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, path);
}

/** Read a file that may be mid-creation by a non-atomic writer: missing → null,
 * EMPTY bytes → null + caller decides (the undecoded-is-never-empty law:
 * absence and emptiness are NAMED, never silently conflated with data). */
export function readOrNull(path: string): string | null {
  if (!existsSync(path)) return null;
  const txt = require("node:fs").readFileSync(path, "utf8");
  return txt.length > 0 ? txt : null;
}

/** Append a line to an append-only ledger (VC ledger.ts:24 law: "atomic append,
 * never rewrite"). One write() call per line — appends under ~4KB are atomic
 * on local filesystems in practice; the journal is the contract, not the page cache. */
export function appendLine(path: string, line: string): void {
  appendFileSync(path, line.endsWith("\n") ? line : `${line}\n`);
}

/** Remove with the scoped-identity law (plutus chrome-watchdog.sh: kill by full
 * identifying pattern, never by name): callers pass the EXACT path; this wrapper
 * refuses wildcard-ish paths outright. */
export function unlinkExact(path: string): boolean {
  if (path.includes("*") || path.includes("?")) {
    throw new Error("MM-UNLINK-REFUSED: wildcard path");
  }
  if (!existsSync(path)) return false;
  unlinkSync(path);
  return true;
}
