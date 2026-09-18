// fleet.ts — the OMP thread-corpus fleet: probes over the live threads store.
// LAW 3 split: per-thread DB minting is MECHANICAL (heal, idempotent);
// missing/retired object paths are SEMANTIC (raise only — never delete).
import { Database } from "bun:sqlite";
import { mkdirSync, existsSync, statSync, copyFileSync, renameSync, unlinkSync } from "node:fs";
import type { Desired } from "./src/reconcile.js";

export const THREADS_STORE = process.env.THREADS_STORE ?? `${process.env.HOME}/.omp/threads/store.sqlite`;
export const PER_THREAD_DIR = process.env.PER_THREAD_DIR ?? `${process.env.HOME}/.omp/threads/per-thread`;

interface ThreadRow { slug: string; kind: string; status: string }
interface CountRow { n: number }
interface MaxRow { m: string | number | null }

function openStore(): Database {
  return new Database(THREADS_STORE, { readonly: true });
}

/** Mint/refresh ONE per-thread sqlite: meta + entries + objects for the slug.
 *  Atomic (tmp + rename). Most-recent-wins: rebuilt whenever the shared store
 *  is newer than the minted file. Old objects are NEVER deleted — history. */
export function mintThreadDb(slug: string): string {
  mkdirSync(PER_THREAD_DIR, { recursive: true });
  const out = `${PER_THREAD_DIR}/${slug}.sqlite`;
  const tmp = `${out}.minting`;
  for (const f of [tmp, `${tmp}-wal`, `${tmp}-shm`]) { try { unlinkSync(f); } catch {} }
  const src = new Database(THREADS_STORE, { readonly: true });
  const dst = new Database(tmp);
  dst.run("CREATE TABLE meta (slug TEXT PRIMARY KEY, kind TEXT, status TEXT, minted_at INTEGER)");
  dst.run("CREATE TABLE entries (id INTEGER PRIMARY KEY, ts TEXT, title TEXT, tags TEXT, body TEXT)");
  dst.run("CREATE TABLE thread_objects (id INTEGER PRIMARY KEY, kind TEXT, okey TEXT, status TEXT, data TEXT, path TEXT, sha16 TEXT, updated_at TEXT)");
  const t = src.query("SELECT slug, kind, status FROM threads WHERE slug = ?").get(slug) as ThreadRow | null;
  if (!t) { src.close(); dst.close(); throw new Error(`thread '${slug}' not in registry`); }
  dst.run("INSERT INTO meta VALUES (?,?,?,?)", [slug, t.kind, t.status, Date.now()]);
  const entries = src.query("SELECT id, ts, title, tags, body FROM entries WHERE slug = ? ORDER BY id").all(slug);
  const ie = dst.prepare("INSERT INTO entries VALUES (?,?,?,?,?)");
  for (const e of entries) ie.run(e.id, e.ts, e.title, e.tags, e.body);
  const objs = src.query("SELECT id, kind, okey, status, data, path, sha16, updated_at FROM thread_objects WHERE slug = ? ORDER BY id").all(slug);
  const io = dst.prepare("INSERT INTO thread_objects VALUES (?,?,?,?,?,?,?,?)");
  for (const o of objs) io.run(o.id, o.kind, o.okey, o.status, o.data, o.path, o.sha16, o.updated_at);
  dst.run("PRAGMA journal_mode = DELETE");
  dst.close(); src.close();
  renameSync(tmp, out);
  return out;
}

function storeNewestSourceTs(): number {
  const src = new Database(THREADS_STORE, { readonly: true });
  const e = src.query("SELECT MAX(rowid) AS n, MAX(updated_at) AS m FROM thread_objects").get() as CountRow & MaxRow;
  const e2 = src.query("SELECT MAX(id) AS n FROM entries").get() as CountRow;
  src.close();
  // a monotonic proxy: entries.rowid + objects.rowid sum changes on ANY write
  return (e?.n ?? 0) + (e2?.n ?? 0) + (typeof e?.m === "number" ? 0 : 0);
}

function mintedFreshness(slug: string): { exists: boolean; fresh: boolean; storeStamp: number; dbStamp: number } {
  const p = `${PER_THREAD_DIR}/${slug}.sqlite`;
  if (!existsSync(p)) return { exists: false, fresh: false, storeStamp: -1, dbStamp: -1 };
  const src = new Database(THREADS_STORE, { readonly: true });
  const storeStamp =
    ((src.query("SELECT COALESCE(MAX(id),0) AS n FROM entries WHERE slug = ?").get(slug) as CountRow).n) * 1e6 +
    ((src.query("SELECT COALESCE(MAX(id),0) AS n FROM thread_objects WHERE slug = ?").get(slug) as CountRow).n);
  src.close();
  const db = new Database(p, { readonly: true });
  const dbStamp =
    ((db.query("SELECT COALESCE(MAX(id),0) AS n FROM entries").get() as CountRow).n) * 1e6 +
    ((db.query("SELECT COALESCE(MAX(id),0) AS n FROM thread_objects").get() as CountRow).n);
  const minted = (db.query("SELECT minted_at AS m FROM meta").get() as { m: number }).m;
  db.close();
  return { exists: true, fresh: dbStamp >= storeStamp && minted > 0, storeStamp, dbStamp };
}

export function buildFleet(): Desired[] {
  const probes: Desired[] = [];

  // 1. STORE HEALTH — mechanical observe; heal only via WAL presence (no-op raise if broken)
  probes.push({
    id: "threads-store-health",
    kind: "store",
    shallow: "threads store.sqlite integrity",
    observe: async () => {
      try {
        const src = new Database(THREADS_STORE, { readonly: true });
        const r = src.query("PRAGMA quick_check").get() as Record<string, string>;
        src.close();
        const wal = existsSync(THREADS_STORE + "-wal");
        const ok = Object.values(r)[0] === "ok";
        return ok && wal ? null : `quick_check=${Object.values(r)[0]} wal=${wal}`;
      } catch (e) {
        return `store unreadable: ${String(e).slice(0, 80)}`;
      }
    },
  });

  // 2. PER-THREAD DB MINTING — one probe per OPEN thread (mechanical heal)
  const src = openStore();
  const open: ThreadRow[] = src.query("SELECT slug, kind, status FROM threads WHERE status = 'open'").all() as ThreadRow[];
  src.close();
  for (const t of open) {
    probes.push({
      id: `per-thread-db:${t.slug}`,
      kind: "per-thread-db",
      shallow: `minted + fresh (${PER_THREAD_DIR}/${t.slug}.sqlite)`,
      observe: async () => {
        const f = mintedFreshness(t.slug);
        if (!f.exists) return "missing";
        if (!f.fresh) return `stale (store stamp ${f.storeStamp} > db stamp ${f.dbStamp})`;
        return null;
      },
      heal: async (drift: string) => {
        const p = mintThreadDb(t.slug);
        return `minted ${p} (${drift})`;
      },
    });
  }

  // 3. STALE OBJECT PATHS — SEMANTIC: raise only (Law 3 — never delete history)
  probes.push({
    id: "object-paths-stale-raise",
    kind: "raise-only",
    shallow: "thread_objects rows whose path is missing on disk (report only)",
    observe: async () => {
      const db = new Database(THREADS_STORE, { readonly: true });
      const rows = db.query("SELECT slug, okey, path FROM thread_objects WHERE path IS NOT NULL AND path != ''").all() as Array<{ slug: string; okey: string; path: string }>;
      db.close();
      const missing = rows.filter((r) => !existsSync(r.path));
      return missing.length > 0
        ? `${missing.length}/${rows.length} object paths missing on disk — RAISE ONLY (history preserved): e.g. ${missing.slice(0, 3).map((m) => `${m.slug}/${m.okey}`).join(", ")}`
        : null;
    },
    // no heal — semantic drift is reported, never auto-rewritten (Law 3)
  });

  return probes;
}

export { statSync };
