/**
 * manager-machine/state.ts — THE STORE.
 *
 * Extracted law-faithfully from the manager-machine lineage:
 *   · trident wave manager  .mimocode/trident/src/tools/wave-db.ts:14-29
 *     ("IS_PRODUCTION_DB — Computed AT IMPORT" — BUG-9 root-cause fix)
 *   · omp wave manager      Trident_OMP_Manager/src/omp-manager/omp-wave-db.ts:18-29
 *   · TCM stores            trident-context-manager/src/seal/stores.ts (WAL+busy5000+3-retry)
 *
 * THE LAW (verbatim, wave-tracker.ts:191-203): "the operator's
 * 8-parallel-session callout... the JSON read-modify-write COLLIDES across
 * processes — each process loads the whole file + rewrites it on mutation →
 * process A's persist clobbers B's rows." ⇒ SQLite WAL is the ONLY durable
 * truth. In-memory Maps are caches, never truth.
 *
 * PORT DELTAS from source (documented per extracting-production-boilerplates):
 *   Δ1 generic table registry (source hardcoded wave tables) — the boilerplate
 *      hosts ANY manager; tables come from the consumer's schema migration.
 *   Δ2 env-injected db path (MM_DB) with HOME-derived default — never a
 *      single hardcoded absolute path (chaos finding F11/F12/F14 class).
 *   Δ3 prune with exemptions surfaced as an option (source: 24h prune exempting
 *      pins/task_queue — stores.ts:536-552).
 */
import { Database } from "bun:sqlite";

export interface StoreOptions {
  path?: string;
  production?: boolean; // import-time in the sources; here: explicit + default-derived
  busyTimeoutMs?: number;
  pruneAfterHours?: number;
  pruneExemptTables?: string[];
}

/** Import-time-style production flag: derived ONCE at module load so a later
 * runtime env change can never re-open a wipe path (the BUG-9 law). */
export const IS_PRODUCTION_DB =
  process.env.MM_DB === undefined &&
  process.env.MM_ALLOW_WIPE !== "1";

export class ManagerStore {
  private db: Database;
  readonly path: string;
  private readonly production: boolean;

  constructor(opts: StoreOptions = {}) {
    this.path =
      opts.path ??
      process.env.MM_DB ??
      `${process.env.HOME ?? ""}/.manager-machine/store.sqlite`;
    if (!this.path || this.path === "/.manager-machine/store.sqlite") {
      // Fail-closed: no HOME, no env → refuse, never guess (finding F14).
      throw new Error("MM-NO-PATH: db path unresolved (set MM_DB or HOME)");
    }
    this.production = opts.production ?? IS_PRODUCTION_DB;
    const parent = this.path.replace(/\/[^/]+$/, "");
    if (parent) require("node:fs").mkdirSync(parent, { recursive: true });
    this.db = this.open();
    this.db.exec("PRAGMA journal_mode = WAL;");
    this.db.exec(`PRAGMA busy_timeout = ${opts.busyTimeoutMs ?? 5000};`);
  }

  /** Health-checked reopen with 3 attempts (omp-wave-tracker.ts:220-244). */
  private open(attempt = 1): Database {
    try {
      return new Database(this.path, { create: true });
    } catch (e) {
      if (attempt >= 3) {
        throw new Error(`MM-STORE-DOWN: ${this.path}: ${String(e)}`);
      }
      const backoff = 100 * attempt;
      const end = Date.now() + backoff;
      while (Date.now() < end) { /* sync spin — bounded, small */ }
      return this.open(attempt + 1);
    }
  }

  /** DDL migration — consumer declares tables; idempotent CREATE IF NOT EXISTS. */
  migrate(ddl: string): void {
    this.db.exec(ddl);
  }

  /** BEGIN IMMEDIATE transaction (omp-wave-db.ts:175-199 withImmediateTx). */
  immediate<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const out = fn();
      this.db.exec("COMMIT");
      return out;
    } catch (e) {
      try { this.db.exec("ROLLBACK"); } catch { /* already rolled */ }
      throw e;
    }
  }

  run(sql: string, ...params: unknown[]): void {
    this.db.run(sql, params);
  }

  rows<T = Record<string, unknown>>(sql: string, ...params: unknown[]): T[] {
    return this.db.query(sql).all(...params) as T[];
  }

  /** Row-keyed upsert helper (wave-tracker.ts:217-244): INSERT OR REPLACE —
   * multi-process safe where JSON read-modify-write was not. */
  upsert(table: string, keyCol: string, key: string, row: Record<string, unknown>): void {
    const cols = Object.keys(row);
    const vals = cols.map((c) => row[c]);
    const ph = cols.map(() => "?").join(",");
    this.db.run(
      `INSERT OR REPLACE INTO ${table} (${keyCol},${cols.join(",")}) VALUES (?,${ph})`,
      [key, ...vals],
    );
  }

  /** Prune with exemptions (stores.ts:536-552): ledger/manifest-class tables
   * prune on age; pinned/truth-class tables are exempt by name. */
  prune(table: string, tsCol: string, opts: { afterHours?: number; exempt?: boolean } = {}): number {
    if (opts.exempt || this.production === false && opts.afterHours === undefined) {
      return 0;
    }
    const hours = opts.afterHours ?? 24;
    const cutoff = Date.now() / 1000 - hours * 3600;
    const r = this.db.run(`DELETE FROM ${table} WHERE ${tsCol} < ?`, [cutoff]);
    return r.changes ?? 0;
  }

  /** THE PRODUCTION-WIPE LAW (BUG-9): clear() refuses on production. */
  clear(): void {
    if (this.production) {
      throw new Error("MM-WIPE-REFUSED: production store (BUG-9 law)");
    }
    this.db.exec("DELETE FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
  }

  health(): { ok: boolean; path: string; error?: string } {
    try {
      this.db.query("SELECT 1").get();
      return { ok: true, path: this.path };
    } catch (e) {
      return { ok: false, path: this.path, error: String(e) };
    }
  }
}
