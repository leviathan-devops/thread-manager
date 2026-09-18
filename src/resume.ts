/**
 * manager-machine/resume.ts — CRASH-FIRST ROWS + RESUME-FROM-PERSIST.
 *
 * Extracted from omp-desk-registry.ts:59-72 ("INSERT OR REPLACE with
 * state=running — the desk exists from the first moment of its lifecycle; a
 * crash AFTER this write still leaves a traceable row with exit_code NULL")
 * + TCM stores.ts:206-226 half-seal detection ("crash-mid-write self-heals
 * loudly") + trident resume (session probe → steer 'continue' → un-archive).
 *
 * THE LAW: the manager writes the RUNNING row BEFORE the work fires. A crash
 * leaves a traceable row with a NULL terminal column — the resume sweep's input.
 * Work is never lost silently; interrupted work is RESUMED by id.
 */
import { ManagerStore } from "./state.js";
import { Journal } from "./journal.js";

const DDL = `
CREATE TABLE IF NOT EXISTS mm_work (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  state TEXT NOT NULL,          -- running|done|failed|interrupted|resumed
  desired TEXT NOT NULL,        -- the desired-state JSON (the re-fire contract)
  checkpoint TEXT,              -- resume token/position (NULL until first checkpoint)
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  finished_at INTEGER           -- NULL = traceable crash/interrupt row
);
`;

export interface WorkRow {
  id: string; kind: string; state: string; desired: string;
  checkpoint: string | null; started_at: number; updated_at: number; finished_at: number | null;
}

export class WorkLedger {
  constructor(private store: ManagerStore, private journal: Journal) {
    store.migrate(DDL);
  }

  /** CRASH-FIRST: call BEFORE firing the work. NULL finished_at is the interrupt marker. */
  start(id: string, kind: string, desired: unknown): void {
    const now = Date.now();
    this.store.immediate(() =>
      this.store.upsert("mm_work", "id", id, {
        kind, state: "running", desired: JSON.stringify(desired),
        checkpoint: null, started_at: now, updated_at: now, finished_at: null,
      }));
  }

  checkpoint(id: string, token: string): void {
    this.store.immediate(() =>
      this.store.run("UPDATE mm_work SET checkpoint=?, updated_at=? WHERE id=?", token, Date.now(), id));
  }

  finish(id: string, state: "done" | "failed"): void {
    this.store.immediate(() =>
      this.store.run("UPDATE mm_work SET state=?, finished_at=?, updated_at=? WHERE id=?",
                     state, Date.now(), Date.now(), id));
  }

  get(id: string): WorkRow | null {
    return (this.store.rows<WorkRow>("SELECT * FROM mm_work WHERE id=?", id))[0] ?? null;
  }

  /** THE RESUME SWEEP: all rows with NULL finished_at = interrupted work. */
  interrupted(): WorkRow[] {
    return this.store.rows<WorkRow>(
      "SELECT * FROM mm_work WHERE finished_at IS NULL ORDER BY started_at ASC");
  }

  /** Mark interrupted rows explicitly + journal them (half-record detection:
   * at open, a crash leaves state=running with stale updated_at — named, never silent).
   * <= (never strict <): a same-millisecond row must not escape the sweep. */
  sweep(olderThanMs: number): WorkRow[] {
    const cutoff = Date.now() - olderThanMs;
    const rows = this.store.rows<WorkRow>(
      "SELECT * FROM mm_work WHERE finished_at IS NULL AND updated_at <= ? ORDER BY started_at ASC", cutoff);
    for (const r of rows) {
      this.store.immediate(() =>
        this.store.run("UPDATE mm_work SET state='interrupted', updated_at=? WHERE id=?", Date.now(), r.id));
      this.journal.append(r.id, "resume",
        `INTERRUPTED detected: state was '${r.state}' with checkpoint=${r.checkpoint ? "present" : "absent"} — resume-by-id available`);
    }
    return rows;
  }

  /** Resume contract: returns the desired-state + checkpoint for the re-fire. */
  resumePlan(id: string): { desired: unknown; checkpoint: string | null } | null {
    const r = this.get(id);
    if (!r) return null;
    this.store.immediate(() =>
      this.store.run("UPDATE mm_work SET state='resumed', updated_at=? WHERE id=?", Date.now(), id));
    this.journal.append(id, "resume", `resumed from checkpoint=${r.checkpoint ? r.checkpoint.slice(0, 60) : "start"}`);
    return { desired: JSON.parse(r.desired), checkpoint: r.checkpoint };
  }
}
