/**
 * manager-machine/registry.ts — THE AUTHORIZATION/CONSUMPTION FSM.
 *
 * Extracted law-faithfully from:
 *   · trident wave-registry.ts:41-75 (evaluateWaveBatchGate: recorded|accepted|failed;
 *     "a failed runtime attempt RE-FIREABLE... an accepted call permanently blocked")
 *   · omp-wave-registry.ts:41-75 (failed → 'recorded' reset, reason:'failed-reset')
 *   · THE canonical incident (BUGREPORT_wave-manager-dispatch-authorization.md:10-13):
 *     "authorization recorded without confirming the dispatch actually succeeded —
 *     a wave whose only dispatch attempt was rejected can never be dispatched."
 *
 * THE LAW: authorization is never CONSUMED by a failed attempt. failed → re-fireable.
 * accepted is permanent. release re-arms deliberately.
 */
import { ManagerStore } from "./state.js";

export type AttemptStatus = "recorded" | "accepted" | "failed";
export type GateState = "ready" | "dispatching" | "dispatched";

export interface AttemptRow {
  key: string;
  status: AttemptStatus;
  ts: number;
}

export interface GateVerdict {
  action: "allow" | "block";
  reason: string;
}

const DDL = `
CREATE TABLE IF NOT EXISTS mm_attempts (
  key TEXT PRIMARY KEY, status TEXT NOT NULL, ts INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS mm_gates (
  key TEXT PRIMARY KEY, state TEXT NOT NULL, ts INTEGER NOT NULL);
`;

export class AuthRegistry {
  constructor(private store: ManagerStore) {
    store.migrate(DDL);
  }

  /** Record an attempt BEFORE the work fires (crash-first law). */
  record(key: string): void {
    this.store.immediate(() =>
      this.store.upsert("mm_attempts", "key", key, { status: "recorded", ts: Date.now() }));
  }

  /** Mark accepted — permanent. The work's success/failure is tracked elsewhere;
   * acceptance only means "the dispatch actually happened." */
  accept(key: string): void {
    this.store.immediate(() =>
      this.store.upsert("mm_attempts", "key", key, { status: "accepted", ts: Date.now() }));
  }

  /** THE BUG-CLASS LAW: failed RESETS to recorded (re-fireable) — never consumed. */
  fail(key: string): GateVerdict {
    this.store.immediate(() =>
      this.store.upsert("mm_attempts", "key", key, { status: "recorded", ts: Date.now() }));
    return { action: "allow", reason: "failed-reset" };
  }

  /** The gate: does a dispatch attempt proceed? accepted = permanently blocked. */
  evaluate(key: string): GateVerdict {
    const rows = this.store.rows<AttemptRow>(
      "SELECT status FROM mm_attempts WHERE key = ?", key);
    const st = rows[0]?.status as AttemptStatus | undefined;
    if (st === "accepted") return { action: "block", reason: "already-accepted" };
    if (st === "failed" || st === undefined) {
      // failed rows were already reset to recorded by fail(); undefined = first attempt.
      return { action: "allow", reason: st === undefined ? "first-attempt" : "failed-reset" };
    }
    return { action: "allow", reason: "recorded" };
  }

  /** Deliberate re-arm of a completed gate (omp-wave-dispatch.ts:1714-1734 release). */
  release(key: string): void {
    this.store.immediate(() =>
      this.store.run("DELETE FROM mm_attempts WHERE key = ?", key));
  }

  /** Gate state for the WHOLE batch (ready→dispatching→dispatched). */
  setGate(key: string, state: GateState): void {
    this.store.immediate(() =>
      this.store.upsert("mm_gates", "key", key, { state, ts: Date.now() }));
  }

  getGate(key: string): GateState | null {
    const rows = this.store.rows<{ state: GateState }>(
      "SELECT state FROM mm_gates WHERE key = ?", key);
    return rows[0]?.state ?? null;
  }

  /** Exactly-once guarded transitions (trident wave-cron.ts:317-329 killDirectiveFired
   * law): a transition fires at most once per state-key; second call is a no-op. */
  fireOnce(flagKey: string, fn: () => void): boolean {
    const rows = this.store.rows<{ key: string }>(
      "SELECT key FROM mm_attempts WHERE key = ?", `__fired__${flagKey}`);
    if (rows.length > 0) return false;
    this.store.immediate(() =>
      this.store.upsert("mm_attempts", "key", `__fired__${flagKey}`,
                        { status: "accepted", ts: Date.now() }));
    fn();
    return true;
  }
}
