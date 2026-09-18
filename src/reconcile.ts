/**
 * manager-machine/reconcile.ts — THE RECONCILIATION TICK.
 *
 * v2 core on the trident cron skeleton (wave-cron.ts: self-rescheduling,
 * catch→log→reschedule "the cron tick never kills the loop"). observe →
 * diff(desired, observed) → heal|raise → journal. Idempotent. Leader-elected
 * via flock on the registry db (the duplicate-tick law: two managers ticking =
 * duplicate heals — omp reminder dedupe was in-RAM; ours is persisted).
 *
 * GRACE LAW (TCM context-gate.ts:75-88): "the gate never breaks the host
 * workflow" — a probe/heal failure degrades to a journal row, never throws
 * out of tick(). PREEMPTION LAW: user-active → the tick YIELDS, journaled.
 */
import { Journal, JournalKind } from "./journal.js";
import { check, ShallowProbe, DeepProbe } from "./health.js";

export interface Desired {
  id: string;
  kind: string;                       // "daemon" | "cron" | "trigger" | "seat" | "channel"
  shallow: ShallowProbe;
  deep?: DeepProbe;
  /** Returns the diff ("drifted") or null ("compliant"). Implementations compare
   * OBSERVED (freshly measured — never cached) against desired. */
  observe: () => Promise<string | null>;
  /** The heal action for the named drift. Idempotent. Returns evidence. */
  heal?: (drift: string) => Promise<string>;
}

export interface TickVerdict {
  ran: boolean;                // false = yielded (preemption) or lost leader election
  checked: number;
  drifted: number;
  healed: number;
  raised: number;
}

export interface ReconcileOpts {
  instanceId: string;
  journal: Journal;
  /** Preemption probe: truthy = a user is active; the tick yields (IL-2 law). */
  userActive?: () => boolean;
  /** Leader-election flock path — derived from the instance db, not /tmp. */
  lockPath: string;
  probes: Desired[];
  slowMs?: number;
  timeoutMs?: number;
}

function flockAcquire(path: string): number | null {
  try {
    const fs = require("node:fs");
    fs.writeFileSync(path, String(process.pid), { flag: "w" });
    const fd = require("node:fs").openSync(path, "r+");
    require("node:fs").flockSync?.(fd, "EX", "NX");  // Bun/node16+: best-effort
    return fd;
  } catch {
    return null; // another leader holds the lock
  }
}

function flockRelease(fd: number): void {
  try {
    const fs = require("node:fs");
    fs.flockSync?.(fd, "UN");
    fs.closeSync(fd);
  } catch { /* lock released by exit anyway */ }
}

export async function tick(o: ReconcileOpts): Promise<TickVerdict> {
  if (o.userActive?.()) {
    // THE YIELD LAW (sync_watch.py:82-87): "A user-active tick yields — the
    // brief is NOT built, digests stay unpromoted, the yield is the result."
    o.journal.append(o.instanceId, "yield", "user-active — reconciliation tick yielded");
    return { ran: false, checked: 0, drifted: 0, healed: 0, raised: 0 };
  }

  const fd = flockAcquire(o.lockPath);
  if (fd === null) {
    o.journal.append(o.instanceId, "status", "tick skipped — another instance holds the leader lock");
    return { ran: false, checked: 0, drifted: 0, healed: 0, raised: 0 };
  }

  let checked = 0, drifted = 0, healed = 0, raised = 0;
  try {
    for (const d of o.probes) {
      checked += 1;
      // (a) liveness — dual-check via health.ts
      const hp = await check({ name: d.id, shallow: d.shallow, deep: d.deep,
                               slowMs: o.slowMs, timeoutMs: o.timeoutMs, journal: o.journal });
      if (hp.state === "down" || hp.state === "degraded") {
        raised += 1;
        o.journal.append(d.id, "raise", `${hp.state.toUpperCase()}: ${hp.evidence}`);
      }
      // (b) drift — OBSERVED now, never cached
      try {
        const drift = await d.observe();
        if (drift !== null) {
          drifted += 1;
          if (d.heal) {
            const evidence = await d.heal(drift);
            healed += 1;
            o.journal.append(d.id, "heal", `drift='${drift.slice(0, 120)}' healed: ${evidence.slice(0, 120)}`);
          } else {
            raised += 1;
            o.journal.append(d.id, "raise", `drift (no heal verb): ${drift.slice(0, 160)}`);
          }
        }
      } catch (e) {
        raised += 1;
        o.journal.append(d.id, "error", `observe failed (grace law — degraded, not thrown): ${String(e).slice(0, 140)}`);
      }
    }
    o.journal.append(o.instanceId, "observe",
      `tick complete: checked=${checked} drifted=${drifted} healed=${healed} raised=${raised}`);
    return { ran: true, checked, drifted, healed, raised };
  } finally {
    flockRelease(fd);
  }
}
