/**
 * manager-machine/watchdog.ts — THE EXACTLY-ONCE ESCALATION LADDER.
 *
 * Extracted from trident wave-cron.ts:317-395 + omp-wave-tracker.ts:46-57:
 * stuck → 1 notify / 2 auto-kick / 3 kill-directive — each escalation fires
 * AT MOST ONCE via a persisted flag ("tickAgent re-enqueued the kill directive
 * EVERY tick with NO state change — the zombie factory"), and kill = the
 * CHECKPOINT+RESUME directive (the heartbeat incident law: a watchdog may
 * never murder work — it stops and hands off, resumable).
 *
 * Zombie expiry: max(4×eta, hardCapMs) — trident wave-cron.ts:70-81
 * ("the 7 zombie waves killed 2026-09-05 ran 11.9h-20.5h past 15-33min ETAs
 * because NOTHING archived them").
 */
import { AuthRegistry } from "./registry.js";
import { Journal } from "./journal.js";

export interface WatchTarget {
  id: string;              // what we watch (seat id, daemon id, job id)
  etaMs: number;           // expected duration — drives zombie expiry
  startedAt: number;
  lastActivityAt: number;  // the ONLY liveness signal (stream/heartbeat evidence)
  stuckAfterMs?: number;   // silence threshold, default 300_000
}

export type Escalation = "none" | "notify" | "kick" | "kill" | "expired";

export interface WatchVerdict {
  target: string;
  escalation: Escalation;
  actioned: boolean;       // did THIS call fire the action (exactly-once)
  reason: string;
}

export interface WatchdogOpts {
  registry: AuthRegistry;   // owns fireOnce flags (persisted, exactly-once)
  journal: Journal;
  hardCapMs?: number;       // default 2h (omp-wave-cron.ts:76-87 "2h zombie ruling")
  onNotify?: (t: WatchTarget) => void;
  onKick?: (t: WatchTarget) => void;          // steer/nudge — NON-destructive
  onKill?: (t: WatchTarget) => void;          // MUST checkpoint+resume, never bare-kill
}

export function zombieExpiryMs(etaMs: number, hardCapMs = 2 * 3600_000): number {
  return Math.max(etaMs * 4, hardCapMs);
}

export function evaluate(o: WatchdogOpts, t: WatchTarget, now = Date.now()): WatchVerdict {
  const stuckAfter = t.stuckAfterMs ?? 300_000;
  const silence = now - t.lastActivityAt;
  const age = now - t.startedAt;

  // Zombie expiry FIRST — max(4×eta, cap) is terminal and exactly-once.
  if (age > zombieExpiryMs(t.etaMs, o.hardCapMs)) {
    const actioned = o.registry.fireOnce(`zombie:${t.id}`, () => {
      o.journal.append(t.id, "kill", `ZOMBIE-EXPIRY: age ${age}ms > max(4×eta=${t.etaMs}, cap=${o.hardCapMs ?? 2 * 3600_000}) — checkpoint+archive`);
      o.onKill?.(t);
    });
    return { target: t.id, escalation: "expired", actioned, reason: "age-expired" };
  }

  if (silence <= stuckAfter) {
    return { target: t.id, escalation: "none", actioned: false, reason: `active (${silence}ms silence)` };
  }

  const stuckCount = countEscalations(o.registry, t.id);
  if (stuckCount === 0) {
    const actioned = o.registry.fireOnce(`esc:1:${t.id}`, () => {
      o.journal.append(t.id, "notify", `STUCK: ${silence}ms silence > ${stuckAfter}ms — notify`);
      o.onNotify?.(t);
    });
    return { target: t.id, escalation: "notify", actioned, reason: "silence-1" };
  }
  if (stuckCount === 1) {
    const actioned = o.registry.fireOnce(`esc:2:${t.id}`, () => {
      o.journal.append(t.id, "kick", `STUCK escalation 2 — kick (non-destructive steer)`);
      o.onKick?.(t);
    });
    return { target: t.id, escalation: "kick", actioned, reason: "silence-2" };
  }
  if (stuckCount === 2) {
    const actioned = o.registry.fireOnce(`esc:3:${t.id}`, () => {
      // THE HEARTBEAT-INCIDENT LAW: kill = checkpoint + resume directive.
      o.journal.append(t.id, "kill", "STUCK escalation 3 — CHECKPOINT+RESUME (never bare-terminate)");
      o.onKill?.(t);
    });
    return { target: t.id, escalation: "kill", actioned, reason: "silence-3" };
  }
  return { target: t.id, escalation: "none", actioned: false, reason: "escalations exhausted — holding for operator" };
}

function countEscalations(reg: AuthRegistry, id: string): number {
  let n = 0;
  for (const lvl of [1, 2, 3]) {
    const hit = reg.evaluate(`__fired__esc:${lvl}:${id}`);
    if (hit.action === "block") n += 1; // accepted fired-flag = escalation spent
  }
  return n;
}

/** Reset the ladder after genuine activity (the success-resets-counter law,
 * trident-hooks.ts:2402-2430 degeneracy-loop killer). */
export function resetLadder(o: WatchdogOpts, id: string): void {
  for (const lvl of [1, 2, 3]) o.registry.release(`__fired__esc:${lvl}:${id}`);
  o.registry.release(`__fired__zombie:${id}`);
}
