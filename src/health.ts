/**
 * manager-machine/health.ts — DUAL-CHECK LIVENESS.
 *
 * Extracted from plutus chrome-watchdog.sh:64-96 ("The browser process can
 * stay alive while the renderer is dead ('zombie Chrome'), so this catches
 * cases that the HTTP check above cannot") + trident wave-read.ts:76
 * ("THE SESSION STREAM IS THE ONLY LIVENESS TRUTH — NEVER task_status").
 *
 * THE LAW: shallow probe (status endpoint) ≠ alive. Alive requires a DEEP
 * real-command round-trip whose RESULT carries evidence. SLOW is distinct
 * from DOWN — the false-down cascade (finding F2) is a defect, not a verdict.
 */
import { Journal } from "./journal.js";

export interface ProbeResult {
  state: "alive" | "degraded" | "slow" | "down";
  evidence: string;      // verbatim probe output — never prose
  latencyMs: number;
}

export type ShallowProbe = () => Promise<{ ok: boolean; body: string }>;
export type DeepProbe = () => Promise<{ ok: boolean; body: string }>;

export interface HealthOpts {
  name: string;
  shallow: ShallowProbe;
  deep?: DeepProbe;              // the zombie catch — omit only for probes with no command surface
  slowMs?: number;               // latency above this = degraded (SLOW), default 5000
  timeoutMs?: number;            // hard probe budget, default 10000
  journal?: Journal;
}

async function withTimeout(p: Promise<{ ok: boolean; body: string }>, ms: number): Promise<{ ok: boolean; body: string; timedOut?: boolean }> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const t = new Promise<{ ok: boolean; body: string; timedOut?: boolean }>((resolve) => {
    timer = setTimeout(() => resolve({ ok: false, body: "MM-PROBE-TIMEOUT", timedOut: true }), ms);
  });
  try {
    return await Promise.race([p, t]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** The dual check. Deep is run whenever shallow passes — the zombie class lives
 * exactly there (process alive, work dead). */
export async function check(o: HealthOpts): Promise<ProbeResult> {
  const t0 = Date.now();
  let shallow: { ok: boolean; body: string; timedOut?: boolean };
  try {
    shallow = await withTimeout(o.shallow(), o.timeoutMs ?? 10000);
  } catch (e) {
    return { state: "down", evidence: `shallow-threw: ${String(e).slice(0, 160)}`, latencyMs: Date.now() - t0 };
  }
  const latencyMs = Date.now() - t0;
  if (shallow.timedOut) {
    o.journal?.append(o.name, "raise", `SHALLOW-TIMEOUT (${o.timeoutMs}ms) — SLOW, not DOWN`);
    return { state: "slow", evidence: "shallow probe timed out", latencyMs };
  }
  if (!shallow.ok) {
    return { state: "down", evidence: shallow.body.slice(0, 160), latencyMs };
  }
  if (!o.deep) {
    const slow = latencyMs > (o.slowMs ?? 5000);
    if (slow) o.journal?.append(o.name, "raise", `SLOW ${latencyMs}ms > ${o.slowMs}ms — degraded, not down`);
    return { state: slow ? "degraded" : "alive", evidence: shallow.body.slice(0, 160), latencyMs };
  }
  const t1 = Date.now();
  let deep: { ok: boolean; body: string; timedOut?: boolean };
  try {
    deep = await withTimeout(o.deep(), o.timeoutMs ?? 10000);
  } catch (e) {
    return { state: "degraded", evidence: `DEEP-THREW (shallow ok): ${String(e).slice(0, 140)}`, latencyMs: Date.now() - t1 };
  }
  if (deep.timedOut) {
    o.journal?.append(o.name, "raise", "DEEP-TIMEOUT — zombie class: shallow ok, command dead");
    return { state: "degraded", evidence: "deep probe timed out (zombie)", latencyMs: Date.now() - t1 };
  }
  if (!deep.ok) {
    o.journal?.append(o.name, "raise", `DEEP-FAIL: ${deep.body.slice(0, 140)}`);
    return { state: "degraded", evidence: `deep fail: ${deep.body.slice(0, 160)}`, latencyMs: Date.now() - t1 };
  }
  return { state: "alive", evidence: `shallow+deep ok (${deep.body.slice(0, 80)})`, latencyMs: Date.now() - t0 };
}
