// main.ts — the threads-manager runner (OMP corpus flavor).
// Commands: tick (one reconcile) · stale (the raise report) · daemon (20m cadence, flock-led)
// Kill path: stop the daemon (hub stop threads-manager) + rm -rf ~/.omp/threads/thread-manager/store.sqlite*
import { Supervisor, REGISTRY_DDL } from "./src/index.js";
import { ManagerStore } from "./src/state.js";
import { Journal } from "./src/journal.js";
import { buildFleet, THREADS_STORE, mintThreadDb } from "./fleet.ts";
import { AuthRegistry } from "./src/registry.js";
import { openSync, closeSync, unlinkSync } from "node:fs";

const LOCK = "/tmp/omp-threads-manager.lock";
const IDENTITY = "omp-threads-corpus";
const DB = process.env.MM_DB ?? `${process.env.HOME}/.omp/threads/thread-manager/store.sqlite`;

function acquireLeader(): boolean {
  try { closeSync(openSync(LOCK, "wx")); return true; }
  catch { return false; } // O_EXCL: second tick loses the election
}
function releaseLeader(): void { try { unlinkSync(LOCK); } catch {} }

function ledger(): AuthRegistry {
  const store = new ManagerStore({ path: DB });
  store.migrate(REGISTRY_DDL);
  return new AuthRegistry(store);
}
function recordedMints(): Array<{ key: string; status: string }> {
  const store = new ManagerStore({ path: DB });
  store.migrate(REGISTRY_DDL);
  return store.rows("SELECT key, status FROM mm_attempts WHERE status = 'recorded'") as Array<{ key: string; status: string }>;
}

/** RESUME SWEEP: crash-first rows (recorded, never accepted) re-fire first.
 *  Lock-free: mints are atomic renames — safe even beside the daemon. */
async function resumeSweep(): Promise<number> {
  const reg = ledger();
  const pending = recordedMints();
  let n = 0;
  for (const a of pending) {
    if (a.status !== "recorded" || !a.key.startsWith("mint:")) continue;
    mintThreadDb(a.key.slice(5));
    reg.accept(a.key);
    n++;
    console.log("  RESUME re-fired:", a.key);
  }
  return n;
}

async function runTick(): Promise<void> {
  if (!acquireLeader()) { console.log("TICK YIELDED: another tick holds the leader lock"); return; }
  try {
    const journal = new Journal(DB.replace(/store\.sqlite$/, "journal.jsonl"));
    const probes = buildFleet();
    let checked = 0, healed = 0, raised = 0;
    const lines: string[] = [];
    for (const p of probes) {
      checked++;
      try {
        const drift = await p.observe();
        if (drift === null) continue;
        if (p.heal) {
          const reg = ledger();
          const key = `mint:${p.id.replace("per-thread-db:", "")}`;
          if (p.id.startsWith("per-thread-db:")) reg.record(key);
          const evidence = await p.heal(drift);
          if (p.id.startsWith("per-thread-db:")) reg.accept(key);
          healed++;
          lines.push(`HEAL ${p.id}: ${evidence}`);
          journal.append({ at: new Date().toISOString(), kind: "heal", id: p.id, detail: evidence });
        } else {
          raised++;
          lines.push(`RAISE ${p.id}: ${drift}`);
          journal.append({ at: new Date().toISOString(), kind: "raise", id: p.id, detail: drift });
        }
      } catch (e) {
        lines.push(`GRACE ${p.id}: ${String(e).slice(0, 100)}`);
        journal.append({ at: new Date().toISOString(), kind: "grace", id: p.id, detail: String(e).slice(0, 200) });
      }
    }
    console.log(`TICK: checked=${checked} healed=${healed} raised=${raised}`);
    for (const l of lines) console.log("  " + l);
  } finally {
    releaseLeader();
  }
}

function runStale(): void {
  // the raise-only report without touching anything
  const probes = buildFleet().filter((p) => !p.heal);
  for (const p of probes) {
    p.observe().then((drift) => console.log(drift ? `RAISE ${p.id}: ${drift}` : `OK ${p.id}`));
  }
}

const cmd = process.argv[2] ?? "tick";
if (cmd === "resume") { const n = await resumeSweep(); console.log(`resume sweep complete: ${n} re-fired`); }
else if (cmd === "tick") await runTick();
else if (cmd === "stale") runStale();
else if (cmd === "daemon") {
  if (!acquireLeader()) { console.error("another daemon holds the lock"); process.exit(1); }
  const sup = new Supervisor({
    identity: IDENTITY,
    storePath: DB,
    journalPath: DB.replace(/store\.sqlite$/, "journal.jsonl"),
    tickSecs: 1200, // 20 minutes — named explicitly
  });
  console.log(`threads-manager daemon: identity=${IDENTITY} db=${DB} cadence=20m`);
  setInterval(() => { runTick().catch((e) => console.error("[grace]", String(e).slice(0, 100))); }, 1_200_000).unref?.();
  runTick().catch((e) => console.error("[grace]", String(e).slice(0, 100)));
  // keep alive
  setInterval(() => {}, 60_000);
} else {
  console.error("usage: main.ts tick|stale|daemon");
  process.exit(2);
}
