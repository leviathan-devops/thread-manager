// main.ts — the threads-manager runner (OMP corpus flavor).
// Commands: tick (one reconcile) · stale (the raise report) · daemon (20m cadence, flock-led)
// Kill path: stop the daemon (hub stop threads-manager) + rm -rf ~/.omp/threads/thread-manager/store.sqlite*
import { Supervisor } from "./src/index.js";
import { Journal } from "./src/journal.js";
import { buildFleet, THREADS_STORE } from "./fleet.ts";
import { openSync, closeSync, unlinkSync } from "node:fs";

const LOCK = "/tmp/omp-threads-manager.lock";
const IDENTITY = "omp-threads-corpus";
const DB = process.env.MM_DB ?? `${process.env.HOME}/.omp/threads/thread-manager/store.sqlite`;

function acquireLeader(): boolean {
  try { closeSync(openSync(LOCK, "wx")); return true; }
  catch { return false; } // O_EXCL: second tick loses the election
}
function releaseLeader(): void { try { unlinkSync(LOCK); } catch {} }

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
          const evidence = await p.heal(drift);
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
if (cmd === "tick") await runTick();
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
