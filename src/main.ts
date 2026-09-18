/**
 * manager-machine/main.ts — SUPERVISOR ENTRY.
 *
 * Everything env-derived (the neutrality law): no project nouns, no
 * hardcoded machine paths. Defaults are loopback-only.
 */
import { Supervisor } from "./supervisor.js";
import { serveRest } from "./restface.js";

const identity = process.env.MM_IDENTITY ?? "";
const sup = new Supervisor({
  identity,
  port: process.env.MM_PORT ? Number(process.env.MM_PORT) : 4210,
  tickSecs: process.env.MM_TICK_SECS ? Number(process.env.MM_TICK_SECS) : 30,
});

// The local daemon row is a declaration, not a discovery: the consumer
// sets MM_DAEMON_BASE (default: loopback). Insert-only — a retired row
// stays retired across restarts (never clobber operator intent).
const daemonId = process.env.MM_DAEMON_ID ?? "local-1";
const daemonBase = process.env.MM_DAEMON_BASE ?? "http://127.0.0.1:4200";
sup.ensureDaemon(daemonId, daemonBase);

sup.start();
serveRest(sup);

async function shutdown() {
  await sup.stop();
  process.exit(0);
}
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());

console.log(`fleet supervisor up: identity-gated, tick=${sup.tickSecs}s port=${sup.port}`);
