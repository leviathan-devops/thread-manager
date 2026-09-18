/**
 * manager-machine/desired.ts — CONSUMER Desired implementations.
 *
 * Generic fleet probes built on reconcile.ts Desired. No project nouns:
 * every endpoint, path, and identity arrives via constructor args or env.
 * Every mutating heal verifies its own persistence with a post-write
 * state read (the CLI-enable-lies law) and throws loud when the
 * read-back disagrees — tick() journals heal throws via the grace law
 * only when the SUPERVISOR wraps runTick; heal itself never swallows.
 */
import type { Desired } from "./reconcile.js";

export interface RestClient {
  get(path: string): Promise<unknown>;
  put(path: string, body: unknown): Promise<unknown>;
  post(path: string, body: unknown): Promise<unknown>;
}

export function httpClient(baseUrl: string, timeoutMs = 10000): RestClient {
  async function req(method: string, path: string, body?: unknown): Promise<unknown> {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await fetch(baseUrl + path, {
        method,
        signal: ctrl.signal,
        headers: { "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`MM-REST-${r.status}: ${method} ${path}`);
      return (await r.json()) as unknown;
    } finally {
      clearTimeout(t);
    }
  }
  return {
    get: (p) => req("GET", p),
    put: (p, b) => req("PUT", p, b),
    post: (p, b) => req("POST", p, b),
  };
}

function asRecord(v: unknown): Record<string, unknown> {
  return (typeof v === "object" && v !== null ? v : {}) as Record<string, unknown>;
}

/** Daemon liveness = the health dual-check (tick runs it). observe() carries
 * no diff — a registered+desired daemon IS the desired state; DOWN is a
 * raise, never a heal (the manager never restarts daemons). */
export function daemonDesired(id: string, client: RestClient): Desired {
  return {
    id: `daemon:${id}`,
    kind: "daemon",
    shallow: async () => {
      const h = asRecord(await client.get("/api/health"));
      const ok = h["status"] === "ok" || Object.keys(h).length > 0;
      return { ok, body: JSON.stringify(h).slice(0, 200) };
    },
    deep: async () => {
      const agents = await client.get("/api/agents");
      const n = Array.isArray(agents) ? agents.length : -1;
      return { ok: n >= 0, body: `agents=${n}` };
    },
    observe: async () => null,
  };
}

export interface CronSpec {
  daemonId: string;
  jobId: string;
  name: string;
  desiredEnabled: boolean;
}

/** Cron enablement: REST list vs desired → heal via PUT + read-back.
 * The read-back is the whole point (CLI enable lies; so can REST). */
export function cronDesired(spec: CronSpec, client: RestClient): Desired {
  const statusPath = `/api/cron/jobs/${spec.jobId}/status`;
  async function readEnabled(): Promise<boolean> {
    const meta = asRecord(await client.get(statusPath));
    // Status meta nests the flag: {job: {enabled: bool}} (the list
    // endpoint carries top-level `enabled`). Read both — never assume.
    if (meta["enabled"] === true) return true;
    return asRecord(meta["job"])["enabled"] === true;
  }
  return {
    id: `cron:${spec.daemonId}:${spec.jobId}`,
    kind: "cron",
    shallow: async () => {
      try {
        await readEnabled();
        return { ok: true, body: `job ${spec.jobId} readable` };
      } catch (e) {
        return { ok: false, body: String(e).slice(0, 160) };
      }
    },
    observe: async () => {
      const enabled = await readEnabled();
      if (enabled === spec.desiredEnabled) return null;
      return `job '${spec.name}' enabled=${enabled} desired=${spec.desiredEnabled}`;
    },
    heal: async (drift: string) => {
      await client.put(statusPath.replace(/\/status$/, "/enable"), {
        enabled: spec.desiredEnabled,
      });
      const after = await readEnabled();
      if (after !== spec.desiredEnabled) {
        throw new Error(
          `MM-CRON-UNVERIFIED: ${drift} — read-back enabled=${after}`
        );
      }
      return `PUT enable → read-back enabled=${after}`;
    },
  };
}

export interface ChannelSpec {
  daemonId: string;
  channel: string;
  desired: string;
}

/** Channels have no mutating REST verb: drift → journaled raise, never a
 * pretend heal. The reader is injected (whatever source the consumer wires). */
export function channelDesired(
  spec: ChannelSpec,
  readStatus: () => Promise<string | null>
): Desired {
  return {
    id: `channel:${spec.daemonId}:${spec.channel}`,
    kind: "channel",
    shallow: async () => {
      const s = await readStatus();
      return { ok: s !== null, body: s ?? "no channel source" };
    },
    observe: async () => {
      const s = await readStatus();
      if (s === null || s === spec.desired) return null;
      return `channel '${spec.channel}' status='${s}' desired='${spec.desired}' (no heal verb — raise only)`;
    },
  };
}

export interface SeatSpec {
  daemonId: string;
  seatId: string;
  jobId: string;
  boardDir: string;
  desired: string; // "active" — retired seats are deregistered, not probed
}

function readSeatState(boardDir: string, jobId: string): string | null {
  try {
    const fs = require("node:fs");
    const raw = fs.readFileSync(`${boardDir}/${jobId}/STATUS.json`, "utf8");
    const s = JSON.parse(raw) as { state?: unknown };
    return typeof s.state === "string" ? s.state : null;
  } catch {
    return null;
  }
}

function pidAlive(pid: unknown): boolean {
  if (typeof pid !== "number") return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Seat drift: dead/missing STATUS while desired active → heal adopts the
 * live pid into the registry when the process still exists, else journals
 * the respawn plan. Never terminates, never spawns (factory owns that). */
export function seatDesired(
  spec: SeatSpec,
  onAdopt: (seatId: string, pid: number | null) => void
): Desired {
  return {
    id: `seat:${spec.daemonId}:${spec.seatId}`,
    kind: "seat",
    shallow: async () => {
      const st = readSeatState(spec.boardDir, spec.jobId);
      return { ok: st !== null, body: st ?? "STATUS unreadable" };
    },
    observe: async () => {
      const st = readSeatState(spec.boardDir, spec.jobId);
      if (st === null) return `seat '${spec.seatId}' STATUS missing (desired ${spec.desired})`;
      if (st === "dead" || st === "blocked") return `seat '${spec.seatId}' state='${st}' (desired ${spec.desired})`;
      return null;
    },
    heal: async (drift: string) => {
      let pid: number | null = null;
      try {
        const fs = require("node:fs");
        const raw = fs.readFileSync(`${spec.boardDir}/${spec.jobId}/STATUS.json`, "utf8");
        const s = JSON.parse(raw) as { pid?: unknown };
        if (pidAlive(s.pid)) pid = s.pid as number;
      } catch { /* adopt nothing */ }
      onAdopt(spec.seatId, pid);
      return pid !== null
        ? `adopted live pid ${pid} into registry (${drift.slice(0, 80)})`
        : `no live pid — respawn plan journaled, factory owns spawn (${drift.slice(0, 80)})`;
    },
  };
}
