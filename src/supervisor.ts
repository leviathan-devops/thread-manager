/**
 * manager-machine/supervisor.ts — THE FLEET SUPERVISOR (v2 assembly).
 *
 * Imports ONLY the boilerplate surface. Owns: the desired-state registry
 * (SQLite WAL via ManagerStore), the reconcile tick (30s + on-demand),
 * the journal (every action lands here), and the Gate (deny-by-default,
 * identity = constructor arg — never a hardcoded project name).
 *
 * Laws carried: desired durable / observed re-measured / reconcile-or-raise
 * (v2 DPL1 §4); kill = journal close row, never bare terminate; every heal
 * verifies its own persistence (desired.ts); grace law — a tick that throws
 * is journaled + swallowed by the loop, never kills it.
 */
import { ManagerStore } from "./state.js";
import { Journal } from "./journal.js";
import { Gate } from "./gating.js";
import { tick, Desired, TickVerdict } from "./reconcile.js";
import {
  RestClient, httpClient,
  daemonDesired, cronDesired, seatDesired,
} from "./desired.js";

export const REGISTRY_DDL = `
CREATE TABLE IF NOT EXISTS devices (id TEXT PRIMARY KEY, host TEXT, capabilities TEXT);
CREATE TABLE IF NOT EXISTS daemons (id TEXT PRIMARY KEY, device_id TEXT, base_url TEXT, desired TEXT);
CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, daemon_id TEXT, kind TEXT, status TEXT, desired TEXT);
CREATE TABLE IF NOT EXISTS crons (id TEXT PRIMARY KEY, daemon_id TEXT, name TEXT, desired_enabled INTEGER);
CREATE TABLE IF NOT EXISTS seats (id TEXT PRIMARY KEY, daemon_id TEXT, job_id TEXT, board_dir TEXT, pid INTEGER, desired TEXT);
CREATE TABLE IF NOT EXISTS findings (id TEXT PRIMARY KEY, class TEXT, severity TEXT, source TEXT, status TEXT, test_name TEXT);
`;

export interface SupervisorOpts {
  storePath?: string;
  journalPath?: string;
  identity: string;             // the ONLY allowed caller — consumer's choice
  port?: number;                // REST face; 0 = disabled (tests)
  tickSecs?: number;            // default 30 (D3)
  userActive?: () => boolean;
  clientFor?: (baseUrl: string) => RestClient;  // injectable (tests use fakes)
}

export class Supervisor {
  readonly store: ManagerStore;
  readonly journal: Journal;
  readonly gate: Gate;
  readonly port: number;
  readonly tickSecs: number;
  private timer?: ReturnType<typeof setInterval>;
  private lastVerdict: (TickVerdict & { at: string }) | null = null;
  private clients = new Map<string, RestClient>();
  private clientFor: (baseUrl: string) => RestClient;
  private userActive?: () => boolean;

  constructor(o: SupervisorOpts) {
    if (!o.identity) throw new Error("MM-NO-IDENTITY: Gate identity required (consumer's choice)");
    this.store = new ManagerStore(o.storePath ? { path: o.storePath } : {});
    this.store.migrate(REGISTRY_DDL);
    const dir = this.store.path.replace(/\/[^/]+$/, "");
    this.journal = new Journal(o.journalPath ?? `${dir}/journal.jsonl`);
    this.gate = new Gate(o.identity);
    this.port = o.port ?? 4210;
    this.tickSecs = o.tickSecs ?? 30;
    this.clientFor = o.clientFor ?? httpClient;
    this.userActive = o.userActive;
  }

  private client(baseUrl: string): RestClient {
    let c = this.clients.get(baseUrl);
    if (!c) {
      c = this.clientFor(baseUrl);
      this.clients.set(baseUrl, c);
    }
    return c;
  }

  // ── registry writes (upserts — the only durable truth) ──
  upsertDaemon(id: string, baseUrl: string, desired = "active", deviceId = ""): void {
    this.store.upsert("daemons", "id", id, {
      device_id: deviceId, base_url: baseUrl, desired,
    });
    this.journal.append(id, "status", `daemon registered base=${baseUrl} desired=${desired}`);
  }

  /** Boot declaration: insert-only. An operator-retired row (or a changed
   * base_url via /register) must NEVER be clobbered back to active by a
   * restart — upsert would resurrect probes. */
  ensureDaemon(id: string, baseUrl: string, deviceId = ""): void {
    const existing = this.store.rows<{ id: string }>(
      "SELECT id FROM daemons WHERE id=?", id);
    if (existing.length === 0) {
      this.upsertDaemon(id, baseUrl, "active", deviceId);
    } else {
      this.journal.append(id, "status", "daemon row present — boot preserves existing desired (no clobber)");
    }
  }

  upsertCron(daemonId: string, jobId: string, name: string, desiredEnabled: boolean): void {
    this.store.upsert("crons", "id", `${daemonId}:${jobId}`, {
      daemon_id: daemonId, name, desired_enabled: desiredEnabled ? 1 : 0,
    });
  }

  upsertChannel(daemonId: string, channel: string, kind: string, desired: string): void {
    this.store.upsert("channels", "id", `${daemonId}:${channel}`, {
      daemon_id: daemonId, kind, status: "unknown", desired,
    });
  }

  upsertSeat(daemonId: string, seatId: string, jobId: string, boardDir: string, desired = "active"): void {
    this.store.upsert("seats", "id", `${daemonId}:${seatId}`, {
      daemon_id: daemonId, job_id: jobId, board_dir: boardDir, pid: null, desired,
    });
  }

  recordFinding(id: string, cls: string, severity: string, source: string, status: string, testName: string): void {
    this.store.upsert("findings", "id", id, {
      class: cls, severity, source, status, test_name: testName,
    });
  }

  registryHash(): string {
    // Per-table hashes concatenated: UNION ALL across different column
    // counts is a SQL error, and one table must never mask another.
    const h = require("node:crypto").createHash("sha256");
    for (const t of ["devices", "daemons", "channels", "crons", "seats", "findings"]) {
      h.update(JSON.stringify(this.store.rows(`SELECT * FROM ${t} ORDER BY id`)));
      h.update("|");
    }
    return h.digest("hex").slice(0, 16);
  }

  // ── probes from registry rows ──
  buildProbes(): Desired[] {
    const probes: Desired[] = [];
    const daemons = this.store.rows<{ id: string; base_url: string; desired: string }>(
      "SELECT id, base_url, desired FROM daemons WHERE desired='active'");
    for (const d of daemons) {
      const client = this.client(d.base_url);
      probes.push(daemonDesired(d.id, client));
      const crons = this.store.rows<{ id: string; name: string; desired_enabled: number }>(
        "SELECT id, name, desired_enabled FROM crons WHERE daemon_id=?", d.id);
      for (const c of crons) {
        const jobId = c.id.split(":").slice(1).join(":");
        probes.push(cronDesired(
          { daemonId: d.id, jobId, name: c.name, desiredEnabled: c.desired_enabled === 1 },
          client));
      }
      const seats = this.store.rows<{ id: string; job_id: string; board_dir: string }>(
        "SELECT id, job_id, board_dir FROM seats WHERE daemon_id=? AND desired='active'", d.id);
      for (const s of seats) {
        const seatId = s.id.split(":").slice(1).join(":");
        probes.push(seatDesired(
          { daemonId: d.id, seatId, jobId: s.job_id, boardDir: s.board_dir, desired: "active" },
          (sid, pid) => {
            this.store.upsert("seats", "id", s.id, {
              daemon_id: d.id, job_id: s.job_id, board_dir: s.board_dir, pid, desired: "active",
            });
          }));
      }
    }
    return probes;
  }

  async runTick(): Promise<TickVerdict> {
    try {
      const v = await tick({
        instanceId: "supervisor",
        journal: this.journal,
        userActive: this.userActive,
        lockPath: `${this.store.path}.lock`,
        probes: this.buildProbes(),
      });
      this.lastVerdict = { ...v, at: new Date().toISOString() };
      return v;
    } catch (e) {
      // Grace law: the loop never dies. Heal throws + lock failures land here.
      this.journal.append("supervisor", "error",
        `tick threw (loop survives): ${String(e).slice(0, 200)}`);
      return { ran: false, checked: 0, drifted: 0, healed: 0, raised: 1 };
    }
  }

  status(): Record<string, unknown> {
    const count = (t: string) =>
      (this.store.rows<{ n: number }>(`SELECT COUNT(*) AS n FROM ${t}`)[0]?.n ?? 0);
    return {
      registryHash: this.registryHash(),
      daemons: count("daemons"), crons: count("crons"),
      channels: count("channels"), seats: count("seats"),
      findings: count("findings"),
      lastTick: this.lastVerdict,
      journal: this.journal.tail(3),
    };
  }

  start(): void {
    this.journal.append("supervisor", "status",
      `start: tick=${this.tickSecs}s port=${this.port} registry=${this.registryHash()}`);
    // Honest backlog: channel rows have no status source wired, so they are
    // never probed. Say so once per boot instead of dropping them silently.
    const unwired = this.store.rows<{ id: string }>("SELECT id FROM channels");
    for (const c of unwired) {
      this.journal.append(c.id, "status",
        "channel unwired: no status source configured — observe backlog (no heal verb exists)");
    }
    this.timer = setInterval(() => void this.runTick(), this.tickSecs * 1000);
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.journal.append("supervisor", "status", "stop: checkpoint — registry durable, loop halted (never bare terminate)");
  }
}
