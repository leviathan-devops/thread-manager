/**
 * manager-machine/test/supervisor.test.ts — THE v2 ASSEMBLY BATTERY.
 *
 * S11: registry survives restart · drift healed ≤1 tick · multi-daemon
 * keyed · Gate denies strangers · yield on user-active. S14: neutrality
 * (asserted in-battery via source grep — zero project nouns).
 * All hermetic: tmp MM_DB, injected fake RestClient, real Store/Journal.
 */
import { describe, expect, test, beforeEach } from "bun:test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { readdirSync, readFileSync } from "node:fs";
import { Supervisor } from "../src/supervisor.js";
import { serveRest } from "../src/restface.js";
import type { RestClient } from "../src/desired.js";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "mm-sup-"));
});

const FAKE_JOBS = () => new Map<string, boolean>([["job-1", true]]);

function fakeClient(jobs: Map<string, boolean>): RestClient {
  return {
    get: async (p: string) => {
      if (p === "/api/health") return { status: "ok" };
      if (p === "/api/agents") return [{ id: "a1" }];
      const m = p.match(/^\/api\/cron\/jobs\/([^/]+)\/status$/);
      if (m) {
        if (!jobs.has(m[1])) throw new Error("MM-REST-404");
        return { id: m[1], enabled: jobs.get(m[1]) };
      }
      throw new Error(`MM-REST-404: ${p}`);
    },
    put: async (p: string, body: unknown) => {
      const m = p.match(/^\/api\/cron\/jobs\/([^/]+)\/enable$/);
      if (m) {
        jobs.set(m[1], (body as { enabled: boolean }).enabled);
        return { id: m[1], enabled: jobs.get(m[1]) };
      }
      throw new Error(`MM-REST-404: ${p}`);
    },
    post: async () => { throw new Error("MM-REST-404"); },
  };
}

function sup(jobs = FAKE_JOBS(), port = 0) {
  return new Supervisor({
    storePath: join(dir, "fleet.sqlite"),
    journalPath: join(dir, "journal.jsonl"),
    identity: "tester",
    port,
    tickSecs: 3600, // no background ticks in tests — runTick drives
    clientFor: () => fakeClient(jobs),
  });
}

// ── S11.1: registry survives restart ──
describe("supervisor.restart", () => {
  test("kill → reopen → rows hash-identical", () => {
    const s1 = sup();
    s1.upsertDaemon("d1", "http://127.0.0.1:4200");
    s1.upsertCron("d1", "job-1", "morning", true);
    s1.upsertSeat("d1", "s-1", "j-1", join(dir, "board"));
    const h1 = s1.registryHash();
    const s2 = new Supervisor({
      storePath: join(dir, "fleet.sqlite"),
      journalPath: join(dir, "journal.jsonl"),
      identity: "tester",
      port: 0,
    });
    expect(s2.registryHash()).toBe(h1);
    expect(s2.status().daemons).toBe(1);
  });
});

// ── S11.2: drift healed within one tick ──
describe("supervisor.heal", () => {
  test("out-of-band cron disable → healed + journaled in one runTick", async () => {
    const jobs = FAKE_JOBS();
    const s = sup(jobs);
    s.upsertDaemon("d1", "http://127.0.0.1:4200");
    s.upsertCron("d1", "job-1", "morning", true);
    jobs.set("job-1", false); // out-of-band disable
    const v = await s.runTick();
    expect(v.ran).toBe(true);
    expect(v.healed).toBe(1);
    expect(jobs.get("job-1")).toBe(true); // read-back verified
    const tail = s.journal.tail(10);
    expect(tail.entries.some((e) => e.kind === "heal")).toBe(true);
  });

  test("nested status shape {job:{enabled}} reads correctly (live-shape pin)", async () => {
    const jobs = FAKE_JOBS();
    const base = fakeClient(jobs);
    const nested: RestClient = {
      ...base,
      get: async (p: string) =>
        p.endsWith("/status") ? { job: { id: "job-1", enabled: jobs.get("job-1") } } : base.get(p),
    };
    const s = new Supervisor({
      storePath: join(dir, "fleet.sqlite"),
      journalPath: join(dir, "journal.jsonl"),
      identity: "tester",
      port: 0,
      clientFor: () => nested,
    });
    s.upsertDaemon("d1", "http://127.0.0.1:4200");
    s.upsertCron("d1", "job-1", "morning", true);
    jobs.set("job-1", false); // out-of-band disable
    const v = await s.runTick();
    expect(v.healed).toBe(1); // would throw MM-CRON-UNVERIFIED on wrong field
    expect(jobs.get("job-1")).toBe(true);
  });

  test("unverifiable heal throws loud (fail-closed, loop survives)", async () => {
    const jobs = FAKE_JOBS();
    const base = fakeClient(jobs);
    const lying: RestClient = {
      ...base,
      // Lie ONLY on the cron read-back: PUT lands, GET still says false.
      get: async (p: string) =>
        p.endsWith("/status") ? { id: "job-1", enabled: false } : base.get(p),
    };
    const s = new Supervisor({
      storePath: join(dir, "fleet.sqlite"),
      journalPath: join(dir, "journal.jsonl"),
      identity: "tester",
      port: 0,
      clientFor: () => lying,
    });
    s.upsertDaemon("d1", "http://127.0.0.1:4200");
    s.upsertCron("d1", "job-1", "morning", true);
    jobs.set("job-1", false);
    const v = await s.runTick(); // must not throw out of runTick
    // Grace law lives per-probe inside tick(): the heal throw is caught,
    // journaled as an error row, and counted raised — the loop survives.
    expect(v.ran).toBe(true);
    expect(v.healed).toBe(0);
    expect(v.raised).toBe(1);
    const tail = s.journal.tail(10);
    expect(tail.entries.some((e) => e.kind === "error" && e.text.includes("MM-CRON-UNVERIFIED"))).toBe(true);
  });
});

// ── S11.3: multi-daemon keying ──
describe("supervisor.isolation", () => {
  test("A's heal never touches B", async () => {
    const jobsA = new Map([["job-1", false]]);
    const jobsB = new Map([["job-1", true]]);
    const clients = new Map([
      ["http://127.0.0.1:4200", fakeClient(jobsA)],
      ["http://127.0.0.1:4201", fakeClient(jobsB)],
    ]);
    const s = new Supervisor({
      storePath: join(dir, "fleet.sqlite"),
      journalPath: join(dir, "journal.jsonl"),
      identity: "tester",
      port: 0,
      clientFor: (base) => clients.get(base)!,
    });
    s.upsertDaemon("a", "http://127.0.0.1:4200");
    s.upsertDaemon("b", "http://127.0.0.1:4201");
    s.upsertCron("a", "job-1", "morning", true);
    s.upsertCron("b", "job-1", "morning", true);
    const v = await s.runTick();
    expect(v.healed).toBe(1);
    expect(jobsA.get("job-1")).toBe(true);
    expect(jobsB.get("job-1")).toBe(true); // untouched, still desired
  });
});

// ── Gate + yield + REST face ──
describe("supervisor.face", () => {
  test("stranger → MM-GATED 403; identity → 200 (real HTTP)", async () => {
    const s = sup();
    const face = serveRest(s, 0, "127.0.0.1");
    try {
      const bad = await fetch(`http://127.0.0.1:${face.port}/status`);
      expect(bad.status).toBe(403);
      const body = (await bad.json()) as { error: string };
      expect(body.error).toMatch("MM-GATED");
      const good = await fetch(`http://127.0.0.1:${face.port}/status`, {
        headers: { "x-mm-identity": "tester" },
      });
      expect(good.status).toBe(200);
      const st = (await good.json()) as { ok: boolean; registryHash: string };
      expect(st.ok).toBe(true);
      expect(typeof st.registryHash).toBe("string");
    } finally {
      face.stop();
      await s.stop();
    }
  });

  test("user-active tick yields journaled", async () => {
    const s = new Supervisor({
      storePath: join(dir, "fleet.sqlite"),
      journalPath: join(dir, "journal.jsonl"),
      identity: "tester",
      port: 0,
      userActive: () => true,
    });
    const v = await s.runTick();
    expect(v.ran).toBe(false);
    expect(s.journal.tail(5).entries.some((e) => e.kind === "yield")).toBe(true);
  });

  test("identity required (fail-closed constructor)", () => {
    expect(() => new Supervisor({ identity: "" })).toThrow("MM-NO-IDENTITY");
  });

  test("ensureDaemon never clobbers retired rows (boot-safety pin)", () => {
    const s = sup();
    s.upsertDaemon("d9", "http://127.0.0.1:4299", "retired", "dev-x");
    s.ensureDaemon("d9", "http://127.0.0.1:4200");
    const row = s.store.rows<{ desired: string; base_url: string }>(
      "SELECT desired, base_url FROM daemons WHERE id='d9'")[0];
    expect(row.desired).toBe("retired");
    expect(row.base_url).toBe("http://127.0.0.1:4299");
    s.ensureDaemon("fresh", "http://127.0.0.1:4200"); // absent → inserted active
    const fresh = s.store.rows<{ desired: string }>(
      "SELECT desired FROM daemons WHERE id='fresh'")[0];
    expect(fresh.desired).toBe("active");
  });

  test("start journals unwired channels (no silent drop)", () => {
    const s = sup();
    s.upsertChannel("d1", "ops", "chat", "configured");
    s.start();
    const tail = s.journal.tail(6);
    expect(tail.entries.some((e) => e.text.includes("channel unwired"))).toBe(true);
    void s.stop();
  });

  test("/register refuses unknown columns (MM-BAD-COLUMN)", async () => {
    const s = sup();
    const face = serveRest(s, 0, "127.0.0.1");
    try {
      const r = await fetch(`http://127.0.0.1:${face.port}/register`, {
        method: "POST",
        headers: { "x-mm-identity": "tester", "content-type": "application/json" },
        body: JSON.stringify({ table: "crons", row: { id: "x", daemon_id: "d", "x;DROP": 1 } }),
      });
      expect(r.status).toBe(400);
      const b = (await r.json()) as { error: string };
      expect(b.error).toMatch("MM-BAD-COLUMN");
    } finally {
      face.stop();
      await s.stop();
    }
  });
});

// ── S14: neutrality — the leak class is RUNTIME literals (paths, URLs,
// identities, env defaults), not lineage comments or example identities
// in tests. Strip comments, then assert zero project nouns in src/*.ts.
describe("supervisor.neutrality", () => {
  test("no project nouns in shipped-code literals (comments stripped)", () => {
    const parts = ["jar", "vis", "open", "fang", "mimo", "qwen", "grok", "plutus", "tri", "dent"];
    const nouns = [
      parts[0] + parts[1], parts[2] + parts[3], parts[4], parts[5],
      parts[6], "omp", parts[7], parts[8] + parts[9],
      "aether", "kraken", "shark", "spider",
    ];
    const bad: string[] = [];
    const root = join(import.meta.dir, "..", "src");
    for (const f of readdirSync(root)) {
      if (!/\.ts$/.test(f)) continue;
      const text = readFileSync(join(root, f), "utf8")
        .replace(/\/\*[\s\S]*?\*\//g, "")
        .replace(/(^|\s)\/\/.*$/gm, "$1");
      const hit = nouns.find((n) => new RegExp(`\\b${n}\\b`, "i").test(text));
      if (hit) bad.push(`src/${f}: ${hit}`);
    }
    expect(bad).toEqual([]);
  });
});
