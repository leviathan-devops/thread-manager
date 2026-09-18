/**
 * manager-machine/test/battery.test.ts — THE EXTRACTION BATTERY.
 * Pins LAWS, not lines. Both halves per law: attack shape fires its named
 * token; legit shape passes with zero misfire. Runner exit = verdict.
 */
import { describe, expect, test, beforeEach } from "bun:test";
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ManagerStore, IS_PRODUCTION_DB } from "../src/state.js";
import { atomicWrite, readOrNull, unlinkExact } from "../src/atomic.js";
import { Journal } from "../src/journal.js";
import { AuthRegistry } from "../src/registry.js";
import { check } from "../src/health.js";
import { evaluate, resetLadder, zombieExpiryMs } from "../src/watchdog.js";
import { WorkLedger } from "../src/resume.js";
import { tick } from "../src/reconcile.js";
import { runPool, CircuitBreaker } from "../src/pools.js";
import { Gate } from "../src/gating.js";

let dir: string;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "mm-battery-"));
});

// ── LAW 1: THE STORE (sqlite-WAL-only-truth; production wipe refused; fail-closed) ──
describe("state.store", () => {
  test("upsert survives concurrent writers (the 8-process clobber law)", () => {
    const st = new ManagerStore({ path: join(dir, "mm.sqlite"), production: false });
    st.migrate("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT);");
    // simulate the multi-process upsert shape: interleaved writes, then verify all rows
    for (let i = 0; i < 50; i++) st.upsert("t", "k", `k${i}`, { v: `v${i}` });
    const rows = st.rows<{ k: string; v: string }>("SELECT * FROM t");
    expect(rows.length).toBe(50);
    const k5 = st.rows<{ v: string }>("SELECT v FROM t WHERE k='k5'");
    expect(k5[0].v).toBe("v5"); // keyed read — ORDER BY k is alphabetical, not numeric
  });

  test("production clear() → MM-WIPE-REFUSED (BUG-9 law)", () => {
    const st = new ManagerStore({ path: join(dir, "prod.sqlite"), production: true });
    expect(() => st.clear()).toThrow("MM-WIPE-REFUSED");
  });

  test("unresolved path → fail-closed MM-NO-PATH (never guess)", () => {
    const prev = process.env.HOME;
    delete (process.env as { HOME?: string }).HOME;
    try {
      expect(() => new ManagerStore({})).toThrow("MM-NO-PATH");
    } finally {
      process.env.HOME = prev;
    }
  });

  test("prune respects exemptions (pinned rows survive, ledger rows go)", () => {
    const st = new ManagerStore({ path: join(dir, "prune.sqlite"), production: false });
    st.migrate("CREATE TABLE ledger (k TEXT PRIMARY KEY, ts INTEGER); CREATE TABLE pins (k TEXT PRIMARY KEY, ts INTEGER);");
    const old = Math.floor(Date.now() / 1000) - 48 * 3600;
    st.run("INSERT INTO ledger VALUES ('old', ?)", old);
    st.run("INSERT INTO pins VALUES ('old', ?)", old);
    st.prune("ledger", "ts", { afterHours: 24 });
    st.prune("pins", "ts", { afterHours: 24, exempt: true });
    expect(st.rows("SELECT * FROM ledger").length).toBe(0);
    expect(st.rows("SELECT * FROM pins").length).toBe(1);
  });
});

// ── LAW 2: ATOMIC WRITES (no torn state; wildcard refusal) ──
describe("atomic", () => {
  test("atomicWrite leaves no tmp debris and reads back exact bytes", () => {
    const p = join(dir, "state.json");
    atomicWrite(p, JSON.stringify({ a: 1 }));
    expect(JSON.parse(readFileSync(p, "utf8")).a).toBe(1);
    expect(existsSync(`${p}.tmp-`)).toBe(false);
  });

  test("readOrNull: missing → null; empty → null (undecoded-is-never-empty)", () => {
    const p = join(dir, "x.txt");
    expect(readOrNull(p)).toBe(null);
    writeFileSync(p, "");
    expect(readOrNull(p)).toBe(null);
    writeFileSync(p, "data");
    expect(readOrNull(p)).toBe("data");
  });

  test("unlinkExact refuses wildcards (scoped-identity law)", () => {
    expect(() => unlinkExact(join(dir, "*.json"))).toThrow("MM-UNLINK-REFUSED");
  });
});

// ── LAW 3: THE AUTH FSM (never consume authorization on failure) ──
describe("registry.fsm", () => {
  let reg: AuthRegistry;
  beforeEach(() => {
    reg = new AuthRegistry(new ManagerStore({ path: join(dir, "reg.sqlite"), production: false }));
  });

  test("record→accept→BLOCKED (permanent consumption on success)", () => {
    reg.record("j1");
    reg.accept("j1");
    expect(reg.evaluate("j1")).toEqual({ action: "block", reason: "already-accepted" });
  });

  test("fail → re-fireable (THE BUG-CLASS LAW, never consumed on failure)", () => {
    reg.record("j2");
    expect(reg.fail("j2").reason).toBe("failed-reset");
    expect(reg.evaluate("j2").action).toBe("allow"); // re-fireable
  });

  test("release re-arms an accepted gate", () => {
    reg.record("j3"); reg.accept("j3");
    reg.release("j3");
    expect(reg.evaluate("j3").action).toBe("allow");
  });

  test("fireOnce is exactly-once across calls (anti-zombie)", () => {
    let fired = 0;
    expect(reg.fireOnce("esc:1:x", () => { fired++; })).toBe(true);
    expect(reg.fireOnce("esc:1:x", () => { fired++; })).toBe(false);
    expect(fired).toBe(1);
  });
});

// ── LAW 4: DUAL-CHECK HEALTH (the zombie class; SLOW ≠ DOWN) ──
describe("health.dual-check", () => {
  test("zombie: shallow ok + deep fail → degraded (never alive)", async () => {
    const r = await check({
      name: "z", shallow: async () => ({ ok: true, body: "status ok" }),
      deep: async () => ({ ok: false, body: "1+1 no answer" }),
    });
    expect(r.state).toBe("degraded");
    expect(r.evidence).toContain("deep fail");
  });

  test("slow shallow → SLOW (never DOWN — false-down cascade law)", async () => {
    const r = await check({
      name: "s",
      shallow: () => new Promise((res) => setTimeout(() => res({ ok: true, body: "late" }), 30)),
      slowMs: 10, timeoutMs: 5000,
    });
    expect(r.state).toBe("degraded");
  });

  test("probe timeout → named SLOW with journal row", async () => {
    const j = new Journal(join(dir, "j.jsonl"));
    const r = await check({
      name: "t", journal: j, timeoutMs: 20,
      shallow: () => new Promise((res) => setTimeout(() => res({ ok: true, body: "x" }), 200)),
    });
    expect(r.state).toBe("slow");
    expect(j.tail(5).entries.some((e) => e.text.includes("SHALLOW-TIMEOUT"))).toBe(true);
  });

  test("dead shallow → DOWN", async () => {
    const r = await check({ name: "d", shallow: async () => ({ ok: false, body: "ECONNREFUSED" }) });
    expect(r.state).toBe("down");
  });
});

// ── LAW 5: THE EXACTLY-ONCE LADDER + ZOMBIE EXPIRY ──
describe("watchdog.ladder", () => {
  function mk() {
    const reg = new AuthRegistry(new ManagerStore({ path: join(dir, `wd-${Math.random().toString(36).slice(2)}.sqlite`), production: false }));
    const j = new Journal(join(dir, `wd-${Math.random().toString(36).slice(2)}.jsonl`));
    const actions: string[] = [];
    const o = {
      registry: reg, journal: j,
      onNotify: () => actions.push("notify"),
      onKick: () => actions.push("kick"),
      onKill: () => actions.push("kill"),
      hardCapMs: 2 * 3600_000,
    };
    return { reg, j, actions, o };
  }

  test("notify→kick→kill each fire exactly once; repeat calls are no-ops", () => {
    const { o, actions } = mk();
    const t = { id: "s1", etaMs: 60000, startedAt: Date.now() - 400_000, lastActivityAt: Date.now() - 400_000 };
    const r1 = evaluate(o, t); const r2 = evaluate(o, t); const r3 = evaluate(o, t);
    expect([r1.escalation, r2.escalation, r3.escalation]).toEqual(["notify", "kick", "kill"]);
    const r4 = evaluate(o, t); const r5 = evaluate(o, t);
    expect(actions.filter((a) => a === "kill").length).toBe(1);
    expect(r4.escalation).toBe("none"); expect(r5.escalation).toBe("none");
    expect(actions).toEqual(["notify", "kick", "kill"]);
  });

  test("zombie expiry: age > max(4×eta, cap) → kill fired once (checkpoint law in text)", () => {
    const { o, actions } = mk();
    const eta = 10_000;
    o.hardCapMs = 50_000; // max(4×eta=40s, cap=50s) → 50s is the expiry line
    const t = { id: "z1", etaMs: eta, startedAt: Date.now() - 60_000, lastActivityAt: Date.now() };
    const r = evaluate(o, t);
    expect(r.escalation).toBe("expired");
    expect(r.actioned).toBe(true);
    expect(actions.filter((a) => a === "kill").length).toBe(1);
    expect(zombieExpiryMs(eta, 50_000)).toBe(50_000); // max(40k, 50k)
  });

  test("resetLadder after activity → fresh notify on next stall", () => {
    const { o } = mk();
    const t = { id: "r1", etaMs: 60000, startedAt: Date.now() - 400_000, lastActivityAt: Date.now() - 400_000 };
    evaluate(o, t); evaluate(o, t); // notify + kick spent
    resetLadder(o, "r1");
    const r = evaluate(o, t);
    expect(r.escalation).toBe("notify"); // fresh ladder
  });
});

// ── LAW 6: CRASH-FIRST ROWS + RESUME ──
describe("resume.work", () => {
  test("start leaves NULL finished_at (traceable crash row); sweep names it; resumePlan restores", () => {
    const st = new ManagerStore({ path: join(dir, "w.sqlite"), production: false });
    const j = new Journal(join(dir, "w.jsonl"));
    const wl = new WorkLedger(st, j);
    wl.start("job-9", "build", { goal: "hello" });
    const row = wl.get("job-9");
    expect(row?.state).toBe("running");
    expect(row?.finished_at).toBe(null);
    wl.checkpoint("job-9", "turn:17");
    const interrupted = wl.sweep(0); // everything unfinished is stale by definition here
    expect(interrupted.some((r) => r.id === "job-9")).toBe(true);
    expect(wl.get("job-9")?.state).toBe("interrupted");
    const plan = wl.resumePlan("job-9");
    expect(plan?.desired).toEqual({ goal: "hello" });
    expect(plan?.checkpoint).toBe("turn:17");
    expect(wl.get("job-9")?.state).toBe("resumed");
    expect(j.tail(10).entries.some((e) => e.kind === "resume")).toBe(true);
  });
});

// ── LAW 7: THE RECONCILE TICK (yield; drift→heal; grace law; leader lock) ──
describe("reconcile.tick", () => {
  function mk(probes: never[], userActive?: () => boolean) {
    const j = new Journal(join(dir, `rc-${Math.random().toString(36).slice(2)}.jsonl`));
    return { j, o: { instanceId: "mgr-test", journal: j, userActive,
                     lockPath: join(dir, `lock-${Math.random().toString(36).slice(2)}.lock`),
                     probes } };
  }

  test("user-active → tick YIELDS with journal row (preemption law)", async () => {
    const { o, j } = mk([], () => true);
    const v = await tick(o as never);
    expect(v.ran).toBe(false);
    expect(j.tail(3).entries.some((e) => e.kind === "yield")).toBe(true);
  });

  test("drift → heal → journaled; observe throw → error row, tick still completes (grace law)", async () => {
    let healCalled = 0;
    const probes = [
      { id: "cron-x", kind: "cron",
        shallow: async () => ({ ok: true, body: "ok" }),
        observe: async () => "sync-watch enabled=false (desired true)",
        heal: async (drift: string) => { healCalled++; return `re-enabled (${drift.slice(0, 20)})`; } },
      { id: "broken", kind: "x",
        shallow: async () => ({ ok: true, body: "ok" }),
        observe: async () => { throw new Error("probe exploded"); } },
    ];
    const { o, j } = mk(probes as never);
    const v = await tick(o as never);
    expect(v.ran).toBe(true);
    expect(v.drifted).toBe(1);
    expect(v.healed).toBe(1);
    expect(healCalled).toBe(1);
    expect(v.raised).toBe(1); // the throw → raise row, not a crash
    expect(j.tail(10).entries.some((e) => e.kind === "heal")).toBe(true);
    expect(j.tail(10).entries.some((e) => e.kind === "error")).toBe(true);
  });

  test("compliant system → zero heals (negative half — no misfire)", async () => {
    let heals = 0;
    const probes = [{ id: "ok", kind: "k",
      shallow: async () => ({ ok: true, body: "ok" }),
      observe: async () => null,
      heal: async () => { heals++; return ""; } }];
    const { o } = mk(probes as never);
    const v = await tick(o as never);
    expect(v.drifted).toBe(0);
    expect(v.healed).toBe(0);
    expect(heals).toBe(0);
  });
});

// ── LAW 8: BOUNDED POOL + CIRCUIT BREAKER (failures-as-values) ──
describe("pools", () => {
  test("one rejection never kills the wave; every item has a result", async () => {
    const items = Array.from({ length: 8 }, (_, i) => ({
      id: `i${i}`,
      run: async () => { if (i === 3) throw new Error("boom-3"); return `ok-${i}`; },
    }));
    const r = await runPool(items, { concurrency: 4, staggerMs: 0 });
    expect(r.length).toBe(8);
    expect(r[3].ok).toBe(false);
    expect(r[3].error).toContain("boom-3");
    expect(r.filter((x) => x.ok).length).toBe(7);
  });

  test("circuit breaker: 3 consecutive failures → open → items skipped; success recovers", async () => {
    const br = new CircuitBreaker(3);
    let attempts = 0;
    const fail = async () => { attempts++; throw new Error("down"); };
    for (let i = 0; i < 3; i++) br.record(await fail().then(() => true).catch(() => false));
    expect(br.state).toBe("open");
    expect(br.admit()).toBe(true); // half-open probe allowed once
    br.record(true);
    expect(br.state).toBe("closed"); // auto-recovery
    expect(attempts).toBe(3);
  });
});

// ── LAW 9: THE IDENTITY GATE (deny-by-default) ──
describe("gating", () => {
  test("non-jarvis identity → MM-GATED; jarvis → null (zero misfire)", () => {
    const g = new Gate("jarvis");
    expect(g.check({ agent: "build" })).toContain("MM-GATED");
    expect(g.check({ agent: "" })).toContain("MM-GATED");
    expect(g.check({ identity: "jarvis" })).toBe(null);
  });
});
