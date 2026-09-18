# MANAGER-MACHINE — the extracted boilerplate (v1.0, 2026-09-12)

**What it is (one paragraph):** the convergent machinery of the five proven
manager machines in this workspace (trident wave manager · omp wave manager ·
trident context manager · plutus browser manager · the VC spawn-verify machine),
extracted law-faithfully into ten self-contained TypeScript modules so ANY
manager — jarvis-manager v2 first — is built FROM this, not beside it. The
store is SQLite-WAL-only-truth; authorization is an FSM that never consumes a
failed attempt; escalations are exactly-once; liveness is dual-check observed
work; interrupted work is crash-first-rows plus resume-by-id; the reconcile
tick yields to the user and heals drift; failures are values; the gate is
deny-by-default.

## THE OPERATOR'S LAWS (verbatim, with lineage anchors)

1. "the operator's 8-parallel-session callout... the JSON read-modify-write
   COLLIBUTES across processes" (wave-tracker.ts:191-203) → **SQLite WAL is
   the only durable truth; in-memory Maps are caches, never truth** → `state.ts`.
2. "authorization recorded without confirming the dispatch actually succeeded
   — a wave whose only dispatch attempt was rejected can never be dispatched"
   (BUGREPORT_wave-manager-dispatch-authorization.md:10-13) → **failed is
   re-fireable; accepted is permanent; release re-arms** → `registry.ts`.
3. "the 7 zombie waves killed 2026-09-05 ran 11.9h-20.5h past 15-33min ETAs
   because NOTHING archived them" (wave-cron.ts:70-81) → **every watched thing
   expires at max(4×eta, cap), exactly once** → `watchdog.ts`.
4. "The browser process can stay alive while the renderer is dead ('zombie
   Chrome'), so this catches cases that the HTTP check above cannot"
   (chrome-watchdog.sh:69-74) → **liveness = shallow probe + DEEP real-command
   round-trip; SLOW ≠ DOWN** → `health.ts`.
5. "INSERT OR REPLACE with state=running — the desk exists from the first
   moment of its lifecycle" (omp-desk-registry.ts:59-72) → **crash-first rows;
   the resume sweep is fed by NULL terminal columns** → `resume.ts`.
6. "A user-active tick yields — the yield is the result" (sync_watch.py:82-87)
   → **the reconcile tick yields, journaled, when the user is active** → `reconcile.ts`.
7. "the gate never breaks the host workflow" (TCM context-gate.ts:75-88) →
   **a probe/heal failure degrades to a journal row; tick() never throws out** → `reconcile.ts`.
8. "failures are encoded as stopReason — never thrown" (VC core.ts:119-124) →
   **failures-as-values + 3-strike circuit breaker → degraded, auto-recovery** → `pools.ts`.
9. "kill = CHECKPOINT+RESUME, never bare-terminate" (the live heartbeat
   incident, agent_loop.rs:546-550 pinned by explore-8) → `watchdog.ts onKill` contract.
10. "gated only to the active jarvis agent" (operator, 2026-09-12) →
    **deny-by-default identity gate** → `gating.ts`.

## ARCHITECTURE

```
┌────────────────────────────────────────────────────────────────┐
│                    MANAGER-MACHINE (v1.0)                      │
├──────────────┬─────────────────────────────────────────────────┤
│ state.ts     │ ManagerStore — SQLite WAL, upserts, immediate(),│
│              │ prune+exemptions, BUG-9 clear() refusal          │
│ atomic.ts    │ atomicWrite / readOrNull / appendLine /          │
│              │ unlinkExact (scoped-identity law)                │
│ journal.ts   │ Journal — append-only typed ledger; tail with    │
│              │ counts-first showing=X of=Y                      │
│ registry.ts  │ AuthRegistry — record/accept/fail/release FSM,   │
│              │ evaluate() gate, fireOnce exactly-once flags     │
│ health.ts    │ check() — dual-check: shallow + DEEP command     │
│              │ round-trip; alive|degraded|slow|down + evidence  │
│ watchdog.ts  │ evaluate() ladder notify→kick→kill exactly-once; │
│              │ zombieExpiryMs = max(4×eta, cap); resetLadder    │
│ resume.ts    │ WorkLedger — crash-first rows, checkpoint,       │
│              │ sweep() interrupted, resumePlan(id)              │
│ reconcile.ts │ tick() — yield-on-user-active, leader flock,     │
│              │ observe→diff→heal|raise, grace-law degradation   │
│ pools.ts     │ runPool bounded+staggered; CircuitBreaker 3-strike│
│ gating.ts    │ Gate — deny-by-default identity check            │
└──────────────┴─────────────────────────────────────────────────┘
```

## WHY-WIRED LEDGER (law → mechanism → reason)

| Law | Mechanism | Reason |
|---|---|---|
| sqlite-only-truth | ManagerStore WAL + row upserts | JSON RMW clobbered rows across 8 processes (proven) |
| re-fireable auth | failed→recorded reset | authorization consumed by failure bricked waves (proven BUGREPORT) |
| exactly-once | fireOnce persisted flags | re-enqueued kill directives = zombie factory (proven) |
| dual-check | shallow+deep probe | zombie Chrome / zombie renderer caught only by deep round-trip |
| crash-first | start() before work | NULL finished_at = the resume sweep's input |
| yield-on-user | tick() preemption probe | subconscious acting through live user work (IL-2) |
| grace law | try/catch → journal rows | manager must degrade, host loop survives |
| failures-as-values | PoolResult.error | one rejection never kills the wave (proven 5/10 fail) |
| deny-by-default | Gate.check | tools-allowlist does not deny unlisted callers (v1 lesson) |

## PLUG-IN GUIDE (numbered)

1. `new ManagerStore({ path })` — migrate your DDL.
2. `new Journal(dir + "/journal.jsonl")` — every action lands here.
3. `new AuthRegistry(store)` — gate every dispatch/claim through it.
4. `new WorkLedger(store, journal)` — `start(id)` BEFORE firing work;
   `checkpoint(id, token)` on progress; `resumePlan(id)` after interruption.
5. Implement `Desired` per managed resource (observe = fresh measurement,
   heal = idempotent action); call `tick({probes})` on your cadence.
6. Wrap dispatch fan-outs in `runPool(items, {concurrency, staggerMs, breaker})`.
7. Watch long work with `evaluate()` + `resetLadder()` on genuine activity.
8. Gate the consumer tool with `new Gate("<your agent identity>")`.
9. NEVER write files without `atomicWrite`; NEVER rm without `unlinkExact`.

## PROVENANCE (variant table + deltas)

| Source | Used for | Deltas taken (documented per port law) |
|---|---|---|
| trident wave-db/wave-tracker/wave-cron/wave-registry | state, registry, watchdog, reconcile skeleton | Δ generic table registry; Δ env-injected path |
| omp-wave-db/registry/desk-registry/runner | state, registry, resume (crash-first), constants | Δ per-verb→per-call budgets |
| TCM stores + context-gate | prune exemptions, grace law, half-record detection | Δ store generalized |
| plutus chrome-watchdog | health dual-check, scoped kill | Δ probes are closures, not curl |
| VC ledger + PiLoop | journal, pools circuit breaker | Δ typed kinds enum |
| chaos audits (63+ findings) | atomic.ts, gating.ts, fail-closed paths | the entire defect catalog as design input |

## AUDIT (extraction battery, this session)

- Battery: `test/battery.test.ts` — **25 tests / 66 expects, 0 fail** (run
  2026-09-12, green before any docs were written per Phase 3→5 order).
- Findings caught BY the battery (the skill's predicted first-draft class):
  (1) `sweep()` used strict `<` — a same-millisecond row escaped the
  interrupted sweep → fixed to `<=`; (2) two test-side bugs (alphabetical
  ORDER BY assumption; zombie cap arithmetic) — fixed in the tests, machinery
  unaffected. Re-run green after fixes.
- Left OUT deliberately (stays in the sources): aether/DPL1 prompt
  generation, E/B templates, RpmLedger provider pool, session/part schemas,
  VLM verification, Chrome lifecycle (use-case layers of the sources).

## SHAs

Recorded at ship time by `sha256sum src/*.ts test/*.ts` — see the session
engineering report (v2 series) for the manifest block.
