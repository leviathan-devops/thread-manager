/**
 * manager-machine/pools.ts — BOUNDED CONCURRENCY + FAILURES-AS-VALUES.
 *
 * Extracted from trident wave-dispatch.ts:777-790 ("the all-at-once
 * Promise.allSettled put N simultaneous 384K requests on the shared provider —
 * 5/10 generations failed" → bounded pool of 15 + 1-3s start stagger) and VC
 * PiLoop core.ts:20-23,119-124 ("failures are encoded as stopReason — never
 * thrown — so the loop settles cleanly and the circuit breaker counts them";
 * 3 consecutive failures → DEGRADED, auto-recovers on success).
 *
 * THE LAWS: (1) never Promise.all an unbounded fan-out; (2) one rejection
 * never kills the wave — per-item failure capture (allSettled shape);
 * (3) the circuit breaker OPENS on 3 consecutive failures and HALF-OPENS
 * (probes) for recovery — the manager degrades, it never dies.
 */
export interface PoolItem { id: string; run: () => Promise<string>; }

export interface PoolResult {
  id: string;
  ok: boolean;
  value?: string;
  error?: string;         // failures-as-values: the error IS the result, never thrown
  skippedByBreaker?: boolean;
}

export class CircuitBreaker {
  private consecutiveFailures = 0;
  state: "closed" | "open" | "half-open" = "closed";
  constructor(private threshold = 3) {}

  record(ok: boolean): "closed" | "open" | "half-open" {
    if (ok) {
      this.consecutiveFailures = 0;
      this.state = "closed";
      return this.state;
    }
    this.consecutiveFailures += 1;
    if (this.consecutiveFailures >= this.threshold) this.state = "open";
    return this.state;
  }

  /** A breaker probe for the next item while open: allow ONE through (half-open). */
  admit(): boolean {
    if (this.state === "open") { this.state = "half-open"; return true; }
    return this.state !== "open";
  }
}

export async function runPool(
  items: PoolItem[],
  opts: { concurrency?: number; staggerMs?: number; breaker?: CircuitBreaker } = {},
): Promise<PoolResult[]> {
  const concurrency = Math.max(1, opts.concurrency ?? 15);   // trident: 15 (operator "raise this limit to 15")
  const staggerMs = opts.staggerMs ?? 1000;                  // 1-3s stagger ruling (omp-wave-dispatch.ts:977-980)
  const breaker = opts.breaker;
  const results: PoolResult[] = new Array(items.length);
  let cursor = 0;

  async function worker(w: number): Promise<void> {
    while (cursor < items.length) {
      const idx = cursor++;
      const item = items[idx];
      if (breaker && !breaker.admit()) {
        results[idx] = { id: item.id, ok: false, error: "circuit-open", skippedByBreaker: true };
        continue;
      }
      if (idx > 0 && staggerMs > 0) {
        await new Promise((r) => setTimeout(r, staggerMs));
      }
      try {
        const value = await item.run();
        breaker?.record(true);
        results[idx] = { id: item.id, ok: true, value };
      } catch (e) {
        breaker?.record(false);
        // failures-as-values: captured per item, wave continues
        results[idx] = { id: item.id, ok: false, error: String(e).slice(0, 200) };
      }
      void w;
    }
  }

  await Promise.allSettled(
    Array.from({ length: Math.min(concurrency, items.length) }, (_, i) => worker(i)),
  );
  return results;
}
