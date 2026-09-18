/**
 * manager-machine/journal.ts — THE APPEND-ONLY TYPED LEDGER.
 *
 * Extracted from VC ledger.ts:19-50 ("atomic append, never rewrite"; typed
 * streams verdicts/warheads/tokens/escalations) + TCM loss-census + the
 * manager-law that the journal IS the resume point. Every manager action,
 * every heal, every yield lands here. Lines are JSON with {ts, actor, kind, ...}.
 */
import { appendLine } from "./atomic.js";

export type JournalKind =
  | "spawn" | "heal" | "yield" | "raise" | "notify" | "kick" | "kill"
  | "status" | "observe" | "resume" | "error" | "gate";

export interface JournalEntry {
  ts: string;            // ISO UTC
  seq: number;           // monotonic per journal file
  actor: string;         // who acted (seat id, "manager", "operator")
  kind: JournalKind;
  text: string;          // the payload — named errors quoted verbatim
}

export class Journal {
  private seq = 0;
  constructor(readonly path: string) {}

  append(actor: string, kind: JournalKind, text: string): JournalEntry {
    this.seq += 1;
    const e: JournalEntry = {
      ts: new Date().toISOString(),
      seq: this.seq,
      actor,
      kind,
      text,
    };
    appendLine(this.path, JSON.stringify(e));
    return e;
  }

  /** Tail read for status surfaces — counts-first (finding F16 law: never bare
   * truncation without "showing X of Y"). */
  tail(n: number): { showing: number; total: number; entries: JournalEntry[] } {
    let raw: string[] = [];
    try {
      raw = require("node:fs").readFileSync(this.path, "utf8")
        .split("\n").filter((l) => l.trim().length > 0);
    } catch {
      return { showing: 0, total: 0, entries: [] };
    }
    const total = raw.length;
    const slice = raw.slice(-n);
    return {
      showing: slice.length,
      total,
      entries: slice.map((l) => {
        try { return JSON.parse(l) as JournalEntry; }
        catch { return { ts: "?", seq: -1, actor: "?", kind: "error", text: `MM-JOURNAL-TORN-LINE: ${l.slice(0, 120)}` }; }
      }),
    };
  }
}
