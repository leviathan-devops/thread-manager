/**
 * manager-machine/gating.ts — THE IDENTITY GATE.
 *
 * The operator's ruling (2026-09-12): the CnC plane is "gated only to the
 * consuming agent (identity is the CONSUMER's choice — any project names its own)."
 * Deny-by-default: ONLY the caller identity presented at construction passes.
 * The v1 lesson: a tools: allowlist does NOT deny unlisted callers
 * (permissions.md:92) — the gate must be mechanical at entry.
 */
export interface GateCall { agent?: string; identity?: string }

export class Gate {
  constructor(private readonly allowedIdentity: string) {}

  /** Returns the named rejection or null (allowed). Deny-by-default. */
  check(call: GateCall): string | null {
    const who = call.identity ?? call.agent ?? "";
    if (who === this.allowedIdentity) return null;
    return `MM-GATED: identity '${who || "(none)"}' denied — ` +
           `this plane serves '${this.allowedIdentity}' only`;
  }
}
