/**
 * manager-machine/restface.ts — THE THIN FACE.
 *
 * Localhost HTTP over the Supervisor. Every route Gate-checked
 * (MM-GATED, deny-by-default); every mutation journaled. Loopback only —
 * the bind address is a constructor arg defaulting to 127.0.0.1 and is
 * never 0.0.0.0 (an explicit non-loopback value still requires the Gate,
 * but the default posture is loopback).
 */
import type { Supervisor } from "./supervisor.js";

export interface RestFace {
  port: number;
  stop(): void;
}

const TABLES = new Set(["devices", "daemons", "channels", "crons", "seats", "findings"]);

// Column allowlist per table: /register interpolates column names into
// SQL, so unknown keys are refused (MM-BAD-COLUMN) rather than trusted.
// Gate+loopback contains the threat model; this is defense in depth.
const COLUMNS: Record<string, Set<string>> = {
  devices: new Set(["device_id", "host", "capabilities"]),
  daemons: new Set(["device_id", "base_url", "desired"]),
  channels: new Set(["daemon_id", "kind", "status", "desired"]),
  crons: new Set(["daemon_id", "name", "desired_enabled"]),
  seats: new Set(["daemon_id", "job_id", "board_dir", "pid", "desired"]),
  findings: new Set(["class", "severity", "source", "status", "test_name"]),
};

export function serveRest(sup: Supervisor, port?: number, host = "127.0.0.1"): RestFace {
  const p = port ?? sup.port;
  const server = Bun.serve({
    hostname: host,
    port: p,
    fetch: async (req: Request) => {
      const url = new URL(req.url);
      const who = req.headers.get("x-mm-identity") ?? url.searchParams.get("identity") ?? "";
      const denied = sup.gate.check({ identity: who });
      if (denied) {
        sup.journal.append(who || "(none)", "gate", `REST ${url.pathname} denied: ${denied}`);
        return Response.json({ ok: false, error: denied }, { status: 403 });
      }
      try {
        if (url.pathname === "/status" && req.method === "GET") {
          return Response.json({ ok: true, ...sup.status() });
        }
        if (url.pathname === "/tick" && req.method === "POST") {
          const v = await sup.runTick();
          return Response.json({ ok: true, tick: v });
        }
        if (url.pathname === "/registry" && req.method === "GET") {
          const kind = url.searchParams.get("kind") ?? "daemons";
          if (!TABLES.has(kind)) {
            return Response.json({ ok: false, error: `MM-UNKNOWN-TABLE: ${kind}` }, { status: 400 });
          }
          const rows = sup.store.rows(`SELECT * FROM ${kind} LIMIT 200`);
          return Response.json({ ok: true, kind, showing: rows.length, rows });
        }
        if (url.pathname === "/register" && req.method === "POST") {
          const body = (await req.json()) as { table?: string; row?: Record<string, unknown> };
          if (!body.table || !TABLES.has(body.table) || !body.row || typeof body.row.id !== "string") {
            return Response.json(
              { ok: false, error: "MM-BAD-REGISTER: need {table, row{id,…}}" }, { status: 400 });
          }
          const { id, ...rest } = body.row;
          const allowed = COLUMNS[body.table];
          const badCol = Object.keys(rest).find((c) => !allowed.has(c));
          if (badCol) {
            return Response.json(
              { ok: false, error: `MM-BAD-COLUMN: '${badCol}' not a ${body.table} column` }, { status: 400 });
          }
          sup.store.upsert(body.table, "id", id as string, rest);
          sup.journal.append(who, "status", `register ${body.table}:${id}`);
          return Response.json({ ok: true, table: body.table, id });
        }
        return Response.json({ ok: false, error: "MM-NO-ROUTE" }, { status: 404 });
      } catch (e) {
        return Response.json({ ok: false, error: String(e).slice(0, 200) }, { status: 500 });
      }
    },
  });
  sup.journal.append("supervisor", "status", `restface: listening ${host}:${p} (Gate: identity-checked)`);
  return { port: server.port, stop: () => server.stop() };
}
