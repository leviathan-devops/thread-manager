/**
 * manager-machine/index.ts — the boilerplate surface (one import for consumers).
 * Provenance + laws: see README.md (written only after the battery is green —
 * extraction Phase 3 precedes Phase 5).
 */
export { ManagerStore, IS_PRODUCTION_DB } from "./state.js";
export type { StoreOptions } from "./state.js";
export { atomicWrite, readOrNull, appendLine, unlinkExact } from "./atomic.js";
export { Journal } from "./journal.js";
export type { JournalEntry, JournalKind } from "./journal.js";
export { AuthRegistry } from "./registry.js";
export type { AttemptRow, AttemptStatus, GateState, GateVerdict } from "./registry.js";
export { check } from "./health.js";
export type { ProbeResult, ShallowProbe, DeepProbe, HealthOpts } from "./health.js";
export { evaluate, zombieExpiryMs, resetLadder } from "./watchdog.js";
export type { WatchTarget, WatchVerdict, Escalation, WatchdogOpts } from "./watchdog.js";
export { WorkLedger } from "./resume.js";
export type { WorkRow } from "./resume.js";
export { tick } from "./reconcile.js";
export type { Desired, TickVerdict, ReconcileOpts } from "./reconcile.js";
export { runPool, CircuitBreaker } from "./pools.js";
export type { PoolItem, PoolResult } from "./pools.js";
export { Gate } from "./gating.js";
export type { GateCall } from "./gating.js";
export { httpClient, daemonDesired, cronDesired, channelDesired, seatDesired } from "./desired.js";
export type { RestClient, CronSpec, ChannelSpec, SeatSpec } from "./desired.js";
export { Supervisor, REGISTRY_DDL } from "./supervisor.js";
export type { SupervisorOpts } from "./supervisor.js";
export { serveRest } from "./restface.js";
export type { RestFace } from "./restface.js";
