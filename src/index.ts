/**
 * pi-process-domain — public entry point.
 *
 * A small, deep API for authenticated cross-Pi process-domain coordination.
 * One per-user broker is launched lazily on demand; clients authenticate over
 * an HMAC-SHA-256 challenge and share an immutable aggregate snapshot of a
 * domain (exact revisions, activity generation, busy/idle, spawn reservations,
 * and lease/disconnect certainty). This module exposes only the public surface;
 * birpc and the wire protocol are internal implementation details.
 */

import { DomainClient } from "./internal/client.js";
import { createDeclaration, readDeclaration } from "./internal/declaration.js";
import { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE } from "./internal/errors.js";
import { resolveEndpoint } from "./internal/runtime-path.js";
import type {
  ActivityState,
  DomainFence,
  DomainSignal,
  DomainSnapshot,
  OpenDomainOptions,
  ProcessDomain,
  SpawnReservation,
} from "./internal/domain-types.js";

export type {
  ActivityState,
  DomainFence,
  DomainSignal,
  DomainSnapshot,
  OpenDomainOptions,
  ProcessDomain,
  SpawnReservation,
};
export { ProcessDomainFatalError, isProcessDomainFatalError, FATAL_EXIT_CODE };

/** Environment variable names that form a domain declaration. */
export const ENV_NAMES = {
  DOMAIN_ID: "PI_PROCESS_DOMAIN_ID",
  DOMAIN_KEY: "PI_PROCESS_DOMAIN_KEY",
  PROTOCOL: "PI_PROCESS_DOMAIN_PROTOCOL",
  RESERVATION: "PI_PROCESS_DOMAIN_RESERVATION",
} as const;

export interface OpenDomainResult {
  /** The process-domain handle (already joined as a participant). */
  domain: ProcessDomain;
  /** True when this process created a brand-new domain declaration. */
  created: boolean;
}

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

/**
 * Open (join) a process domain.
 *
 * If a complete domain declaration exists in the environment (set by a parent
 * that called `openDomain()` with `created === true`), this joins that existing
 * domain with the declared key. Otherwise a new domain and a fresh 32-byte key
 * are created and written to the environment for descendants to inherit.
 *
 * Fail-closed: any partial/malformed/wrong declaration is a fatal error rather
 * than being silently ignored or regenerated.
 */
export async function openDomain(options?: Partial<OpenDomainOptions>): Promise<OpenDomainResult> {
  const existing = readDeclaration();
  const created = existing === null;
  const declaration = existing ?? createDeclaration();

  const timeout = options?.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
  if (!Number.isFinite(timeout) || timeout <= 0) {
    throw new ProcessDomainFatalError("INVALID_DECLARATION", "connectTimeoutMs must be a positive finite number");
  }
  const client = new DomainClient({
    declaration,
    initialActivity: options?.initialActivity ?? "idle",
    metadata: options?.metadata ?? {},
    connectTimeoutMs: timeout,
    onFatal: options?.onFatal ?? ((error) => {
      process.exitCode = FATAL_EXIT_CODE;
      console.error(`pi-process-domain fatal: ${error.message}`);
    }),
    createDomain: created,
  });
  await client.open();
  return { domain: client as unknown as ProcessDomain, created };
}

/** Resolve the per-user broker endpoint (path or named pipe) for diagnostics. */
export function brokerEndpoint(): string {
  return resolveEndpoint().endpointPath;
}
