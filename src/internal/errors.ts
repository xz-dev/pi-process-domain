/**
 * Typed fatal errors for pi-process-domain.
 */

export const LEGACY_PROCESS_DOMAIN_PROTOCOL_MAJOR = 1 as const;
export const EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR = 2 as const;
export const PROCESS_DOMAIN_PROTOCOL_MAJOR = EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR;
export const PROCESS_DOMAIN_PROTOCOL_MINOR = 0 as const;

export function isSupportedProtocol(major: number, minor: number): boolean {
  return minor === PROCESS_DOMAIN_PROTOCOL_MINOR &&
    (major === LEGACY_PROCESS_DOMAIN_PROTOCOL_MAJOR || major === EMBEDDED_PROCESS_DOMAIN_PROTOCOL_MAJOR);
}

export type ProcessDomainFatalCode =
  | "INVALID_DECLARATION"
  | "AUTHENTICATION_FAILED"
  | "DOMAIN_ABSENT"
  | "BROKER_UNAVAILABLE"
  | "PROTOCOL_MISMATCH"
  | "DOMAIN_UNRECOVERABLE"
  | "LEASE_REJECTED"
  | "RUNTIME_UNSAFE";

export class ProcessDomainFatalError extends Error {
  readonly code: ProcessDomainFatalCode;
  readonly isProcessDomainFatalError = true as const;

  constructor(code: ProcessDomainFatalCode, message: string, cause?: unknown) {
    super(message);
    this.name = "ProcessDomainFatalError";
    this.code = code;
    if (cause !== undefined) {
      (this as { cause?: unknown }).cause = cause;
    }
  }
}

/** Stable fatal exit code used by the CLI harness / declared-domain processes. */
export const FATAL_EXIT_CODE = 78;

export function isProcessDomainFatalError(value: unknown): value is ProcessDomainFatalError {
  return (
    value instanceof Error &&
    (value as ProcessDomainFatalError).isProcessDomainFatalError === true
  );
}
