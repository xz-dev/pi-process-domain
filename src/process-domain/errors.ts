export type ProcessDomainOpenErrorCode =
  | "INVALID_DECLARATION"
  | "AUTHENTICATION_FAILED"
  | "CONNECTION_UNAVAILABLE";

const PROCESS_DOMAIN_OPEN_ERROR_CODES = new Set<ProcessDomainOpenErrorCode>([
  "INVALID_DECLARATION",
  "AUTHENTICATION_FAILED",
  "CONNECTION_UNAVAILABLE",
]);

export class ProcessDomainOpenError extends Error {
  readonly isProcessDomainOpenError = true as const;

  constructor(
    readonly code: ProcessDomainOpenErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ProcessDomainOpenError";
  }
}

export function isProcessDomainOpenError(
  value: unknown,
): value is ProcessDomainOpenError {
  return (
    value instanceof Error &&
    (value as Partial<ProcessDomainOpenError>).isProcessDomainOpenError === true &&
    PROCESS_DOMAIN_OPEN_ERROR_CODES.has(
      (value as Partial<ProcessDomainOpenError>).code as ProcessDomainOpenErrorCode,
    )
  );
}

export function invalidDeclarationError(cause: unknown): ProcessDomainOpenError {
  return new ProcessDomainOpenError(
    "INVALID_DECLARATION",
    "process-domain declaration is invalid",
    { cause },
  );
}

export function authenticationFailedError(cause: unknown): ProcessDomainOpenError {
  return new ProcessDomainOpenError(
    "AUTHENTICATION_FAILED",
    "process-domain authentication failed",
    { cause },
  );
}

export function connectionUnavailableError(cause: unknown): ProcessDomainOpenError {
  return new ProcessDomainOpenError(
    "CONNECTION_UNAVAILABLE",
    "process-domain connection is unavailable",
    { cause },
  );
}
