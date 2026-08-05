/**
 * Typed fatal errors for pi-process-domain.
 */
export const PROCESS_DOMAIN_PROTOCOL_MAJOR = 1;
export const PROCESS_DOMAIN_PROTOCOL_MINOR = 0;
export class ProcessDomainFatalError extends Error {
    code;
    isProcessDomainFatalError = true;
    constructor(code, message, cause) {
        super(message);
        this.name = "ProcessDomainFatalError";
        this.code = code;
        if (cause !== undefined) {
            this.cause = cause;
        }
    }
}
/** Stable fatal exit code used by the CLI harness / declared-domain processes. */
export const FATAL_EXIT_CODE = 78;
export function isProcessDomainFatalError(value) {
    return (value instanceof Error &&
        value.isProcessDomainFatalError === true);
}
