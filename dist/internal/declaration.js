/**
 * Domain environment declaration and bootstrap.
 *
 * Environment variable names:
 *   PI_PROCESS_DOMAIN_ID          -- domain id (128 random bits, base64url)
 *   PI_PROCESS_DOMAIN_KEY         -- 32 random bytes, base64url
 *   PI_PROCESS_DOMAIN_PROTOCOL    -- "<major>.<minor>"
 *   PI_PROCESS_DOMAIN_RESERVATION -- optional single-use child claim
 *
 * If no declaration variable is present, openDomain() creates a new domain and
 * a cryptographically random 32-byte key, writes the complete declaration to
 * process.env, and joins it. If any declaration variable is present, the whole
 * group must be present and valid; malformed/missing/wrong fields are fatal.
 */
import { randomBytes } from "node:crypto";
import { base64url, unbase64url, AUTH_KEY_BYTES } from "./auth.js";
import { PROCESS_DOMAIN_PROTOCOL_MAJOR, PROCESS_DOMAIN_PROTOCOL_MINOR, ProcessDomainFatalError } from "./errors.js";
export const ENV = {
    DOMAIN_ID: "PI_PROCESS_DOMAIN_ID",
    DOMAIN_KEY: "PI_PROCESS_DOMAIN_KEY",
    PROTOCOL: "PI_PROCESS_DOMAIN_PROTOCOL",
    RESERVATION: "PI_PROCESS_DOMAIN_RESERVATION",
};
export function protocolString() {
    return `${PROCESS_DOMAIN_PROTOCOL_MAJOR}.${PROCESS_DOMAIN_PROTOCOL_MINOR}`;
}
const ID_REGEX = /^[A-Za-z0-9_-]+$/;
function parseDeclaration() {
    const id = process.env[ENV.DOMAIN_ID];
    const key = process.env[ENV.DOMAIN_KEY];
    const proto = process.env[ENV.PROTOCOL];
    // If nothing is present, no declaration.
    if (id === undefined && key === undefined && proto === undefined)
        return null;
    // Any presence makes the declaration intentional.
    if (id === undefined || key === undefined || proto === undefined) {
        throw new ProcessDomainFatalError("INVALID_DECLARATION", "domain declaration is incomplete: all of ID, KEY and PROTOCOL must be set together");
    }
    if (!ID_REGEX.test(id) || id.length < 8 || id.length > 128) {
        throw new ProcessDomainFatalError("INVALID_DECLARATION", "domain id is malformed");
    }
    const keyBytes = unbase64url(key, AUTH_KEY_BYTES);
    if (!keyBytes) {
        throw new ProcessDomainFatalError("INVALID_DECLARATION", "domain key must decode to exactly 32 bytes");
    }
    const parts = proto.split(".");
    const major = Number(parts[0]);
    const minor = Number(parts[1]);
    if (parts.length !== 2 || !Number.isInteger(major) || !Number.isInteger(minor)) {
        throw new ProcessDomainFatalError("INVALID_DECLARATION", "protocol string is malformed");
    }
    if (major !== PROCESS_DOMAIN_PROTOCOL_MAJOR) {
        throw new ProcessDomainFatalError("PROTOCOL_MISMATCH", `protocol major ${major} is incompatible with supported ${PROCESS_DOMAIN_PROTOCOL_MAJOR}`);
    }
    return { domainId: id, domainKey: keyBytes, protocolMajor: major, protocolMinor: minor };
}
/** Create a fresh domain declaration and publish it to process.env. */
export function createDeclaration() {
    const domainId = base64url(randomBytes(16));
    const domainKey = new Uint8Array(randomBytes(AUTH_KEY_BYTES));
    const decl = {
        domainId,
        domainKey,
        protocolMajor: PROCESS_DOMAIN_PROTOCOL_MAJOR,
        protocolMinor: PROCESS_DOMAIN_PROTOCOL_MINOR,
    };
    process.env[ENV.DOMAIN_ID] = domainId;
    process.env[ENV.DOMAIN_KEY] = base64url(domainKey);
    process.env[ENV.PROTOCOL] = protocolString();
    return decl;
}
export function readDeclaration() {
    return parseDeclaration();
}
/** Base64url encode a reservation claim for environment transport. */
export function encodeReservationClaim(token) {
    return base64url(token);
}
export function decodeReservationClaim(value) {
    if (value === undefined)
        return null;
    const bytes = unbase64url(value, 32);
    if (!bytes) {
        throw new ProcessDomainFatalError("INVALID_DECLARATION", "reservation claim is malformed");
    }
    return bytes;
}
